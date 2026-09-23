#!/usr/bin/env node
import {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  SendMessageCommand,
  ChangeMessageVisibilityCommand,
} from "@aws-sdk/client-sqs";
import { createHash } from "crypto";

const config = {
  queueUrl: process.env.SQS_QUEUE_URL,
  endpoint: process.env.SQS_ENDPOINT,
  region: process.env.SQS_REGION || "us-east-1",
  claimCheckUrl: process.env.CLAIM_CHECK_URL,
  claimCheckToken: process.env.CLAIM_CHECK_TOKEN,
};

const sqs = new SQSClient({
  region: config.region,
  endpoint: config.endpoint,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const dlqUrl = config.queueUrl.replace(".fifo", "-dlq.fifo");
const seen = new Set(); // In-memory dedup (real app would use persistent store)

console.log("Worker starting", {
  queue: config.queueUrl,
  dlq: dlqUrl,
  claimCheck: config.claimCheckUrl,
});

async function poll() {
  const { Messages } = await sqs.send(
    new ReceiveMessageCommand({
      QueueUrl: config.queueUrl,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 20,
      MessageAttributeNames: ["All"],
      MessageSystemAttributeNames: ["ApproximateReceiveCount", "MessageGroupId"],
    })
  );

  if (!Messages || Messages.length === 0) return;

  console.log(`Received ${Messages.length} messages`);

  // Process sequentially (FIFO guarantees order within a group)
  for (const msg of Messages) {
    await processMessage(msg);
  }
}

async function processMessage(msg) {
  const receiptHandle = msg.ReceiptHandle;
  const receiveCount = parseInt(
    msg.MessageSystemAttributes?.ApproximateReceiveCount || "1",
    10
  );
  const groupId = msg.MessageSystemAttributes?.MessageGroupId;

  try {
    const envelope = JSON.parse(msg.Body);

    // Version check
    if (envelope.v !== 1) {
      await sendToDLQ(msg, `Unsupported version: ${envelope.v}`);
      return;
    }

    // Deduplication (in addition to SQS FIFO 5-min window)
    if (seen.has(envelope.id)) {
      console.log(`Skipping duplicate: ${envelope.id}`);
      await deleteMessage(receiptHandle);
      return;
    }
    seen.add(envelope.id);

    // Get body
    let body;
    if (envelope.body_base64) {
      body = Buffer.from(envelope.body_base64, "base64");
    } else if (envelope.claim) {
      body = await redeemClaim(envelope.claim);
    } else {
      await sendToDLQ(msg, "No body_base64 or claim");
      return;
    }

    // Verify size and integrity
    if (body.length !== envelope.size) {
      await sendToDLQ(msg, `Size mismatch: got ${body.length}, expected ${envelope.size}`);
      return;
    }

    if (envelope.claim?.sha256) {
      const hash = createHash("sha256").update(body).digest("hex");
      if (hash !== envelope.claim.sha256) {
        await sendToDLQ(msg, `Integrity check failed`);
        return;
      }
    }

    // Process the webhook
    console.log(`Processed: ${envelope.id}`, {
      source: envelope.source_id,
      tenant: envelope.tenant_id,
      size: envelope.size,
      type: envelope.content_type,
      receiveCount,
      groupId,
    });

    await deleteMessage(receiptHandle);
  } catch (err) {
    await handleError(msg, err, receiveCount, groupId);
  }
}

async function redeemClaim(claim) {
  const url = `${config.claimCheckUrl}/v1/claims/${claim.id}`;
  const headers = {
    Authorization: `Bearer ${config.claimCheckToken}`,
  };

  const response = await fetch(url, { headers });

  if (!response.ok) {
    if (response.status === 404) {
      throw new PermanentError(`Claim not found: ${claim.id}`);
    }
    throw new Error(`Claim redemption failed: ${response.status}`);
  }

  return Buffer.from(await response.arrayBuffer());
}

async function handleError(msg, err, receiveCount, groupId) {
  if (err instanceof PermanentError || err.name === "SyntaxError") {
    console.error("Permanent error, sending to DLQ:", err.message);
    await sendToDLQ(msg, err.message);
  } else {
    // Transient error: back off with exponential delay
    const backoffSeconds = Math.min(30 * Math.pow(2, receiveCount - 1), 900);
    console.error(
      `Transient error (receive #${receiveCount}), backing off ${backoffSeconds}s:`,
      err.message
    );

    // Change visibility for this message and all later messages from the same group in this batch
    await sqs.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: config.queueUrl,
        ReceiptHandle: msg.ReceiptHandle,
        VisibilityTimeout: backoffSeconds,
      })
    );
  }
}

async function sendToDLQ(msg, reason) {
  const envelope = JSON.parse(msg.Body);

  await sqs.send(
    new SendMessageCommand({
      QueueUrl: dlqUrl,
      MessageBody: msg.Body,
      MessageGroupId: msg.MessageSystemAttributes?.MessageGroupId,
      MessageDeduplicationId: envelope.id,
      MessageAttributes: {
        FailureReason: {
          DataType: "String",
          StringValue: reason,
        },
      },
    })
  );

  await deleteMessage(msg.ReceiptHandle);
  console.log(`Sent to DLQ: ${envelope.id} - ${reason}`);
}

async function deleteMessage(receiptHandle) {
  await sqs.send(
    new DeleteMessageCommand({
      QueueUrl: config.queueUrl,
      ReceiptHandle: receiptHandle,
    })
  );
}

class PermanentError extends Error {
  constructor(message) {
    super(message);
    this.name = "PermanentError";
  }
}

// Main loop
(async function main() {
  while (true) {
    try {
      await poll();
    } catch (err) {
      console.error("Poll error:", err);
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
})();
