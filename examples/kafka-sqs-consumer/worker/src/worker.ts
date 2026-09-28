// Example webhook consumer reading an SQS FIFO queue that a Redpanda Connect
// bridge fills from the `ankusa.events` Kafka topic. The ingest framework
// never knows this queue exists. Fat hooks are redeemed through the
// claim-check gateway's HTTP API: this worker holds SQS credentials only,
// never object-store credentials. Prints everything; swap `handleHook` for
// real processing.
//
// The claim-check client comes from the framework's own `ankusa` SDK
// package (a `file:` dependency on `../../../packages/sdk-typescript` — see
// "Redeem a claim" in docs/claim-check.md), generated from
// `priv/openapi/claim_check.v1.yaml` instead of hand-maintained ref parsing
// and URL-building.

import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
  type Message,
} from "@aws-sdk/client-sqs";
import { ClaimCheckError, createClaimCheckClient, parseClaimRef } from "ankusa";

const QUEUE_URL = required("SQS_QUEUE_URL");
const DLQ_URL = required("SQS_DLQ_URL");
const CLAIM_CHECK_URL = process.env.CLAIM_CHECK_URL ?? "http://localhost:4001";

const sqs = new SQSClient({
  region: process.env.AWS_REGION ?? "us-east-1",
  endpoint: process.env.SQS_ENDPOINT,
});

// The gateway is open: no bearer token. Auth belongs in front of it.
const claimCheck = createClaimCheckClient({ baseUrl: CLAIM_CHECK_URL });

// The message `claim` field is a single claim-check ref URN, not a ticket
// object:
//   urn:ankusa:claim:v1:<tenant>:<claim_id ULID>
// and a message carrying `claim` also carries `sha256`, the lowercase hex
// digest of the claim's bytes, which `redeem` verifies them against.
type HookMessage = {
  v: number;
  id: string;
  source_id: string;
  tenant_id: string;
  received_at: number;
  content_type: string | null;
  size: number;
  body_base64?: string;
  claim?: string;
  sha256?: string;
};

// Retrying won't help: dead-letter now so the rest of the FIFO group moves.
class PermanentError extends Error {}

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function parse(body: string | undefined): HookMessage {
  let msg: HookMessage;
  try {
    msg = JSON.parse(body ?? "") as HookMessage;
  } catch {
    throw new PermanentError("message body is not JSON");
  }
  if (msg.v !== 1) throw new PermanentError(`unsupported message version: ${msg.v}`);
  if (msg.claim && !msg.sha256) throw new PermanentError("claim message has no sha256");
  return msg;
}

async function resolveBody(msg: HookMessage): Promise<{ body: Buffer; via: string }> {
  if (msg.claim && msg.sha256) {
    const { claimId } = parseClaimRef(msg.claim);
    return { body: await claimCheck.redeem(msg.claim, msg.sha256), via: `claim:${claimId}` };
  }
  return { body: Buffer.from(msg.body_base64 ?? "", "base64"), via: "inline" };
}

function handleHook(msg: HookMessage, group: string, body: Buffer, via: string): void {
  console.log("─".repeat(72));
  console.log(`hook id=${msg.id} source=${msg.source_id} tenant=${msg.tenant_id}`);
  console.log(`group=${group} size=${msg.size} via=${via}`);
  console.log(body.toString("utf8"));
}

// Delivery is at-least-once: FIFO dedup only covers 5 minutes. A real
// consumer records processed ids durably; an in-memory set shows the idea.
const processed = new Set<string>();

async function remove(m: Message): Promise<void> {
  await sqs.send(new DeleteMessageCommand({ QueueUrl: QUEUE_URL, ReceiptHandle: m.ReceiptHandle }));
}

// Send, then delete: a crash in between duplicates the message in the DLQ
// rather than losing it.
async function deadLetter(m: Message, reason: string): Promise<void> {
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: DLQ_URL,
      MessageBody: m.Body,
      MessageGroupId: m.Attributes?.MessageGroupId,
      MessageDeduplicationId: m.MessageId,
      MessageAttributes: { failure_reason: { DataType: "String", StringValue: reason } },
    }),
  );
  await remove(m);
}

async function backOff(m: Message): Promise<void> {
  const receives = Number(m.Attributes?.ApproximateReceiveCount ?? "1");
  const seconds = Math.min(30 * 2 ** (receives - 1), 900);
  await sqs.send(
    new ChangeMessageVisibilityCommand({
      QueueUrl: QUEUE_URL,
      ReceiptHandle: m.ReceiptHandle,
      VisibilityTimeout: seconds,
    }),
  );
}

async function processBatch(messages: Message[]): Promise<void> {
  // Groups with a transient failure in this batch: every later message of
  // the group backs off too, or it would be processed out of order.
  const blocked = new Set<string>();

  for (const m of messages) {
    const group = m.Attributes?.MessageGroupId ?? "";

    if (blocked.has(group)) {
      await backOff(m);
      continue;
    }

    try {
      const msg = parse(m.Body);
      if (!processed.has(msg.id)) {
        const { body, via } = await resolveBody(msg);
        handleHook(msg, group, body, via);
        processed.add(msg.id);
      }
      await remove(m);
    } catch (err) {
      if (err instanceof PermanentError || (err instanceof ClaimCheckError && !err.retryable)) {
        console.error(`[worker] permanent failure, dead-lettering: ${err.message}`);
        await deadLetter(m, err.message);
      } else {
        console.error(`[worker] transient failure, backing off group ${group}:`, err);
        blocked.add(group);
        await backOff(m);
      }
    }
  }
}

let stopping = false;
process.on("SIGTERM", () => {
  console.log("[worker] SIGTERM: finishing the current batch");
  stopping = true;
});

async function main(): Promise<void> {
  console.log(`[worker] polling ${QUEUE_URL}`);
  console.log(`[worker] dead letters to ${DLQ_URL}`);
  console.log(`[worker] redeeming claims via ${CLAIM_CHECK_URL}`);

  while (!stopping) {
    const { Messages = [] } = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: QUEUE_URL,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 20,
        MessageAttributeNames: ["All"],
        MessageSystemAttributeNames: ["ApproximateReceiveCount", "MessageGroupId"],
      }),
    );
    await processBatch(Messages);
  }
}

main().catch((err) => {
  console.error("[worker] fatal:", err);
  process.exit(1);
});
