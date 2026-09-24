// Example webhook consumer reading an SQS FIFO queue that a Redpanda Connect
// bridge fills from the `ankusa.events` Kafka topic. The ingest framework
// never knows this queue exists. Fat hooks are redeemed through the
// claim-check gateway's HTTP API: this worker holds SQS credentials only,
// never object-store credentials. Prints everything; swap `handleHook` for
// real processing.
//
// The claim-check client is generated from the framework's own OpenAPI
// contract (`priv/openapi/claim_check.v1.yaml`, `npm run generate:types` ->
// `src/claim-check-schema.d.ts`) instead of a hand-maintained ref type
// and URL-building — see "Redeem a claim" in
// docs/claim-check.md.

import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
  type Message,
} from "@aws-sdk/client-sqs";
import { createHash } from "node:crypto";
import createClient from "openapi-fetch";
import type { paths } from "./claim-check-schema.d.ts";

const QUEUE_URL = required("SQS_QUEUE_URL");
const DLQ_URL = required("SQS_DLQ_URL");
const CLAIM_CHECK_URL = process.env.CLAIM_CHECK_URL ?? "http://localhost:4001";

const sqs = new SQSClient({
  region: process.env.AWS_REGION ?? "us-east-1",
  endpoint: process.env.SQS_ENDPOINT,
});

// The gateway is open: no bearer token. Auth belongs in front of it.
const claimCheck = createClient<paths>({ baseUrl: CLAIM_CHECK_URL });

// The message `claim` field is a single claim-check ref URN, not a ticket
// object:
//   urn:ankusa:claim:v1:<tenant>:<object_id>:<offset>:<length>:sha256-<hex>
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
};

type ClaimRef = {
  tenant_id: string;
  object_id: string;
  offset: string;
  length: string;
  digest: string;
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
  return msg;
}

// Split the ref on ":" — nine parts. The sha256 digest is the hex after
// `sha256-`; it's never sent to the gateway, only used to check the bytes.
function parseClaimRef(claim: string): ClaimRef {
  const parts = claim.split(":");
  if (parts.length !== 9 || !claim.startsWith("urn:ankusa:claim:v1:")) {
    throw new PermanentError(`invalid claim ref: ${claim}`);
  }
  return {
    tenant_id: parts[4],
    object_id: parts[5],
    offset: parts[6],
    length: parts[7],
    digest: parts[8].slice("sha256-".length),
  };
}

async function redeemClaim(claim: string): Promise<Buffer> {
  const { tenant_id, object_id, offset, length, digest } = parseClaimRef(claim);

  let data: ArrayBuffer | undefined;
  let status: number;
  let errorBody: unknown;
  try {
    const result = await claimCheck.GET("/v1/claims/{tenant_id}/{object_id}/{offset}/{length}", {
      params: { path: { tenant_id, object_id, offset, length } },
      parseAs: "arrayBuffer",
    });
    data = result.data as ArrayBuffer | undefined;
    status = result.response.status;
    errorBody = result.error;
  } catch (err) {
    throw new Error(`claim-check gateway unreachable: ${(err as Error).message}`);
  }

  if (status === 404) {
    throw new PermanentError(`claim not found: ${tenant_id}/${object_id}`);
  }
  if (status >= 400 && status < 500) {
    throw new PermanentError(`claim-check rejected redeem (${status}): ${JSON.stringify(errorBody)}`);
  }
  if (data === undefined) {
    throw new Error(`claim-check gateway error: ${status}`);
  }

  const body = Buffer.from(data);

  // Integrity is verified by the redeemer, never trusted from the gateway.
  if (body.length !== Number(length)) {
    throw new PermanentError(`claim size mismatch: expected ${length}, got ${body.length}`);
  }
  const sha256 = createHash("sha256").update(body).digest("hex");
  if (sha256 !== digest) {
    throw new PermanentError(`claim sha256 mismatch for ${tenant_id}/${object_id}`);
  }

  return body;
}

async function resolveBody(msg: HookMessage): Promise<{ body: Buffer; via: string }> {
  if (msg.claim) {
    const { object_id } = parseClaimRef(msg.claim);
    return { body: await redeemClaim(msg.claim), via: `claim:${object_id}` };
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
      if (err instanceof PermanentError) {
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
