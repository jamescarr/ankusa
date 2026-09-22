// Example webhook consumer. Owns its own queue and binding — the ingest
// framework only ever publishes to the exchange, it never knows this queue
// exists. Fetches the payload for "fat" hooks from the object store instead
// of trusting the queue message to carry it, and prints everything out.
// Not production processing logic — swap the body of `handleHook` for that.

import amqp from "amqplib";
import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";

const RABBITMQ_URL = process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";
const EXCHANGE = process.env.RABBITMQ_EXCHANGE ?? "ankusa.events";
const ROUTING_PATTERN = process.env.ROUTING_PATTERN ?? "ankusa.#";
const QUEUE_NAME = process.env.QUEUE_NAME ?? "ankusa-example-worker";

const S3_BUCKET = process.env.S3_BUCKET ?? "ankusa-example";
const s3 = new S3Client({
  region: process.env.S3_REGION ?? "us-east-1",
  endpoint: process.env.S3_ENDPOINT ?? "http://localhost:4566",
  forcePathStyle: true,
  credentials: {
    accessKeyId: process.env.S3_ACCESS_KEY_ID ?? "test",
    secretAccessKey: process.env.S3_SECRET_ACCESS_KEY ?? "test",
  },
});

type HookMessage = {
  id: string;
  source_id: string;
  tenant_id: string;
  received_at: number;
  content_type: string | null;
  size: number;
  body_base64?: string;
  blob?: { store: string; key: string; size: number };
};

async function fetchBlob(key: string): Promise<Buffer> {
  const res = await s3.send(new GetObjectCommand({ Bucket: S3_BUCKET, Key: key }));
  const bytes = await res.Body?.transformToByteArray();
  if (!bytes) throw new Error(`empty body fetching ${key}`);
  return Buffer.from(bytes);
}

async function resolveBody(msg: HookMessage): Promise<{ body: Buffer; via: string }> {
  if (msg.blob) {
    return { body: await fetchBlob(msg.blob.key), via: `blob:${msg.blob.key}` };
  }
  return { body: Buffer.from(msg.body_base64 ?? "", "base64"), via: "inline" };
}

// Replace this with real processing. It only prints here on purpose — the
// framework's job ends at "delivered to your queue, payload fetchable";
// what a consumer does with a hook is out of scope for the framework itself.
function handleHook(msg: HookMessage, routingKey: string, body: Buffer, via: string): void {
  console.log("─".repeat(72));
  console.log(`hook id=${msg.id} source=${msg.source_id} tenant=${msg.tenant_id}`);
  console.log(`routing_key=${routingKey} size=${msg.size} via=${via}`);
  console.log(body.toString("utf8"));
}

async function main() {
  const conn = await amqp.connect(RABBITMQ_URL);
  const chan = await conn.createChannel();

  // Consumer-owned topology. The ingest framework never declares a queue —
  // it only ever publishes to the exchange.
  await chan.assertExchange(EXCHANGE, "topic", { durable: true });
  const { queue } = await chan.assertQueue(QUEUE_NAME, { durable: true });
  await chan.bindQueue(queue, EXCHANGE, ROUTING_PATTERN);
  await chan.prefetch(10);

  console.log(`[worker] consuming "${queue}" bound to "${EXCHANGE}" (${ROUTING_PATTERN})`);

  await chan.consume(queue, (msg) => {
    if (!msg) return;

    void (async () => {
      try {
        const decoded = JSON.parse(msg.content.toString("utf8")) as HookMessage;
        const { body, via } = await resolveBody(decoded);
        handleHook(decoded, msg.fields.routingKey, body, via);
        chan.ack(msg);
      } catch (err) {
        console.error("[worker] failed to process message, nacking for redelivery:", err);
        chan.nack(msg, false, true);
      }
    })();
  });
}

main().catch((err) => {
  console.error("[worker] fatal:", err);
  process.exit(1);
});
