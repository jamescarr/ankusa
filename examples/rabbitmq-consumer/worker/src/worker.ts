// Example webhook consumer. Owns its own queue and binding — the ingest
// framework only ever publishes to the exchange, it never knows this queue
// exists. Redeems the claim-check ref for "fat" hooks through the
// claim-check gateway's HTTP API instead of talking to S3 directly — this
// worker holds no cloud storage credentials at all, on purpose: that's the
// whole point of putting a Claim Check gateway in front of the object
// store. Prints everything out; not production processing logic — swap the
// body of `handleHook` for that.
//
// The claim-check client comes from the framework's own `ankusa` SDK
// package (a `file:` dependency on `../../../sdks/typescript` — see
// "Redeem a claim" in docs/claim-check.md), generated from
// `priv/openapi/claim_check.v1.yaml` instead of hand-maintained ref parsing
// and URL-building.

import amqp from "amqplib";
import { ClaimCheckError, createClaimCheckClient, parseClaimRef } from "ankusa";

const RABBITMQ_URL = process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";
const EXCHANGE = process.env.RABBITMQ_EXCHANGE ?? "ankusa.events";
const ROUTING_PATTERN = process.env.ROUTING_PATTERN ?? "ankusa.#";
const QUEUE_NAME = process.env.QUEUE_NAME ?? "ankusa-example-worker";

const CLAIM_CHECK_URL = process.env.CLAIM_CHECK_URL ?? "http://localhost:4001";

// The gateway is open: no bearer token. Auth belongs in front of it.
const claimCheck = createClaimCheckClient({ baseUrl: CLAIM_CHECK_URL });

// The message `claim` field is a single claim-check ref URN, not a ticket
// object:
//   urn:ankusa:claim:v1:<tenant>:<object_id>:<offset>:<length>:sha256-<hex>
type HookMessage = {
  id: string;
  source_id: string;
  tenant_id: string;
  received_at: number;
  content_type: string | null;
  size: number;
  body_base64?: string;
  claim?: string;
};

async function resolveBody(msg: HookMessage): Promise<{ body: Buffer; via: string }> {
  if (msg.claim) {
    const { objectId } = parseClaimRef(msg.claim);
    return { body: await claimCheck.redeem(msg.claim), via: `claim:${objectId}` };
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
  console.log(`[worker] redeeming claims via ${CLAIM_CHECK_URL}`);

  await chan.consume(queue, (msg) => {
    if (!msg) return;

    void (async () => {
      try {
        const decoded = JSON.parse(msg.content.toString("utf8")) as HookMessage;
        const { body, via } = await resolveBody(decoded);
        handleHook(decoded, msg.fields.routingKey, body, via);
        chan.ack(msg);
      } catch (err) {
        if (err instanceof ClaimCheckError && !err.retryable) {
          console.error(`[worker] permanent redeem failure, dead-lettering: ${err.message}`);
          chan.nack(msg, false, false);
        } else {
          console.error("[worker] transient failure, nacking for redelivery:", err);
          chan.nack(msg, false, true);
        }
      }
    })();
  });
}

main().catch((err) => {
  console.error("[worker] fatal:", err);
  process.exit(1);
});
