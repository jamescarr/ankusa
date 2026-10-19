// The fleet example's consumer: the rabbitmq-consumer worker (own queue and
// binding, claim redemption through the gateway with no storage credentials of
// its own) with `handleHook` replaced by a Postgres upsert, so
// `mix loadgen.verify` can prove every acknowledged hook arrived.
//
// The row is keyed by the message's `idempotency_key`: a provider retry that
// reached the other edge node is a second hook with a new id but the same key,
// and lands on the same row with `deliveries + 1` and its id added to `hook_ids`
// (the distinct hooks Ankusa committed for the key: `duplicate_hooks` in the
// verify report is `cardinality(hook_ids) - 1`). A message RabbitMQ delivers
// again after this process was killed mid-handling is one more delivery of the
// *same* hook: `deliveries + 1`, `hook_ids` unchanged.

import amqp from "amqplib";
import { ClaimCheckError, createClaimCheckClient, InvalidClaimRefError, parseClaimRef } from "ankusa";
import { createHash } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import pg from "pg";

const RABBITMQ_URL = process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";
const EXCHANGE = process.env.RABBITMQ_EXCHANGE ?? "ankusa.events";
const ROUTING_PATTERN = process.env.ROUTING_PATTERN ?? "ankusa.#";
const QUEUE_NAME = process.env.QUEUE_NAME ?? "ankusa-fleet-consumer";
const CLAIM_CHECK_URL = process.env.CLAIM_CHECK_URL ?? "http://localhost:4001";
const DATABASE_URL = process.env.DATABASE_URL ?? "postgres://ankusa:ankusa@localhost:15433/consumer";

const claimCheck = createClaimCheckClient({ baseUrl: CLAIM_CHECK_URL });
const db = new pg.Pool({ connectionString: DATABASE_URL, max: 10 });

// `Ankusa.Sink.Message` v1. A message carrying `claim` also carries `sha256`,
// the lowercase hex digest the redeemed bytes are verified against.
type HookMessage = {
  id: string;
  source_id: string;
  tenant_id: string;
  idempotency_key?: string;
  received_at: number;
  content_type: string | null;
  size: number;
  body_base64?: string;
  claim?: string;
  sha256?: string;
};

async function resolveBody(msg: HookMessage): Promise<{ body: Buffer; via: string }> {
  if (msg.claim) {
    if (!msg.sha256) throw new InvalidClaimRefError("claim message has no sha256");
    const { claimId } = parseClaimRef(msg.claim);
    return { body: await claimCheck.redeem(msg.claim, msg.sha256), via: `claim:${claimId}` };
  }
  return { body: Buffer.from(msg.body_base64 ?? "", "base64"), via: "inline" };
}

async function record(msg: HookMessage, body: Buffer, via: string): Promise<void> {
  await db.query(
    `INSERT INTO processed_webhooks
       (idempotency_key, ankusa_id, source_id, tenant_id, body, body_sha256, deliveries, processed_at, via, hook_ids)
     VALUES ($1, $2, $3, $4, $5, $6, 1, now(), $7, ARRAY[$2::text])
     ON CONFLICT (idempotency_key) DO UPDATE
       SET deliveries = processed_webhooks.deliveries + 1,
           hook_ids = CASE WHEN $2::text = ANY (processed_webhooks.hook_ids)
                           THEN processed_webhooks.hook_ids
                           ELSE processed_webhooks.hook_ids || $2::text END`,
    [
      msg.idempotency_key ?? msg.id,
      msg.id,
      msg.source_id,
      msg.tenant_id,
      body,
      createHash("sha256").update(body).digest("hex"),
      via,
    ],
  );
}

// RabbitMQ can report healthy before its AMQP listener accepts connections, so
// a consumer started beside it retries instead of exiting: a consumer that never
// declared its queue leaves the exchange with nothing to route to, and the
// nodes' publishes fail as unroutable.
async function connect(): Promise<amqp.ChannelModel> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await amqp.connect(RABBITMQ_URL);
    } catch (err) {
      if (attempt >= 60) throw err;
      console.error(`[consumer] rabbitmq not ready (attempt ${attempt}), retrying: ${String(err)}`);
      await sleep(2000);
    }
  }
}

async function main() {
  const conn = await connect();
  const chan = await conn.createChannel();

  await chan.assertExchange(EXCHANGE, "topic", { durable: true });
  const { queue } = await chan.assertQueue(QUEUE_NAME, { durable: true });
  await chan.bindQueue(queue, EXCHANGE, ROUTING_PATTERN);
  await chan.prefetch(32);

  console.log(`[consumer] consuming "${queue}" bound to "${EXCHANGE}" (${ROUTING_PATTERN})`);
  console.log(`[consumer] redeeming claims via ${CLAIM_CHECK_URL}`);

  await chan.consume(queue, (msg) => {
    if (!msg) return;

    void (async () => {
      try {
        const decoded = JSON.parse(msg.content.toString("utf8")) as HookMessage;
        const { body, via } = await resolveBody(decoded);
        await record(decoded, body, via);
        chan.ack(msg);
      } catch (err) {
        if (err instanceof ClaimCheckError && !err.retryable) {
          console.error(`[consumer] permanent redeem failure, dead-lettering: ${err.message}`);
          chan.nack(msg, false, false);
        } else {
          console.error("[consumer] transient failure, nacking for redelivery:", err);
          chan.nack(msg, false, true);
        }
      }
    })();
  });
}

main().catch((err) => {
  console.error("[consumer] fatal:", err);
  process.exit(1);
});
