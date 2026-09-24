// Example webhook consumer. Owns its own queue and binding — the ingest
// framework only ever publishes to the exchange, it never knows this queue
// exists. Redeems the claim check ticket for "fat" hooks through the
// claim-check gateway's HTTP API instead of talking to S3 directly — this
// worker holds no cloud storage credentials at all, on purpose: that's the
// whole point of putting a Claim Check gateway in front of the object
// store. Prints everything out; not production processing logic — swap the
// body of `handleHook` for that.
//
// The claim-check client is generated from the framework's own OpenAPI
// contract (`priv/openapi/claim_check.v1.yaml`, `npm run generate:types` ->
// `src/claim-check-schema.d.ts`) instead of a hand-maintained ticket type
// and URL-building — see "Redeem a claim" in
// docs/claim-check.md.

import amqp from "amqplib";
import { createHash } from "node:crypto";
import createClient from "openapi-fetch";
import type { components, paths } from "./claim-check-schema.d.ts";

const RABBITMQ_URL = process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";
const EXCHANGE = process.env.RABBITMQ_EXCHANGE ?? "ankusa.events";
const ROUTING_PATTERN = process.env.ROUTING_PATTERN ?? "ankusa.#";
const QUEUE_NAME = process.env.QUEUE_NAME ?? "ankusa-example-worker";

const CLAIM_CHECK_URL = process.env.CLAIM_CHECK_URL ?? "http://localhost:4001";
const CLAIM_CHECK_TOKEN = process.env.CLAIM_CHECK_TOKEN ?? "dev-claim-check-token";

const claimCheck = createClient<paths>({
  baseUrl: CLAIM_CHECK_URL,
  headers: { authorization: `Bearer ${CLAIM_CHECK_TOKEN}` },
});

// The ticket schema, straight from the spec's `components.schemas.Ticket` —
// no separately hand-typed duplicate to drift from the gateway.
type Claim = components["schemas"]["Ticket"];

type HookMessage = {
  id: string;
  source_id: string;
  tenant_id: string;
  received_at: number;
  content_type: string | null;
  size: number;
  body_base64?: string;
  claim?: Claim;
};

// Permanent redeem failures (the claim is gone, or the bytes we got back
// don't match the ticket) should dead-letter, not loop forever. Anything
// else (gateway down, network hiccup) is worth retrying.
class PermanentRedeemError extends Error {}

async function redeemClaim(claim: Claim): Promise<Buffer> {
  let data: ArrayBuffer | undefined;
  let status: number;
  let errorBody: unknown;
  try {
    const result = await claimCheck.GET("/v1/claims/{tenant_id}/{id}", {
      params: { path: { tenant_id: claim.tenant_id, id: claim.id } },
      parseAs: "arrayBuffer",
    });
    data = result.data as ArrayBuffer | undefined;
    status = result.response.status;
    errorBody = result.error;
  } catch (err) {
    throw new Error(`claim-check gateway unreachable: ${(err as Error).message}`);
  }

  if (status === 404) {
    throw new PermanentRedeemError(`claim not found: ${claim.tenant_id}/${claim.id}`);
  }
  if (status >= 400 && status < 500) {
    throw new PermanentRedeemError(`claim-check rejected redeem (${status}): ${JSON.stringify(errorBody)}`);
  }
  if (data === undefined) {
    throw new Error(`claim-check gateway error: ${status}`);
  }

  const body = Buffer.from(data);

  // Integrity is verified here, end to end, by the actual redeemer — never
  // trusted from the gateway. Same discipline `Ankusa.ClaimCheck.redeem/3`
  // applies on the Elixir side.
  if (body.length !== claim.size) {
    throw new PermanentRedeemError(`claim size mismatch: expected ${claim.size}, got ${body.length}`);
  }
  const sha256 = createHash("sha256").update(body).digest("hex");
  if (sha256 !== claim.sha256) {
    throw new PermanentRedeemError(`claim sha256 mismatch for ${claim.tenant_id}/${claim.id}`);
  }

  return body;
}

async function resolveBody(msg: HookMessage): Promise<{ body: Buffer; via: string }> {
  if (msg.claim) {
    return { body: await redeemClaim(msg.claim), via: `claim:${msg.claim.id}` };
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
        if (err instanceof PermanentRedeemError) {
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
