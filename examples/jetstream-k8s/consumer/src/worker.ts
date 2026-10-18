// Autoscaled JetStream consumer for the jetstream-k8s example.
//
// Owns its topology (the Ankusa NATS sink never creates a stream): an R3
// stream `ANKUSA` over `ankusa.>` and one durable pull consumer `worker` that
// every replica of this Deployment shares. KEDA scales the replicas on that
// consumer's lag. Each processed hook is announced on a plain core subject
// (`demo.consumed`) so the control panel can show a live tail; that subject is
// outside the stream's subjects, so the announcements are never persisted.
//
// Bodies over the sink's `inline_max_bytes` arrive as a claim ref and are
// redeemed through the claim-check gateway (`createClaimCheckClient`); this
// worker holds no object-store credentials.

import { createServer } from "node:http";
import { hostname } from "node:os";
import { setTimeout as sleep } from "node:timers/promises";

import { AckPolicy, DiscardPolicy, jetstream, jetstreamManager, RetentionPolicy, type JsMsg } from "@nats-io/jetstream";
import { connect, headers } from "@nats-io/transport-node";
import {
  ClaimCheckError,
  createClaimCheckClient,
  decodeMessage,
  idempotencyKey,
  InvalidClaimRefError,
  InvalidMessageError,
  type Message,
} from "ankusa";

const NATS_URL = process.env.NATS_URL ?? "nats://localhost:4222";
const CLAIM_CHECK_URL = process.env.CLAIM_CHECK_URL ?? "http://localhost:4001";
const STREAM = process.env.STREAM ?? "ANKUSA";
const CONSUMER = process.env.CONSUMER ?? "worker";
const SUBJECT_FILTER = process.env.SUBJECT_FILTER ?? "ankusa.>";
const STREAM_REPLICAS = Number(process.env.STREAM_REPLICAS ?? "3");
// Under each NATS node's 1Gi volume (infra/nats-values.yaml), with room for the
// stream's metadata and indexes.
const STREAM_MAX_BYTES = Number(process.env.STREAM_MAX_BYTES ?? String(640 * 1024 * 1024));
const CONSUMED_SUBJECT = process.env.CONSUMED_SUBJECT ?? "demo.consumed";
// Simulated per-hook work. The loop is sequential, so one pod handles about
// 1000 / WORK_MS hooks a second: that is what makes lag (and KEDA) visible.
const WORK_MS = Number(process.env.WORK_MS ?? "25");
const HEALTH_PORT = Number(process.env.HEALTH_PORT ?? "8080");
const POD_NAME = process.env.POD_NAME ?? hostname();

const DEDUPE_CAP = 20_000;
const NANOS_PER_MS = 1_000_000;
const DLQ_STREAM = process.env.DLQ_STREAM ?? "ANKUSA_DLQ";
const DLQ_SUBJECT = process.env.DLQ_SUBJECT ?? "dlq.ankusa";

// The gateway is open: no bearer token. Auth belongs in front of it.
const claimCheck = createClaimCheckClient({ baseUrl: CLAIM_CHECK_URL });

// Per-replica idempotency memory, the oldest key evicted first (a Set iterates
// in insertion order). Two replicas can still process the same hook (an
// ack_wait expiry, a replay); a real consumer keeps its idempotency keys in
// its database.
const seen = new Set<string>();

function remember(key: string) {
  seen.add(key);
  if (seen.size > DEDUPE_CAP) seen.delete(seen.values().next().value!);
}

async function resolveBody(msg: Message): Promise<"inline" | "claim"> {
  if (msg.body !== null) return "inline";
  // A claim message with no sha256 has nothing to verify the bytes against.
  if (!msg.claim || !msg.sha256) throw new InvalidClaimRefError("claim message has no claim/sha256");
  await claimCheck.redeem(msg.claim, msg.sha256);
  return "claim";
}

let connected = false;

function startHealthServer() {
  const server = createServer((req, res) => {
    if (req.method === "GET" && req.url === "/health") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ status: "ok", connected }));
    } else {
      res.writeHead(404).end();
    }
  });
  server.listen(HEALTH_PORT, () => console.log(`[consumer] health on :${HEALTH_PORT}`));
  return server;
}

async function main() {
  const health = startHealthServer();

  // waitOnFirstConnect: a replica KEDA adds while NATS is unreachable keeps
  // trying instead of crash-looping, so it is ready the moment the route is back.
  const nc = await connect({ servers: NATS_URL, maxReconnectAttempts: -1, waitOnFirstConnect: true, name: POD_NAME });
  connected = true;
  void (async () => {
    for await (const status of nc.status()) {
      if (status.type === "disconnect") connected = false;
      if (status.type === "reconnect") connected = true;
      if (["disconnect", "reconnect", "ldm", "error"].includes(status.type)) {
        console.log(`[consumer] nats ${status.type}`);
      }
    }
  })();

  // Consumer-owned topology, run by every replica on boot: `add` is a no-op for
  // an identical stream/consumer, and a drifted stream is brought back with
  // `update`. A failure is logged and left alone rather than crash-looping the pod.
  //
  // Work-queue retention: a hook leaves the stream once it is acked, so the
  // stream holds only the backlog. With `discard: new` a full stream (the
  // consumers are far behind) refuses new publishes, which ankusa answers with
  // a 503 the provider retries. The defaults, limits retention and `discard:
  // old`, would keep every consumed hook until the stream is full and then
  // silently delete the oldest hooks, consumed or not.
  const jsm = await jetstreamManager(nc);
  const streamConfig = {
    name: STREAM,
    subjects: [SUBJECT_FILTER],
    num_replicas: STREAM_REPLICAS,
    max_bytes: STREAM_MAX_BYTES,
    retention: RetentionPolicy.Workqueue,
    discard: DiscardPolicy.New,
  };
  const dlqConfig = {
    name: DLQ_STREAM,
    subjects: [DLQ_SUBJECT],
    num_replicas: STREAM_REPLICAS,
    max_age: 24 * 60 * 60 * 1_000 * NANOS_PER_MS,
    max_bytes: 64 * 1024 * 1024,
  };
  try {
    await jsm.streams.add(streamConfig);
  } catch {
    try {
      await jsm.streams.update(STREAM, streamConfig);
    } catch (err) {
      console.error(`[consumer] stream ${STREAM}: ${(err as Error).message}`);
    }
  }
  try {
    await jsm.streams.add(dlqConfig);
  } catch {
    try {
      await jsm.streams.update(DLQ_STREAM, dlqConfig);
    } catch (err) {
      console.error(`[consumer] stream ${DLQ_STREAM}: ${(err as Error).message}`);
    }
  }
  const consumerConfig = {
    ack_wait: 30_000 * NANOS_PER_MS,
    // A hook is retried until it succeeds or is dead-lettered: a bounded count
    // would strand it in the work queue after a long outage.
    max_deliver: -1,
    max_ack_pending: 200,
  };
  try {
    await jsm.consumers.add(STREAM, {
      ...consumerConfig,
      durable_name: CONSUMER,
      ack_policy: AckPolicy.Explicit,
      filter_subject: SUBJECT_FILTER,
    });
  } catch {
    try {
      await jsm.consumers.update(STREAM, CONSUMER, consumerConfig);
    } catch (err) {
      console.error(`[consumer] consumer ${STREAM}/${CONSUMER}: ${(err as Error).message}`);
    }
  }

  const js = jetstream(nc);
  const consumer = await js.consumers.get(STREAM, CONSUMER);
  const messages = await consumer.consume({ max_messages: 50 });
  console.log(`[consumer] ${POD_NAME} consuming ${STREAM}/${CONSUMER}, claims via ${CLAIM_CHECK_URL}`);

  let stopping = false;
  const shutdown = async () => {
    if (stopping) return;
    stopping = true;
    console.log("[consumer] shutting down");
    await messages.close();
    await nc.drain();
    health.close();
    process.exit(0);
  };
  process.on("SIGTERM", () => void shutdown());
  process.on("SIGINT", () => void shutdown());

  // Tell the control panel, the demo's ledger of what was consumed, that a hook
  // was processed, and wait for its reply before the hook is acked. A plain
  // publish could vanish while the panel's own connection is being re-opened
  // (an AuthorizationPolicy change makes Envoy drain every connection to the
  // NATS pods, not just the denied one), after this replica's idempotency
  // memory had already marked the hook done. With the reply, no ledger or no
  // reply means the hook is naked and processed again; the ledger counts a key
  // once however often it hears it.
  const announce = async (msg: Message, key: string, via: "inline" | "claim", duplicate: boolean) => {
    const now = Date.now();
    await nc.request(
      CONSUMED_SUBJECT,
      JSON.stringify({
        hook_id: msg.id,
        idempotency_key: key,
        dedupe_key: msg.dedupe_key,
        source_id: msg.source_id,
        size: msg.size,
        via,
        consumer: POD_NAME,
        received_at: msg.received_at,
        consumed_at: now,
        latency_ms: now - msg.received_at,
        duplicate,
      }),
      { timeout: 2_000 },
    );
  };

  // A hook this consumer can never process is copied, with the reason, to the
  // dead-letter stream before it is terminated, so a permanent failure leaves a
  // record instead of vanishing. If even that publish fails the hook is naked
  // and tried again.
  const deadLetter = async (m: JsMsg, reason: string) => {
    const h = headers();
    h.set("Dlq-Reason", reason.slice(0, 200));
    h.set("Dlq-Consumer", POD_NAME);
    h.set("Dlq-Stream-Seq", String(m.seq));
    await js.publish(DLQ_SUBJECT, m.data, { headers: h });
    m.term(reason);
  };

  const fail = async (m: JsMsg, err: unknown) => {
    const reason = (err as Error).message;
    const permanent =
      err instanceof InvalidMessageError ||
      err instanceof InvalidClaimRefError ||
      (err instanceof ClaimCheckError && !err.retryable);
    if (permanent) {
      try {
        await deadLetter(m, reason);
        console.error(`[consumer] seq=${m.seq} permanent failure, dead-lettered: ${reason}`);
        return;
      } catch (dlqErr) {
        console.error(`[consumer] seq=${m.seq} dead-letter publish failed: ${(dlqErr as Error).message}`);
      }
    }
    // Retried for as long as it takes (max_deliver is unlimited), backing off to 10 s.
    const delay = Math.min(10_000, 1_000 * m.info.deliveryCount);
    console.error(`[consumer] seq=${m.seq} transient failure, nak in ${delay} ms: ${reason}`);
    m.nak(delay);
  };

  const handle = async (m: JsMsg) => {
    try {
      const msg = decodeMessage(m.data);
      const key = idempotencyKey(msg);
      if (seen.has(key)) {
        await announce(msg, key, msg.body !== null ? "inline" : "claim", true);
        m.ack();
        return;
      }
      const via = await resolveBody(msg);
      await sleep(WORK_MS);
      await announce(msg, key, via, false);
      remember(key);
      console.log(`[consumer] hook=${msg.id} via=${via} size=${msg.size} latency_ms=${Date.now() - msg.received_at}`);
      m.ack();
    } catch (err) {
      await fail(m, err);
    }
  };

  for await (const m of messages) {
    await handle(m);
  }
}

main().catch((err) => {
  console.error("[consumer] fatal:", err);
  process.exit(1);
});
