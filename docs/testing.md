# Testing

Every package's test suite is run from its own directory — there's no
top-level test runner spanning all three, because each has a genuinely
different infrastructure dependency (none, Postgres, RabbitMQ).

## `ankusa` core — `mix test`

```sh
mix test                              # 123 tests, no external infra needed
mix test --include integration        # +8 tests, needs floci running (see below)
```

The 123 always-on tests cover:

- **WAL** (`WAL.DiskLog`): group commit, dedup (including tenant-scoped),
  crash-replay (torn-frame handling), truncation, and the measurements
  `[:commit, :stop]` reports.
- **HTTP adapters** (outbound): the SigV4 signing `BlobStore.S3` puts on the
  wire, pinned against AWS's published reference signatures and against the
  request `Req.Test` captures; `Sink.Http` forwarding, status mapping, and the
  rule that a redirect is reported rather than followed; the shared
  `Ankusa.HttpClient` allowlist.
- **Edge**: accept/duplicate/verify/quarantine/load-shed/oversize, pluggable
  route resolvers (`Path` and `TenantPath`), tenant-scoped dedup end to end
  through the HTTP layer.
- **Dispatch**: retry, DLQ.
- **Storage**: compaction round-trip, and the live index — a lookup after a
  later compaction sees every row, and one taken while the compactor is down
  falls back to the file and is correct again after its restart.
- **Claim Check** (`test/ankusa/claim_check/`): ticket validation and
  traversal-safe key derivation; `Direct` round-trip over `LocalFS`;
  idempotent re-check-in; tampered-object integrity detection;
  `validate_config!/1` boot-time rejections; the `:claim_check` role's HTTP
  API (auth, tenant scoping, size cap, status-code mapping); a **real**
  cross-mode proof — a ticket checked in via `Direct` redeems via `Remote`
  over an actual HTTP hop (`ThousandIsland.listener_info/1` resolves the
  live port), and back; the `LocalFS` retention sweeper.
- **A loss checker**: acks 500 hooks concurrently, hard-kills the instance
  mid-flight, and proves every acked id survives replay from the WAL. Zero
  tolerance — this is the test that actually backs the core invariant claim
  in [`architecture.md`](architecture.md), not just the description of it.

The 8 `:integration`-tagged tests (`test/ankusa/blob_store_s3_test.exs`,
`blob_store_gcs_test.exs`) exercise `BlobStore.S3`/`BlobStore.GCS` against
real running emulators — put/get round-trip, `get_range` byte-slicing,
`:not_found`, `list`+`delete`. Excluded by default
(`test_helper.exs`:`ExUnit.start(exclude: [:integration])`) because they
need live infra:

```sh
docker compose up -d          # floci (S3, :4566) + floci-gcp (GCS, :4588)
mix test --include integration
docker compose down -v
```

## `ankusa_postgres` — `mix test`

Every test here inherently needs a live Postgres — there's no meaningful
"offline" mode for a WAL adapter, so nothing is tagged `:integration`; the
whole suite just requires the container:

```sh
cd ankusa_postgres
docker compose up -d --wait   # Postgres on :5433
mix test                      # 10 tests
docker compose down -v
```

Notably covers, against the *real* database (not a mock):

- Put/get round-trip via `append` + `read`.
- Intra-batch duplicate collapse (two records in one `append/2` call sharing
  a dedup key).
- Cross-batch duplicate (separate `append/2` calls).
- Nil dedup keys never colliding.
- **Concurrent writers racing the same dedup key** — 8 `Task`s calling
  `append/2` simultaneously with the same key; exactly one commits, seven
  come back `:duplicate` pointing at the same `seq`. This is the test that
  backs the "many ingest servers share one WAL safely" claim.
- **Truncation preserves dedup** — append, truncate through that seq
  (physically deleting the row), re-append the same dedup key, still get
  `:duplicate` at the original seq. This caught a real bug during
  development (the dedup ledger originally joined back to the truncated
  table); see the adapter's own moduledoc.
- Two instances sharing one database never cross-contaminate seq or dedup.

## `ankusa_rabbitmq` — `mix test`

Same pattern — every test needs live RabbitMQ:

```sh
cd ankusa_rabbitmq
docker compose up -d --wait   # RabbitMQ on :5673 (AMQP), :15673 (management UI)
mix test                      # 4 tests
docker compose down -v
```

Covers: inline-payload publish + decode, fat-payload claim check-in (message
carries a ticket, the claim round-trips through `Ankusa.ClaimCheck.redeem/3`
against a real `BlobStore`), routing key as both a static string and a
function, and a fast-fail check (`:econnrefused`, not a hang) against an
unreachable broker.

## `ankusa_kafka` — `mix test`

Same pattern, against Redpanda:

```sh
cd ankusa_kafka
docker compose up -d --wait   # Redpanda on :19092
mix test                      # 5 tests
docker compose down -v
```

`KAFKA_BROKERS` (default `localhost:19092`) points the suite at another
broker. Each test creates its own topic in `setup` and deletes it in
`on_exit`, and records are read back with `:brod.fetch/4`, so no consumer
group is involved. Covers: inline record round-trip (value, key, headers,
event timestamp), fat record claim check-in plus redeem, `:key` as a static
string and as a function, an unknown topic being an error that is **not**
auto-created, and an unreachable broker failing within `produce_timeout_ms`
instead of hanging.

brod's `crc32cer` NIF compiles from source, so the first `mix deps.compile`
needs a C toolchain and CMake ≥ 3.16 (`apk add build-base cmake` on Alpine,
`brew install cmake` on macOS) — or run the suite in a container, see
[`AGENTS.md`](https://github.com/jamescarr/ankusa/blob/main/AGENTS.md).

## Verifying the worked example

[`examples/rabbitmq-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/rabbitmq-consumer/) isn't a Mix
test suite — it's verified by actually running it:

```sh
cd examples/rabbitmq-consumer
docker compose up --build
curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' -d '{"id":"evt_1"}'
docker compose logs worker     # confirm the hook printed
docker compose down -v
```

[`examples/kafka-sqs-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/kafka-sqs-consumer/) is verified
the same way, plus its three failure drills (bridge down, worker down, poison
claim — each with its own `docker compose` commands and expected output in
that example's README). After it's up, one small and one fat hook exercise
both payload paths end to end:

```sh
cd examples/kafka-sqs-consumer
docker compose up --build -d --wait
curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' -d '{"id":"evt_1"}'
python3 -c "import json;print(json.dumps({'id':'evt_2','items':[{'n':i} for i in range(2000)]}))" \
  | curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' --data-binary @-
docker compose logs worker   # via=inline, then via=claim:<id>
docker compose down -v
```

Follow `ankusa_postgres`/`ankusa_rabbitmq`/`ankusa_kafka`: a
`docker-compose.yml` for the real
dependency, and tests that
hit the real thing. A mock proves your code calls a mock correctly; it
proves nothing about whether a hand-rolled protocol implementation (SQL,
AMQP, SigV4, whatever) is actually right. Every adapter in this repo that
talks to external infrastructure is tested against a real instance of that
infrastructure, not a stand-in for it — that standard applies to new
adapters too.

## Load and end-to-end (kind + Oban)

[`examples/oban-consumer/run.sh`](https://github.com/jamescarr/ankusa/blob/main/examples/oban-consumer/run.sh)
is the only test in this repo that proves zero loss on a real, multi-node
Kubernetes deployment rather than in-process. It stands up a `kind` cluster
(3-replica `ankusa-edge`, a singleton `ankusa-worker` running `dispatch,storage`,
2-replica `consumer` running Oban), then drives [`tools/loadgen`](https://github.com/jamescarr/ankusa/blob/main/tools/loadgen)
through three phases against it:

1. **steady** — a paced `RATE` req/s for `DURATION` seconds.
2. **chaos** — the same load, with `kubectl delete pod` against one `ankusa-edge`
   pod, `ankusa-worker-0`, and one `consumer` pod at +10s/+20s/+30s.
3. **burst** — closed-loop at `CONCURRENCY` workers, no rate cap, for
   `BURST_SECONDS`; the drain time this reports is the dispatch throughput
   ceiling referenced in [`deployment.md`](deployment.md#dispatch-throughput)
   (`Ankusa.Dispatch.Pipeline` delivers one envelope at a time).

`mix loadgen.verify` polls `processed_webhooks` (the consumer's ground truth,
written by the idempotent `WebhookWorker.perform/1` upsert) until every acked
id shows up or its timeout expires, and fails the run on any `missing > 0` or
`sha_mismatches > 0`.

Run it yourself: `cd examples/oban-consumer && ./run.sh` (needs `kind`,
`kubectl`, `docker`, `mix`; `brew install kind` if missing). `RATE` defaults
to 60 — a first pass at `RATE=100` built a growing backlog under load on a
resource-constrained laptop `kind` cluster (`drain_s` hit the verify timeout
with `missing > 0` while dispatch was still progressing, not a loss); halving
it against the measured burst drain rate cleared that up entirely. Results
from a verification run, `kind` cluster otherwise idle:

| Machine | `RATE` | `DURATION` | Phase | accepted/s | p50 | p95 | p99 | shed | errors | missing | extra deliveries | `drain_s` |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Apple M4 Pro, macOS, OrbStack | 60 | 60s | steady | 23.6 | 32.9 | 42.4 | 46.4 | 0 | 0 | **0** | 0 | 0.04 |
| Apple M4 Pro, macOS, OrbStack | 60 | 60s | chaos | 23.5 | 32.9 | 43.6 | 49.5 | 0 | 4 | **12 (of 1581)** | 0 | 301.7 (timeout) |
| Apple M4 Pro, macOS, OrbStack | — (burst, 64 concurrency) | 15s | burst | 3296.0 | 17.6 | 29.7 | 37.0 | 0 | 0 | **0** | 0 | 325.1 |

(Latencies in ms.) This is the same machine every other number in this doc
was measured on, not a production-scale claim.

### Known issue: chaos-phase loss under an `ankusa-worker` pod kill

**`steady` and `burst` are clean (`missing: 0`) every run once the `kind`
cluster isn't resource-starved by other work on the host — see the `RATE`
note above.** `chaos` is not: it reproduces a small (~0.5–1.5%, single- to
low-double-digit envelope) permanent loss, isolated by bisection to killing
`ankusa-worker-0` alone (killing only `ankusa-edge` and/or `consumer` does
not reproduce it). After the loss, the affected envelope id is absent from
`ankusa_wal`, `oban_jobs`, and `processed_webhooks` alike, the dispatch
cursor has already advanced past its seq, and
`Ankusa.Dispatch.DLQ`'s log file was never created — so dispatch believes it
delivered successfully, but no receiver ever recorded it, and it never
entered the give-up path either.

A preStop hook (`sleep 5` before SIGTERM, giving `kube-proxy` time to drain
Service endpoints — see `k8s/20-consumer.yaml`/`k8s/30-ankusa.yaml`) does not
fix it, which rules out the ordinary "new connection routed to a terminating
pod" class of Kubernetes race. Code review of `Ankusa.Dispatch.Pipeline`
(`drain/1`, `deliver_with_retry/4`), `Ankusa.WAL.Postgres` (`read/2`,
`get_cursor/2`, `put_cursor/3`), and `Ankusa.Storage.Compactor`'s
never-truncate-past-dispatch guard did not turn up the mechanism — the
cursor-then-deliver ordering looks self-healing by construction. Root cause
is **not isolated**; reproducing and fixing it needs deeper live tracing of
`ankusa-worker` across a real kill (a debugger attached to the pod, or
telemetry spanning the crash) that's out of scope for this change, and
`Ankusa.Dispatch.Pipeline` is explicitly off-limits to modify here (see
`plans/prepare-release.md`'s critical files list). Filed as a known issue
for 0.1.0 rather than silently reported as passing: **the chaos-phase
acceptance criterion (`missing: 0`) is not met** by the current `ankusa`
core, only the infrastructure and example wiring around it.
