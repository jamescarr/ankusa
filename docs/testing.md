# Testing

Every package's test suite is run from its own directory — there's no
top-level test runner spanning all five, because each has a genuinely
different infrastructure dependency (none, Postgres, RabbitMQ, Redpanda,
NATS).

## `ankusa` core — `mix test`

```sh
mix test                              # 152 tests, no external infra needed
mix test --include integration        # +8 tests, needs floci running (see below)
```

The 152 always-on tests cover:

- **WAL** (`WAL.DiskLog`): group commit, dedup (including tenant-scoped),
  crash-replay (torn-frame handling), truncation, that a restart after a full
  truncation does **not** reuse seqs, and the measurements `[:commit, :stop]`
  reports.
- **HTTP adapters** (outbound): the SigV4 signing `BlobStore.S3` puts on the
  wire, pinned against AWS's published reference signatures and against the
  request `Req.Test` captures; `Sink.Http` forwarding, status mapping, and the
  rule that a redirect is reported rather than followed; the shared
  `Ankusa.HttpClient` allowlist.
- **Edge**: accept/duplicate/verify/quarantine/load-shed/oversize, shedding with
  `503` once the batcher's queue fills while a commit is in flight, pluggable
  route resolvers (`Path` and `TenantPath`), tenant-scoped dedup end to end
  through the HTTP layer.
- **Dispatch**: retry, DLQ, a sink that *raises* being retried and dead-lettered
  instead of killing the pipeline, and ordering — a blocked delivery holds the
  cursor while another ordering key proceeds, and same-key deliveries stay in
  `seq` order.
- **Storage**: compaction round-trip, `roll_bytes` splitting a backlog into
  several segments in one tick, and the live index — a lookup after a
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
mix test                      # 11 tests
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
- **Commit order equals seq order** — a statement trigger sleeps inside every
  insert, widening the allocation→commit window, while 8 writers append
  concurrently and a reader follows the cursor. Every committed seq must be
  seen by the reader. Before the per-instance advisory lock, this lost ~10-20%
  of commits (34 seqs in one run); it is the chaos-phase loss, reproduced as a
  test.
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

## `ankusa_nats` — `mix test`

Same pattern, against NATS with JetStream enabled:

```sh
cd ankusa_nats
docker compose up -d --wait   # NATS on :4223 (client), :8223 (monitoring)
mix test                      # 6 tests
docker compose down -v
```

`NATS_SERVERS` (default `localhost:4223`) points the suite at another server.
Each test creates its own stream with `Gnat.Jetstream.API.Stream.create/2`
(`subjects: ["ankusa.test.<n>.>"]`, memory storage) and deletes it in
`on_exit`, so nothing depends on a stream being pre-provisioned. Covers:

- an inline message: the subject it landed on, the five headers, the
  `Ankusa.Sink.Message` body — **and** `Gnat.Jetstream.API.Stream.info` reporting
  the message as stored, which is what proves the `:ok` came from JetStream's
  publish ack rather than from a successful socket write;
- a fat payload checked in through `ClaimCheck`, the message carrying a
  ticket that redeems to the original bytes;
- `:subject` as a static string and as a 1-arity fun;
- a subject **no stream covers** being `{:error, :no_stream}`, with
  `Gnat.Jetstream.API.Stream.list` confirming no stream was created for it —
  the sink never creates one;
- a publish the stream itself refuses (`max_msg_size` exceeded) being an
  error, not a stored hook. JetStream answers that with `"seq": 0` *and* an
  `"error"` in the same ack, so this test is the one that pins the sink's
  error-first ack parsing;
- an unreachable server failing fast (`:econnrefused`, not a hang).

gnat is pure Elixir, so unlike `ankusa_kafka` this suite needs no C toolchain
and no container.

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

Follow `ankusa_postgres`/`ankusa_rabbitmq`/`ankusa_kafka`/`ankusa_nats`: a
`docker-compose.yml` for the real
dependency, and tests that
hit the real thing. A mock proves your code calls a mock correctly; it
proves nothing about whether a hand-rolled protocol implementation (SQL,
AMQP, SigV4, whatever) is actually right. Every adapter in this repo that
talks to external infrastructure is tested against a real instance of that
infrastructure, not a stand-in for it — that standard applies to new
adapters too.

## Core bench — `bench/core_bench.exs`

An in-process ingest → dispatch bench for core alone: no HTTP hop, no consumer,
no Kubernetes.

```sh
MIX_ENV=test N=20000 CONCURRENCY=256 SINK_LATENCY_MS=5 mix run bench/core_bench.exs
```

It ingests `N` hooks through `Ankusa.Edge.Ingest` at `CONCURRENCY` workers, then
waits for a sink that sleeps `SINK_LATENCY_MS` per delivery to receive every
acked envelope. One JSON line plus a table; exit code is non-zero if anything
acked never arrived. `MIX_ENV=test` because `config/config.exs` autostarts the
default instance on :4000 outside `:test`.

On the reference machine (Apple M4 Pro, macOS), before and after the
reliability/perf pass:

| | `end_to_end_per_s` | `drain_s` | `ingest_per_s` | `missing` |
| --- | --- | --- | --- | --- |
| before (sequential dispatch) | 120–123 | 162–165 | 15k–53k¹ | 0 |
| after (concurrent dispatch) | 4.1k–4.7k | 3.9–4.6 | 66k–78k | 0 |

¹ The two baseline runs differ (52.8k with the machine idle, 15.2k while builds
and tests ran concurrently on the same host); dispatch was the ceiling either
way, and `end_to_end_per_s` was unaffected. The "after" range is across repeated
runs (the high end on an idle host, the low end with other work in flight).

Dispatch *was* that ceiling: one envelope at a time, cursor written after each.
`drain_s` is now ~40× lower, and the theoretical ceiling at
`dispatch.concurrency` 32 with a 5 ms sink is 6.4k/s — the bench lands at
4.1–4.7k/s, with the pipeline idle most of the time.

Profiling this bench also surfaced two bottlenecks unrelated to the pipeline's
shape, both fixed in this pass:

- `WAL.DiskLog.read/3` used `:ets.select/3` with a `>` guard, which makes ETS
  scan the `ordered_set` from the front on every read: 371 µs per call with the
  cursor at the tail of a 20k-record log, versus 0.05 µs for the keyed
  `:ets.next/2` walk it uses now.
- the dispatch task closure reached into `state.instance`/`state.config`, which
  captures the *whole* pipeline state — so every spawn copied `runnable`
  (thousands of admitted envelopes) into the new process: ~580 µs per spawn,
  46 µs once the fields are bound before the closure.

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
   `BURST_SECONDS`; the drain time it reports is how long the pipeline needed
   after ingest stopped, so it is the dispatch throughput ceiling referenced in
   [`deployment.md`](deployment.md#dispatch-throughput).

`mix loadgen.verify` polls `processed_webhooks` (the consumer's ground truth,
written by the idempotent `WebhookWorker.perform/1` upsert) until every acked
id shows up or its timeout expires, and fails the run on any `missing > 0` or
`sha_mismatches > 0`.

Run it yourself: `cd examples/oban-consumer && ./run.sh` (needs `kind`,
`kubectl`, `docker`, `mix`; `brew install kind` if missing). `RATE` defaults to
300.

Results from `RATE=60` and `RATE=300` verification runs, `kind` cluster
otherwise idle (`RATE=60` is the same load the pre-fix numbers below used, so
the two tables are directly comparable):

| Machine | `RATE` | Phase | accepted/s | p50 | p95 | p99 | shed | errors | missing | extra deliveries | `drain_s` |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Apple M4 Pro, macOS, OrbStack | 60 | steady | 56.0 | 5.3 | 9.6 | 13.5 | 0 | 0 | **0** | 0 | 0.04 |
| Apple M4 Pro, macOS, OrbStack | 60 | chaos | 56.1 | 5.3 | 9.5 | 14.3 | 0 | 0 | **0** | 0 | 0.04 |
| Apple M4 Pro, macOS, OrbStack | 60 | burst (64 workers) | 1496.5 | 39.0 | 69.7 | 89.7 | 0 | 0 | **0** | 0 | 0.05 |
| Apple M4 Pro, macOS, OrbStack | 300 | steady | 283.5 | 3.8 | 9.2 | 34.5 | 0 | 0 | **0** | 0 | 0.08 |
| Apple M4 Pro, macOS, OrbStack | 300 | chaos | 283.5 | 4.1 | 48.0 | 165.0 | 0 | 0 | **0** | 0 | 0.09 |
| Apple M4 Pro, macOS, OrbStack | 300 | burst (64 workers) | 1236.5 | 43.0 | 88.7 | 282.8 | 0 | 0 | **0** | 0 | 0.09 |

(Latencies in ms. `drain_s` is the time from the end of ingest to the last ack
being visible in the consumer, measured on the verify poll; the burst generator
is closed-loop, so its `accepted/s` is what 64 workers and a 40 ms round trip
sustain, not a dispatch ceiling.) Every phase, including chaos, reports
`missing: 0` and `sha_mismatches: 0`. This is the same machine every other
number in this doc was measured on, not a production-scale claim.

Re-verified after rebasing onto `main` (NATS JetStream adapter, HMAC verifier
engine): `RATE=300` again reported `missing: 0` and `sha_mismatches: 0` in all
three phases — steady 284.0/s, chaos 284.0/s, burst 1360.2/s, `shed: 0`, and
`drain_s` 0.05–0.07 s.

`drain_s` in the tenths of a second with `shed: 0` at both rates is what makes
`RATE=300` the default: dispatch keeps up with ingest in real time, so the
paced phases never build a backlog for the burst phase to inherit.

### The chaos-phase loss: root cause and fix

Killing an `ankusa-worker` pod used to drop ~0.5–1.5% of acked hooks
permanently. The mechanism was not in the dispatch pipeline: `WAL.Postgres`
allocates `seq` at INSERT time (`BIGSERIAL`) but a row only becomes visible at
COMMIT, so two writers could allocate 100 and 101 and commit in the opposite
order. A reader following the log with `seq > cursor` read 101, advanced its
cursor past it, and never saw 100 when it landed — and the compactor, whose
truncation is bounded by that same cursor, then deleted the row. The hook was
in `ankusa_wal` no longer, in no `oban_jobs` row, no `processed_webhooks` row,
no DLQ — exactly the observed signature, with the cursor already advanced. It
took killing the *worker* to reproduce because that bursts the catch-up load
onto the shared Postgres and widens the allocation→commit window.

`WAL.Postgres.append/2` now takes a per-instance advisory lock
(`pg_advisory_xact_lock`) before allocating seqs and holds it until COMMIT, so
seq order *is* commit order; fleet-wide serialized commits per instance is the
accepted cost. The regression test (`ankusa_postgres`) widens the window with a
sleeping statement trigger and, on the pre-fix code, fails with ~34 seqs a
cursor-following reader never sees. The chaos phase now reports `missing: 0`.

Pre-fix code, same machine, same harness — `RATE=60`, `POOL_SIZE=30`, and the
fixed loadgen, so this isolates the core change:

| Phase | accepted/s | p50 | p95 | p99 | missing | `drain_s` |
| --- | --- | --- | --- | --- | --- | --- |
| steady | 56.3 | 11.1 | 13.5 | 15.6 | 0 | 0.04 |
| chaos | 56.3 | 11.2 | 15.4 | 18.7 | 0 | 0.04 |
| burst (64 workers) | 3168.1 | 18.0 | 31.7 | 40.7 | 0 | **320.6** |

The paced phases were fine: 60/s is far below the ~150/s the old
one-envelope-at-a-time dispatch could sustain. The burst phase is where the
ceiling shows — 47,632 accepted envelopes took **320.6 seconds** to drain after
ingest stopped (≈149/s), against 0.05–0.09 s now.

That run's chaos phase came back clean, which is honest but not reassuring:
the old loss was a race, and it needs the allocation→commit window to be wide
enough at the exact moment a cursor reader passes the tail. The earlier
documented run of the same harness did lose 12 of 1581 acked hooks in chaos
(with a 301.7 s verify timeout), and the `ankusa_postgres` regression test
reproduces the mechanism deterministically rather than by luck.
