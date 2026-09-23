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
[`AGENTS.md`](../AGENTS.md).

## Verifying the worked example

[`examples/rabbitmq-consumer/`](../examples/rabbitmq-consumer/) isn't a Mix
test suite — it's verified by actually running it:

```sh
cd examples/rabbitmq-consumer
docker compose up --build
curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' -d '{"id":"evt_1"}'
docker compose logs worker     # confirm the hook printed
docker compose down -v
```

[`examples/kafka-sqs-consumer/`](../examples/kafka-sqs-consumer/) is verified
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

## Writing a new adapter's tests

Follow `ankusa_postgres`/`ankusa_rabbitmq`/`ankusa_kafka`: a
`docker-compose.yml` for the real
dependency, `config/config.exs` setting `autostart: false`, and tests that
hit the real thing. A mock proves your code calls a mock correctly; it
proves nothing about whether a hand-rolled protocol implementation (SQL,
AMQP, SigV4, whatever) is actually right. Every adapter in this repo that
talks to external infrastructure is tested against a real instance of that
infrastructure, not a stand-in for it — that standard applies to new
adapters too.
