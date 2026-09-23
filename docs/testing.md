# Testing

Every package's test suite is run from its own directory — there's no
top-level test runner spanning all three, because each has a genuinely
different infrastructure dependency (none, Postgres, RabbitMQ).

## `ankusa` core — `mix test`

```sh
mix test                              # 98 tests, no external infra needed
mix test --include integration        # +8 tests, needs floci running (see below)
```

The 98 always-on tests cover:

- **WAL** (`WAL.DiskLog`): group commit, dedup (including tenant-scoped),
  crash-replay (torn-frame handling), truncation.
- **Edge**: accept/duplicate/verify/quarantine/load-shed/oversize, pluggable
  route resolvers (`Path` and `TenantPath`), tenant-scoped dedup end to end
  through the HTTP layer.
- **Dispatch**: retry, DLQ.
- **Storage**: compaction round-trip.
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

## Verifying the worked example

[`examples/rabbitmq-consumer/`](../examples/rabbitmq-consumer/) isn't a Mix
test suite — it's verified by actually running it:

```sh
cd examples/rabbitmq-consumer
docker compose up --build
curl -XPOST localhost:4000/hooks/demo -H 'content-type: application/json' -d '{"id":"evt_1"}'
docker compose logs worker     # confirm the hook printed
docker compose down -v
```

## Writing a new adapter's tests

Follow `ankusa_postgres`/`ankusa_rabbitmq`: a `docker-compose.yml` for the real
dependency, `config/config.exs` setting `autostart: false`, and tests that
hit the real thing. A mock proves your code calls a mock correctly; it
proves nothing about whether a hand-rolled protocol implementation (SQL,
AMQP, SigV4, whatever) is actually right. Every adapter in this repo that
talks to external infrastructure is tested against a real instance of that
infrastructure, not a stand-in for it — that standard applies to new
adapters too.
