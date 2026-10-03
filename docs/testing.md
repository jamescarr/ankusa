# Testing

Every package's test suite runs from its own directory under `packages/`;
there's no single test runner spanning them, because each has a genuinely
different infrastructure dependency (none, RabbitMQ, Redpanda, NATS, Redis).
`mise run check:package <pkg>` runs one package's full CI check (format,
warnings-as-errors, tests, docs), starting and stopping that package's own
`docker-compose.yml` around the suite; `mise run check` runs all of them. The
eight client SDKs are also checked against one shared, language-neutral vector
suite — `mise run check:conformance`, below.

## `ankusa` core: `mix test`

```sh
mise run check:package ankusa         # no external infra needed
mise run test:integration             # +16 tests against the floci emulators (see below)
```

The always-on tests cover:

- **Store and queue** (`store_test.exs`, `queue_test.exs`): the RocksDB store
  applies one batch across column families (puts, deletes, range deletes) and
  refuses an unreadable or missing store with `:store_unavailable` rather than
  reporting it empty; `fold/6` is half-open, ordered and never yields a
  sentinel; a corrupt blob or SST block fails the scan and the point read
  instead of silently shortening results, and a damaged WAL record refuses to
  open while a torn tail drops cleanly; `reopen/1` republishes working handles
  and keeps every committed key, and a store left closed by a failed reopen
  opens again by itself; the database opens with the recovery, blob and buffer
  settings asked for (read back from the `OPTIONS` file RocksDB writes, since
  RocksDB keeps its default for a value it cannot parse). The queue writer
  assigns dense, strictly
  increasing seqs (the hook's seq comes from its key); seqs never repeat after
  every hook is delivered, reclaimed and the instance restarts; a commit while
  the store is down is refused and recovers with a higher seq; and
  `[:commit, :stop]` measures successful commits only.
- **Migration** (`store_migrate_test.exs`): a 0.3 data dir imports once — only
  what 0.3 had not finished (undelivered hooks deliver, unarchived hooks
  archive), dead letters carry over and replay to the current sinks, the
  quarantine pen, API sources, rate-limit overrides and the segment index all
  carry over, and the next seq clears everything imported. An artifact that
  cannot be trusted stops the boot: damage in the middle of the 0.3 WAL with acked
  frames after it, or a cursor file 0.3 did not write, refuses to start rather
  than guess; a torn final frame imports every complete frame before it. An
  artifact that shows up after the first boot is never imported over live
  data, and under `wal: :none` the queue artifacts are left for a node that
  has a queue.
- **HTTP adapters** (outbound): the SigV4 signing `BlobStore.S3` puts on the
  wire, pinned against AWS's published reference signatures and against the
  request `Req.Test` captures; the RSA-SHA256 *Signature version 1* signing
  `BlobStore.OCI` puts on the wire, pinned against OCI's published reference
  signature (computed with OpenSSL) and reconstructed from captured requests;
  the `BlobStore.Azure` request plumbing (Put Blob headers, SAS appending vs.
  bearer `:token_provider`, `get_range` windows, `list` XML, `:not_found`); the
  `Ankusa.BlobStore.Azure.ManagedIdentity` token provider (IMDS fetch shape,
  caching, expiry-window refresh); `Sink.Http` forwarding, status mapping, and the
  rule that a redirect is reported rather than followed; the shared
  `Ankusa.HttpClient` allowlist.
- **Edge**: accept/verify/quarantine/load-shed/oversize, shedding with
  `503` once the batcher's queue fills while a commit is in flight, pluggable
  route resolvers (`Path` and `TenantPath`), and that the same body posted
  twice is stored twice (two ids, two stored hooks). A commit is never
  abandoned: a record still buffered at its deadline behind a stuck commit is
  answered `503` and never stored, the writer refuses a batch whose deadline
  passed before it could start, a commit stuck longer than the old 5 s writer
  timeout is waited out and every `201` is stored, and a commit task killed from
  outside while its call waits for the writer fails only its own batch and
  leaves nothing of it stored. The batcher's status shows how many records it
  holds, never their sinks. `edge_direct_test.exs`
  covers the other ack path (`wal: :none`): the `201` body is exactly
  `{id, status}`, a sink sees the envelope before the response, a refusing,
  raising or bad-returning sink is `503` with `Retry-After` and is called
  exactly once, no queue writer runs and no hook is stored, an oversized
  body reaches the sink with `ctx.claim`, and a claim check whose blob store
  exits or returns garbage is a `503` before any sink runs.
- **Failure domains** (`instance_test.exs`): a dispatch subtree that exhausts
  its restart budget leaves the edge listener, batchers and store untouched,
  still acks hooks during the outage and delivers them when the subtree comes
  back; a restart that fails is retried with a longer delay while the manager
  stays up; a `Registry` partition crash rebuilds the instance with every
  process registered again; and no process's status (`:sys.get_status/1`, what
  a crash report prints) contains sink options, nor does the crash report of a
  batcher or source store that dies handling a hook or a source write.
- **Dispatch** (`dispatch_test.exs`): a hook reaches every sink of its source
  exactly once; a failing sink is retried until it succeeds; a failing sink's
  retries do not hold the slots a healthy source needs (D1); one dead row per
  exhausted sink, with the failing sink named in the stored reason, and the
  hook kept until every obligation clears; replay re-delivers a dead hook once
  and then reclaims it (D5); a raising sink and a bad-returning sink are
  dead-lettered, not fatal to dispatch; hooks whose source was deleted are
  dead-lettered after a restart (D2); a claimed row is retried after a
  restart while a delivered row is not (D7); a row that became visible below
  the scan floor (a stalled commit) is delivered when its wake arrives; and a
  window that drained is refilled even when housekeeping flushes the outcomes
  first. Ordering is not asserted: lanes are gone.
- **Filesystem durability** (`fsync_test.exs`): the helpers return an error
  for a path they cannot open, write or rename, never raise, and `write_file`
  replaces content in one step and leaves no temp file. The fsync order on
  disk is checked by `strace` on Linux, not in the suite.
- **Storage** (`storage_test.exs`): compaction round-trips every hook
  byte-for-byte with its seq, and a tick writes one segment plus one index
  object; a hook survives one cleared obligation and is reclaimed once both
  clear, in either order; without the `:storage` role a delivered hook is
  reclaimed straight away (W5); `roll_bytes` caps a segment's size; a failing
  blob store fails the tick without crashing it and the next tick retries; and
  the catalogue lives in the store, so fetch survives an instance restart.
- **Claim Check** (`test/ankusa/claim_check/`, `test/ankusa/dispatch/claim_check_test.exs`):
  reference parsing (URN grammar, tenant and ULID claim-id rejection, date
  partitions from the pack id's timestamp, claim ids locating their pack and
  position); pack building (index rows, ZIP offsets, manifest, standard-reader
  round-trip); pack/redeem round-trips over `LocalFS`; tampered-object
  integrity detection; `404` for a claim id past the end of its pack's index;
  packing per tenant per batch with `pack_max_bytes` splitting and
  failure isolation; check-in-once (a fat hook on two sinks and a failing
  retry writes one object); `validate_config!/1` boot-time rejections; the
  `:claim_check` role's read-only HTTP API; and the `LocalFS` retention sweeper
  deleting whole `dt=` day partitions.
- **Route management** (`test/ankusa/net_test.exs`, `net/client_ip_test.exs`,
  `routes/matcher_test.exs`, `routes_test.exs`, `edge_route_guard_test.exs`,
  `routes/router_test.exs`): example-based, no property tests. `cidr` (the
  dependency) does the prefix bit math, so `net_test.exs` pins the small surface
  Ankusa keeps on top of `:inet` — address parsing, canonical rendering, the
  IPv4-mapped-to-IPv4 normalization, and `parse_cidr/1`, where junk is an
  `:error` rather than a raise and an IPv4-mapped *range* is rejected because a
  normalized address could never match one.
  `net/client_ip_test.exs` walks the forwarded chain entry by entry: the header
  is read only from a trusted peer, every `x-forwarded-for` value is joined in
  order, the chain is walked right to left, an entry with a port or IPv6 brackets
  is read, an unreadable entry before the client denies the request, and no
  header falls back to the peer. `routes/matcher_test.exs` covers the
  path-pattern grammar and every negative case (`%2F`, `.`/`..`, a wildcard that
  is not last). `routes_test.exs` covers the ETS store's cap, `authorize/4`'s
  decision matrix (including a route's own rules replacing the global list, and
  a global deny beating them), the decision cache, the dry run's rule and scope
  reporting, telemetry, and CRUD. `edge_route_guard_test.exs` drives the guard
  with real requests and asserts its store effects (a rejected request writes
  **nothing**); `routes/router_test.exs` drives the management API over both
  `Plug.Test.conn` and a real socket, unauthenticated by design like
  `Ankusa.Admin.Router`. `routes/router_openapi_test.exs` is the
  contract test for that API: it drives every documented operation from the
  examples in `priv/openapi/admin.v1.yaml` — path parameters, query parameters,
  and request bodies included — and drives every documented error response with
  a request that produces it, running each through the real router and checking
  the status, schema, and field names against the document, plus the documented
  method matrix, 404s for the near-misses of the documented surface, and every
  example against its own schema, so the spec and the code cannot drift apart.
- **A loss checker** (`loss_test.exs`): the one test here that runs a separate
  OS process. It starts a child BEAM that owns the same store as the test's
  instance and ingests concurrently, waits for 500 printed acks, then
  `kill -9`s the child — no clean shutdown, no flush beyond the `sync: true`
  commit that preceded each ack — reopens only the store over the same data
  dir and proves every acked id reads back. Zero tolerance: this is the test
  that actually backs the `wal.type: disk` half of the core invariant claim in
  [`architecture.md`](architecture.md), not just
  the description of it. The `wal.type: none` half is backed by
  `edge_direct_test.exs`, which asserts the response really does wait for the
  sink's confirm — and is a `503` when that confirm never comes.
- **Lifecycle events** (`lifecycle_test.exs`): through a real instance, a
  source's create/update/delete and a route's create/replace/patch/delete each
  deliver one CloudEvent to the configured sink, the source's secret arrives
  redacted, a refused change emits nothing, and the store assigns no seq to an
  event (they bypass it). A refusing sink is retried until it confirms; when
  the retries run out the event is dropped and counted but the change stands; a
  broken sink (one that raises, or returns neither `:ok` nor an error) never
  holds back another or crashes the publisher; a full queue and a publisher that isn't
  running each drop and count the event; with no lifecycle sinks there is no
  publisher. `ankusa:lifecycle` is a `404` at ingest, an event over a sink's
  inline threshold is claim-checked under a valid tenant and redeems to the
  event, and each invalid lifecycle config is refused at boot.
- **The AsyncAPI document** (`async_api_test.exs`, plus `GET /asyncapi.json` in
  `admin/router_test.exs`): sources on one address share a channel, sinks
  without a channel are absent, a computed address is a channel of its own, the
  tenant is fixed in a message only when the route resolver takes it from the
  source, ids that collide once sanitized stay distinct, lifecycle sinks add a
  channel naming the CloudEvent schema, and every document `async_api_spex`
  validates. The adapters' `*_describe_test.exs` pin what each sink advertises
  (and that no credential reaches it) without a broker.

The 16 `:integration`-tagged tests (`test/ankusa/blob_store_s3_test.exs`,
`blob_store_gcs_test.exs`, `blob_store_azure_integration_test.exs`,
`blob_store_oci_integration_test.exs`) exercise `BlobStore.{S3,GCS,Azure,OCI}`
against real running emulators: put/get round-trip, `get_range` byte-slicing,
`:not_found`, `list`+`delete`. Excluded by default
(`test_helper.exs`:`ExUnit.start(exclude: [:integration])`) because they
need live infra:

```sh
mise run test:integration
```

It starts `packages/ankusa/docker-compose.integration.yml` (floci S3 on
:4566, floci-gcp on :4588, floci-az on :4577, floci-oci on :4599), waits for
the bucket/container bootstrap, runs `mix test --include integration` in
`packages/ankusa`, and tears the emulators down.

## `async_api_spex`: `mix test`

```sh
mise run check:package async_api_spex    # 16 tests, no external infra needed
```

Covers: a document declared with `use AsyncApiSpex.Schema` / `Message` encoding
to the exact AsyncAPI 3.0 JSON (lowerCamel keys, no nulls, `$ref`s into
`components`, `x-` extensions inlined), two modules claiming one component name
raising, each validator rule failing with one error that names its JSON path,
bad macro options raising at compile time, `AsyncApiSpex.Plug.RenderSpec`
answering `application/asyncapi+json`, and `mix async_api_spex.gen` writing a
decodable file.

## `ankusa_rabbitmq`: `mix test`

Same pattern: every test needs live RabbitMQ (on :5673 AMQP, :15673
management UI):

```sh
mise run check:package ankusa_rabbitmq   # 8 tests
```

Covers: inline-payload publish + decode, fat-payload claim check-in (message
carries a claim reference and its sha256, the claim round-trips through `Ankusa.ClaimCheck.redeem/3`
against a real `BlobStore`), routing key as both a static string and a
function, and a fast-fail check (`:econnrefused`, not a hang) against an
unreachable broker.

## `ankusa_kafka`: `mix test`

Same pattern, against Redpanda on :19092:

```sh
mise run check:package ankusa_kafka      # 10 tests
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
needs a C toolchain and CMake ≥ 3.16; `.mise.toml` pins CMake, so
`mise install` covers the second.

## `ankusa_nats`: `mix test`

Same pattern, against NATS with JetStream enabled (on :4223 client, :8223
monitoring):

```sh
mise run check:package ankusa_nats       # 9 tests
```

`NATS_SERVERS` (default `localhost:4223`) points the suite at another server.
Each test creates its own stream with `Gnat.Jetstream.API.Stream.create/2`
(`subjects: ["ankusa.test.<n>.>"]`, memory storage) and deletes it in
`on_exit`, so nothing depends on a stream being pre-provisioned. Covers:

- an inline message: the subject it landed on, the five headers, the
  `Ankusa.Sink.Message` body, **and** `Gnat.Jetstream.API.Stream.info` reporting
  the message as stored, which is what proves the `:ok` came from JetStream's
  publish ack rather than from a successful socket write;
- a fat payload checked in through `ClaimCheck`, the message carrying a
  claim reference that redeems to the original bytes;
- `:subject` as a static string and as a 1-arity fun;
- a subject **no stream covers** being `{:error, :no_stream}`, with
  `Gnat.Jetstream.API.Stream.list` confirming no stream was created for it:
  the sink never creates one;
- a publish the stream itself refuses (`max_msg_size` exceeded) being an
  error, not a stored hook. JetStream answers that with `"seq": 0` *and* an
  `"error"` in the same ack, so this test is the one that pins the sink's
  error-first ack parsing;
- an unreachable server failing fast (`:econnrefused`, not a hang).

gnat is pure Elixir, so unlike `ankusa_kafka` this suite needs no C
toolchain.

## `ankusa_redis`: `mix test`

Same pattern, against Redis on :6399:

```sh
mise run check:package ankusa_redis      # 29 tests
```

`REDIS_URL` (default `redis://localhost:6399`) points the suite at another
server. Two instances in one VM share one namespace, which is how the suite
tests multi-node behaviour without a cluster: CRUD round-trips through
`Ankusa.Routes`, the cap is enforced against the shared hash, a write on one
node reaches the other over pub/sub **without** waiting for a tick, a raw write
with no broadcast is still picked up by the tick, a Redis error on a write is
`:store_unavailable` and leaves the local mirror untouched, and a boot against a
Redis that is not there (or against a corrupted definition) fails instead of
starting a node that would deny everything. The suite deletes only the keys under
its own namespace (`ankusa:routes:test`) between tests — never `FLUSHDB`, which
would wipe a Redis you happen to share with it.

`test/ankusa/sink/redis_test.exs` (6 tests) covers `Ankusa.Sink.Redis` against
the same server, subscribing with `Redix.PubSub` in the test process: an inline
message arriving on the channel with the `Ankusa.Sink.Message` body, a fat
payload checked in through `ClaimCheck` and the message carrying a redeemable
claim, `:channel` as a static string and as a 1-arity fun, a publish to a
channel with no subscriber being `{:error, :no_subscribers}`, `durable?` pinned
— including `Ankusa.Queue.validate_config!` refusing a
`wal: none` source whose only sink is this one — and an unreachable server
failing fast (`:econnrefused`, not a hang). Every publish in the suite goes to
a per-test unique channel and instance, so tests never see each other's
messages.

## SDK conformance: one harness across the SDKs

`packages/sdk-python`, `packages/sdk-typescript`, `packages/sdk-rust`,
`packages/sdk-ruby`, `packages/sdk-go`, `packages/sdk-php`,
`packages/sdk-elixir`, and `packages/sdk-java` are the eight client SDKs.
Rather than hand-write each one's edge-case tests, all eight are checked
against the same language-neutral vectors in `conformance/`: a feature manifest
(`features.json`), JSON cases (`cases/*.json`), and a native runner per SDK
(`packages/sdk-python/tests/test_conformance.py`,
`packages/sdk-typescript/src/conformance/conformance.test.ts`,
`packages/sdk-rust/tests/conformance.rs`,
`packages/sdk-ruby/test/conformance_test.rb`,
`packages/sdk-go/conformance_test.go`,
`packages/sdk-php/tests/Conformance/ConformanceTest.php` — the PHP runner
stands up PHP's built-in server on a free port to answer as the mock gateway —
`packages/sdk-elixir/test/conformance_test.exs`, whose runner hand-writes a
`:gen_tcp` gateway so it can abandon a delayed response mid-flight the way the
timeout vectors require, and
`packages/sdk-java/src/test/java/io/github/jamescarr/ankusa/conformance/ConformanceTest.java`,
a JUnit Jupiter `@TestFactory` that starts the JDK's own
`com.sun.net.httpserver.HttpServer` for the mock gateway).

```sh
mise run check:conformance    # validate conformance/, then run every SDK
```

The checker (`conformance/check.mjs`) fails if a `packages/sdk-*` directory is
unregistered, a feature has no cases, or a case names an unknown feature or
operation; then it runs each SDK's runner in that package's directory. Each
runner also runs inside its own `mise run check:package <pkg>`. Adding a
feature to `features.json` makes every SDK fail until it implements the
feature; the vector format and runner contract are in
`conformance/README.md`.

## Verifying the worked example

[`examples/rabbitmq-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/rabbitmq-consumer/) isn't a Mix
test suite. It's verified by actually running it:

```sh
cd examples/rabbitmq-consumer
docker compose up --build
curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' -d '{"id":"evt_1"}'
docker compose logs worker     # confirm the hook printed
docker compose down -v
```

[`examples/kafka-sqs-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/kafka-sqs-consumer/) is verified
the same way, plus its three failure drills (bridge down, worker down, poison
claim, each with its own `docker compose` commands and expected output in
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

Follow `ankusa_rabbitmq`/`ankusa_kafka`/`ankusa_nats`/`ankusa_redis`: a
`docker-compose.yml` for the real
dependency, and tests that
hit the real thing. A mock proves your code calls a mock correctly; it
proves nothing about whether a hand-rolled protocol implementation (SQL,
AMQP, SigV4, whatever) is actually right. Every adapter in this repo that
talks to external infrastructure is tested against a real instance of that
infrastructure, not a stand-in for it. That standard applies to new
adapters too.

## Core bench: `bench/core_bench.exs`

An in-process ingest → dispatch bench for core alone: no HTTP hop, no consumer,
no Kubernetes.

```sh
N=20000 CONCURRENCY=256 SINK_LATENCY_MS=5 mise run bench
```

It ingests `N` hooks through `Ankusa.Edge.Ingest` at `CONCURRENCY` workers, then
waits for a sink that sleeps `SINK_LATENCY_MS` per delivery to receive every
acked envelope. One JSON line plus a table; exit code is non-zero if anything
acked never arrived. It runs under `MIX_ENV=test` because core's
`config/config.exs` autostarts the default instance on :4000 outside `:test`.

The bench reports `ingest_per_s`, `end_to_end_per_s`, `drain_s` and `missing`,
and exits non-zero if anything acked never arrived. The numbers are
machine-dependent and move with the pipeline, so run it on your own hardware
rather than reading a figure here.

## Load and end-to-end (kind + Oban)

[`examples/oban-consumer/run.sh`](https://github.com/jamescarr/ankusa/blob/main/examples/oban-consumer/run.sh)
is the only test in this repo that proves zero loss on a real, multi-node
Kubernetes deployment rather than in-process. It stands up a `kind` cluster
(a 3-pod `ankusa` StatefulSet, each pod a self-contained all-role node with
its own store on a persistent volume, and a 2-replica `consumer` running Oban),
then drives [`tools/loadgen`](https://github.com/jamescarr/ankusa/blob/main/tools/loadgen)
through three phases against it:

1. **steady**: a paced `RATE` req/s for `DURATION` seconds.
2. **chaos**: the same load, with `kubectl delete pod` against `ankusa-0`,
   `ankusa-1`, and one `consumer` pod at +10s/+20s/+30s.
3. **burst**: closed-loop at `CONCURRENCY` workers, no rate cap, for
   `BURST_SECONDS`; the drain time it reports is how long the pipeline needed
   after ingest stopped, so it is the dispatch throughput ceiling referenced in
   [`deployment.md`](deployment.md#dispatch-throughput).

`mix loadgen.verify` polls `processed_webhooks` (the consumer's ground truth,
written by the idempotent `WebhookWorker.perform/1` upsert) until every acked
id shows up or its timeout expires, and fails the run on any `missing > 0` or
`sha_mismatches > 0`.

Run it yourself: `mise run e2e` (needs `docker`; `kind`, `kubectl`, and
Elixir come from `.mise.toml`). `RATE` defaults to 300; `KEEP=1` keeps the
cluster.

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
three phases: steady 284.0/s, chaos 284.0/s, burst 1360.2/s, `shed: 0`, and
`drain_s` 0.05–0.07 s.

`drain_s` in the tenths of a second with `shed: 0` at both rates is what makes
`RATE=300` the default: dispatch keeps up with ingest in real time, so the
paced phases never build a backlog for the burst phase to inherit.

### The chaos-phase loss: root cause and fix

> **Historical note.** This bug lived in the WAL.Postgres adapter, which has
> since been removed, along with the `ankusa_postgres` package; Ankusa's queue
> now commits to the RocksDB store, not to a log adapter. The write-up is kept
> because the failure mode (a reader
> losing a commit that landed out of `seq` order) is a real hazard for any log
> adapter, and because the chaos phase below is still the proof that an
> all-store node loses nothing under pod kills.

Killing an `ankusa-worker` pod used to drop ~0.5–1.5% of acked hooks
permanently. The mechanism was not in the dispatch pipeline: WAL.Postgres
allocated `seq` at INSERT time (`BIGSERIAL`) but a row only became visible at
COMMIT, so two writers could allocate 100 and 101 and commit in the opposite
order. A reader following the log with `seq > cursor` read 101, advanced its
cursor past it, and never saw 100 when it landed, and the compactor, whose
truncation is bounded by that same cursor, then deleted the row. The hook was
in `ankusa_wal` no longer, in no `oban_jobs` row, no `processed_webhooks` row,
no DLQ, exactly the observed signature, with the cursor already advanced. It
took killing the *worker* to reproduce because that bursts the catch-up load
onto the shared Postgres and widens the allocation→commit window.

WAL.Postgres.append/2 took a per-instance advisory lock
(`pg_advisory_xact_lock`) before allocating seqs and held it until COMMIT, so
seq order *was* commit order; fleet-wide serialized commits per instance was the
accepted cost. The regression test (`ankusa_postgres`) widened the window with a
sleeping statement trigger and, on the pre-fix code, failed with ~34 seqs a
cursor-following reader never saw. The chaos phase reported `missing: 0`.

Pre-fix code, same machine, same harness: `RATE=60`, `POOL_SIZE=30`, and the
fixed loadgen, so this isolates the core change:

| Phase | accepted/s | p50 | p95 | p99 | missing | `drain_s` |
| --- | --- | --- | --- | --- | --- | --- |
| steady | 56.3 | 11.1 | 13.5 | 15.6 | 0 | 0.04 |
| chaos | 56.3 | 11.2 | 15.4 | 18.7 | 0 | 0.04 |
| burst (64 workers) | 3168.1 | 18.0 | 31.7 | 40.7 | 0 | **320.6** |

The paced phases were fine: 60/s is far below the ~150/s the old
one-envelope-at-a-time dispatch could sustain. The burst phase is where the
ceiling shows: 47,632 accepted envelopes took **320.6 seconds** to drain after
ingest stopped (≈149/s), against 0.05–0.09 s now.

That run's chaos phase came back clean, which is honest but not reassuring:
the old loss was a race, and it needs the allocation→commit window to be wide
enough at the exact moment a cursor reader passes the tail. The earlier
documented run of the same harness did lose 12 of 1581 acked hooks in chaos
(with a 301.7 s verify timeout), and the `ankusa_postgres` regression test
reproduced the mechanism deterministically rather than by luck.
