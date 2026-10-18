# Claims register

Every claim, number, and caveat in `marketing/` traces to a row in this file.
If a sentence in a blog post, forum post, or Reddit post is not covered here,
either add a row (with a source you checked) or cut the sentence.

- Checked against the repo at `HEAD` on 2026-10-08. Line numbers are the
  current ones; the launch plan's line numbers had drifted, so they were
  re-derived, not copied.
- Source paths are relative to the repo root.
- "Exact wording to use" is the sentence shape the copy may use. Rephrase for
  voice, but do not make the claim stronger.

## Machine caveat (attach to every number in §Measured)

> Measured on an Apple M4 Pro laptop under kind/OrbStack, not a production-scale claim.

Source: docs/testing.md:468-469 ("This is the same machine every other number
in this doc was measured on, not a production-scale claim.").

## Required caveats

|Caveat|Exact wording to use|Source|
|---|---|---|
|At-least-once|"Delivery is at-least-once; consumers are idempotent receivers. Dedupe on `x-ankusa-idempotency-key`."|docs/delivery.md:163-170; docs/architecture.md:17-18|
|0.x|"Everything is 0.x and APIs may change."|packages/ankusa/CHANGELOG.md (versions 0.x); §Versions below|
|Durability scope|"Durable to power loss on this host only." Fleet durability comes from running N independent nodes, each with its own store.|docs/storage.md:69-71; docs/architecture.md:28-31; README.md:127-130|
|Unacked crash window|The one loss window this design cannot close is a provider that does not retry on a timeout or `5xx`.|docs/architecture.md:24-26|
|Zero-loss qualifier|Only "no acked hook is lost" or "zero loss once acked". Never bare "zero-loss".|docs/storage.md:70-71; docs/testing.md:467-468|
|Unauthenticated control ports|Ports 4001 (claim-check gateway), 4002 (admin API, `/metrics`) and 4003 (route management) have no authentication by design. Since the loopback change they listen on `127.0.0.1` by default (`claim_check.ip`, `admin.ip`, `routes.admin.ip`; env `ANKUSA_CLAIM_CHECK_IP`, `ANKUSA_ADMIN_IP`); inside a container set `0.0.0.0` so a published port reaches them, publish them on the host's `127.0.0.1`, and front anything exposed beyond that with your own proxy or network policy. Ingest (4000) listens on every interface.|docs/deployment.md:68; docs/configuration.md:58, 63-64, 157-158, 275-289, 396-400; docs/claim-check.md:71-75, 88-94; docs/quickstart.md:121-123; packages/ankusa/CHANGELOG.md:190-195|

## Guarantees (safe to state)

|Claim|Exact wording to use|Source|
|---|---|---|
|Never `2xx` before durable accept|"Ankusa never answers `2xx` until the hook is durably accepted." Which system accepts it is one config key: `wal.type: disk` (default, the node's RocksDB store) or `wal.type: none` (the sinks confirm).|docs/architecture.md:3-11; README.md:82-90|
|`201` is the only committed response|"`201 accepted` is the only committed response; there is no `200`."|docs/architecture.md:173; docs/quickstart.md:173-177|
|Crash before / after accept|Crash before the accept: no `2xx` was sent, the provider retries, nothing was promised. Crash after the accept but before the response leaves: the provider retries anyway, and that retry is a new hook with a new `id`.|docs/architecture.md:13-18|
|Default: no ingest dedupe|"By default ingest does no deduplication: a provider retry is a new hook with a new `id`." A source can opt in to collapsing provider retries with `dedupe:` (presets `github`, `standard_webhooks`, `svix`, `shopify`, `stripe`, or a header/JSON path), scoped to tenant and source, 72 h default TTL; a duplicate gets the same `201` and the original `id` with `"duplicate": true`.|docs/architecture.md:16-17; docs/quickstart.md:40-53; docs/configuration.md:87; packages/ankusa/lib/ankusa/dedupe.ex:1-50|
|Batcher|One `GenServer` per partition (default 2). It blocks the caller until the batch it lands in commits; the flush runs in a `Task`, so the batcher keeps accepting during a commit. `max_batch` 256, `max_delay_ms` 0 (no linger), `max_queue` 10,000 per partition (buffered and in-flight) — full means `503` with `Retry-After`.|docs/architecture.md:125-134; docs/configuration.md:59, 259-262|
|Record deadline|A record still buffered behind a stalled commit after its deadline (15 s by default) is answered `503` and dropped.|docs/architecture.md:172|
|One synced commit|One synced RocksDB batch per commit holds: the hook keyed by seq, one pending delivery row and one due key per sink, an archive obligation (only while the `:storage` role runs), and the next-seq marker. The batch is atomic. "Hundreds of hooks, one fsync." `kill -9` loses no acked hook.|docs/storage.md:48-71|
|Boot log line|`[ankusa] store at <path>. Durable to power loss on THIS host only.`|docs/storage.md:69-71|
|Store down / full disk|A commit that fails is `503 store_unavailable`, nothing acked. On a full disk writers resume by themselves once space frees, with no restart. Any failed store write (ingest, dispatch, the archive, the quarantine pen, source and rate-limit edits) also tells the store to close and reopen itself, at most once every 5 s, which clears a latched RocksDB write error if one outlives the freed space. (The changelog calls this a safety net, not a measured fix: RocksDB recovered from a full disk on its own in a container drill.)|docs/storage.md:82-87; docs/deployment.md:77-82; packages/ankusa/CHANGELOG.md:136-137, 279-285|
|Torn tail|A torn trailing write (the last, never-acked one) is dropped on open; damage before it refuses to open rather than being treated as empty.|docs/storage.md:73-81|
|Ingest status map|`201` accepted; `202` quarantined after a failed verification; `400` body unreadable, or a header holds a byte outside visible ASCII, space and tab (`invalid_header`, refused before the body is read; no sink could carry it); `401` verification failed; `404` unknown source; `413` body over `max_body_bytes`; `429` over a rate limit (nothing stored); `503` no durable destination right now (retry later).|docs/quickstart.md:156-171; docs/architecture.md:138-143|
|Request path modules|`Ankusa.Edge.Router` -> `Ankusa.RouteResolver` -> `Ankusa.Edge.Ingest` (verify; `Ankusa.Verifier`) -> rate limit -> `Ankusa.Edge.Batcher` -> `Ankusa.Queue` (`Ankusa.Queue.Writer` assigns seq) -> `201`. Router refuses a too-large `Content-Length` (`413`), an unknown source (`404`) and an invalid header byte (`400`) before reading the body.|docs/architecture.md:96-148; docs/storage.md:50-51|
|Rate limit placement|The tenant rate limit is charged after verification, so a flood of forged requests spends no budget. Over the limit is `429` with `Retry-After`, nothing stored.|docs/architecture.md:119-124|
|Dispatch is decoupled|A commit writes the hook and one delivery row per sink; dispatch is a scheduler over due rows (`Ankusa.Dispatch.Pipeline`) and the compactor (`Ankusa.Storage.Compactor`) works over archive obligations. Neither is an RPC caller of the other. Take the compactor or dispatch down and ingest keeps acking, hooks wait in the store.|docs/architecture.md:62-74, 150-165|
|Dispatch outcome writes|Dispatch writes its outcomes without a per-write fsync, so a power failure right after one can undo it and the hook is retried again ("at-least-once, never lost").|docs/delivery.md:504-508|
|At-least-once, not ordered|Dispatch is at-least-once to every sink, with exponential backoff and jitter, dead-letter on give-up, not ordered.|docs/architecture.md:176|
|Default retry budget|Defaults: `base_ms` 100, `max_ms` 300,000 (5 minutes), `max_attempts` 84, jitter on. "100 ms doubling to the 5-minute cap by attempt 13, then 5 minutes apart until attempt 84: about 6 hours, 3–6 h with jitter." Single retry policy for all sources today.|docs/delivery.md:481-497; docs/configuration.md:60|
|Quickstart drill retry config|The quickstart `ankusa.yml` caps retries at 6 attempts (roughly 8–15 s with jitter) so the dead-letter drill takes seconds; the default is 84 attempts backing off to 5 minutes (about 6 hours).|docs/quickstart.md:83-84; examples/quickstart/ankusa.yml:14-20|
|Attempt timeout|An attempt not returned after `dispatch.attempt_timeout_ms` (default 30 s) is killed and counts as failed.|docs/delivery.md:42-46|
|Direct mode (`wal.type: none`)|No store for hooks, no batcher, no dispatch, no compactor, no DLQ. The request publishes to the source's sinks (concurrently, under one deadline, default 8 s via the publish timeout) and answers `201` only after every sink confirms; the first refusal is `503` with `Retry-After`; the provider is the retry. No replay in this mode. Boot refuses a config where no sink of a static source is durable. If a source has a non-durable sink such as Redis pub/sub, every ingest is `503` while it has no subscribers.|docs/architecture.md:76-94; docs/delivery.md:67-100; packages/ankusa/lib/ankusa/edge/publish.ex:1-40|
|Replay jobs|`POST /v1/replays` with `kind: dlq`, `archive`, or `quarantine`. A job is durable (cursor commits with the rows it moves, restart resumes it), rate-limited (`rate` default 1,000, max 100,000), and only runs while dispatch's oldest-due lag is at most `max_lag_ms` (2 s default). `GET /v1/replays`, `GET|PATCH /v1/replays/{id}`; `PATCH {"state":"cancelled"}` stops one. A replayed delivery keeps the original `id` and idempotency key and carries the job's `replay_id`.|docs/delivery.md:48-65, 510-544; docs/quickstart.md:86-107|
|DLQ|The DLQ is the set of dead delivery rows, listed by `GET /v1/dlq` on the admin port. A dead row keeps its hook until replayed and delivered.|docs/delivery.md:499-508|
|Quarantine|A source with `on_verify_failure: quarantine` answers a failed verification `202`, holds the hook in a durable pen bounded by a per-source token bucket (burst 100, 20 per s; over it `429 quarantine_rate_limited`) and a byte cap (1 GiB; full is `503 quarantine_full`, never evicts). A `quarantine` replay job re-verifies against the source's current verifier and commits the ones that pass with their original `id`. `secret` takes a list for rotation windows.|docs/delivery.md:546-612; docs/architecture.md:177|
|Idempotency key|Computed once per hook: `tenant:source_id:dedupe_key` when the source extracted a provider event key (tenant is `default` when absent), else the hook's `id`. Header `x-ankusa-idempotency-key` on `Sink.Http`; `ankusa_idempotency_key` header on RabbitMQ, Kafka, NATS; `idempotency_key` field in the queue message. Read the key, do not rebuild it.|docs/delivery.md:163-170, 254-267|
|`x-ankusa-id` vs key|`x-ankusa-id` identifies one stored hook; every redelivery of it carries the same `id`. A provider retry is a different stored hook with a different `id`.|docs/delivery.md:171-177|
|Named HMAC schemes|`github`, `shopify`, `slack`, `stripe`, `standard_webhooks`, plus any body-HMAC scheme described in config.|packages/ankusa/lib/ankusa/verifier/schemes.ex:16-60; README.md:113|
|Sinks|`Sink.Log` (default, logs only), `Sink.Http`, `Sink.RabbitMQ`, `Sink.Kafka`, `Sink.NATS` (JetStream), `Sink.Redis` (pub/sub). All but `Log` and `Redis` are durable sinks (a `wal.type: none` ack can rest on them).|docs/delivery.md:148-155; docs/storage.md:105-108; docs/configuration.md:135-139|
|Queue message shape|`v: 1`, `id`, `source_id`, `tenant_id`, `received_at`, `content_type`, `size`, `dedupe_key`, `replay_id`, `idempotency_key`, `headers`, `sha256`, and either `body_base64` or `claim`. Consumers ignore keys they do not know.|docs/delivery.md:228-252|
|Object stores|`BlobStore.LocalFS`, S3 (AWS, MinIO, Cloudflare R2), GCS, Azure Blob (SAS token or managed identity), OCI Object Storage.|docs/storage.md:174-179; README.md:118|
|Claim check|A body over a sink's `inline_max_bytes` (64 KiB default) is written to the object store and the message carries a reference: `urn:ankusa:claim:v1:<tenant>:<claim_id>` plus `sha256`. Redeem with `GET /v1/claims/{tenant}/{claim_id}` on port 4001 (`200` returns exactly the claim's bytes, cacheable forever). The gateway is read-only, does not check integrity (the holder of the message compares sha256 and treats a mismatch as permanent), and holds no auth. Errors: `400` (not retryable, a bug), `404` (not retryable, dead-letter), `503` `store_unavailable` (retry).|docs/claim-check.md:3-8, 25-53, 55-129|
|Claim packs|Claims are packed: dispatch claims up to `dispatch.batch` (128) delivery rows per store scan and checks each batch's claims in per tenant as one object; a group over `claim_check.pack_max_bytes` (16 MiB default) splits; a bigger body gets its own object. One claim write per envelope, shared by every sink and every retry. Packs never mix tenants.|docs/claim-check.md:355-381; docs/configuration.md:60, 63, 276|
|Claim-check pricing sentence (vendor list price)|"S3 Standard and GCS regional Standard both charge around $0.005 per 1,000 writes (vendor list price; check current pricing)."|docs/claim-check.md:357-359|
|Claim-check limits|No presigned URLs (every redeemed byte passes through the gateway); no streaming or multipart for bodies over `max_body_bytes`; no write API (dispatch nodes are the only writers).|docs/claim-check.md:469-474|
|Claim-check retention|"Retention has to outlast your slowest consumer." A claim that expires before it is redeemed is the one way a claim check loses data: the worker gets `404`. On S3 or GCS Ankusa does not expire claims; add a lifecycle rule on the `claims/` prefix.|docs/claim-check.md:423-432|
|Claim-check single-node today|Claims are written through the same blob store as segments, and segments need one bucket per node, so the gateway can redeem only the claims in the bucket it reads: a claim-check topology with several ingest nodes and one gateway does not work today. (A gateway on its own node is fine: it is stateless and reads only the blob store.)|docs/critical-review.md:961-968; README.md:131-135; docs/deployment.md:25-30|
|Multi-tenancy resolvers|`Ankusa.RouteResolver.Path` (default, `POST /webhooks/:source_id`) and `Ankusa.RouteResolver.TenantPath` (`POST /webhooks/:tenant_id/:source_id`, tenant in the URL is authoritative). Custom resolvers implement one callback, `resolve/3`; an unresolvable URL is `404`, same as an unknown source.|docs/multi-tenancy.md:25-95|
|Tenant scoping|Resolution order: `route.tenant_id`, else `source.tenant_id`, else `"default"`. `tenant_id` travels with every delivery. Claim packs are tenant-prefixed, so a tenant's claims are deletable by prefix.|docs/multi-tenancy.md:97-117|
|Dynamic sources|`SourceStore.Static` (default, read-only; the admin API's write routes answer `409 source_store_read_only` against it) or `SourceStore.Persistent` (`source_store.type: persistent`), which adds `GET|POST /v1/tenants/{tenant}/sources` and `GET|PUT|DELETE /v1/tenants/{tenant}/sources/{name}` on the admin port. Sources live in this node's store: a created source works immediately and survives a restart, but it is node-local, so a fleet wants one node serving the source API or an external store behind the `Ankusa.SourceStore` behaviour.|docs/multi-tenancy.md:119-142|
|Route management|Opt-in (`routes.enabled`, default `false`). Route store `ets` (default, memory-only, seeded from `routes.seed` on every boot) or `redis` (the `ankusa_redis` package; every node with the same `namespace` enforces the same routes). Management API on port 4003 (`routes.admin.ip`, loopback by default): `/admin/routes`, `/admin/ip-rules`. Unauthenticated by design.|docs/configuration.md:64, 289, 369-400; implement-route-management.md:9-34|
|Lifecycle events|Off by default (`lifecycle.sinks`). CloudEvents 1.0 structured events `io.ankusa.source.{created,updated,deleted}` and `io.ankusa.route.{created,updated,deleted}`. Best effort: the publisher's queue caps at 10,000 pending sink deliveries, events dropped when retries run out or the queue is full are counted in `ankusa_lifecycle_dropped_total`, pending events are lost when the node stops, and there is no ordering. Only the node that served the change emits.|docs/asyncapi.md:58-120|
|AsyncAPI document|With `admin.enabled: true`, `GET /asyncapi.json` on port 4002 returns an AsyncAPI 3.0 document built from the configuration as it is now, carrying no credentials.|docs/asyncapi.md:9-32|
|Roles|`edge`, `dispatch`, `storage` (the default set) and `claim_check` (opt-in), chosen with `ANKUSA_ROLES`. With `wal.type: disk` every role that reads or writes hooks must run in one BEAM node (the RocksDB store is node-local); `dispatch` and `storage` are singletons per node; scale out by adding whole nodes. `claim_check` can run on its own node.|docs/deployment.md:8-58|
|Ports|4000 ingest (every interface), 4001 claim-check gateway, 4002 admin API + `/metrics`, 4003 route management; 4001/4002/4003 listen on `127.0.0.1` by default.|docs/deployment.md:62-68; docs/configuration.md:58, 63-64|
|Fleet|N independent all-role nodes behind a load balancer, each with its own data volume, store, DLQ and admin API. Give each node its own bucket (or LocalFS) for segments.|README.md:125-135|
|Image|`jamescarr/ankusa` is multi-arch, linux/amd64 and linux/arm64, and ships a healthcheck on `:4002/health` that runs inside the container. An orchestrator probe from outside the container cannot reach `:4002` unless `admin.ip` is routable; probe `:4000/health` instead.|.github/workflows/docker.yml:116-119; docs/deployment.md:102-108|
|Embeddable|Ankusa is a Hex library (`{:ankusa, "~> 0.4"}`); `config :ankusa, autostart: true`; `Ankusa.Instance.start_link/1` for manual supervision; `Ankusa.Sink`, `Ankusa.Verifier`, `Ankusa.RouteResolver` are behaviours. Building from source needs cmake >= 3.12, a C++20 compiler, and zstd + OpenSSL headers for the RocksDB NIF (the Docker image already has them).|docs/elixir.md:8-36, 143-181; docs/deployment.md:87-92|
|Failure domains|The instance supervisor is `:rest_for_one`. The core (store, source store, edge) restarts what depends on it. Other domains (dispatch, storage, lifecycle, metrics, admin/route-admin/claim-check listeners) run under their own restart budget and back off from 1 s to 60 s rather than taking the instance down; hooks wait in the store.|docs/architecture.md:264-284|
|Oban handoff|The HTTP sink delivers to an endpoint that keys on `x-ankusa-idempotency-key` (the primary key of `processed_webhooks`) and inserts the Oban job in the same transaction; a redelivery bumps a counter and enqueues nothing. An embedding app can skip the HTTP hop with an in-process `Ankusa.Sink` (`MyApp.ObanSink`).|docs/integrations.md:80-239|

## SDK claims

|Claim|Exact wording to use|Source|
|---|---|---|
|Eight SDKs|TypeScript (npm `ankusa`), Python (PyPI `ankusa`), Rust (crates.io `ankusa`), Ruby (RubyGems `ankusa-sdk`), Go (module `github.com/jamescarr/ankusa/packages/sdk-go`), PHP (Packagist `jamescarr/ankusa`), Elixir (Hex `ankusa_sdk`), Java (Maven Central `io.github.jamescarr:ankusa-sdk`, **not yet released**).|conformance/sdks.json; §Versions below|
|One conformance suite|131 shared conformance cases across 7 files (`admin` 15, `claim_ref` 21, `health` 7, `message` 31, `redeem` 25, `routes` 25, `webhook` 7), 22 feature ids, 25 operations. Counted with `jq` over `conformance/cases/*.json` (`.cases|length`); `features.json` has 22 `features` and 25 `operations`. (The launch plan said 125; the repo says 131 today.)|conformance/cases/*.json; conformance/features.json|
|Runner rules|Each SDK's runner loads every case, never skips, fails on an unknown operation, imports only from the package's public entry point, and maps errors by exact exported class name. `mise run check:conformance` is the gate; CI runs it as the `sdk conformance` job.|conformance/README.md:142-151; .github/workflows/ci.yml:64-73|
|What an SDK does|Parses `x-ankusa-*` headers; decodes the v1 queue message (with size and sha256 integrity checks); reads the idempotency key; redeems a claim with the sha256 check; classifies errors as retryable or not; drives the admin and routes APIs.|conformance/README.md:52-125; docs/claim-check.md:145-353|
|What an SDK does not do|SDKs do not verify provider signatures (Ankusa does that at the edge) and ship no broker client: bring your own.|packages/sdk-elixir/README.md:11-14; conformance/features.json|
|Framework piece|Only `ankusa_sdk` (Elixir) ships a framework piece: `Ankusa.SDK.Receiver`, a `Plug` that hands `Ankusa.Sink.Http` deliveries to your handler module.|packages/sdk-elixir/README.md:7-10|
|Language hooks|TypeScript: generated from `packages/ankusa/priv/openapi/claim_check.v1.yaml`, Node 20+ (ESM only), `parseHeaders` takes `req.headers`. Python: 3.11+, `ClaimCheckClient` owns an `httpx.Client` and is a context manager. Rust: async Tokio, `ClientBuilder::with_transport` for an injected `Transport`, MSRV 1.85 (Cargo.toml `rust-version`). Ruby: stdlib only (`ankusa-sdk.gemspec` declares no runtime dependency). Go: stdlib only (`go.mod` has no `require`), `errors.As(err, &apiErr) && !apiErr.Retryable()`. PHP: PSR-18 (Guzzle by default, any PSR-18 client injectable), PHP 8.3+, Packagist via the `jamescarr/ankusa-php` split mirror. Java: 17+, `java.net.http.HttpClient`, Jackson 3 (`tools.jackson.core:jackson-databind`).|packages/sdk-*/README.md; packages/sdk-rust/Cargo.toml:8; packages/sdk-go/go.mod; packages/sdk-java/README.md:11-12; packages/sdk-php/README.md:11; docs/claim-check.md:145-353|

## Measured numbers (the only figures allowed)

All of these carry the machine caveat above. Source for the whole section:
docs/testing.md:424-478. Harness: `examples/oban-consumer/run.sh` drives
`tools/loadgen` against a kind cluster (3-pod `ankusa` StatefulSet, each pod an
all-role node with its own store on a persistent volume, plus a 2-replica Oban
`consumer`); `RATE` defaults to 300; `KEEP=1` keeps the cluster. Phases:
**steady** (paced `RATE` req/s), **chaos** (same load, with `kubectl delete pod`
against `ankusa-0`, `ankusa-1`, and one `consumer` pod at +10s/+20s/+30s),
**burst** (closed-loop, 64 workers, no rate cap). Proof: `mix loadgen.verify`
polls `processed_webhooks` until every acked id shows up and fails the run on
any `missing > 0` or `sha_mismatches > 0` (docs/testing.md:442-445). The load
generator counts `201` as the only success, `503` as shed (backpressure), and
anything else, including a `200`, a transport error or a timeout, as an error
(tools/loadgen/README.md:38-42).

Verbatim from docs/testing.md:455-462:

| Machine | `RATE` | Phase | accepted/s | p50 | p95 | p99 | shed | errors | missing | extra deliveries | `drain_s` |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Apple M4 Pro, macOS, OrbStack | 60 | steady | 56.0 | 5.3 | 9.6 | 13.5 | 0 | 0 | **0** | 0 | 0.04 |
| Apple M4 Pro, macOS, OrbStack | 60 | chaos | 56.1 | 5.3 | 9.5 | 14.3 | 0 | 0 | **0** | 0 | 0.04 |
| Apple M4 Pro, macOS, OrbStack | 60 | burst (64 workers) | 1496.5 | 39.0 | 69.7 | 89.7 | 0 | 0 | **0** | 0 | 0.05 |
| Apple M4 Pro, macOS, OrbStack | 300 | steady | 283.5 | 3.8 | 9.2 | 34.5 | 0 | 0 | **0** | 0 | 0.08 |
| Apple M4 Pro, macOS, OrbStack | 300 | chaos | 283.5 | 4.1 | 48.0 | 165.0 | 0 | 0 | **0** | 0 | 0.09 |
| Apple M4 Pro, macOS, OrbStack | 300 | burst (64 workers) | 1236.5 | 43.0 | 88.7 | 282.8 | 0 | 0 | **0** | 0 | 0.09 |

Verbatim from docs/testing.md:464-469: "(Latencies in ms. `drain_s` is the
time from the end of ingest to the last ack being visible in the consumer,
measured on the verify poll; the burst generator is closed-loop, so its
`accepted/s` is what 64 workers and a 40 ms round trip sustain, not a dispatch
ceiling.) Every phase, including chaos, reports `missing: 0` and
`sha_mismatches: 0`."

Verbatim from docs/testing.md:471-474: "Re-verified after rebasing onto `main`
(NATS JetStream adapter, HMAC verifier engine): `RATE=300` again reported
`missing: 0` and `sha_mismatches: 0` in all three phases: steady 284.0/s,
chaos 284.0/s, burst 1360.2/s, `shed: 0`, and `drain_s` 0.05–0.07 s."

Use for the sentence form "Every phase, including chaos, reported `missing: 0`
and `sha_mismatches: 0` (measured on an Apple M4 Pro laptop under
kind/OrbStack, not a production-scale claim)."

There is no ingest-ceiling claim. For your own hardware: `N=20000 CONCURRENCY=256 SINK_LATENCY_MS=5 mise run bench`
reports `ingest_per_s`, `end_to_end_per_s`, `drain_s` and `missing`, and exits
non-zero if anything acked never arrived (docs/testing.md:404-422).

### Historical comparison (Post 03 only)

Labelled as historical wherever used. Source: docs/testing.md:480-528.

- The loss lived in the `WAL.Postgres` adapter, since removed along with the
  `ankusa_postgres` package; the queue now commits to the RocksDB store
  (docs/testing.md:482-488).
- Killing a worker pod used to drop ~0.5–1.5% of acked hooks permanently. One
  documented run of the same harness lost 12 of 1,581 acked hooks in chaos
  (docs/testing.md:490, 525-528).
- Mechanism: `seq` allocated at INSERT (`BIGSERIAL`) but a row only visible at
  COMMIT, so writers could allocate 100 and 101 and commit in the opposite
  order; a reader with `seq > cursor` read 101, advanced its cursor, never saw
  100, and the compactor deleted the row. Fix: a per-instance advisory lock
  held until COMMIT so seq order was commit order (docs/testing.md:490-507).
- Pre-fix burst, `RATE=60`, 64 workers: 47,632 accepted envelopes took 320.6 s
  to drain after ingest stopped (about 149/s) against 0.05–0.09 s now; the
  cause was dispatch handling one envelope at a time, "the ~150/s the old
  one-envelope-at-a-time dispatch could sustain" (docs/testing.md:509-521).
  The pre-fix paced phases were fine (steady 56.3/s, chaos 56.3/s, `missing: 0`):
  "60/s is far below the ~150/s the old one-envelope-at-a-time dispatch could
  sustain" (docs/testing.md:518-519).

Pre-fix table, verbatim from docs/testing.md:512-516 (`RATE=60`,
`POOL_SIZE=30`, same machine and harness, the code before the core dispatch
change; historical, labelled as such):

| Phase | accepted/s | p50 | p95 | p99 | missing | `drain_s` |
| --- | --- | --- | --- | --- | --- | --- |
| steady | 56.3 | 11.1 | 13.5 | 15.6 | 0 | 0.04 |
| chaos | 56.3 | 11.2 | 15.4 | 18.7 | 0 | 0.04 |
| burst (64 workers) | 3168.1 | 18.0 | 31.7 | 40.7 | 0 | **320.6** |

That run's chaos phase came back clean, "which is honest but not reassuring":
the old loss was a race that needs the allocation-to-commit window to be wide
enough at the exact moment a cursor reader passes the tail, and the
`ankusa_postgres` regression test reproduced the mechanism deterministically
rather than by luck (docs/testing.md:523-528).

## Versions and registries

`mise run status`, run 2026-10-08:

|Package|Kind|Version|Published|
|---|---|---|---|
|`ankusa`, `ankusa_kafka`, `ankusa_nats`, `ankusa_rabbitmq`, `ankusa_redis`|hex|0.4.0|yes|
|`async_api_spex`|hex|0.1.0|yes|
|`ankusa_server` (image `jamescarr/ankusa`)|docker|0.4.0|yes|
|`sdk-elixir`|hex|0.3.0|yes|
|`sdk-typescript`|npm|0.3.0|yes|
|`sdk-python`|python|0.3.0|yes|
|`sdk-rust`|cargo|0.3.0|yes|
|`sdk-ruby`|ruby|0.3.0|yes|
|`sdk-go`|go|0.3.0|yes|
|`sdk-php`|php|0.3.0|yes|
|`sdk-java`|java|0.0.0|**no** (gated)|

Install lines:

|Language|Install|
|---|---|
|TypeScript|`npm install ankusa`|
|Python|`pip install ankusa` (or `uv add ankusa`)|
|Rust|`cargo add ankusa`|
|Ruby|`gem install ankusa-sdk`|
|Go|`go get github.com/jamescarr/ankusa/packages/sdk-go`|
|PHP|`composer require jamescarr/ankusa`|
|Elixir (SDK)|`{:ankusa_sdk, "~> 0.3"}`|
|Elixir (core)|`{:ankusa, "~> 0.4"}` (use the version on Hex at posting time)|
|Java (gated)|`io.github.jamescarr:ankusa-sdk`|
|Server|`docker run … jamescarr/ankusa:edge`|

Release gates (details in `marketing/README.md` §Launch gates):

- **Core.** Replay jobs (`POST /v1/replays`), the `x-ankusa-idempotency-key`
  header, ingest dedupe, the 6-hour retry budget, quarantine release, and
  failure domains are in core's `[Unreleased]`
  (packages/ankusa/CHANGELOG.md:12-296), not in the 0.4.0 tag. The
  `jamescarr/ankusa:edge` image tracks `main`, so the `docker run` blocks
  already have them.
- **SDKs.** `decode_message` (with the size/sha256/tenant integrity checks),
  the `idempotency_key` helpers, and the replay admin client are in each SDK's
  `[Unreleased]` changelog section (`packages/sdk-*/CHANGELOG.md`), not in the
  published 0.3.0 packages. Every SDK sentence in `marketing/` that mentions
  decoding queue messages, the idempotency-key helper, or replay clients is
  true only once the SDKs are released.
- **Java.** `sdk-java` is 0.0.0 and unpublished.

## Forbidden

Never write these. Each has a documented open gap or is outside what was
measured.

|Claim|Why|Source|
|---|---|---|
|"exactly-once"|Delivery is at-least-once by design; a killed delivery can still complete.|docs/architecture.md:17-18; docs/delivery.md:42-46|
|"zero-loss" without "once acked"|Loss before the accept is possible (nothing was promised); a non-retrying provider is the one loss window.|docs/architecture.md:13-26|
|"smart retries", "error classification", "honors `Retry-After` from your worker"|Nothing classifies delivery errors yet: a permanent `400`/`404`/`410` takes every attempt before it reaches the DLQ, and `Retry-After` from a sink is ignored. (Ankusa does send `Retry-After` on its own `503`/`429` to the provider; that is fine to state.)|docs/critical-review.md:538-542, 577, 818-819|
|"production-ready observability", "meaningful readiness probes"|Metrics are counters and histograms, not gauges; `/health` does not touch the store.|docs/critical-review.md:1005, 1012, 1019|
|Multi-node or "fleet" claim-check gateway|The documented multi-node claim-check topology cannot work with per-node buckets today; the gateway is single-node.|docs/critical-review.md:961-968|
|Per-sink circuit breakers / isolation|One pool of dispatch slots serves every sink; per-sink windows and breakers are open.|docs/critical-review.md:538-545|
|"secure admin API"|Ports 4001, 4002 and 4003 are unauthenticated by design: say so, say they listen on `127.0.0.1` by default, and say "front with your proxy" if published.|docs/quickstart.md:121-123; implement-route-management.md:27-31; docs/configuration.md:289, 396-400; docs/deployment.md:68|
|Archive retention|"Planned, not shipped" (the archive retention window).|docs/architecture.md:179-184|
|The historical "14–16k hooks/s" probe figure|Pre-RocksDB code; not re-measured.|docs/critical-review.md:17, 1055|
|Any throughput figure not in §Measured|Only the two tables, the re-verification sentence and the historical comparison are allowed.|docs/testing.md:422, 455-474|
|A node-global adapter-name claim ("no global process names")|Adapters register fixed node-global supervisors.|docs/critical-review.md:1049|
|"CI fails the build on dependency advisories"|Failing CI on Hex advisories is still open.|docs/critical-review.md:930-934|
|The ETS route store as durable|Memory-only; `routes.seed` plus an idempotent external apply, or the Redis store, is the durability story.|implement-route-management.md:32-34|
|`wal: none` as retried / replayable|No retry policy, no DLQ, no replay in direct mode.|docs/delivery.md:76-81|
|Bare "no deduplication"|Say "by default"; sources can opt in to `dedupe:`.|docs/configuration.md:87|

## Doc drift found while writing this register (do not copy these lines)

- docs/claim-check.md:155-156, 191-192, 249-250, 278-279 still say "before its
  first npm/PyPI/Packagist/RubyGems release, a path dep"; every SDK except Java
  is published. Posts quote the SDK READMEs (fixed in Step 0), not these
  comments.
- docs/architecture.md:145 and docs/delivery.md:96 say direct mode publishes
  to sinks "in declaration order"; the code and docs/delivery.md:70-73 say
  concurrently (packages/ankusa/lib/ankusa/edge/publish.ex:7, 36-40). Copy
  says "concurrently, under one deadline" or nothing.
- docs/architecture.md:251 lists three roles; docs/deployment.md:10 lists
  four (`claim_check` is opt-in). Copy uses four.
- docs/architecture.md:132-134 does not say `max_queue` is per partition;
  docs/configuration.md:262 does. Copy says per partition.
- The SDK conformance count in older commit messages ("94 vectors") and in the
  launch plan (125) is stale; 131 is what the repo has.
- The launch plan was written against `b70a656`. `origin/main` moved to
  `143aa54` (#65) while the copy was being drafted: 4001/4002/4003 now listen
  on `127.0.0.1` by default, so `README.md`'s `docker run` gained
  `-e ANKUSA_ADMIN_IP=0.0.0.0`, the claim-check gateway YAML gained `ip:`, the
  `400` status gained `invalid_header`, and the full-disk text changed. The
  branch was fast-forwarded to `143aa54`, every verbatim block was re-copied
  from the current files, and every citation above was remapped. The
  `jamescarr/ankusa:edge` image already runs the loopback default, so the
  plan's original `docker run` (without the flag) leaves `:4002` unreachable
  from the host.
- The plan's Step 3 diff command for Post 01
  (`sed -n '25,36p' README.md` against the awk-extracted block) would never be
  empty, because lines 25 and 36 are the code fences; the fence-less range is
  26-35, and since #65 the block is 26-35 with the new flag. The provenance
  check used here compares every blank-line-separated paragraph of every code
  block with the repo files instead.
