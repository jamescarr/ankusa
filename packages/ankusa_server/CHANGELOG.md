# Changelog

All notable changes to the Ankusa server image are documented here. This
project is versioned independently of the `ankusa` Hex packages: it is the
`jamescarr/ankusa` Docker image, tagged `ankusa_server-vX.Y.Z`.

## [Unreleased]

### Added

- **`dedupe.max_json_bytes` (PERF-1).** The biggest body a `json` dedupe source
  decodes (default 1048576, an integer >= 1); a larger body is stored with no
  dedupe key and never decoded. See the `ankusa` changelog.
- **`dedupe: salesforce` (CON-3).** A source's `dedupe` accepts the preset
  `salesforce` (`json: id`; see `configuration.md` for why it is a convention and
  not a Salesforce-documented delivery id). A source with no `dedupe` logs one
  boot warning and counts in `ankusa_sources_without_dedupe`. See the `ankusa`
  changelog.
- **SLI metrics (OPS-3).** `/metrics` gains `ankusa_delivery_latency_seconds{sink,result}`
  (a delivery's age when it settled, buckets to 60 s), `ankusa_dispatch_attempt_duration_seconds{sink,result}`,
  `ankusa_compact_failures_total{stage}`, `ankusa_claim_check_failures_total{op}` and
  `ankusa_backup_failures_total`. No new config key; `metrics.detail: source` also puts
  `tenant_id` and `source_id` on the delivery latency histogram. The SLO table is in
  [`architecture.md`](../../docs/architecture.md#slis-and-slos).
- `ordered` and `ordering_key` (`{header: name}` / `{json: path}`) on every sink
  but `log` (ordering lanes, ADR 0004).

- **Structured JSON logs (OPS-2).** `log.format: json` (`ANKUSA_LOG_FORMAT`;
  default `text`, the output as before) writes one JSON object per line:
  `time`, `level`, `msg`, `instance`, `hook_id`, `tenant_id`, `source_id`,
  `sink`, `trace_id` (the keys a line has) and any other log metadata, with the
  value of every key named `secret`, `token`, `password`, `authorization` or
  `cookie` replaced by `"[REDACTED]"` (`AnkusaServer.LogFormatter`). The ASCII
  banner is not printed under `json`. `log.level` is the level the node starts
  at; `PUT /v1/log-level` (scope `config:write` + `tenant:*`, audited) changes
  it while the node runs. `AnkusaServer.Config.load!/1` now answers
  `log: %{format, level}` where it answered `log_level`.

- **`sqs` and `google_pubsub` sinks batch (DEL-4).** Up to 10 (SQS
  `SendMessageBatch`) or 1 000 (Pub/Sub `publish`) deliveries to one queue or topic
  are one request. New sink keys, all optional: `batch` (`false` sends one request
  per delivery), `batch_linger_ms` (20), `batch_max_entries`, `batch_max_bytes`,
  `batch_queue_max` (4096), `batch_queue_max_bytes` (134217728),
  `batch_queue_tenant_max` (1024) and `batch_max_inflight` (default
  `dispatch.sink_concurrency`). New on `/metrics`:
  `ankusa_sink_batch_{requests,entries,bisections,refused}_total` and
  `ankusa_sink_batch_{batchers,queued,queued_bytes}`. A batching sink's attempts are
  no longer counted against `dispatch.sink_concurrency`, a tenant's `concurrency` or
  `dispatch.concurrency`. See the `ankusa` changelog and
  [`delivery.md`](../../docs/delivery.md#sinksqs-queue-delivery).
- **OpenTelemetry tracing (OPS-1).** `tracing: {enabled, otlp_endpoint,
  sample_ratio}` (off by default; `http://localhost:4318`; `1.0`) starts the
  OpenTelemetry SDK and the OTLP/HTTP exporter before the instance, with a
  parent-based sampler over `sample_ratio`. The endpoint may not carry
  `user:password`; collector credentials are the standard
  `OTEL_EXPORTER_OTLP_HEADERS`. With it off, only core's no-op API runs and a
  hook's `traceparent` still reaches the consumer unchanged. Not part of
  `/v1/config`: it configures the image, not an instance, and holds no secret.
- **Readiness keys (OPS-5).** `node.min_free_bytes` (unset): `/ready` answers `503`
  with `disk: failing` while `data_dir`'s file system has fewer bytes free.
  `health.sink_error_window_ms` (5000): under `wal.type: none`, a sink whose last
  publish failed within this long ago makes `/ready` `503` with `sinks: failing`.
  `queue.max_oldest_due_ms` (unset): past it `/ready` carries `backlog: failing`,
  a label that never makes the node unready. See `config-examples/reference.yml`.
  `/ready`'s body now lists every check with `ok`/`failing`/`draining` and
  `reasons` (see the `ankusa` changelog for the wire change).

- **Graceful drain on SIGTERM (OPS-4).** `docker stop` (and a pod deletion) now
  drains the node before it stops: `/ready` answers `503` with `"status":
  "draining"` at once, the ingest port keeps serving for `listener.drain_delay_ms`
  (5000, an integer >= 0), then closes after the requests it is serving, then
  every batcher is waited on, all within `listener.drain_timeout_ms` (30000, an
  integer >= 1; on a node with the `edge` role at least the ingest listener's
  wait, 22 s by default). The ingest listener waits 22 s (13 s under
  `wal.type: none`, 24 s under `dual`) for requests in flight instead of Thousand
  Island's 15 s, so a commit that starts late is no longer killed mid-fsync and
  no provider sees a reset for a committed hook. Set `terminationGracePeriodSeconds`
  (and `docker stop -t`) to at least `(drain_delay_ms + drain_timeout_ms) / 1000 +
  10`, plus `backup.final_timeout_ms` when the node backs up. See
  `config-examples/reference.yml` and `docs/deployment.md`.

## [0.7.0] - 2026-10-24

### Added

- **Forwarded-header caps and sink message limits (DEL-5).** A source takes
  `max_headers` (default 64) and `forward_headers_max_bytes` (default 16384):
  a forwarded header past either is dropped whole and counted in
  `ankusa_sink_headers_dropped_total{reason}`. `rabbitmq`, `nats` and `redis`
  sinks take `max_message_bytes` (defaults 134217728, 1048576, 536870912).
  A message over a sink's limit is checked in as a claim, or, when the body is
  over it by itself, dead-lettered at once as `message_too_large` with no
  broker call. Both source keys are positive integers; `reference.yml` and
  `configuration.md` document them.
- **`egress.pools` section (DEL-3).** `pool_size` (32), `pool_count` (1),
  `pool_timeout` (5000), `pool_max_idle_ms` (60000), `conn_max_idle_ms` (30000),
  `max_pools` (1024), `max_pools_per_tenant` (256) and `protocols` (`[http1,
  http2]`) size and bound the outbound HTTP connection pools every HTTP sink
  delivery goes through; core range-checks them when the config loads. See the
  `ankusa` changelog, [`configuration.md`](../../docs/configuration.md#outbound-connection-pools)
  and the `egress.pools` block of `config-examples/reference.yml`.
- **`dispatch.retry.max_elapsed_ms`, `dispatch.max_retry_after_ms` and a per-source
  `retry` block (DEL-2).** `max_elapsed_ms` (default `86400000`, 24 h) is the
  horizon after which a delivery still failing is dead-lettered, so
  `dispatch.retry.max_attempts` now defaults to `null` (no attempt limit; was 84,
  about 6 hours). `sources.<id>.retry` (and the admin API's source spec) takes
  `base_ms`, `max_ms`, `max_attempts`, `jitter`, `max_elapsed_ms` over the
  instance's. A source whose `retry.max_elapsed_ms` outlasts
  `claim_check.bucket_expiry_days` (S3/GCS claims) is refused: boot, or `422
  retry_exceeds_claim_expiry`. See `config-examples/reference.yml` and the `ankusa`
  changelog.

- **`claim_check.integrity_check` (`off` or `manifest`, default `manifest`).** The
  claim gateway checks every whole-claim `GET` against the sha256 its pack's
  manifest records and answers `502 integrity_mismatch` on a difference;
  `200`/`206` carry `x-ankusa-sha256`. New on `/metrics`:
  `ankusa_claim_sweeps_total{deleted}`, `ankusa_archive_failures_total` and
  `ankusa_archive_oldest_unarchived_age_seconds`. See the `ankusa` changelog and
  [`claim-check.md#integrity`](../../docs/claim-check.md#integrity).
- **`storage.max_segments_per_tick` (DUR-8).** The most segments one compactor tick
  writes (default 16); `storage.roll_ms` now bounds how long a partial segment waits.
  Segments are written under `archive/v1/dt=<day>/<node>/` (the node is
  `intake.node_id`, else the host name) with a header and a sha256, and the store is
  format 7. See [`storage.md`](../../docs/storage.md#ankusacodec-and-segment-format).

- **`s3` and `gcs` blocks take the object-store transport options (DUR-7).**
  `timeout_ms`, `upload_timeout_ms`, `op_deadline_ms`, `multipart_threshold_bytes`,
  `multipart_part_bytes`, `retries`, `breaker_failures`, `breaker_open_ms`,
  `breaker_slow_ms`, `pool_size`, `pool_count` and `pool_timeout`, under `storage`,
  `claim_check.store(s)` and `backup.store`/`secondary`, range-checked when the
  instance starts. See [`configuration.md`](../../docs/configuration.md).
- **`batcher.max_commits`, `group_commit_max_bytes`, `group_commit_max_records` and
  `group_commit_delay_ms` (DUR-4), and `batcher.partitions` defaults to 4.** YAML
  keys for the cross-partition group commit: the commits a partition keeps in
  flight (default 64), the most body bytes (default 4 194 304) and hooks (default
  1 024) the queue writer merges into one synced commit across the batcher
  partitions, and how long a saturated writer waits for the callers it just
  answered to join the next group (default 1 ms, `0` disables). See
  `docs/configuration.md`.

- **`wal.type: dual`, the `intake.*` keys and the `recover` command (DUR-1.b.b,
  [ADR 0002](https://github.com/jamescarr/ankusa/blob/main/docs/adr/0002-dual-commit-intake-log.md)).** `wal.type: dual` (or `ANKUSA_WAL_TYPE=dual`)
  acks a hook only after it is fsynced on the node **and** confirmed in a replicated
  Kafka topic, so a lost host loses nothing: `intake.type: kafka`, `intake.node_id`
  (also `ANKUSA_INTAKE_NODE_ID`: the node's stable identity, the same on a
  replacement), `intake.fleet`, `intake.topic`, `intake.kafka.{brokers,ssl,sasl}`
  (the SASL password is redacted in `print-config` and `GET /v1/config`),
  `intake.publish_timeout_ms`, `reconcile_deadline_ms`, `breaker_failures`,
  `probe_ms`, `retry_after_s`, `offset_flush_ms`, `pending.{max_records,max_bytes}`,
  `max_record_bytes`, `recover_on_boot` and `verify_topic`. Boot refuses `dual`
  without `intake.type`, `intake.node_id`, `backup.enabled` and `backup.max_age_ms`.
  `docker run … recover [--node ID] [--from backup|earliest|N] [--dry-run]
  [--allow-gap]` replays a stopped node's intake log into its store (a replacement
  node does the same at boot); `--allow-gap` writes an `intake.recover.allow_gap`
  audit row. See `docs/configuration.md` and `docs/deployment.md`.
- **`queue.rocksdb.*` (DUR-3).** YAML keys for the store's RocksDB settings:
  `block_cache_bytes`, `bloom_bits_per_key`, `max_open_files`, `compaction_style`,
  `rate_limiter_bytes_per_sec`, `max_background_jobs` and
  `write_buffer_bytes.<family>`. The defaults put a bloom filter on the `index` and
  `deliveries` families; see `docs/configuration.md` and
  `config-examples/reference.yml`.
- **`claim_check.redeem_grace_ms`, `sweep_max_objects` and `bucket_expiry_days`
  (DUR-2.c).** YAML keys for the claim leases: how long a claim pack stays redeemable
  after its hook is delivered (default 86 400 000 ms), how many packs and leases one
  sweeper tick deletes (default 10 000), and the age of the bucket's own lifecycle
  rule for S3/GCS claims (boot refuses one below the retry horizon + DLQ retention +
  grace). The image's store moves to **format 6** on first boot (a resumable
  migration leasing the claims of live hooks); an older image cannot open it
  afterwards. `soak.sh` also fails when `ankusa_dlq_rows` or `ankusa_claim_leases`
  is higher late in the run than early. See `docs/configuration.md`.

- **`queue.dlq`, `quarantine.retention_days`, `storage.catalogue_retention_days`,
  `archive.retention_days` and `audit.max_rows|max_bytes|outbox_max_backlog`
  (DUR-2.b).** YAML keys for the DLQ, quarantine, catalogue and audit retention and
  ceilings. Store format 5: an image that writes format 4 cannot open the store
  afterwards. See `docs/configuration.md`.

- **`dedupe.max_entries` and `dedupe.max_entries_per_tenant` (DUR-2.a).** The most
  live ingest-dedupe keys the node, and one tenant, holds (defaults 100 000 000 and
  30 000 000, integers ≥ 1); a hook whose key would pass either is `503
  dedupe_quota` with `Retry-After: 60`. The image's store moves to **format 4** on
  first boot (a resumable migration counting the existing dedupe keys); an older
  image cannot open it afterwards. See `docs/configuration.md`.
- **`backup.secondary`, `verify_interval_ms`, `signing_key`, `timeout_ms`,
  `final_timeout_ms`, `max_age_ms`, `require_fresh` (DUR-6).** A second backup
  store with restore fallback, a verify job, HMAC-SHA256-signed manifests (a list
  or comma-separated `${VAR}`, each at least 32 bytes), a final backup on SIGTERM,
  and a `/ready` `backup: stale` label (`503` with `require_fresh`). See
  `config-examples/reference.yml` and `docs/storage.md#backup-and-restore`.
- **`routes.memory_budget_bytes`, `routes.store.timeout_ms`/`load_timeout_ms` and
  `source_store.timeout_ms`/`load_timeout_ms` (TEN-9).** `routes.max_routes`
  defaults to 1 000 000 (was 10 000) behind `routes.memory_budget_bytes` (default
  8589934592), checked at boot against ~5 KiB per route. The two timeouts are
  Redis-only keys, like `tick_ms` (a config error with `type: ets`): `timeout_ms`
  bounds each Redis command and `load_timeout_ms` a whole load, which now reads
  the routes and sources a page at a time. See `config-examples/reference.yml`
  and `docs/configuration.md#route-management`.
- **`metrics.detail` and `metrics.max_series_per_metric` (TEN-7).** The scrape's
  cardinality policy: `detail` is `tenant` (default: `tenant_id` on the per-hook
  series), `source` (`tenant_id` and `source_id`) or `none` (neither, and no
  per-tenant gauges); `max_series_per_metric` (default `5000`) caps the label sets
  one metric keeps, later tenants or sources are folded into `"other"`. **Behaviour
  change:** `source_id` is no longer a label by default; set `metrics.detail:
  source` to get it back. See `config-examples/reference.yml` and
  `docs/configuration.md#metric-cardinality`. `scripts/soak.sh` fails a run whose
  largest `/metrics` scrape exceeds `MAX_SERIES` (10 000) lines.
- **`rate_limits.scope`, `dedupe` and `fleet` (TEN-6).** `rate_limits.scope: fleet`
  and `dedupe: {scope: fleet}` share the tenant quota and the provider event key
  across edge nodes through `fleet.store` (`{type: redis, url, namespace,
  pool_size}`), with the call budget and breaker keys `timeout_ms`,
  `pending_ttl_ms`, `breaker_window_ms`, `breaker_min_calls`,
  `breaker_failure_rate` and `breaker_open_ms`. Core's ranges and the rule that a
  fleet scope needs `fleet.store` fail the boot with its message. See
  `config-examples/reference.yml` and `docs/configuration.md#fleet-scope`.
- **`dispatch.tenants` (TEN-3).** `default` and one entry per tenant id, each
  any of `max_inflight`, `max_inflight_bytes` and `concurrency`: a tenant's
  share of the global dispatch limits, so one tenant's slow destination fills its
  own window and nobody else's (`default` there is always the default entry). See
  the `ankusa` changelog,
  [`configuration.md`](../../docs/configuration.md) and
  [`delivery.md`](../../docs/delivery.md#fairness-between-tenants).

- **Admission keys under `rate_limits` (TEN-2).** `per_ip` and `node`
  (`{rate, burst}`, off by default), `sources.default` (`bytes_per_second`,
  optional `burst_bytes`; 100 MB/s unless changed, `default: null` is off), and
  on `default` and each `tenants` entry the optional `bytes_per_second`,
  `burst_bytes`, `max_body_bytes`, `max_queue` and `max_queue_bytes`. `per_ip`,
  `node` and `sources` are charged before the body is read (`429
  ip_rate_limited` / `node_rate_limited` / `source_rate_limited`); a tenant past
  its `max_queue` / `max_queue_bytes` share gets `429 tenant_overload`, never
  `503`, and past `max_body_bytes` `413`. See the `ankusa` changelog,
  [`configuration.md#rate-limits`](../../docs/configuration.md#rate-limits) and
  `config-examples/reference.yml`.
- **Tenant-aware routes (TEN-4).** A `routes.seed` entry takes `tenant_id` (the
  owning tenant, default `default`) and `source_id` (the one source it admits):
  a route owned by a tenant admits hooks only into that tenant, and a route bound
  to a source admits only that source (`404` otherwise). The route-management
  listener serves `/v1/tenants/<tenant>/routes`, which needs `tenant:<tenant>`
  rather than `tenant:*`, beside the all-tenants `/admin/routes`. See
  `docs/configuration.md` and `docs/multi-tenancy.md`.

- **`verify.type`: `standard_webhooks_asym`, `asymmetric`, `token`, `client_cert`
  (SEC-12).** A source can be verified by the provider's public key (Standard
  Webhooks `v1a` Ed25519 by preset, or any Ed25519/RSA scheme described inline:
  `public_key` one or a rotation list, `algorithm`, `signature_header`, `signed`,
  `timestamp_header`, ...), by a static credential (`secret` plus `location:
  bearer | basic | header | query`, with `header` or `param`), or by a pinned
  mutual-TLS client certificate (`fingerprints`, `subjects`; boot and
  `check-config` refuse it unless `listener.tls` has `verify: verify_peer`). A
  key, secret or pin core cannot use is a load error naming
  `sources.<id>.verify.<key>`; the admin API's source writes go through the same
  loader. `/v1/config` shows `location`, `fingerprints` and `subjects` and hides
  `secret` and `public_key`. See `docs/configuration.md`.

- **Signed, expiring claim refs (SEC-11).** `claim_check` takes
  `ref_signing_keys` (a list, or a comma-separated string for `${VAR}`; each
  at least 16 bytes, `[current, previous...]`), `ref_ttl_ms`,
  `accept_unsigned_refs` and `max_concurrent_reads` (1024). With keys, refs
  are v2 (`urn:ankusa:claim:v2:...`), the gateway refuses a forged one (`400
  bad_signature`) or an expired one (`410 ref_expired`) and, unless told
  otherwise, a v1 one (`400 unsigned_ref`). `claim_check.authorize` is
  Elixir config only. See `config-examples/reference.yml`.
- **Secret sources and rotation without a redeploy (SEC-9).** A secret no
  longer has to come from the environment. `${file:/abs/path}` reads a file
  (one trailing newline removed: a Kubernetes secret volume, a Docker secret);
  `secret_file: /abs/path` (or a list) is `secret:` read from a file, on a
  source's `verify` and `http` sinks and on `lifecycle` sinks;
  `${sm:<provider>:<ref>}` asks a secret manager named under the new
  `secrets.providers` (`type: vault` for HashiCorp Vault KV v2, `type: aws` for
  AWS Secrets Manager; `#<field>` picks a field of a JSON secret) with
  `secrets.cache_ttl_ms` (default 0). A reference that cannot be resolved at
  boot is a load error naming the key and the reference, never a value.
  `AnkusaServer.Secrets.reload/2` re-reads the file, resolves every reference
  again and republishes a static source store's sources, so a rotated secret
  verifies without a restart (`secret: [new, old]` stays the rotation window);
  a reference that fails on a reload keeps its last value and logs a warning.
  Nothing triggers a reload on a signal or over HTTP yet. The image now lists
  `aws_signature` as a direct dependency (the lock is unchanged). A spec an API
  client submits cannot use `secret_file` or `${file:}`. See
  `docs/configuration.md#secret-sources`.
- **Signed, attested image (SEC-8).** Every published image now carries an
  SPDX SBOM and a SLSA provenance attestation (`mode=max`) for `linux/amd64`
  and `linux/arm64`, and its index digest is signed keylessly with cosign by
  `docker.yml`; the workflow refuses to finish without the attestations and
  verifies the signature (`cosign verify`, in `scripts/smoke.sh`) against the
  digest it just published. Verify one yourself with the command in the image
  README ("Verify an image"). Both base images and the Dockerfile syntax
  frontend are pinned by digest, and every GitHub action by commit SHA
  (`mise run lint:pins` fails a bare tag).
- `verify.tolerance_seconds` accepts 0 to 3600; `3601` or a negative value is a
  config error naming `sources.<id>.verify.tolerance_seconds`. A `hash: sha1`
  source boots with a warning and counts on `ankusa_verifier_weak_hash_total`.
  See the `ankusa` changelog.
- `audit.retention_days` (365): how long the audit trail (`GET /v1/audit`, scope
  `audit:read`) keeps its rows. The store format is now 2: an image that has
  booted it cannot roll back to a format 1 image without restoring a backup.
  See the `ankusa` changelog and `docs/architecture.md`.

- `secrets: {kek, kek_previous}` (use `${VAR}`): the key-encryption key that
  source specs' secrets are encrypted under before an API-managed source is
  stored (`source_store.type: persistent` or `redis`), and the keys that may
  still decrypt during a rotation. A stored source no key opens refuses the
  boot; startup logs which key encrypts new rows; `print-config` and
  `GET /v1/config` show the keys as `[REDACTED]`. Rotate by listing the old
  key in `kek_previous`, then `Ankusa.SourceStore.Persistent.rewrap(:default)`
  (Redis: `Ankusa.SourceStore.Redis.rewrap(:default)`) from `remote`.
- `sse: {type: aes256 | kms, key_id}` on every `s3` and `gcs` block
  (`storage`, `claim_check.store(s)`, `backup.store`): server-side encryption
  sent on every object that store writes, `x-amz-server-side-encryption(-aws-kms-key-id)`
  for S3 and `x-goog-encryption-kms-key-name` for GCS. A misspelled option, or
  `kms` without the `key_id` GCS needs, fails the load naming the key. See the
  `ankusa` changelog and `docs/storage.md#encryption-at-rest`.
- `admin.auth`, `routes.admin.auth` and `claim_check.auth` (`mode`
  `none`/`token`/`jwt`/`mtls`, `allow_insecure`, `tokens` with `token:` (hashed
  at load) or `sha256:` and `scopes`, `jwt`, `mtls.subject_map`), and the env
  shortcuts `ANKUSA_ADMIN_TOKEN`, `ANKUSA_ROUTES_ADMIN_TOKEN` and
  `ANKUSA_CLAIM_CHECK_TOKEN` (one token per control plane). **Breaking:** the
  image refuses to boot with `ANKUSA_ADMIN_IP=0.0.0.0` (or an off-loopback
  `claim_check.ip` / `routes.admin.ip`) and no authentication: add
  `ANKUSA_ADMIN_TOKEN`, an `auth` block, or `auth: {mode: none, allow_insecure:
  true}` behind your own proxy. The shipped fanout configs take
  `CLAIM_CHECK_TOKEN`. See the `ankusa` changelog and `docs/configuration.md`.

- `http.store_headers` (`drop`, `hash`; default drops `authorization`,
  `proxy-authorization`, `cookie`, `set-cookie`, `x-api-key`, `x-auth-token`)
  and a per-source `store_headers` override: which request headers a hook is
  stored with, applied after the signature is verified. A dropped header is in
  no stored hook, archive segment, quarantine entry or sink message; a hashed
  one is kept as `sha256:<hex>`. **Behaviour change:** those credential headers
  are no longer stored by default; `drop: []` restores the old behaviour. See
  the `ankusa` changelog and `docs/configuration.md#stored-headers`.

- `listener` (`max_connections`, `num_acceptors`, `read_timeout_ms`,
  `body_read_timeout_ms`, `request_deadline_ms`, `max_requests_per_connection`,
  `max_header_count`, `max_header_length`, `http2`, `tls`), `metrics`
  (`enabled`, `port`, `ip`, `tls`; env `ANKUSA_METRICS_ENABLED`,
  `ANKUSA_METRICS_PORT`, `ANKUSA_METRICS_IP`) and a `tls` block
  (`certfile`, `keyfile`, `cacertfile`, `verify`, `versions`, `ciphers`) on
  `admin`, `routes.admin` and `claim_check`: TLS and mutual TLS on every
  listener, a metrics listener that serves only `/metrics`, `/health` and
  `/ready`, and hardened connection limits. The image's `HEALTHCHECK` falls
  back to the admin and then the metrics listener when the ingest listener
  terminates TLS. See the `ankusa` changelog and `docs/configuration.md`.
- `claim_check.stores`: a list of claim stores (each the `claim_check.store`
  shape), exclusive with `store`. A pack whose store fails is written to the
  next one, a store that fails a write or a read is skipped by writes and
  searched last by reads for `claim_check.store_cooldown_ms` (default 30000),
  and the gateway searches every store.
  `claim_check.placement: ordered | hash` (default `ordered`) picks a new
  pack's store. See the `ankusa` changelog.
- `claim_check.key_layout: flat | sharded` (default `flat`): `sharded` adds a
  `shard=<c>/` folder under the tenant, 32 key prefixes per tenant, so a busy
  tenant doesn't hit S3's per-prefix request limit. The gateway reads both
  layouts.
- `ankusa_blob_store_retries_total` and
  `ankusa_claim_check_store_breaker_transitions_total` on `/metrics`.
- `claim_check.read_deadline_ms` (5000), `read_timeout_ms` (3000),
  `read_retries` (1), `read_max_inflight` (48), `read_queue_max` (256),
  `read_cache_bytes` (134217728) and `read_cache_max_entry_bytes` (1048576):
  the gateway caches pack indexes and claims, coalesces concurrent reads,
  backs off a throttling store with an adaptive per-store limit, and answers
  every request within the deadline. Its `503 store_unavailable` carries a
  random `Retry-After` of 1–3 s. See the `ankusa` changelog.
- `ankusa_claim_gateway_requests_total{status}`,
  `ankusa_claim_gateway_duration_seconds`,
  `ankusa_claim_check_reads_total` and `ankusa_claim_check_read_limit`/
  `_read_inflight`/`_read_queued` on `/metrics`.
- **`egress` section** (`allow_insecure_http`, `deny_cidrs`, `allow_cidrs`,
  `allow_hosts`, `deny_hosts`) and the HTTP sink keys `cacertfile` and
  `allow_insecure_tls`: where `type: http` sinks (sources' and
  `lifecycle.sinks`) may connect, and how their TLS is verified. See the
  `ankusa` changelog and [`configuration.md#egress`](../../docs/configuration.md#egress).

### Changed

- **`dispatch.sink_concurrency` defaults to `8` (was unset), per tenant, and may
  exceed `dispatch.concurrency` (TEN-3).** `config-examples/reference.yml`
  shows both; `sink_concurrency: null` switches the cap off. One sink key now
  delivers at most 8 attempts at once (8 ÷ the destination's latency per
  second), so raise it for a high-volume source. A deployment whose hooks share
  one tenant is held to the per-tenant defaults (`max_inflight: 1024`,
  `max_inflight_bytes: 33554432`, `concurrency: 16`): raise
  `dispatch.tenants.default` for the old ceilings.

- **`routing: tenant_path` needs `rate_limits.default` (TEN-2).** The URL
  names the tenant, so a config with `routing: tenant_path` and no
  `rate_limits.default` fails to load (and `check-config`) with
  `rate_limits.default is required with routing: tenant_path`.
  `config-examples/multi-tenant.yml` sets one.
- **Breaking (SEC-10):** a `type: http` sink needs a `secret` (a `whsec_`
  secret, or a list while rotating) to load, or `allow_unsigned: true` to send
  deliveries with no signature. The same check runs when the admin API writes a
  source (`400 invalid_source`); a source already stored by
  `source_store.type: persistent` or `redis` with an unsigned HTTP sink is
  skipped at boot with a warning until it is rewritten with a `secret` (or
  `allow_unsigned: true`). Every shipped example config sets
  `secret: "${SINK_WHSEC}"`; the quickstart demo sets `allow_unsigned: true`
  because its worker does not verify.

- **Breaking:** a `type: http` sink URL must be `https://` to load (a plain
  `http://` one needs `egress.allow_insecure_http: true`), and at delivery its
  host may not resolve to a loopback, private or link-local address unless
  `egress.allow_cidrs` or `egress.allow_hosts` allows it. The same check runs
  when the admin API writes a source (`400 invalid_source`). A source already
  stored by `source_store.type: persistent` or `redis` with an `http://` sink is
  skipped at boot with a warning until `allow_insecure_http` is set. Existing
  `type: http` deployments to a private receiver add, for example,
  `egress: {allow_insecure_http: true, allow_hosts: [worker]}`.
- The image's dependencies are relocked to their latest releases: `brod
  4.7.0` (Kafka), `amqp 4.3.0` (RabbitMQ), `req 0.7.5` on `finch 0.24.0`, and
  `ed25519`/`kcl 1.5.2` (NATS nkeys).

### Fixed

- The image's NATS client is `gnat 1.18.1`: no gnat process holds the NATS
  password, token or nkey seed in the clear. See the `ankusa_nats` changelog.

## [0.6.0] - 2026-10-14

### Added

- Every delivery says how the hook was verified: `Sink.Http` sends
  `x-ankusa-verification` (`ok`, `failed` or `skipped`) and, for a hook a
  source accepted despite a failed check (`on_verify_failure: accept_flag`),
  `x-ankusa-verification-flagged: true`; a queue sink's message carries
  `verification: {status, flagged}`. The admin API's source routes show
  `trust_url_tenant`.
- `idle_timeout_ms` on `rabbitmq`, `kafka`, `nats` and `redis` sinks: close a
  broker connection after that long with no delivery (default 10 minutes, `0`
  never).
- Source key `trust_url_tenant` (default `false`). With
  `routing: tenant_path`, a source with `tenant: default` and
  `verify: {type: none}` files hooks under whatever tenant the URL names:
  boot and `check-config` now refuse it (exit 78, naming the source) unless it
  sets `trust_url_tenant: true`, and an admin API write of one is
  `400 invalid_source`. One `source_store: persistent`/`redis` already held
  keeps serving, with a warning at boot. `config-examples/multi-tenant.yml`'s
  `demo` source sets it.
- `source_store.type: redis` (`url`, `namespace`, `tick_ms`; env
  `ANKUSA_SOURCE_STORE_URL`; a `url` with no `type` means `redis`): sources
  created through the admin API are shared by every node through
  `ankusa_redis`'s `Ankusa.SourceStore.Redis`. The three keys are rejected on
  `static`/`persistent`.
- YAML keys for everything core added: `admin.gauge_interval_ms`,
  `batcher.max_queue_bytes`, `dispatch.sink_concurrency`,
  `dispatch.breaker_failures` / `breaker_open_ms` / `breaker_max_open_ms`,
  `storage.key_prefix` (env `ANKUSA_STORAGE_KEY_PREFIX`), `claim_check.store`
  (a dedicated claim store, same shape as `storage` plus `root`),
  `storage.s3.session_token`, http sink `secret` and `max_response_bytes`,
  rabbitmq sink `max_inflight`, kafka sink `max_record_bytes`.
- A `backup` section (`enabled`, `interval_ms`, `keep`, `store` — the
  `claim_check.store` shape; env `ANKUSA_BACKUP_ENABLED`) for core's store
  backup: with it on and segments (or `backup.store`) in a bucket, a container
  started on an empty volume restores the latest backup before it serves, and
  exits with `store_restore_failed` when the bucket can't be read.
  `check-config` runs `Ankusa.Store.Backup.validate_config!/1`.
- `docker-entrypoint remote` (an IEx shell in the running node) and
  `docker-entrypoint rpc EXPR`. Distribution stays on, bound to loopback with
  epmd (`rel/vm.args.eex`, `rel/env.sh.eex`); `RELEASE_DISTRIBUTION=none`
  turns it off.
- GCP metadata tokens (GCS and Pub/Sub) come from `AnkusaServer.GcpToken`,
  one cached token fetched single-flight and refreshed before expiry.
- `type: sqs` sinks (`Ankusa.Sink.SQS`): `queue_url`, `region`, `endpoint`,
  `message_group_id`, `inline_max_bytes`, `max_message_bytes`, `timeout_ms`,
  and optional static `access_key_id` / `secret_access_key` /
  `session_token` (else the `AWS_*` env, IRSA, or the instance role).
  `reference.yml` documents it.
- `type: google_pubsub` sinks (`Ankusa.Sink.GooglePubSub`): `project`,
  `topic`, `endpoint`, `ordering_key`, `inline_max_bytes`,
  `max_message_bytes`, `timeout_ms`, and `auth` (`metadata` | `token` |
  `none`, default `metadata`; `token` is required when `auth: token`), the
  same choice `storage.gcs.auth` offers. `reference.yml` documents it.
- Every sink now sends the tenant-scoped idempotency key: the message gains
  `idempotency_key` (still `v: 1`), `Sink.Http` sends `x-ankusa-idempotency-key`,
  and RabbitMQ, Kafka and NATS send `ankusa_idempotency_key`. It is
  `tenant:source_id:dedupe_key` when the source has a dedupe key, else the hook
  `id`. Consumers should dedupe on it; the SDK helpers read it.
- `dispatch.attempt_timeout_ms` (default `30000`): a delivery attempt that has
  not returned after it is killed and counts as a failed attempt
  (`{:attempt_timeout, ms}` in the retry error or the DLQ reason), so a hung
  sink frees its slot. A value that is not a positive integer fails
  `check-config`.
- A `quarantine:` section (`burst`, `rate`, `max_bytes`): one quarantine
  bucket per source, and a cap on the pen's bytes. A full pen answers
  `503 quarantine_full`; a source over its bucket answers
  `429 quarantine_rate_limited`.
- `verify.secret` takes a list of strings (at most 8), newest first, for a
  secret rotation with no cut-over: `secret: ["${NEW}", "${OLD}"]`.
- Release held hooks with `POST /v1/replays {"kind":"quarantine"}`, and purge
  them with `DELETE /v1/quarantine`. See `docs/delivery.md#quarantine`.
- `ankusa_ingest_refused_total{instance,reason}` on `GET /metrics`: requests
  the edge answered without ingesting them (`unknown_source`,
  `payload_too_large`, `body_read_failed`, `invalid_header`).
- `admin.ip`, `claim_check.ip` and `routes.admin.ip`: the address each listener
  binds (a strict IPv4 or IPv6 literal, default `127.0.0.1`), and the
  `ANKUSA_ADMIN_IP` and `ANKUSA_CLAIM_CHECK_IP` overrides. `check-config`
  rejects anything that is not an IP address, naming the key.
- Replay jobs: `POST /v1/replays` (`kind: dlq | archive | quarantine`),
  `GET /v1/replays` and `GET|PATCH /v1/replays/{id}`.
- Source key `dedupe` (a preset, or `{preset|header|json, ttl_seconds}`): a
  repeat of an event inside the TTL answers `201` with the original `id` and
  `"duplicate": true`. Source key `forward_headers`.
- `wal.publish_timeout_ms` (default `8000`): the deadline every sink must
  confirm under with `wal.type: none`.
- Metrics `ankusa_replay_moved_total`, `ankusa_replay_throttled_total` and
  `ankusa_quarantine_full_total`.

### Changed

- The image's `HEALTHCHECK` asks `GET /ready` (`docker-entrypoint
  healthcheck`, ingest port first, then admin), so a node whose store refuses
  writes reports `unhealthy`.
- An unresolved `${…}` in the file is a load error naming the key (a
  substituted value that itself contains `${` is fine); a static source whose
  verifier secret is missing or undecodable fails `check-config` and boot.
- `AnkusaServer.GcpToken` answers `:error` to every caller for a second after
  a failed metadata fetch, so a queue of callers behind a dead metadata server
  costs one request.
- `:os_mon` runs (disk gauges); only `disksup` is configured. The image adds
  `coreutils`: `disksup` runs `df -lk -x squashfs`, which busybox's `df`
  rejects.
- `GET /asyncapi.json`: `SinkMessageV1` now requires `sha256`, `dedupe_key`,
  `replay_id`, `idempotency_key` and `headers`, and `SinkMessageHeadersV1`
  requires `ankusa_idempotency_key`. A consumer that validates messages
  against the previous document rejects the new fields only if it also
  forbids additional properties.
- **Breaking: an empty `verify.secret` is a load error.** An empty string, or
  an empty element of a secret list (an unset `${OLD:-}` included), fails
  `check-config` and the boot, naming the key (`sources.a.verify.secret[1]:
  must not be empty`). It used to load and verify with the empty HMAC key,
  which accepts anything signed with it.
- **Breaking: a source over its quarantine bucket answers `429
  quarantine_rate_limited`** with `Retry-After`, not `401 verification_failed`;
  a full pen answers `503 quarantine_full` with `Retry-After: 60`.
- **The default retry policy retries for about 6 hours:** `dispatch.retry`
  defaults to `max_ms: 300000` and `max_attempts: 84` (they were `30000` and
  `12`, about 83 s). A deployment that relied on the old default to
  dead-letter within minutes should set `max_attempts` explicitly. Errors are
  not classified yet, so a permanent failure (an HTTP `400`, say) also takes
  every attempt before it reaches the DLQ.
- **Breaking: the image's admin API (4002) and claim gateway (4001) listen on
  `127.0.0.1` inside the container.** `-p 127.0.0.1:4002:4002` alone no longer
  reaches the admin API, and the claim gateway is unreachable from other
  containers. Set `ANKUSA_ADMIN_IP=0.0.0.0` and `ANKUSA_CLAIM_CHECK_IP=0.0.0.0`
  (or `admin.ip` and `claim_check.ip`) to listen on the container's interface;
  the Docker `HEALTHCHECK` keeps working. The compose files, the smoke script
  and the examples set it where they publish or share a port.
- **Breaking: a request with a header name or value outside visible ASCII (and
  space and tab in a value) is `400 invalid_header`.** It used to be acked and
  then dead-lettered when a sink could not carry it; see the core changelog.
- **Breaking for anything that reads `GET /v1/config`:** adapter options are
  shown only under known non-secret keys, and every other value (a
  `sas_token`, a `private_key`, an NATS `jwt`) is `"[REDACTED]"`; URL query
  values are redacted too. `print-config` uses the same view.
- **Breaking for dashboards: refused requests are no longer on
  `ankusa_ingest_requests_total`.** Requests for sources that do not exist no
  longer count under `ankusa_ingest_requests_total{outcome="unknown_source"}`;
  a panel that sums that counter for all traffic must add
  `ankusa_ingest_refused_total`.
- **Breaking: `POST /v1/dlq/replay` is removed.** Use
  `POST /v1/replays {"kind":"dlq"}`.
- `Sink.Http` forwards the provider's request headers by default (per the
  source's `forward_headers`) and adds `x-ankusa-dedupe-key` and
  `x-ankusa-replay-id` when present.
- `wal.type: none` publishes to all of a source's sinks concurrently under
  `wal.publish_timeout_ms`; any failure or timeout is a `503`.
- **Breaking: every RabbitMQ publish is `mandatory`.** A publish to an
  exchange with no bound queue now retries and then dead-letters instead of
  silently answering `201`. RabbitMQ also sets AMQP `message_id` to the hook
  id.
- NATS sets `Nats-Msg-Id` on every publish (the hook `id`, or
  `id:replay:<replay_id>` on a replay).

### Fixed

- `GET /metrics` no longer grows with unauthenticated traffic: requests for
  sources that do not exist are one `ankusa_ingest_refused_total` series, not
  a `source_id` series per URL.
- A `Content-Length` above `http.max_body_bytes`, or a source that does not
  exist, is answered before the request body is read.
- The image's HTTP client stack is `mint 1.11.0` (was `1.10.1`, which has
  three published advisories: EEF-CVE-2026-91043, EEF-CVE-2026-94194 and
  EEF-CVE-2026-92103) and `hpax 1.1.0`.
- The image's HTTP server is `bandit 1.13.1` (was `1.12.5`, which has two
  published advisories: EEF-CVE-2026-105659 and EEF-CVE-2026-105660) on
  `thousand_island 1.6.0`.
- An unreachable object store is retried with backoff (up to 60 s) instead of
  every second, and any failed store write asks the store for a rate-limited
  reopen, not only ingest's: a fallback for a latched RocksDB write error after
  a full disk. See the core changelog.

## [0.4.0] - 2026-10-02

### Added

- `GET /asyncapi.json` on the admin port: an AsyncAPI 3.0 document
  (`application/asyncapi+json`) of the Kafka topics, RabbitMQ routing keys,
  NATS subjects, and Redis channels this node publishes to, built from the
  sources configured right now. See `docs/asyncapi.md`.
- A `lifecycle:` section (`lifecycle.sinks`, the same sink types as a
  source's). With it, creating, updating, or deleting a source through the
  admin API, or a route through the route-management API, publishes a
  CloudEvents 1.0 event (`io.ankusa.source.created`, …) to those sinks from
  memory, bypassing the store: retried with the dispatch retry policy, not
  persisted. Absent, nothing changes. The source id `ankusa:lifecycle` is
  reserved for them and refused in `sources:`.

### Changed

- **Breaking: `dispatch.poll_ms` and the http sink's `ordered` key are
  removed**, and the loader now rejects either as an unknown key. Dispatch is
  woken by a commit (or by the next due row) instead of polling, and deliveries
  are unordered.
- `wal.type: disk` is now the node's RocksDB store (still the default, and the
  same key). The data volume holds `store/` (hooks, delivery rows, the
  quarantine pen, API-managed sources, rate-limit overrides, the segment
  catalogue), segments at `segments/seg/<first_seq>-<last_seq>.seg` with a new
  sibling `.idx` per segment, and `claims/...`. It replaces the WAL, DLQ,
  quarantine, and segment-index files and the `sources.json` /
  `rate_limits.json` state files. `check-config` prints `wal=disk`. A 0.3 data
  volume is imported on the first boot of a new store, each artifact renamed
  `*.migrated-*` (never deleted; rename them back to roll back).
- The image builds RocksDB from source: the build stage adds `cmake`,
  `linux-headers`, `openssl-dev`, and `zstd-dev` on top of `build-base` and
  `git`, and the build is cached in its own layer so it only reruns when a
  lockfile changes. The runtime stage is unchanged.

### Fixed

- A node that cannot read its store refuses to start, instead of treating it as
  empty. An unreadable 0.3 artifact also refuses the import: a damaged frame in
  `wal/` with valid frames after it, or an unreadable `.cursors`/`.truncated`
  sidecar, fails boot with a log line naming the file, and a store that will not
  open logs and exits rather than booting empty.

## [0.3.0] - 2026-10-01

### Added

- `wal: {type: none}` (or `ANKUSA_WAL_TYPE=none`): a stateless edge node. Ingest
  publishes to the source's sinks inside the request and acks on their confirm;
  nothing is written to the data volume, so the image runs as a plain
  `Deployment` — no volume, no `StatefulSet`. `node.roles` may keep naming
  `edge, dispatch, storage`: the WAL's reader roles are dropped from the
  effective roles (visible as `roles` on `GET /health`), and `check-config`
  prints `wal=none`. Boot and `check-config` refuse the config when a source in
  the file has no sink whose `:ok` means durable
  (`c:Ankusa.Sink.durable?/1`), naming the source.
  [`config-examples/direct.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/direct.yml)
  is the runnable starting point.

- `rate_limits: {default: …, tenants: {…}}`: per-tenant ingest rate limits. A
  hook over its tenant's limit gets `429` with `Retry-After` and nothing is
  written; the charge happens after verification, so a flood of forged requests
  never spends a tenant's budget. Enforced by each edge node in its own memory,
  so N edges admit N × the limit. A `rate` and `burst` pair is required per
  entry, ranges and tenant ids are validated at boot (and by `check-config`,
  with the same messages as core), and `reference.yml` documents the keys.
  Limits are also readable and adjustable at runtime through the admin API —
  `GET /v1/rate-limits`, `GET|PUT|DELETE /v1/tenants/{tenant}/rate-limit`, on
  the `edge` role — which persists overrides to `rate_limits.json` on the
  node, the same node-local model as API-managed sources.

- `- {type: redis, ...}` sinks: each delivered hook is `PUBLISH`ed to a Redis
  pub/sub channel as an `Ankusa.Sink.Message`, the same value the broker sinks
  publish. Keys: `url` (credentials and db live in it), `channel` (a static
  string), `inline_max_bytes`, `publish_timeout_ms`. Pub/sub keeps no copy, so
  a publish with no live subscriber is an error — retried, then dead-lettered
  for replay — and the sink never counts as durable when `wal: none` is
  checked. `reference.yml` documents it. Comes from `ankusa_redis`, already in
  the image for the Redis route store.

- `source_store: {type: persistent}` in the config file: the server keeps
  sources in a writable store and persists the ones created or updated through
  the admin API to `sources.json` on the data volume, so they survive a restart.

### Fixed

- `routes.store` no longer silently ignores `url`, `namespace` and `tick_ms`
  when the resolved store is ETS: they are only valid with `type: redis`, and
  setting one without it is a config error naming the key. `reference.yml`
  ships `type: ets` live, so an operator who set only
  `ANKUSA_ROUTES_STORE_URL` previously got a per-node store while believing the
  fleet shared its route definitions.
- The boot banner only prints `routes=on:<store>` when the route machinery
  actually runs, i.e. `routes.enabled` on an `edge` node; otherwise it prints
  `off` or `n/a (no edge role)`. It still prints the store type alone, never
  `store.url`, which can carry a password.
- The image `EXPOSE`s 4003, the route-management listener, and the README
  documents that port (it listens only when `routes.enabled` is set on an
  `edge` node), the two `ANKUSA_ROUTES_*` env overrides, and the store
  type/url rule.

## [0.2.4] - 2026-09-28

### Added

- A `routes:` section: `enabled`, `max_routes`, `store` (`type: ets`, or
  `type: redis` with `url`/`namespace`/`tick_ms` — a `url` with no `type` means
  redis), `cache`, `trusted_proxies`, `ip_rules`, `admin.port` (4003, its own
  listener, unauthenticated by design — front it like the operator admin API's
  `admin.port`), `log_sample`, `ip_denied_status`, and `seed`. `reference.yml`
  documents the whole section, including the retry caveat for senders that
  treat a `4xx` as retryable. Env overrides: `ANKUSA_ROUTES_ENABLED`,
  `ANKUSA_ROUTES_STORE_URL`.
- `ankusa_redis` in the image, so `store: {type: redis, url: ...}` shares route
  definitions across edge nodes without a rebuild.
- The boot banner reports `routes=off` or `routes=on:<store>`.

### Changed

- Config loading now runs core's `Ankusa.Routes.validate_config!/1`, so
  `check-config` fails on a route config that would refuse to boot — an
  unparseable CIDR, a seed that conflicts with itself or exceeds
  `max_routes` — with the same message the server would exit with.

## [0.2.1] - 2026-09-28

### Added

- `type: nats` sink: publishes delivered hooks to a NATS JetStream subject,
  with `servers`, `subject`, `inline_max_bytes` (`8192`),
  `publish_timeout_ms` (`5000`), `tls`, and an `auth` block taking one scheme
  (`username` + `password`, `token`, or `nkey_seed` + `jwt`). The stream has
  to exist: the sink never creates one.
- [`config-examples/nats-fanout.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/nats-fanout.yml),
  and a `nats` sink in `reference.yml`.
- `dispatch.concurrency` (default 32), `dispatch.max_inflight` (4096) and
  `dispatch.max_inflight_bytes` (134217728), for the now-concurrent dispatch
  pipeline.
- `ordered` on `http` sinks, mapping to `Ankusa.Sink.Http`'s `:ordered` option
  (per-`{tenant, source}` serialization; off by default).

### Changed

- `batcher.partitions` defaults to 2 and `batcher.max_delay_ms` to 0;
  `reference.yml` reflects both, and documents that `max_queue` counts buffered
  *and* in-flight records.
- `claim_check` auth (`tokens`) removed: the gateway is open, so put your
  proxy/mesh/network policy in front; `pack_max_bytes` added (default 16 MiB in
  core), and `remote`/`max_bytes` removed.

## [0.1.0]

### Added

- First release of the standalone server: one image with every adapter
  (Postgres WAL, RabbitMQ, Kafka), configured by a YAML file with `${VAR}`
  interpolation and `ANKUSA_*` env overrides.
- `docker run jamescarr/ankusa` serves ingest on 4000, the claim-check gateway on
  4001 (`claim_check` role), and the operator API with Prometheus `/metrics` on
  4002.
- `check-config` / `print-config` / `version` entrypoints, and a container
  healthcheck against `/health`.
- Shipped configs: a baked demo config, a full `reference.yml`, single-node,
  fleet (Postgres + S3), Kafka/RabbitMQ fan-out, and multi-tenant examples.
- Compose files for a single node and for a fleet behind nginx basic auth.

[Unreleased]: https://github.com/jamescarr/ankusa/compare/ankusa_server-v0.7.0...HEAD
[0.7.0]: https://github.com/jamescarr/ankusa/compare/ankusa_server-v0.6.0...ankusa_server-v0.7.0
[0.6.0]: https://github.com/jamescarr/ankusa/compare/ankusa_server-v0.4.0...ankusa_server-v0.6.0
[0.4.0]: https://github.com/jamescarr/ankusa/compare/ankusa_server-v0.3.0...ankusa_server-v0.4.0
[0.3.0]: https://github.com/jamescarr/ankusa/compare/ankusa_server-v0.2.4...ankusa_server-v0.3.0
[0.2.4]: https://github.com/jamescarr/ankusa/compare/ankusa_server-v0.2.1...ankusa_server-v0.2.4
[0.2.1]: https://github.com/jamescarr/ankusa/releases/tag/ankusa_server-v0.2.1
