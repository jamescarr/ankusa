# Changelog

All notable changes to the Ankusa server image are documented here. This
project is versioned independently of the `ankusa` Hex packages: it is the
`jamescarr/ankusa` Docker image, tagged `ankusa_server-vX.Y.Z`.

## [Unreleased]

### Added

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

## [0.5.0] - 2026-10-08

### Added

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

[Unreleased]: https://github.com/jamescarr/ankusa/compare/ankusa_server-v0.5.0...HEAD
[0.5.0]: https://github.com/jamescarr/ankusa/compare/ankusa_server-v0.4.0...ankusa_server-v0.5.0
[0.4.0]: https://github.com/jamescarr/ankusa/compare/ankusa_server-v0.3.0...ankusa_server-v0.4.0
[0.3.0]: https://github.com/jamescarr/ankusa/compare/ankusa_server-v0.2.4...ankusa_server-v0.3.0
[0.2.4]: https://github.com/jamescarr/ankusa/compare/ankusa_server-v0.2.1...ankusa_server-v0.2.4
[0.2.1]: https://github.com/jamescarr/ankusa/releases/tag/ankusa_server-v0.2.1
