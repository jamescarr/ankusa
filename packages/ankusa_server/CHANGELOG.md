# Changelog

All notable changes to the Ankusa server image are documented here. This
project is versioned independently of the `ankusa` Hex packages: it is the
`jamescarr/ankusa` Docker image, tagged `ankusa_server-vX.Y.Z`.

## [Unreleased]

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
- [`config-examples/nats-fanout.yml`](https://github.com/jamescarr/ankusa/blob/main/ankusa_server/config-examples/nats-fanout.yml),
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

[Unreleased]: https://github.com/jamescarr/ankusa/compare/ankusa_server-v0.3.0...HEAD
[0.3.0]: https://github.com/jamescarr/ankusa/compare/ankusa_server-v0.2.4...ankusa_server-v0.3.0
[0.2.4]: https://github.com/jamescarr/ankusa/compare/ankusa_server-v0.2.1...ankusa_server-v0.2.4
[0.2.1]: https://github.com/jamescarr/ankusa/releases/tag/ankusa_server-v0.2.1
