# Changelog

All notable changes to the Ankusa server image are documented here. This
project is versioned independently of the `ankusa` Hex packages: it is the
`jamescarr/ankusa` Docker image, tagged `ankusa_server-vX.Y.Z`.

## [Unreleased]

### Added

- `type: nats` sink: publishes delivered hooks to a NATS JetStream subject,
  with `servers`, `subject`, `inline_max_bytes` (`8192`),
  `publish_timeout_ms` (`5000`), `tls`, and an `auth` block taking one scheme
  (`username` + `password`, `token`, or `nkey_seed` + `jwt`). The stream has
  to exist — the sink never creates one.
- [`config-examples/nats-fanout.yml`](https://github.com/jamescarr/ankusa/blob/main/ankusa_server/config-examples/nats-fanout.yml),
  and a `nats` sink in `reference.yml`.
- `dispatch.concurrency` (default 32), `dispatch.max_inflight` (4096) and
  `dispatch.max_inflight_bytes` (134217728), for the now-concurrent dispatch
  pipeline.
- `ordered` on `http` sinks, mapping to `Ankusa.Sink.Http`'s `:ordered` option
  (per-`{tenant, source}` serialization; off by default).
- `dispatch.dedup_store` (`ets`, the in-process default, or `ra`), plus
  `dispatch.partitions` and `dispatch.dedup_ttl_ms`. `dedup_store: ra` keeps the
  idempotent receiver's ledger in the WAL cluster's replicated state; its
  members are that cluster's, so it needs `wal.type: ra`, and it refuses to load
  without one rather than starting with a ledger nobody replicates.

### Changed

- The image smoke check expects two `202`s from the demo hook instead of a `201`
  and a `200`: dedup moved off the ack path, so the second copy is its own
  record with its own envelope id, and `smoke.sh` now asserts exactly that.
- `batcher.partitions` defaults to 2 and `batcher.max_delay_ms` to 0;
  `reference.yml` reflects both, and documents that `max_queue` counts buffered
  *and* in-flight records.
- `claim_check` auth (`tokens`) removed — the gateway is open, so put your
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
