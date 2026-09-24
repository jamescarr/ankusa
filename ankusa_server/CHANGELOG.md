# Changelog

All notable changes to the Ankusa server image are documented here. This
project is versioned independently of the `ankusa` Hex packages: it is the
`ankusa/ankusa` Docker image, tagged `ankusa_server-vX.Y.Z`.

## [Unreleased]

## [0.1.0]

### Added

- First release of the standalone server: one image with every adapter
  (Postgres WAL, RabbitMQ, Kafka), configured by a YAML file with `${VAR}`
  interpolation and `ANKUSA_*` env overrides.
- `docker run ankusa/ankusa` serves ingest on 4000, the claim-check gateway on
  4001 (`claim_check` role), and the operator API with Prometheus `/metrics` on
  4002.
- `check-config` / `print-config` / `version` entrypoints, and a container
  healthcheck against `/health`.
- Shipped configs: a baked demo config, a full `reference.yml`, single-node,
  fleet (Postgres + S3), Kafka/RabbitMQ fan-out, and multi-tenant examples.
- Compose files for a single node and for a fleet behind nginx basic auth.
