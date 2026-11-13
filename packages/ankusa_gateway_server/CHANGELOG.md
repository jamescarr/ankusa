# Changelog

All notable changes to the Ankusa gateway image are documented here. It is the
`jamescarr/ankusa-gateway` Docker image, tagged `ankusa_gateway_server-vX.Y.Z`
([`docs/releasing.md`](../../docs/releasing.md)).

## [Unreleased]

### Added

- **The gateway image (ADR 0005).** `jamescarr/ankusa-gateway`: a core
  instance with no role and `Ankusa.Gateway` beside it, configured by
  `/etc/ankusa/gateway.yml` (`config_version: 2`, root keys `config_version
  node log listener metrics tracing secrets egress storage claim_check
  gateway`; a section only `jamescarr/ankusa` reads exits 78 naming it;
  `node.roles`, when set, is `[claim_check]`). The env overrides of the full
  image that apply (`ANKUSA_CLAIM_CHECK_PORT|IP|TOKEN`, `ANKUSA_DATA_DIR`,
  `ANKUSA_STORAGE_TYPE`, `ANKUSA_S3_*`, `ANKUSA_GCS_BUCKET`,
  `ANKUSA_METRICS_*`, `ANKUSA_LOG_*`, `ANKUSA_REGION`, `ANKUSA_ROLES`).
  Commands `start`, `check-config`, `print-config`, `version`, `healthcheck`,
  `remote`, `rpc`. Exposes 4001 (gateway) and 4004 (metrics).
- The YAML machinery both images share lives here: `AnkusaGatewayServer.Document`
  (locate, migrate, interpolate, env overrides), `AnkusaGatewayServer.Schema`
  (typed accessors), `AnkusaGatewayServer.Sections` (`log`, `tracing`,
  `listener`, `metrics`, `tls`, `auth`, `storage`, `secrets`, `claim_check`,
  `egress`), `AnkusaGatewayServer.GatewaySection` (`gateway:`), and the
  modules moved from `ankusa_server`: `ConfigError`, `Secrets` and its
  providers, `LogFormatter`, `Tracing` (now told the service it traces),
  `GcpToken`.
