# Changelog

All notable changes to `ankusa` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

Release process: bump `version` in the package's `mix.exs` and move the
relevant `[Unreleased]` entries under a new dated heading in the same PR, in
accordance with SemVer. A pushed `<pkg>-vX.Y.Z` git tag publishes — see
[`docs/releasing.md`](docs/releasing.md) for the mechanics.

## [Unreleased]

### Added

- Admin API (`Ankusa.Admin.Router`), started when `admin.enabled` is true
  (default `false`, port `4002`): `GET /health`, `GET /metrics`,
  `GET /v1/config` (redacted), `GET /v1/dlq`, `POST /v1/dlq/replay`,
  `GET /v1/quarantine`. Unauthenticated by design — front it with your own
  proxy or network policy. Contract:
  [`priv/openapi/admin.v1.yaml`](https://github.com/jamescarr/ankusa/blob/main/priv/openapi/admin.v1.yaml).
- `Ankusa.Metrics`: a per-instance Prometheus reporter behind `/metrics`,
  covering ingest, verification, WAL commit, dedup, load shedding,
  quarantine, dispatch, compaction, and claim check. New dependencies:
  `telemetry_metrics` and `telemetry_metrics_prometheus_core`.
- `:instance` in the metadata of `[:ankusa, :dispatch, :stop]`,
  `[:ankusa, :dispatch, :dlq]`, `[:ankusa, :claim_check, :check_in]`, and
  `[:ankusa, :claim_check, :redeem]`.
- `Ankusa.Verifier.Hmac`: a configurable HMAC signature engine driven by a
  `%Ankusa.Verifier.Hmac.Scheme{}` descriptor, with named presets in
  `Ankusa.Verifier.Schemes` for Stripe, GitHub, Standard Webhooks, Shopify,
  and Slack, and an inline `type: hmac` YAML surface for any other body-HMAC
  provider. The `shopify` and `slack` `verify.type` values are new.
- `scheme_name/1` optional callback on `Ankusa.Verifier`, a `scheme` field on
  `Ankusa.Verification`, and a `scheme` label on `ankusa.verify.failures.total`.

### Removed

- `Ankusa.Verifier.Stripe`, `Ankusa.Verifier.GitHub`, and
  `Ankusa.Verifier.StandardWebhooks` — replaced by `Ankusa.Verifier.Hmac`
  presets. Elixir embedders use `{Ankusa.Verifier.Hmac, scheme: :stripe, …}`.

### Changed

- `claim_check.api_tokens` is optional. Empty (the default) leaves the
  claim-check gateway open, with authentication delegated to whatever fronts
  the port; a `:claim_check`-role node no longer fails to boot without
  tokens.
- `Ankusa.ClaimCheck.Remote`'s `:token` is optional; omit it when the gateway
  has no `api_tokens`.
- `Ankusa.Admin.Redact` also redacts `nkey_seed`, the private key a
  `Sink.NATS` deployment authenticates with, so `GET /v1/config` cannot print
  it.

## [0.1.0] - 2026-09-23

### Added

- Core ingest pipeline: Bandit edge, group-commit batcher, durable WAL
  (`Ankusa.WAL.DiskLog`), idempotent receiver, dispatch pipeline, segment
  compactor.
- Pluggable behaviours: `Ankusa.RouteResolver`, `Ankusa.WAL`,
  `Ankusa.Verifier`, `Ankusa.DedupKey`, `Ankusa.SourceStore`, `Ankusa.Sink`,
  `Ankusa.RetryPolicy`, `Ankusa.BlobStore`, `Ankusa.Codec`.
- Verifiers: Standard Webhooks, Stripe, GitHub.
- Object store adapters: `BlobStore.LocalFS` (default), `BlobStore.S3`
  (AWS/MinIO/R2, SigV4 via `aws_signature`), `BlobStore.GCS`.
- Multi-tenant catch-URL routing (`RouteResolver.Path`,
  `RouteResolver.TenantPath`) with tenant-scoped dedup/storage.
- Quarantine (rate-limited durable pen for verification failures) and DLQ +
  replay for dispatch give-ups.
- Loss checker: proves every acked id survives a hard instance crash.
- `Ankusa.ClaimCheck` gateway: check bytes in, get a versioned ticket back;
  present the ticket, get the bytes back. `Ankusa.ClaimCheck.Direct` (calls
  the instance's configured `BlobStore` in-process) and
  `Ankusa.ClaimCheck.Remote` (HTTP client against a `:claim_check`-role
  node, for callers that must not hold blob-store credentials). New
  `:claim_check` role and `Ankusa.ClaimCheck.Router` HTTP API
  (`PUT`/`GET /v1/claims/:tenant_id/:id`), off by default, bearer-token
  authenticated and tenant-scoped. See
  [`docs/claim-check.md`](docs/claim-check.md).
- `Ankusa.Sink.Message`: the wire format every queue-style sink publishes
  (`Sink.RabbitMQ`, `Sink.Kafka`) — inline base64 up to `inline_max_bytes` or
  a claim ticket above it, with an additive `"v": 1` version field.
- `Sink.Http` adds `x-ankusa-tenant` to its identity headers when the
  envelope has a tenant id.
- `Ankusa.Config.parse_roles!/1` validates `ANKUSA_ROLES` against the fixed
  role set (`edge`, `dispatch`, `storage`, `claim_check`) and fails boot on
  an unknown name, instead of silently atomizing arbitrary input.
  `Ankusa.Config.new/1` rejects an unknown nested key (e.g. `batcher: %{max_queu:
  5}`) and correctly deep-merges a keyword-list section instead of
  replacing the whole defaults map.
- HTTP is `Req` throughout — the S3 and GCS blob stores, the claim-check
  `Remote` adapter, and `Sink.Http` no longer hand-roll `:httpc` plumbing
  (and `:inets` is no longer started by this package). Outbound requests go
  through `Ankusa.HttpClient`, which never follows a redirect (a followed one
  re-sends a hook as a `GET`) and takes `:req_options` from an allowlist:
  transport tuning only, so a caller cannot rewrite a URL that has already been
  signed.
- Ankusa.Application's built-in default instance is opt-in: `autostart`
  defaults to `false`, so depending on `ankusa` never binds a port as a side
  effect.

[Unreleased]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.1.0...HEAD
[0.1.0]: https://github.com/jamescarr/ankusa/releases/tag/ankusa-v0.1.0
