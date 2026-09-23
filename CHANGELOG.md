# Changelog

All notable changes to `ankusa` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

Release process: bump `version` in `mix.exs` and move the relevant
`[Unreleased]` entries under a new dated heading in the same PR, in
accordance with SemVer. Merging to `main` publishes automatically — see
[`docs/deployment.md`](docs/deployment.md#releasing) for the mechanics.

## [Unreleased]

### Added

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

### Changed

- HTTP is `Req` throughout — the S3 and GCS blob stores, the claim-check
  `Remote` adapter, and `Sink.Http` no longer hand-roll `:httpc` plumbing
  (and `:inets` is no longer started by this package).
- `Ankusa.BlobStore.S3` signs requests with `aws_signature`, the SigV4
  implementation behind the official aws-elixir SDK, replacing ~80 lines of
  hand-rolled canonical-request/HMAC code. Signing is now pinned by tests that
  reproduce AWS's published reference signatures — the emulator the integration
  suite uses accepts *any* signature, so it never covered this.
- New dependencies: `req` and `aws_signature`. See
  [`docs/packaging.md`](docs/packaging.md) for when a dependency is worth taking
  and where it belongs.

### Fixed

- `Ankusa.BlobStore.LocalFS.get/3` and `get_range/5` now map a missing file
  to `{:error, :not_found}` (previously `:enoent`), matching `BlobStore.S3`
  and `BlobStore.GCS` — `:not_found` is now part of the `BlobStore` contract
  for every adapter.
- `Ankusa.Instance` no longer starts the configured `WAL` on a node running
  none of `:edge`, `:dispatch`, `:storage` — a `:claim_check`-only node
  needs only blob-store credentials.

## [0.1.0] - 2026-09-22

### Added

- Core ingest pipeline: Bandit edge, group-commit batcher, durable WAL
  (`Ankusa.WAL.DiskLog`), idempotent receiver, dispatch pipeline, segment
  compactor.
- Pluggable behaviours: `Ankusa.RouteResolver`, `Ankusa.WAL`,
  `Ankusa.Verifier`, `Ankusa.DedupKey`, `Ankusa.SourceStore`, `Ankusa.Sink`,
  `Ankusa.RetryPolicy`, `Ankusa.BlobStore`, `Ankusa.Codec`.
- Verifiers: Standard Webhooks, Stripe, GitHub.
- Object store adapters: `BlobStore.LocalFS` (default), `BlobStore.S3`
  (AWS/MinIO/R2, hand-rolled SigV4, zero deps), `BlobStore.GCS`.
- Multi-tenant catch-URL routing (`RouteResolver.Path`,
  `RouteResolver.TenantPath`) with tenant-scoped dedup/storage.
- Quarantine (rate-limited durable pen for verification failures) and DLQ +
  replay for dispatch give-ups.
- Loss checker: proves every acked id survives a hard instance crash.

[Unreleased]: https://github.com/ankusa-elixir/ankusa/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ankusa-elixir/ankusa/releases/tag/v0.1.0
