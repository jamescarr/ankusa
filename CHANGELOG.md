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
  (and `:inets` is no longer started by this package). Outbound requests go
  through `Ankusa.HttpClient`, which never follows a redirect (a followed one
  re-sends a hook as a `GET`) and takes `:req_options` from an allowlist:
  transport tuning only, so a caller cannot rewrite a URL that has already been
  signed.
- `Ankusa.Storage.Index` is backed by an ETS table owned by the compactor
  instead of a `:persistent_term` map rebuilt from the file on lookup, so a
  lookup never re-decodes the index and a compaction tick only touches the rows
  it wrote. A node without the compactor — or the window while it restarts —
  reads the file, as before.
- `Ankusa.BlobStore.S3` signs requests with `aws_signature`, the SigV4
  implementation behind the official aws-elixir SDK, replacing ~80 lines of
  hand-rolled canonical-request/HMAC code. Signing is now pinned by tests that
  reproduce AWS's published reference signatures — the emulator the integration
  suite uses accepts *any* signature, so it never covered this.
- New dependencies: `req` and `aws_signature`. See
  [`docs/packaging.md`](docs/packaging.md) for when a dependency is worth taking
  and where it belongs.
- Telemetry now emits what `Ankusa.Telemetry` documents: `[:ankusa, :commit]` is
  a span reporting `:duration` on `:stop` alongside `:batch_size` and `:bytes`
  (measurements), `[:ankusa, :verify, :stop]` carries `:status`, and
  `[:ankusa, :compact, :stop]` carries `:duration`. Verifiers share one failure
  vocabulary — Standard Webhooks reports `:missing_signature` where it reported
  `:missing_headers`.

### Fixed

- `Ankusa.BlobStore.LocalFS.get/3` and `get_range/5` now map a missing file
  to `{:error, :not_found}` (previously `:enoent`), matching `BlobStore.S3`
  and `BlobStore.GCS` — `:not_found` is now part of the `BlobStore` contract
  for every adapter.
- `Ankusa.Instance` no longer starts the configured `WAL` on a node running
  none of `:edge`, `:dispatch`, `:storage` — a `:claim_check`-only node
  needs only blob-store credentials.
- `Ankusa.BlobStore.S3.list/3` reads a `200` body defensively: a response that
  isn't ListObjectsV2 XML — or isn't even valid UTF-8 — reads as an empty
  listing instead of taking the claim-check sweeper down, and a listing
  containing a non-ASCII key no longer comes back empty. The XML scanner is
  handed raw bytes, which is what it decodes.

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
