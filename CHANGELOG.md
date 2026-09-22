# Changelog

All notable changes to `ankusa` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

Release process: bump `version` in `mix.exs` and move the relevant
`[Unreleased]` entries under a new dated heading in the same PR, in
accordance with SemVer. Merging to `main` publishes automatically — see
[`docs/deployment.md`](docs/deployment.md#releasing) for the mechanics.

## [Unreleased]

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
