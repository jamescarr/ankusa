# Changelog

All notable changes to `ankusa_postgres` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-09-23

### Added

- `Ankusa.WAL.Postgres`: shared, multi-node WAL adapter. Group commit
  translated to one `Postgrex.transaction/2` per batch (claim dedup keys →
  insert winners → resolve losers' seq), correlated by envelope id, never
  array position.
- Permanent dedup ledger (`ankusa_wal_dedup`), separate from `ankusa_wal`
  and never touched by truncation — a duplicate of an already-compacted
  event is still caught.
- Every table scoped by `instance`, so one database backs multiple
  instances, including the same instance name running on independent BEAM
  nodes at once.

[Unreleased]: https://github.com/jamescarr/ankusa/compare/ankusa_postgres-v0.1.0...HEAD
[0.1.0]: https://github.com/jamescarr/ankusa/releases/tag/ankusa_postgres-v0.1.0
