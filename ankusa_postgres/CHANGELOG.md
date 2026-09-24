# Changelog

All notable changes to `ankusa_postgres` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- `append/2` takes a per-instance advisory lock
  (`pg_advisory_xact_lock(hashtext("ankusa_wal:" <> instance))`) before
  inserting, and holds it until the transaction commits. `seq` order therefore
  equals commit order, which is what `Ankusa.WAL` promises readers. Appends for
  one instance now serialize fleet-wide; that is the cost of a cursor that
  cannot skip a commit.
- The dedup-ledger backfill is part of the winning `INSERT` (a data-modifying
  CTE that keys on `ankusa_wal_dedup`'s primary key) instead of a separate
  `UPDATE ... WHERE event_id = ...`, which scanned the ever-growing ledger
  because `event_id` is not indexed.

### Fixed

- A cursor-following reader could permanently miss a commit: `seq` was
  allocated mid-transaction by `BIGSERIAL`, so a later transaction could commit
  a higher `seq` first, dispatch would advance its cursor past it, and the
  lower `seq` landing afterwards was never read — then deleted by the next
  truncation. This was the e2e chaos-phase loss (killing the worker widened the
  allocation→commit window); the new regression test reproduces it against a
  live Postgres.

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
