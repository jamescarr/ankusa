# Changelog

All notable changes to `ankusa_redis` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.4] - 2026-09-28

### Added

- `Ankusa.Routes.Store.Redis`: route definitions in Redis, shared across edge
  nodes, with a version counter and pub/sub invalidation. The node keeps a
  compiled snapshot in memory (the guard never reads Redis per request), reloads
  it when it sees a newer version, and falls back to a periodic tick
  (`tick_ms`) when a broadcast is missed.
- Depends on `ankusa` `~> 0.2`, which introduced `Ankusa.Routes.Store` and
  `Ankusa.Routes.Snapshot`.

[Unreleased]: https://github.com/jamescarr/ankusa/compare/ankusa_redis-v0.2.4...HEAD
[0.2.4]: https://github.com/jamescarr/ankusa/releases/tag/ankusa_redis-v0.2.4
