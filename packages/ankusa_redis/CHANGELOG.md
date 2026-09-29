# Changelog

All notable changes to `ankusa_redis` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- **Needs the `ankusa` release that carries the version-checked
  `Ankusa.Routes.Store` callbacks** (`insert/3`, `replace/3`); release the two
  together. An older `Ankusa.Routes.Store.Redis` does not implement the new
  arities.
- Every write is one Lua script. `insert` and `replace` are conditioned on the
  version the caller validated against and answer `{:error, :stale}` — after the
  node has reloaded — when Redis holds another; `delete` and `put_ip_rules` bump
  the version in the same step. `insert` checks `max_routes` inside the script.
- A node reloads when Redis's version *differs* from its own, not only when it is
  higher, and reads the version and the definitions in one transaction. A pub/sub
  message is now a nudge to check the version; its payload is ignored.
- The node subscribes before it loads, seeding is one atomic script, and the
  supervisor is `:rest_for_one` with the pub/sub connection ahead of the state
  process. Writes wait up to 20s for the state process (Redis's own timeout is 5s
  per command) instead of giving up at the 5s `GenServer.call` default.
- The suite deletes only its own three keys instead of `FLUSHDB`, so it can run
  against a Redis that holds other data.

### Fixed

- Two nodes could both create the same id, take one path and method, or exceed
  `max_routes`: each validated against a mirror that lags by a pub/sub round trip
  and the cap was a `HLEN` followed by a separate `HSET`.
- A node whose mirror lagged another node's write took the version its own write
  returned without that write, then dropped the other node's broadcast as stale.
  It served a table missing a route — or still allowing a deleted one — until an
  unrelated write.
- A delete removed the route and then bumped the version in a second round trip;
  a failure in between left the route gone and no node told to reload.
- A Redis flushed or restored to an older state was never followed, because a
  node reloaded only for a higher version.
- A pub/sub connection that died left the node subscribed to nothing, so it heard
  no broadcast until its tick.
- Two nodes booting on a fresh namespace could both seed it, resurrecting routes
  an operator had already deleted and setting the version back to 1.

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
