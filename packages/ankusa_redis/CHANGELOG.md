# Changelog

All notable changes to `ankusa_redis` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- The Redis route store keeps serving its last known table when the
  namespace's version key disappears (a flushed or evicted Redis) instead of
  publishing an empty table that rejects every webhook; it logs a warning
  once and reloads when the namespace is written again. It publishes through
  core's ETS route snapshot and drops retired generations on request.
- The Redis sink's connection dials on its own (`sync_connect: false`), so a
  dead Redis never blocks the supervisor that starts it.

## [0.4.0] - 2026-10-02

### Added

- `Ankusa.Sink.Redis` implements `c:Ankusa.Sink.describe/2`, advertising its
  server, database, and channel to the AsyncAPI document the admin API serves.
  Credentials in `:url` are never part of it.

### Changed

- The Hex requirement on `ankusa` is `~> 0.4` (was `~> 0.3`): `describe/2`
  needs `Ankusa.Sink.Description`, which core has from 0.4.0.

### Removed

- The sink no longer implements the removed Ankusa.Sink.ordering_key/2
  callback: deliveries are unordered. The channel still decides the
  destination, so Redis pub/sub's own per-channel order is unchanged. Requires
  the matching `ankusa` core.

## [0.3.0] - 2026-10-01

### Added

- `Ankusa.Sink.Redis`: publishes each delivered hook to a Redis pub/sub channel
  (`PUBLISH`) as the same `Ankusa.Sink.Message` the broker sinks publish, with
  the claim check above `inline_max_bytes`. Pub/sub keeps no copy, so a publish
  that reaches zero subscribers is `{:error, :no_subscribers}` — retried, then
  dead-lettered — and `durable?/1` is `false`, which makes `wal.type: none`
  refuse a source whose only sink is this one. The package gains its own
  `Application` to supervise one Redix connection per `{instance, url}`,
  started on demand.

### Changed

- **Requires `ankusa ~> 0.3`** (it was `~> 0.2`). The store implements the
  version-checked `Ankusa.Routes.Store` callbacks (`insert/3`, `replace/3`) that
  core has from the 0.3 line on; a 0.2.x core calls `insert/2` and `replace/2`
  and every route write would fail, so Hex no longer pairs them. Release it with
  that core. In the other direction, `ankusa_redis` 0.2.x does not implement the
  new arities and cannot be used with it.
- Every write is one Lua script. `insert` and `replace` are conditioned on the
  version the caller validated against and answer `{:error, :stale}` — after the
  node has reloaded — when Redis holds another; `delete` and `put_ip_rules` bump
  the version in the same step. `insert` checks `max_routes` inside the script.
- A node reloads when Redis's version *differs* from its own, not only when it is
  higher, and reads the version and the definitions in one transaction. A pub/sub
  message is now a nudge to check the version; its payload is ignored.
- The node subscribes, and waits for Redis to confirm the subscription (Redix's
  `subscribe/3` returns before Redis has registered anything), before it loads.
  Seeding is one atomic script, and the supervisor is `:rest_for_one` with the
  pub/sub connection ahead of the state process. Writes wait up to 20s for the
  state process (Redis's own timeout is 5s per command) instead of giving up at
  the 5s `GenServer.call` default.
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

[Unreleased]: https://github.com/jamescarr/ankusa/compare/ankusa_redis-v0.4.0...HEAD
[0.4.0]: https://github.com/jamescarr/ankusa/compare/ankusa_redis-v0.3.0...ankusa_redis-v0.4.0
[0.3.0]: https://github.com/jamescarr/ankusa/compare/ankusa_redis-v0.2.4...ankusa_redis-v0.3.0
[0.2.4]: https://github.com/jamescarr/ankusa/releases/tag/ankusa_redis-v0.2.4
