# Changelog

All notable changes to `ankusa_redis` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.7.0] - 2026-10-30

### Changed

- Version aligned with `ankusa` 0.7: the Elixir packages (`ankusa`, the
  adapters, the server image) now share major.minor
  ([`docs/releasing.md`](https://github.com/jamescarr/ankusa/blob/main/docs/releasing.md#versions-the-elixir-family-shares-majorminor)). No code change since
  0.6.0, which already requires `ankusa ~> 0.7`.

## [0.6.0] - 2026-10-28

### Added

- `:max_message_bytes` on `Ankusa.Sink.Redis` (default 512 MiB, Redis's bulk
  limit; DEL-5): the size preflight of `Ankusa.Sink.Messaging`. Requires an
  `ankusa` release with the `max_message_bytes` sink callback; publish core
  first.
- **Streaming loads and incremental writes for 10⁶ entries (TEN-9).** The route
  store and the source store read their hash a page at a time (`HSCAN … COUNT
  1000`, `Ankusa.Redis.Scan`) instead of one `HGETALL`, with `timeout_ms`
  (default 5 s, every command, a page included) and `load_timeout_ms` (default
  5 min, the whole load) as store options. The version is read before and after
  a scan (an `HSCAN` is not a snapshot): a scan that saw it move is repeated up
  to three times and then kept with the earlier version, so the next broadcast
  or tick loads again. A write that arrives in step with a node's version is
  applied to the route snapshot as one row change (`Snapshot.mutate/3`) instead
  of republishing the whole table; only a write that raced a peer's reloads it.
  A route table that does not fit `routes.memory_budget_bytes` refuses the boot
  (and keeps the serving table on a reload). The `GenServer.call` timeout of a
  write is `3 × timeout_ms + load_timeout_ms`, since a write that raced a peer
  reloads the table inside it.
- **`Ankusa.RateLimiter.Store.Redis` (TEN-6)**: the fleet store behind core's
  `rate_limits.scope: :fleet` and `dedupe.scope: :fleet`, implementing
  `Ankusa.RateLimiter.Store` and `Ankusa.Dedupe.Store`. One `EVALSHA` per hook runs
  GCRA on the tenant's hook and byte buckets (Redis's own clock) and claims the
  provider event key (`SET NX PX`, pending) in one step; a denied bucket writes
  nothing and claims nothing, a taken key spends no bucket. The claim is promoted
  to `committed:<hook id>` (or released, compare-and-delete) after the commit by
  pipelined `settle/3` batches. Scripts are loaded at boot and a `NOSCRIPT` is
  answered with `EVAL` inside the call budget; every command carries the caller's
  `timeout_ms`; connecting never blocks the boot. Options: `url`, `namespace`
  (default `ankusa:fleet:<instance>`), `pool_size` (4). Requires the `ankusa`
  release with fleet scopes.
- `Ankusa.Routes.Store.Redis` follows core's tenant-aware routes (TEN-4):
  `delete/3` takes the tenant the delete is made for, and the delete script
  reads the stored route's owner in the same step and answers `:not_found` for
  another tenant's route, so a tenant-scoped delete can never remove a route that
  was re-created under another tenant between the caller's read and the delete.
  A definition is stored with `Ankusa.Routes.Route.to_stored_json/1`, which
  leaves out `tenant_id` while it is `"default"` and `source_id` while it is
  absent, so a node from before tenant routes still reads a table that has none;
  once a route owned by a tenant is stored, an older node refuses the reload and
  keeps serving what it had. Requires the `ankusa` release with tenant routes.
- `Ankusa.SourceStore.Redis.ready?/1` (core's optional `ready?/1` source-store
  callback): whether the mirror has loaded from Redis, which core's lifecycle
  outbox waits for before it settles a source change a killed node left
  pending. Requires the `ankusa` release with the lifecycle outbox.
- `Ankusa.SourceStore.Redis` encrypts the secret-bearing fields of each stored
  source spec with core's `secrets.kek` (`Ankusa.Crypto.Envelope`), so Redis,
  its persistence files and its backups hold no verify secret or sink
  credential in the clear. Nodes decrypt as they load; an update that omits the
  secret merges Redis's decrypted copy. An entry no configured key opens stops
  a node's first boot (the log names it) and, on a later reload, keeps the
  mirror. `Ankusa.SourceStore.Redis.rewrap/1` moves plaintext and
  previous-key entries to the current key, compare-and-set in batches, safe
  beside other nodes. Requires the `ankusa` release with `secrets`.
- `Sink.Redis`: a hook of an `ordered: true` sink carries the lane's key as
  `ordering_key` in the message JSON (consumer-side ordering, ADR 0004).

### Changed

- Requires `ankusa ~> 0.7` (`Ankusa.Sink.Messaging`, `Ankusa.Sink.Connections`);
  publish core first.
- `Ankusa.Sink.Redis` is built on core's `Ankusa.Sink.Messaging` and
  `Ankusa.Sink.Connections`. What it publishes is unchanged.

## [0.5.0] - 2026-10-14

### Added

- `Ankusa.SourceStore.Redis`: API-managed, tenant-scoped sources kept in Redis
  and shared by every node configured with the same `namespace` (a hash of
  sources and a version counter; Lua-scripted writes; pub/sub invalidation with
  a `tick_ms` safety net). Reads go to an ETS mirror that survives a restart of
  the store's state process. Seeds stay config-only. A first boot against an
  unreachable Redis fails; a write during an outage answers
  `{:error, :store_unavailable}` (the admin API's `503 store_unavailable`).
- `Ankusa.Sink.Redis`: `:idle_timeout_ms` (default `600_000`, `0` never, at
  least `:publish_timeout_ms` plus 1 s): a connection with no delivery for that
  long is closed (`Ankusa.Sink.Reaper`, requires core's) and the next delivery
  opens a new one.

### Changed

- Requires `ankusa ~> 0.6` (the ETS route snapshot API, and the source table
  `SourceStore.Redis` shares with `SourceStore.Persistent`); publish core first.
- The Redis route store keeps serving its last known table when the
  namespace's version key disappears (a flushed or evicted Redis) instead of
  publishing an empty table that rejects every webhook; it logs a warning
  once and reloads when the namespace is written again. It publishes through
  core's ETS route snapshot and drops retired generations on request.
- The Redis sink's connection dials on its own (`sync_connect: false`), so a
  dead Redis never blocks the supervisor that starts it.
- `Ankusa.Routes.Store.Redis` and `Ankusa.SourceStore.Redis` start with
  `[instance: instance]` (core's new store contract) and read their options
  from `Ankusa.config(instance)`: no child spec of theirs carries the config,
  the seeds or the decoder. Their Redix connections get the URL's password as
  Redix's `{m, f, a}` form, so neither a child spec nor Redix's state holds it.
- `SourceStore.Redis` refuses a write of a shared source with no verifier
  under a resolver that takes the tenant from the request unless it sets
  `trust_url_tenant: true` (`{:error, :invalid, message}`), and names one
  already in Redis in a warning at first boot.
- A route store restarted after the instance has published a table resumes from
  that table without waiting on Redis (its connections dial in the background),
  so a store that crashes during a Redis outage does not hold the edge down. It
  loads once the subscription is confirmed and on every tick. A first boot
  still fails loudly.
- `Ankusa.Sink.Redis` registers its connection under a digest of the URL
  instead of the URL, and starts Redix with the URL's password as an
  `{m, f, a}` over a closure: the password is in no registry key, child spec
  or Redix state.

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

[Unreleased]: https://github.com/jamescarr/ankusa/compare/ankusa_redis-v0.7.0...HEAD
[0.7.0]: https://github.com/jamescarr/ankusa/compare/ankusa_redis-v0.6.0...ankusa_redis-v0.7.0
[0.6.0]: https://github.com/jamescarr/ankusa/compare/ankusa_redis-v0.5.0...ankusa_redis-v0.6.0
[0.5.0]: https://github.com/jamescarr/ankusa/compare/ankusa_redis-v0.4.0...ankusa_redis-v0.5.0
[0.4.0]: https://github.com/jamescarr/ankusa/compare/ankusa_redis-v0.3.0...ankusa_redis-v0.4.0
[0.3.0]: https://github.com/jamescarr/ankusa/compare/ankusa_redis-v0.2.4...ankusa_redis-v0.3.0
[0.2.4]: https://github.com/jamescarr/ankusa/releases/tag/ankusa_redis-v0.2.4
