# ankusa_redis

Redis adapters for the Ankusa webhook ingestion framework: the multi-node
route store (`Ankusa.Routes.Store.Redis`), the multi-node source store
(`Ankusa.SourceStore.Redis`), the shared SCIM directory
(`Ankusa.Directory.Store.Redis`), and the pub/sub sink (`Ankusa.Sink.Redis`).

This package exists so `ankusa` core stays free of the `:redix` dependency. It
is needed only by deployments that keep route definitions, API-managed
sources or SCIM-provisioned users in Redis so every node serves the same set (a
single node can use core's in-memory route store, `SourceStore.Persistent` and
`Directory.Store.Persistent`), or that deliver hooks to a Redis pub/sub channel.

## Installation

```elixir
def deps do
  [
    {:ankusa, "~> 0.5"},
    {:ankusa_redis, "~> 0.4"}
  ]
end
```

## Route store

`Ankusa.Routes.Store.Redis` implements the `Ankusa.Routes.Store` behaviour:
definitions live in one Redis hash, a version counter plus pub/sub invalidates
every node's compiled copy, and the guard keeps reading its in-memory snapshot —
never Redis — per request.

Point `routes.store` at the adapter with the Redis URL and, in a multi-node
deployment, an explicit `namespace` that every node shares:

```elixir
config =
  Ankusa.Config.new(
    routes: [
      enabled: true,
      store: {Ankusa.Routes.Store.Redis, url: "redis://localhost:6379", namespace: "ankusa:routes"}
    ]
  )
```

See `Ankusa.Routes.Store.Redis` for the key layout, invalidation, and what
happens when Redis is unavailable.

## Source store

`Ankusa.SourceStore.Redis` implements the writable `Ankusa.SourceStore`
callbacks: sources created, updated or deleted through any node's admin API
(`/v1/tenants/{tenant}/sources`) live in one Redis hash, and every node with
the same `namespace` serves them. Ingest reads each node's ETS mirror, never
Redis; a version counter plus pub/sub keeps the mirrors current, with
`tick_ms` as the safety net. Seeds from `sources:` stay config-only.

```elixir
config =
  Ankusa.Config.new(
    source_store:
      {Ankusa.SourceStore.Redis,
       url: "redis://localhost:6379",
       namespace: "ankusa:sources",
       decoder: &MyApp.Sources.decode!/2}
  )
```

The `decoder` turns a stored JSON spec into `Ankusa.Source` options, as for
`Ankusa.SourceStore.Persistent`. In the server image this is
`source_store: {type: redis, url: ...}`.

## SCIM directory

`Ankusa.Directory.Store.Redis` implements `Ankusa.Directory.Store`: the users and
groups an identity provider provisions over SCIM (`scim.enabled`), shared by
every node configured with the same `namespace`, so a user deactivated through
one node is refused by all of them. Nothing is cached (authenticating a request
reads the directory), every mutation is one Lua script (uniqueness, version
check, indexes and membership together), and a Redis error is
`{:error, :unavailable}`, which the admin API answers `503`:

```elixir
config =
  Ankusa.Config.new(
    admin: %{enabled: true},
    auth: %{enabled: true, verifier: {Ankusa.Auth.OIDC, issuer: "...", audience: "ankusa"}},
    scim: %{
      enabled: true,
      token: System.fetch_env!("ANKUSA_SCIM_TOKEN"),
      directory:
        {Ankusa.Directory.Store.Redis,
         url: "redis://localhost:6379", namespace: "ankusa:directory"}
    }
  )
```

On Redis Cluster put a hash tag in the namespace (`"{ankusa}:directory"`): the
write scripts touch several keys derived from it, which must share a slot. See
`Ankusa.Directory.Store.Redis` for the key layout.

## Pub/sub sink

`Ankusa.Sink.Redis` `PUBLISH`es each delivered hook to a channel, carrying the
same `Ankusa.Sink.Message` JSON the broker sinks publish. Pub/sub keeps no copy,
so a publish nobody is subscribed to is an error (retried, then dead-lettered),
and the sink never counts as durable for `wal.type: none`:

```elixir
config :ankusa,
  sources: %{
    "orders" => [
      sinks: [
        {Ankusa.Sink.Redis,
         url: "redis://localhost:6379",
         channel: "ankusa.events"}
      ]
    ]
  }
```

Sinks belong to a source. `Ankusa.Config.new/1` takes the same `sources` for a
non-default instance.

See `Ankusa.Sink.Redis` moduledoc for the option list, the error contract, and
the connection lifecycle.

## Testing

Requires a live Redis:

```sh
docker compose up -d --wait
mix test
docker compose down -v
```

`REDIS_URL` overrides the default `redis://localhost:6399`; every suite — the
route store's, the source store's and `Ankusa.Sink.Redis`'s — runs against it.
