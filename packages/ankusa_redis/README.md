# ankusa_redis

Redis adapters for the Ankusa webhook ingestion framework: the multi-node
route store (`Ankusa.Routes.Store.Redis`) and the pub/sub sink
(`Ankusa.Sink.Redis`).

This package exists so `ankusa` core stays free of the `:redix` dependency. It
is needed only by deployments that keep route definitions in Redis so every edge
node enforces the same set (a single node can use core's default in-memory store
with a `routes.seed`), or that deliver hooks to a Redis pub/sub channel.

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

`REDIS_URL` overrides the default `redis://localhost:6399`; both suites — the
route store's and `Ankusa.Sink.Redis`'s — run against it.
