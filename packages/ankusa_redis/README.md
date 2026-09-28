# ankusa_redis

Redis-backed route store for the Ankusa webhook ingestion framework.

This package exists so `ankusa` core stays free of the `:redix` dependency. It
is needed only by deployments that keep route definitions in Redis so every edge
node enforces the same set; a single node can use core's default in-memory store
with a `routes.seed`.

`Ankusa.Routes.Store.Redis` implements the `Ankusa.Routes.Store` behaviour:
definitions live in one Redis hash, a version counter plus pub/sub invalidates
every node's compiled copy, and the guard keeps reading its in-memory snapshot —
never Redis — per request.

## Installation

```elixir
def deps do
  [
    {:ankusa, "~> 0.2"},
    {:ankusa_redis, "~> 0.2"}
  ]
end
```

## Usage

Point `routes.store` at the adapter with the Redis URL and, in a multi-node
deployment, an explicit `namespace` that every node shares:

```elixir
config =
  Ankusa.Config.new(
    routes: [
      enabled: true,
      admin: [token: System.fetch_env!("ANKUSA_ROUTES_ADMIN_TOKEN")],
      store: {Ankusa.Routes.Store.Redis, url: "redis://localhost:6379", namespace: "ankusa:routes"}
    ]
  )
```

See `Ankusa.Routes.Store.Redis` for the key layout, invalidation, and what
happens when Redis is unavailable.

## Testing

Requires a live Redis:

```sh
docker compose up -d --wait
mix test
docker compose down -v
```

`REDIS_URL` overrides the default `redis://localhost:6399`.
