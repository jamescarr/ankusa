# ankusa_postgres

Shared, multi-node `Ankusa.WAL` adapter backed by Postgres, for the Ankusa
webhook ingestion framework.

This package exists so `ankusa` core stays free of the `:postgrex`
dependency. Only deployments that opt into a shared, multi-node WAL need this
package — a single-node deployment is fine with the built-in `WAL.DiskLog`.

## Installation

```elixir
def deps do
  [
    {:ankusa, "~> 0.1"},
    {:ankusa_postgres, "~> 0.1"}
  ]
end
```

## Usage

See `Ankusa.WAL.Postgres` moduledoc for configuration and semantics.

```elixir
config :ankusa,
  wal: {Ankusa.WAL.Postgres, hostname: "localhost", port: 5433,
        username: "ankusa", password: "ankusa", database: "ankusa_dev",
        pool_size: 10}
```

## Multi-node deployments

Every node calls `Ankusa.WAL.Postgres.Migration.run!/1` at start by default
(`migrate: true`), and concurrent `CREATE TABLE IF NOT EXISTS` across nodes
can race in Postgres. For a fleet: run `Ankusa.WAL.Postgres.Migration.run!/1`
once — a release task or a one-shot Job — then set `migrate: false` on every
node. See
[`examples/oban-consumer`](https://github.com/jamescarr/ankusa/tree/main/examples/oban-consumer)
for a worked example of this pattern (a Kubernetes `Job` runs the migration,
then the ingest fleet boots with `migrate: false`).

## Testing

Requires a live Postgres:

```sh
docker compose up -d --wait
mix test
docker compose down -v
```

See [`https://hexdocs.pm/ankusa`](https://hexdocs.pm/ankusa) for the full
Ankusa documentation.
