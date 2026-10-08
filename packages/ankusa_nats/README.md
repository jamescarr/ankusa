# ankusa_nats

NATS JetStream sink adapter for the Ankusa webhook ingestion framework.

This package exists so `ankusa` core stays free of the `:gnat` dependency
(and everything it pulls in: `jason`, `nkeys`, `nimble_parsec`). Only
deployments that opt into a NATS sink need this package.

## Installation

```elixir
def deps do
  [
    {:ankusa, "~> 0.5"},
    {:ankusa_nats, "~> 0.5"}
  ]
end
```

## Usage

See `Ankusa.Sink.NATS` moduledoc for configuration and semantics. The stream
is yours to create. The sink publishes to a subject and never creates or
updates a stream:

```sh
nats stream add ANKUSA --subjects="ankusa.>"
```

```elixir
config :ankusa,
  sources: %{
    "orders" => [
      sinks: [
        {Ankusa.Sink.NATS,
         servers: ["localhost:4222"],
         subject: "ankusa.stripe"}
      ]
    ]
  }
```

Sinks belong to a source. `Ankusa.Config.new/1` takes the same `sources` for a
non-default instance.

## Testing

Requires a live NATS server with JetStream enabled:

```sh
docker compose up -d --wait
mix test
docker compose down -v
```

See [`https://hexdocs.pm/ankusa`](https://hexdocs.pm/ankusa) for the full
Ankusa documentation.
