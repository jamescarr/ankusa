# ankusa_rabbitmq

RabbitMQ sink adapter for the Ankusa webhook ingestion framework.

This package exists so `ankusa` core stays free of the `:amqp` dependency
(and everything it pulls in: `amqp_client`, `rabbit_common`). Only
deployments that opt into a RabbitMQ sink need this package.

## Installation

```elixir
def deps do
  [
    {:ankusa, "~> 0.5"},
    {:ankusa_rabbitmq, "~> 0.5"}
  ]
end
```

## Usage

See `Ankusa.Sink.RabbitMQ` moduledoc for configuration and semantics.

```elixir
config :ankusa,
  sources: %{
    "orders" => [
      sinks: [
        {Ankusa.Sink.RabbitMQ,
         exchange: "ankusa.events",
         url: "amqp://guest:guest@localhost:5672"}
      ]
    ]
  }
```

Sinks belong to a source. `Ankusa.Config.new/1` takes the same `sources` for a
non-default instance.

## Testing

Requires a live RabbitMQ:

```sh
docker compose up -d --wait
mix test
docker compose down -v
```

See [`https://hexdocs.pm/ankusa`](https://hexdocs.pm/ankusa) for the full
Ankusa documentation.
