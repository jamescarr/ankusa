# ankusa_rabbitmq

RabbitMQ sink adapter for the Ankusa webhook ingestion framework.

This package exists so `ankusa` core stays free of the `:amqp` dependency
(and everything it pulls in — `amqp_client`, `rabbit_common`). Only
deployments that opt into a RabbitMQ sink need this package.

## Installation

```elixir
def deps do
  [
    {:ankusa, "~> 0.1"},
    {:ankusa_rabbitmq, "~> 0.1"}
  ]
end
```

## Usage

See `Ankusa.Sink.RabbitMQ` moduledoc for configuration and semantics.

```elixir
config :ankusa, :default,
  sinks: [
    {Ankusa.Sink.RabbitMQ,
     exchange: "ankusa.events",
     url: "amqp://guest:guest@localhost:5672"}
  ]
```

## Testing

Requires a live RabbitMQ:

```sh
docker compose up -d --wait
mix test
docker compose down -v
```

See [`https://hexdocs.pm/ankusa`](https://hexdocs.pm/ankusa) for the full
Ankusa documentation.
