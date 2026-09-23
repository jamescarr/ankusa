# ankusa_kafka

Kafka sink adapter for the Ankusa webhook ingestion framework.

This package exists so `ankusa` core stays free of the `:brod` dependency (and
the `crc32cer` NIF it pulls in). Only deployments that opt into a Kafka sink
need this package.

## Installation

```elixir
def deps do
  [
    {:ankusa, "~> 0.1"},
    {:ankusa_kafka, "~> 0.1"}
  ]
end
```

## Usage

See `Ankusa.Sink.Kafka` moduledoc for configuration and semantics.

```elixir
config :ankusa, :default,
  sinks: [
    {Ankusa.Sink.Kafka,
     brokers: ["localhost:9092"],
     topic: "ankusa.events"}
  ]
```

## Testing

Requires a live Redpanda/Kafka broker:

```sh
docker compose up -d --wait
mix test
docker compose down -v
```
