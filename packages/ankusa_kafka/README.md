# ankusa_kafka

Kafka sink adapter for the Ankusa webhook ingestion framework.

This package exists so `ankusa` core stays free of the `:brod` dependency (and
the `crc32cer` NIF it pulls in). Only deployments that opt into a Kafka sink
need this package.

## Installation

```elixir
def deps do
  [
    {:ankusa, "~> 0.6"},
    {:ankusa_kafka, "~> 0.6"}
  ]
end
```

## Usage

See `Ankusa.Sink.Kafka` moduledoc for configuration and semantics.

```elixir
config :ankusa,
  sources: %{
    "orders" => [
      sinks: [
        {Ankusa.Sink.Kafka,
         brokers: ["localhost:9092"],
         topic: "ankusa.events"}
      ]
    ]
  }
```

Sinks belong to a source. `Ankusa.Config.new/1` takes the same `sources` for a
non-default instance.

## The intake log of `wal.type: dual`

`Ankusa.Intake.Kafka` is the adapter of the dual-commit intake log
([ADR 0002](https://github.com/jamescarr/ankusa/blob/main/docs/adr/0002-dual-commit-intake-log.md)): a hook is acked only after it is fsynced on the node
**and** confirmed in a replicated topic, so a lost host loses nothing.

```elixir
config :ankusa, :config,
  wal: :dual,
  intake: %{
    type: :kafka,
    node_id: "ankusa-a",
    kafka: [brokers: ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"]]
  },
  backup: %{enabled: true, max_age_ms: 300_000, blob_store: {Ankusa.BlobStore.S3, bucket: "backups"}}
```

The topic (`ankusa.intake.<fleet>`) is the operator's, and the adapter never
creates one. `verify/1` refuses it at boot unless it has replication factor 3 or
more, `min.insync.replicas` 2 or more, `unclean.leader.election.enable=false`, a
`retention.ms` of at least twice `backup.max_age_ms` and a `max.message.bytes` of
at least `intake.max_record_bytes`:

```sh
kafka-topics.sh --bootstrap-server kafka-1:9092 --create --topic ankusa.intake.default \
  --partitions 3 --replication-factor 3 --config min.insync.replicas=2 \
  --config unclean.leader.election.enable=false --config retention.ms=3600000 \
  --config max.message.bytes=11000000
```

See [`docs/deployment.md`](https://github.com/jamescarr/ankusa/blob/main/docs/deployment.md#host-loss-and-the-intake-log) for sizing, the replacement
procedure and `recover`.

## Testing

The compose file has two clusters: a single Redpanda broker (the sink's tests) and
a three-broker KRaft cluster (the intake adapter's tests, which stop and restart
brokers with `docker compose`, so they need the docker CLI):

```sh
docker compose up -d --wait
mix test
docker compose down -v
```

`mix run bench/partition_bench.exs` measures what one partition of that cluster
carries at the plan's per-node rate.
