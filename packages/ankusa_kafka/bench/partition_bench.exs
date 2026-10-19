# How much one Kafka partition carries through `Ankusa.Intake.Kafka`: the number
# behind "one partition per node" (ADR 0002, section 6: the record key is the node
# id, the partition is the node's, stamped once).
#
#     docker compose up -d --wait kafka-1 kafka-2 kafka-3
#     mix run bench/partition_bench.exs
#
# Against this package's three-broker KRaft compose (replication factor 3,
# `min.insync.replicas=2`, `acks=all`), one partition, 10 KB records (the plan's
# mean hook body), batches of up to 256 records (the writer's batch limit), the
# way the node publishes them: `Ankusa.Intake.Kafka.publish_batch/3`, one
# produce on the wire per partition.
#
#   * paced: the plan's per-node rate, 1 000 records/s (about 10 MB/s), in
#     batches of 10 every 10 ms, for PACED_SECONDS: the confirm latency a node sees
#     at its own SLO rate
#   * saturated: SATURATED_PUBLISHERS (2, the batcher's partitions) publishers each
#     with a 256-record batch in flight, as fast as the partition confirms, for
#     SATURATED_SECONDS: what one partition can take
#
# Headroom is saturated MB/s over the paced MB/s. The figures are those of
# whatever machine and docker the bench runs on (the brokers share its disk and
# CPU), so read them as an order of magnitude: DUR-1.b.b records them in the plan.
#
# Env knobs: KAFKA_INTAKE_BROKERS (localhost:29092,localhost:29093,localhost:29094),
# RECORD_BYTES (10000), PACED_SECONDS (20), SATURATED_SECONDS (20),
# SATURATED_PUBLISHERS (2).

Logger.configure(level: :warning)

brokers =
  "KAFKA_INTAKE_BROKERS"
  |> System.get_env("localhost:29092,localhost:29093,localhost:29094")
  |> String.split(",")

record_bytes = String.to_integer(System.get_env("RECORD_BYTES", "10000"))
paced_seconds = String.to_integer(System.get_env("PACED_SECONDS", "20"))
saturated_seconds = String.to_integer(System.get_env("SATURATED_SECONDS", "20"))
publishers = String.to_integer(System.get_env("SATURATED_PUBLISHERS", "2"))

hosts =
  Enum.map(brokers, fn b ->
    [h, p] = String.split(b, ":")
    {String.to_charlist(h), String.to_integer(p)}
  end)

topic = "ankusa.intake.bench.#{System.unique_integer([:positive])}"

:ok =
  :brod.create_topics(
    hosts,
    [
      %{
        name: topic,
        num_partitions: 1,
        replication_factor: 3,
        assignments: [],
        configs: [
          %{name: "min.insync.replicas", value: "2"},
          %{name: "unclean.leader.election.enable", value: "false"},
          %{name: "retention.ms", value: "3600000"},
          %{name: "max.message.bytes", value: "11000000"}
        ]
      }
    ],
    %{timeout: 15_000}
  )

System.at_exit(fn _ -> :brod.delete_topics(hosts, [topic], 5_000) end)

instance = :partition_bench

config =
  Ankusa.Config.new(
    instance: instance,
    data_dir: Path.join(System.tmp_dir!(), "ankusa_partition_bench"),
    port: 0,
    roles: [:edge],
    wal: :dual,
    intake: %{
      type: :kafka,
      node_id: "bench-node",
      topic: topic,
      publish_timeout_ms: 10_000,
      kafka: [brokers: brokers]
    },
    backup: %{enabled: true, interval_ms: 30_000, max_age_ms: 60_000}
  )

Ankusa.put_config(config)
{:ok, _} = Supervisor.start_link([Ankusa.Intake.Kafka.child_spec(instance: instance)], strategy: :one_for_one)

wait = fn wait, tries ->
  cond do
    Ankusa.Intake.Kafka.ready?(instance) -> :ok
    tries == 0 -> raise "the intake adapter never became ready"
    true -> Process.sleep(500) && wait.(wait, tries - 1)
  end
end

wait.(wait, 60)

defmodule Bench.Partition do
  alias Ankusa.Intake.Kafka

  @meta %{id: "bench", tenant: "default", source: "bench", incarnation: "0"}

  def batch(first_seq, count, bytes) do
    bin = :binary.copy("x", bytes)
    for seq <- first_seq..(first_seq + count - 1), do: {seq, bin, @meta}
  end

  # One confirmed publish: the confirm latency in microseconds.
  def publish(instance, topic, records) do
    started = System.monotonic_time(:microsecond)

    {:ok, _} =
      Kafka.publish_batch(instance, records,
        timeout: 10_000,
        node: "bench-node",
        topic: topic,
        partition: 0
      )

    System.monotonic_time(:microsecond) - started
  end

  def percentile(samples, p) do
    sorted = Enum.sort(samples)
    Enum.at(sorted, min(length(sorted) - 1, max(0, ceil(p * length(sorted)) - 1))) / 1000
  end
end

# warm the producer and the connection
Bench.Partition.publish(instance, topic, Bench.Partition.batch(0, 10, record_bytes))

# paced: 1 000 records/s as 100 batches of 10 a second
paced_batches = paced_seconds * 100
paced_started = System.monotonic_time(:millisecond)

paced =
  for i <- 0..(paced_batches - 1) do
    due = paced_started + i * 10
    wait_ms = due - System.monotonic_time(:millisecond)
    if wait_ms > 0, do: Process.sleep(wait_ms)
    Bench.Partition.publish(instance, topic, Bench.Partition.batch(1 + i * 10, 10, record_bytes))
  end

paced_elapsed = (System.monotonic_time(:millisecond) - paced_started) / 1000
paced_mb = paced_batches * 10 * record_bytes / 1_048_576 / paced_elapsed

IO.puts(
  "paced      : #{round(paced_batches * 10 / paced_elapsed)} records/s, " <>
    "#{Float.round(paced_mb, 1)} MiB/s, confirm p50 #{Float.round(Bench.Partition.percentile(paced, 0.5), 1)} ms, " <>
    "p99 #{Float.round(Bench.Partition.percentile(paced, 0.99), 1)} ms, " <>
    "max #{Float.round(Bench.Partition.percentile(paced, 1.0), 1)} ms"
)

# saturated: publishers with a full batch in flight each
deadline = System.monotonic_time(:millisecond) + saturated_seconds * 1000
sat_started = System.monotonic_time(:millisecond)

results =
  1..publishers
  |> Task.async_stream(
    fn publisher ->
      Stream.repeatedly(fn -> nil end)
      |> Enum.reduce_while({0, []}, fn _, {sent, latencies} ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:halt, {sent, latencies}}
        else
          first = 1_000_000 * publisher + sent
          latency = Bench.Partition.publish(instance, topic, Bench.Partition.batch(first, 256, record_bytes))
          {:cont, {sent + 256, [latency | latencies]}}
        end
      end)
    end,
    max_concurrency: publishers,
    timeout: :infinity
  )
  |> Enum.map(fn {:ok, result} -> result end)

sat_elapsed = (System.monotonic_time(:millisecond) - sat_started) / 1000
records = results |> Enum.map(&elem(&1, 0)) |> Enum.sum()
latencies = Enum.flat_map(results, &elem(&1, 1))
sat_mb = records * record_bytes / 1_048_576 / sat_elapsed

IO.puts(
  "saturated  : #{round(records / sat_elapsed)} records/s, #{Float.round(sat_mb, 1)} MiB/s " <>
    "(#{publishers} publishers x 256-record batches), confirm p50 " <>
    "#{Float.round(Bench.Partition.percentile(latencies, 0.5), 1)} ms, p99 " <>
    "#{Float.round(Bench.Partition.percentile(latencies, 0.99), 1)} ms"
)

IO.puts("headroom   : #{Float.round(sat_mb / paced_mb, 1)}x the per-node SLO rate (#{record_bytes} B records)")
