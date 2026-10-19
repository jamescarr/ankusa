defmodule Ankusa.Intake.KafkaTest do
  @moduledoc """
  `Ankusa.Intake.Kafka` against the three-broker KRaft cluster of this package's
  `docker-compose.yml` (ADR 0002, "The Kafka adapter"): `docker compose up -d
  --wait` in this directory. `KAFKA_INTAKE_BROKERS` (default
  `localhost:29092,localhost:29093,localhost:29094`) points elsewhere. The broker
  outage test stops and restarts brokers with `docker compose`, so it needs the
  docker CLI and the compose file's service names (`kafka-1`..`kafka-3`).
  """

  use ExUnit.Case, async: false

  @moduletag capture_log: true

  require Record

  alias Ankusa.Intake.Kafka

  Record.defrecordp(
    :kafka_message,
    Record.extract(:kafka_message, from_lib: "kafka_protocol/include/kpro_public.hrl")
  )

  @brokers System.get_env(
             "KAFKA_INTAKE_BROKERS",
             "localhost:29092,localhost:29093,localhost:29094"
           )
           |> String.split(",")
  @hosts Enum.map(@brokers, fn b ->
           [h, p] = String.split(b, ":")
           {String.to_charlist(h), String.to_integer(p)}
         end)

  @max_age_ms 60_000
  @good_topic [
    partitions: 1,
    replication_factor: 3,
    configs: %{
      "min.insync.replicas" => "2",
      "unclean.leader.election.enable" => "false",
      "retention.ms" => Integer.to_string(10 * @max_age_ms),
      "max.message.bytes" => "11000000"
    }
  ]

  setup do
    instance = :"intake_kafka_#{System.unique_integer([:positive])}"
    on_exit(fn -> :persistent_term.erase({Ankusa, :config, instance}) end)
    %{instance: instance}
  end

  # ── helpers ───────────────────────────────────────────────────────────────

  defp create_topic(opts \\ @good_topic) do
    topic = "ankusa.intake.test.#{System.unique_integer([:positive])}"

    configs =
      for {name, value} <- Keyword.fetch!(opts, :configs), do: %{name: name, value: value}

    spec = %{
      name: topic,
      num_partitions: Keyword.fetch!(opts, :partitions),
      replication_factor: Keyword.fetch!(opts, :replication_factor),
      assignments: [],
      configs: configs
    }

    :ok = :brod.create_topics(@hosts, [spec], %{timeout: 15_000})
    on_exit(fn -> :brod.delete_topics(@hosts, [topic], 5_000) end)

    # every broker has to know the topic before a test asks one of them about it
    for host <- @hosts do
      wait(fn -> topic_visible?(host, topic) end, 30_000, "#{topic} to reach #{inspect(host)}")
    end

    topic
  end

  defp topic_visible?(host, topic) do
    with {:ok, metadata} <- :brod.get_metadata([host], [topic]),
         [t] <- :kpro.find(:topics, metadata),
         :no_error <- :kpro.find(:error_code, t) do
      Enum.all?(:kpro.find(:partitions, t), fn p ->
        leader = :kpro.find(:leader_id, p, nil) || :kpro.find(:leader, p, nil)
        is_integer(leader) and leader >= 0
      end)
    else
      _ -> false
    end
  end

  defp configure(instance, topic, node_id \\ "node-a", intake \\ %{}) do
    config =
      Ankusa.Config.new(
        instance: instance,
        data_dir: Path.join(System.tmp_dir!(), "ankusa_intake_kafka_#{instance}"),
        port: 0,
        roles: [:edge],
        wal: :dual,
        intake:
          Map.merge(
            %{
              type: :kafka,
              node_id: node_id,
              topic: topic,
              publish_timeout_ms: 2_000,
              kafka: [brokers: @brokers, health_interval_ms: 200]
            },
            intake
          ),
        backup: %{enabled: true, interval_ms: 30_000, max_age_ms: @max_age_ms}
      )

    Ankusa.put_config(config)
    config
  end

  defp start_adapter(instance) do
    start_supervised!(Kafka.child_spec(instance: instance))
    wait(fn -> Kafka.ready?(instance) end, 30_000, "the intake adapter to be ready")
  end

  # The client is up (a topic that does not exist keeps `ready?/1` false).
  defp start_adapter_without_ready(instance) do
    start_supervised!(Kafka.child_spec(instance: instance))
    wait(fn -> Process.whereis(Kafka.client(instance)) != nil end, 15_000, "the client")
  end

  # The adapter reads its settings once, when it starts.
  defp restart_adapter(instance) do
    _ = stop_supervised({Kafka, instance})
    start_adapter_without_ready(instance)
  end

  defp wait(fun, timeout_ms, what) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait(fun, deadline, what)
  end

  defp do_wait(fun, deadline, what) do
    cond do
      fun.() -> :ok
      System.monotonic_time(:millisecond) > deadline -> flunk("timed out waiting for #{what}")
      true -> Process.sleep(250) && do_wait(fun, deadline, what)
    end
  end

  defp meta(id), do: %{id: id, tenant: "default", source: "src", incarnation: "0"}

  defp record(seq, bin \\ nil), do: {seq, bin || "hook-#{seq}", meta("id-#{seq}")}

  defp opts(topic, partition, extra \\ []) do
    Keyword.merge([timeout: 2_000, node: "node-a", topic: topic, partition: partition], extra)
  end

  defp fetch_all(topic, partition) do
    {:ok, {_high, messages}} = :brod.fetch(@hosts, topic, partition, 0)

    Enum.map(messages, fn message ->
      %{
        offset: kafka_message(message, :offset),
        key: kafka_message(message, :key),
        value: kafka_message(message, :value),
        headers: Map.new(kafka_message(message, :headers))
      }
    end)
  end

  defp replay(instance, from, node \\ "node-a", partition \\ nil) do
    parent = self()

    result =
      Kafka.replay(
        instance,
        from,
        fn replayed ->
          send(parent, {:replayed, replayed})
          :ok
        end,
        node: node,
        partition: partition
      )

    {result, drain_replayed([])}
  end

  defp drain_replayed(acc) do
    receive do
      {:replayed, replayed} -> drain_replayed([replayed | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp compose(args) do
    {out, status} =
      System.cmd("docker", ["compose", "-f", "docker-compose.yml" | args],
        cd: File.cwd!(),
        stderr_to_stdout: true
      )

    assert status == 0, "docker compose #{Enum.join(args, " ")}: #{out}"
  end

  defp restart_brokers, do: compose(["up", "-d", "--wait", "kafka-1", "kafka-2", "kafka-3"])

  defp leader_of(topic, partition) do
    {:ok, metadata} = :brod.get_metadata(@hosts, [topic])
    [t] = :kpro.find(:topics, metadata)

    t
    |> then(&:kpro.find(:partitions, &1))
    |> Enum.find(fn p ->
      (:kpro.find(:partition_index, p, nil) || :kpro.find(:partition, p, nil)) == partition
    end)
    |> then(fn p -> :kpro.find(:leader_id, p, nil) || :kpro.find(:leader, p, nil) end)
  end

  # ── publish / confirm ─────────────────────────────────────────────────────

  describe "publish_batch" do
    test "is confirmed at quorum: the position is the broker's offset, the record is whole", %{
      instance: instance
    } do
      topic = create_topic()
      configure(instance, topic)
      start_adapter(instance)

      assert {:ok, %{position: 2}} =
               Kafka.publish_batch(instance, [record(1), record(2), record(3)], opts(topic, 0))

      assert {:ok, %{position: 3}} = Kafka.publish_batch(instance, [record(4)], opts(topic, 0))
      assert {:ok, 4} = :brod.resolve_offset(@hosts, topic, 0)

      [first | _] = records = fetch_all(topic, 0)
      assert Enum.map(records, & &1.offset) == [0, 1, 2, 3]
      assert Enum.map(records, & &1.value) == ["hook-1", "hook-2", "hook-3", "hook-4"]

      assert first.key == "node-a"

      assert first.headers == %{
               "ankusa_intake_v" => "1",
               "ankusa_intake_kind" => "hook",
               "ankusa_intake_node" => "node-a",
               "ankusa_intake_seq" => "1",
               "ankusa_intake_id" => "id-1",
               "ankusa_intake_incarnation" => "0",
               "ankusa_intake_tenant" => "default",
               "ankusa_intake_source" => "src"
             }
    end

    test "an empty batch answers the head of the partition", %{instance: instance} do
      topic = create_topic()
      configure(instance, topic)
      start_adapter(instance)

      assert {:ok, %{position: -1}} = Kafka.publish_batch(instance, [], opts(topic, 0))
      assert {:ok, %{position: 0}} = Kafka.publish_batch(instance, [record(1)], opts(topic, 0))
      assert {:ok, %{position: 0}} = Kafka.publish_batch(instance, [], opts(topic, 0))
    end

    test "confirms with one broker down and is unavailable with two down", %{instance: instance} do
      topic = create_topic()
      configure(instance, topic)
      start_adapter(instance)
      on_exit(fn -> restart_brokers() end)

      assert {:ok, _} = Kafka.publish_batch(instance, [record(1)], opts(topic, 0))

      leader = leader_of(topic, 0)
      [follower_a, follower_b] = Enum.reject([1, 2, 3], &(&1 == leader))

      # One follower down: the in-sync set shrinks to two (min.insync.replicas=2),
      # so the produce confirms once the broker has dropped the stopped replica.
      compose(["stop", "kafka-#{follower_a}"])

      wait(
        fn -> match?({:ok, _}, Kafka.publish_batch(instance, [record(2)], opts(topic, 0))) end,
        60_000,
        "a confirm with one broker down"
      )

      # Two down: one replica is left, below min.insync.replicas, and nothing
      # confirms: not_enough_replicas once the in-sync set has shrunk, a missed
      # ack_timeout before it. Either way the adapter says `unavailable`.
      compose(["stop", "kafka-#{follower_b}"])

      for _ <- 1..4 do
        assert {:error, :unavailable} =
                 Kafka.publish_batch(instance, [record(3)], opts(topic, 0))
      end

      restart_brokers()

      wait(
        fn -> match?({:ok, _}, Kafka.publish_batch(instance, [record(4)], opts(topic, 0))) end,
        90_000,
        "a confirm after the brokers returned"
      )
    end

    test "an explicit partition survives a partition-count change", %{instance: instance} do
      topic = create_topic(Keyword.put(@good_topic, :partitions, 3))

      # A node id the hash moves when the topic grows from 3 to 6 partitions.
      node_id =
        Enum.find(1..1_000, fn n ->
          rem(:erlang.phash2("node-#{n}"), 3) != rem(:erlang.phash2("node-#{n}"), 6)
        end)
        |> then(&"node-#{&1}")

      configure(instance, topic, node_id)
      start_adapter(instance)

      assert {:ok, stamped} = Kafka.assign_partition(instance, node_id)
      assert stamped == Kafka.partition_for(node_id, 3)

      assert {:ok, %{position: 0}} =
               Kafka.publish_batch(instance, [record(1)], opts(topic, stamped, node: node_id))

      {_, 0} =
        System.cmd(
          "docker",
          ~w(compose -f docker-compose.yml exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh
             --bootstrap-server localhost:9092 --alter --topic) ++ [topic, "--partitions", "6"],
          cd: File.cwd!()
        )

      refute Kafka.partition_for(node_id, 6) == stamped

      # The client has to see the new partition count; the stamped partition is
      # still the one the node writes.
      assert {:ok, %{position: 1}} =
               Kafka.publish_batch(instance, [record(2)], opts(topic, stamped, node: node_id))

      assert Enum.map(fetch_all(topic, stamped), & &1.headers["ankusa_intake_seq"]) == ["1", "2"]
    end
  end

  # ── replay ────────────────────────────────────────────────────────────────

  describe "replay" do
    test "filters by node, skips unknown kinds and versions, and starts where asked", %{
      instance: instance
    } do
      topic = create_topic()
      configure(instance, topic)
      start_adapter(instance)

      {:ok, _} = Kafka.publish_batch(instance, [record(1), record(2)], opts(topic, 0))
      {:ok, _} = Kafka.publish_batch(instance, [record(1)], opts(topic, 0, node: "node-b"))

      raw = fn kind, version, seq ->
        {:ok, _} =
          :brod.produce_sync_offset(Kafka.client(instance), topic, 0, "node-a", %{
            value: "raw-#{seq}",
            headers: [
              {"ankusa_intake_v", version},
              {"ankusa_intake_kind", kind},
              {"ankusa_intake_node", "node-a"},
              {"ankusa_intake_seq", Integer.to_string(seq)},
              {"ankusa_intake_id", "raw-#{seq}"}
            ]
          })
      end

      raw.("tombstone", "1", 90)
      raw.("hook", "2", 91)
      {:ok, _} = Kafka.publish_batch(instance, [record(3)], opts(topic, 0))

      handler = "intake-kafka-skipped-#{System.unique_integer([:positive])}"
      parent = self()

      :telemetry.attach(
        handler,
        [:ankusa, :intake, :replay],
        fn _event, _measurements, meta, _config -> send(parent, {:telemetry, meta.result}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      assert {{:ok, 5}, replayed} = replay(instance, :earliest)
      assert Enum.map(replayed, & &1.seq) == [1, 2, 3]
      assert Enum.map(replayed, & &1.position) == [0, 1, 5]
      assert Enum.map(replayed, & &1.bin) == ["hook-1", "hook-2", "hook-3"]

      assert Enum.all?(
               replayed,
               &(&1.node == "node-a" and &1.kind == "hook" and &1.version == "1")
             )

      assert Enum.map(replayed, & &1.id) == ["id-1", "id-2", "id-3"]

      assert_received {:telemetry, :skipped_unknown}
      assert_received {:telemetry, :skipped_unknown}
      refute_received {:telemetry, :skipped_unknown}

      # From a position: positions at and after it, and the other node's record
      # (position 2) never.
      assert {{:ok, 5}, [%{seq: 2}, %{seq: 3}]} = replay(instance, 1)
      assert {{:ok, 2}, [%{seq: 1, node: "node-b"}]} = replay(instance, 2, "node-b")
      assert {{:ok, nil}, []} = replay(instance, 6)
    end

    test "a duplicate record is returned twice (it is applied once upstream)", %{
      instance: instance
    } do
      topic = create_topic()
      configure(instance, topic)
      start_adapter(instance)

      # a lost ack and a republish: the same (node, seq) twice, once behind a newer one
      {:ok, _} = Kafka.publish_batch(instance, [record(1)], opts(topic, 0))
      {:ok, _} = Kafka.publish_batch(instance, [record(2)], opts(topic, 0))
      {:ok, _} = Kafka.publish_batch(instance, [record(1)], opts(topic, 0))

      assert {{:ok, 2}, replayed} = replay(instance, :earliest)
      assert Enum.map(replayed, &{&1.seq, &1.id}) == [{1, "id-1"}, {2, "id-2"}, {1, "id-1"}]
    end

    test "a fun that fails stops the replay with an error class", %{instance: instance} do
      topic = create_topic()
      configure(instance, topic)
      start_adapter(instance)
      {:ok, _} = Kafka.publish_batch(instance, [record(1)], opts(topic, 0))

      assert {:error, :unavailable} =
               Kafka.replay(instance, :earliest, fn _ -> {:error, :nope} end,
                 node: "node-a",
                 partition: 0
               )
    end

    test "without a stamped partition every partition is read", %{instance: instance} do
      topic = create_topic(Keyword.put(@good_topic, :partitions, 3))
      configure(instance, topic)
      start_adapter(instance)

      {:ok, _} = Kafka.publish_batch(instance, [record(1)], opts(topic, 0))
      {:ok, _} = Kafka.publish_batch(instance, [record(2)], opts(topic, 2))

      assert {{:ok, 0}, replayed} = replay(instance, :earliest)
      assert replayed |> Enum.map(& &1.seq) |> Enum.sort() == [1, 2]
    end
  end

  # ── verify, earliest, readiness ───────────────────────────────────────────

  describe "verify" do
    test "accepts a topic the promise holds on and describes its settings", %{instance: instance} do
      topic = create_topic()
      configure(instance, topic)
      start_adapter(instance)

      wait(fn -> Kafka.verify(instance) == :ok end, 30_000, "the topic to verify")
      assert Kafka.describe(instance) == "acks=all, min.insync.replicas=2, replicas=3"
    end

    test "refuses min.insync.replicas below 2, retention below 2x backup.max_age_ms and max.message.bytes below max_record_bytes",
         %{instance: instance} do
      refused = fn override, fragment ->
        configs = Map.merge(@good_topic[:configs], override)
        topic = create_topic(Keyword.put(@good_topic, :configs, configs))
        configure(instance, topic)
        restart_adapter(instance)

        assert {:error, {:intake_topic_settings, [problem]}} = Kafka.verify(instance)
        assert problem =~ fragment
      end

      refused.(%{"min.insync.replicas" => "1"}, "min.insync.replicas is 1")
      refused.(%{"retention.ms" => "#{2 * @max_age_ms - 1}"}, "retention.ms")
      refused.(%{"max.message.bytes" => "1000"}, "max.message.bytes is 1000")
      refused.(%{"unclean.leader.election.enable" => "true"}, "unclean.leader.election.enable")

      # infinite retention is fine
      topic =
        create_topic(
          Keyword.put(@good_topic, :configs, Map.put(@good_topic[:configs], "retention.ms", "-1"))
        )

      configure(instance, topic)
      restart_adapter(instance)
      assert :ok = Kafka.verify(instance)
    end

    test "refuses a replication factor below 3", %{instance: instance} do
      topic =
        create_topic(
          partitions: 1,
          replication_factor: 1,
          configs: Map.delete(@good_topic[:configs], "min.insync.replicas")
        )

      configure(instance, topic)
      start_adapter(instance)

      assert {:error, {:intake_topic_settings, problems}} = Kafka.verify(instance)
      assert Enum.any?(problems, &(&1 =~ "replication.factor is 1"))
      assert Enum.any?(problems, &(&1 =~ "min.insync.replicas is 1"))
    end

    test "a topic that does not exist is not_found, and a dead broker list is unavailable", %{
      instance: instance
    } do
      configure(instance, "ankusa.intake.test.missing")
      start_adapter_without_ready(instance)
      assert {:error, {:intake_topic, :not_found}} = Kafka.verify(instance)

      configure(instance, "ankusa.intake.test.missing", "node-a", %{
        kafka: [brokers: ["127.0.0.1:1"]]
      })

      restart_adapter(instance)
      assert {:error, {:intake_topic, :unavailable}} = Kafka.verify(instance)
      refute Kafka.ready?(instance)
    end
  end

  describe "earliest, assign_partition and ready?" do
    test "earliest is nil for an empty log and the first position otherwise", %{
      instance: instance
    } do
      topic = create_topic()
      configure(instance, topic)
      start_adapter(instance)

      assert {:ok, nil} = Kafka.earliest(instance)
      {:ok, _} = Kafka.publish_batch(instance, [record(1), record(2)], opts(topic, 0))
      assert {:ok, 0} = Kafka.earliest(instance)
    end

    test "assign_partition is the node's hash modulo the partition count", %{instance: instance} do
      topic = create_topic(Keyword.put(@good_topic, :partitions, 4))
      configure(instance, topic, "node-q")
      start_adapter(instance)

      assert {:ok, partition} = Kafka.assign_partition(instance, "node-q")
      assert partition == rem(:erlang.phash2("node-q"), 4)
    end

    test "ready? is false for an adapter that is not running", %{instance: instance} do
      refute Kafka.ready?(instance)
    end

    test "publishing without a client is unavailable, never a crash", %{instance: instance} do
      assert {:error, :unavailable} = Kafka.publish_batch(instance, [record(1)], opts("x", 0))
    end

    test "a publish to a topic that does not exist is not_found", %{instance: instance} do
      configure(instance, "ankusa.intake.test.never-created")
      start_adapter_without_ready(instance)

      assert {:error, :not_found} =
               Kafka.publish_batch(
                 instance,
                 [record(1)],
                 opts("ankusa.intake.test.never-created", 0)
               )
    end

    defp start_adapter_without_ready(instance) do
      start_supervised!(Kafka.child_spec(instance: instance))
      wait(fn -> Process.whereis(Kafka.client(instance)) != nil end, 15_000, "the client")
    end
  end

  # ── error codes ───────────────────────────────────────────────────────────

  describe "error classes" do
    test "a record the broker will never take is permanent", %{instance: instance} do
      topic =
        create_topic(
          Keyword.put(
            @good_topic,
            :configs,
            Map.put(@good_topic[:configs], "max.message.bytes", "2000")
          )
        )

      configure(instance, topic)
      start_adapter(instance)

      assert {:error, :permanent} =
               Kafka.publish_batch(
                 instance,
                 [record(1, String.duplicate("x", 5_000))],
                 opts(topic, 0)
               )

      # the producer restarts after its delay; a record that fits is confirmed again
      wait(
        fn -> match?({:ok, _}, Kafka.publish_batch(instance, [record(2)], opts(topic, 0))) end,
        30_000,
        "a confirm after a permanent refusal"
      )
    end

    test "broker error codes map to the four classes, wherever brod nests them" do
      produce_error = fn code ->
        {:producer_down, {:not_retriable, {:produce_response_error, "t", 0, -1, code}}}
      end

      for code <- [
            :message_too_large,
            :invalid_message,
            :invalid_record,
            :topic_authorization_failed
          ] do
        assert Kafka.classify(produce_error.(code)) == :permanent
      end

      assert Kafka.classify(produce_error.(:unknown_topic_or_partition)) == :not_found
      assert Kafka.classify(:unknown_topic_or_partition) == :not_found
      assert Kafka.classify(produce_error.(:throttling_quota_exceeded)) == :throttled

      for reason <- [
            :not_enough_replicas,
            :not_enough_replicas_after_append,
            :leader_not_available,
            :request_timed_out,
            :not_leader_or_follower,
            :timeout,
            :client_down,
            {:producer_down,
             {:reached_max_retries, {:produce_response_error, "t", 0, -1, :not_enough_replicas}}},
            {:connect_failure, :econnrefused},
            :something_nobody_has_heard_of
          ] do
        assert Kafka.classify(reason) == :unavailable
      end

      # the most specific class wins when a reason carries more than one
      assert Kafka.classify({:retried, :not_enough_replicas, :message_too_large}) == :permanent
    end
  end
end
