defmodule Ankusa.QueueTest do
  @moduledoc """
  The queue contract: seqs are dense and never reused, a commit is refused when
  the store is down, and the commit span measures real commits only.
  """
  use ExUnit.Case, async: true

  import Ankusa.TestHelpers

  alias Ankusa.{Envelope, Queue}
  alias Ankusa.Queue.Deliveries
  alias Ankusa.Sink.Log

  defp config(overrides) do
    test_config(
      Keyword.merge(
        [
          source_store: {Ankusa.SourceStore.Static, sources: %{"load" => [sinks: [{Log, []}]]}}
        ],
        overrides
      )
    )
  end

  @doc false
  def handle_commit_telemetry(event, measurements, metadata, test_pid) do
    send(test_pid, {:telemetry, event, measurements, metadata})
  end

  defp envelope(n) do
    body = ~s({"n":#{n}})

    %Envelope{
      id: Ankusa.UUIDv7.generate(),
      source_id: "load",
      tenant_id: "default",
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/webhooks/load",
      headers: [],
      body: body,
      size: byte_size(body)
    }
  end

  test "seqs are dense and increasing, and hooks carry the seq from the key" do
    config = config(roles: [:edge])
    start_supervised!({Ankusa.Instance, config})

    entries = for n <- 1..5, do: %{envelope: envelope(n), sinks: [{Log, []}]}
    {:ok, committed} = Queue.enqueue(config.instance, entries)

    assert Enum.map(committed, fn {:committed, env} -> env.seq end) == [1, 2, 3, 4, 5]

    {:ok, hooks} = Queue.hooks(config.instance, 0, 100)
    assert Enum.map(hooks, & &1.seq) == [1, 2, 3, 4, 5]
    assert Enum.map(hooks, & &1.id) == Enum.map(committed, fn {:committed, env} -> env.id end)

    {:ok, tail} = Queue.hooks(config.instance, 3, 100)
    assert Enum.map(tail, & &1.seq) == [4, 5]
  end

  test "seqs never repeat after every hook is delivered, reclaimed and the instance restarts" do
    config = config(roles: [:edge, :dispatch])
    start_supervised!({Ankusa.Instance, config})

    for n <- 1..3, do: enqueue!(config.instance, envelope(n))

    # tick is the drain barrier: when it returns nothing is claimed, running or due.
    {:ok, _settled} = Ankusa.Dispatch.Pipeline.tick(config.instance)
    assert {:ok, []} = Queue.hooks(config.instance, 0, 10)

    stop_supervised!({Ankusa.Instance, config.instance})
    start_supervised!({Ankusa.Instance, config})

    committed = enqueue!(config.instance, envelope(4))
    assert committed.seq == 4
  end

  test "a commit is refused while the store is down and recovers with a higher seq" do
    config = config(roles: [:edge])
    inst = config.instance
    start_supervised!({Ankusa.Instance, config})

    first = enqueue!(inst, envelope(1))
    assert first.seq == 1

    instance_sup = Ankusa.via(inst, :instance)
    :ok = Supervisor.terminate_child(instance_sup, {Ankusa.Store, inst})

    assert {:error, _} =
             Queue.enqueue(inst, [%{envelope: envelope(2), sinks: [{Log, []}]}])

    assert {:error, :store_unavailable} =
             Ankusa.Edge.Ingest.ingest(inst, request("load", ~s({"n":2})))

    {:ok, _pid} = Supervisor.restart_child(instance_sup, {Ankusa.Store, inst})

    later = enqueue!(inst, envelope(3))
    assert later.seq > first.seq
  end

  test "commit telemetry measures successful commits and skips failed ones" do
    config = config(roles: [:edge])
    inst = config.instance
    start_supervised!({Ankusa.Instance, config})

    test_pid = self()
    handler = "queue-commit-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler,
      [[:ankusa, :commit, :stop], [:ankusa, :commit, :exception]],
      &__MODULE__.handle_commit_telemetry/4,
      test_pid
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    enqueue!(inst, envelope(1))

    # The handler sees every instance's commits and this module is async: match
    # this test's instance, not whichever commit arrives first.
    assert_receive {:telemetry, [:ankusa, :commit, :stop], measurements, %{instance: ^inst}}
    assert measurements.batch_size == 1
    assert measurements.bytes > 0
    assert is_integer(measurements.duration)

    :ok = Supervisor.terminate_child(Ankusa.via(inst, :instance), {Ankusa.Store, inst})
    assert {:error, _} = Queue.enqueue(inst, [%{envelope: envelope(2), sinks: [{Log, []}]}])

    # The span emits :exception, never :stop, so the commit series counts only
    # real commits.
    assert_receive {:telemetry, [:ankusa, :commit, :exception], _measurements, %{instance: ^inst}}
    refute_receive {:telemetry, [:ankusa, :commit, :stop], _, %{instance: ^inst}}, 200
  end

  test "a stored hook that does not decode is a corrupt_value error, not a crash" do
    config = config(roles: [:edge])
    inst = config.instance
    start_supervised!({Ankusa.Instance, config})

    :ok =
      Ankusa.Store.write(inst, [{:put, :hooks, Ankusa.Store.Keys.hook(5), "garbage"}], sync: true)

    assert {:error, {:corrupt_value, :hooks, _message}} = Queue.hooks(inst, 0, 10)
  end

  describe "delivery row encoding" do
    @row %{
      module: Log,
      state: :pending,
      attempts: 2,
      at: 1_737_500_000_000,
      error: nil,
      size: 12,
      source_id: "load"
    }

    test "the default encoding is the bare term every release up to 0.6 wrote" do
      assert Deliveries.encode_row(@row) == :erlang.term_to_binary(@row)
      assert Deliveries.decode_row(:erlang.term_to_binary(@row)) == {:ok, @row}
    end

    test "v1 is the AD prefix, the version byte and the same term" do
      v1 = Deliveries.encode_row(@row, version: 1)

      assert <<"AD", 1, term::binary>> = v1
      assert term == Deliveries.encode_row(@row)
      assert Deliveries.decode_row(v1) == {:ok, @row}
    end

    test "a key a newer node added passes through, and survives a rewrite" do
      future = Map.put(@row, :first_attempt_at, 1_737_500_000_001)

      for version <- [0, 1] do
        assert {:ok, ^future} =
                 future |> Deliveries.encode_row(version: version) |> Deliveries.decode_row()
      end
    end

    test "an unknown version is an error value, and so is damage" do
      <<"AD", 1, term::binary>> = Deliveries.encode_row(@row, version: 1)

      assert Deliveries.decode_row(<<"AD", 9, term::binary>>) ==
               {:error, :unsupported_delivery_row_version}

      for bad <- ["", "garbage", <<"AD", 1>>, :erlang.term_to_binary(7)] do
        assert Deliveries.decode_row(bad) == {:error, :undecodable}
      end
    end

    test "rows the writer commits decode, with the keys the pipeline reads" do
      config = config(roles: [:edge])
      start_supervised!({Ankusa.Instance, config})

      {:ok, [{:committed, env}]} =
        Queue.enqueue(config.instance, [%{envelope: envelope(1), sinks: [{Log, []}]}])

      assert {:ok, %{module: Log, state: :pending, attempts: 0, source_id: "load"}} =
               Deliveries.row(config.instance, env.seq, 0)
    end
  end

  test "a stored hook in an envelope version this node does not know is a corrupt_value error, not a crash" do
    config = config(roles: [:edge])
    inst = config.instance
    start_supervised!({Ankusa.Instance, config})

    <<"AE", 1, term::binary>> = Envelope.to_binary(envelope(1), version: 1)

    :ok =
      Ankusa.Store.write(
        inst,
        [{:put, :hooks, Ankusa.Store.Keys.hook(5), <<"AE", 9, term::binary>>}],
        sync: true
      )

    assert {:error, {:corrupt_value, :hooks, message}} = Queue.hooks(inst, 0, 10)
    assert message =~ "unsupported_envelope_version"
  end

  test "a hook written by a newer node (v1 envelope, an added field) reads back on this one" do
    config = config(roles: [:edge])
    inst = config.instance
    start_supervised!({Ankusa.Instance, config})

    env = envelope(1)
    {_field, bin} = Ankusa.TestHelpers.envelope_from_the_future(env, 1)

    :ok = Ankusa.Store.write(inst, [{:put, :hooks, Ankusa.Store.Keys.hook(5), bin}], sync: true)

    assert {:ok, [read]} = Queue.hooks(inst, 0, 10)
    assert read == %{env | seq: 5}
  end

  test "a \"AE\", 9 hook is dead-lettered by the pipeline with reason :unsupported_envelope_version" do
    config = config(roles: [:edge, :dispatch])
    inst = config.instance
    start_supervised!({Ankusa.Instance, config})

    at = System.system_time(:millisecond) - 1_000
    <<"AE", 1, term::binary>> = Envelope.to_binary(envelope(1), version: 1)
    hook = <<"AE", 9, term::binary>>

    row =
      Deliveries.encode_row(%{
        module: Log,
        state: :pending,
        attempts: 0,
        at: at,
        error: nil,
        size: byte_size(hook),
        source_id: "load"
      })

    :ok =
      Ankusa.Store.write(
        inst,
        [
          {:put, :hooks, Ankusa.Store.Keys.hook(7), hook},
          {:put, :deliveries, Ankusa.Store.Keys.delivery(7, 0), row}
        ] ++ due_index_ops(inst, 7, 0, at, byte_size(hook)),
        sync: true
      )

    assert {:ok, _} = Ankusa.Dispatch.Pipeline.tick(inst)

    assert {:ok, %{state: :dead, error: ":unsupported_envelope_version"}} =
             Deliveries.row(inst, 7, 0)

    # The dead letter lists, with the reason, under the source its row names;
    # the hook stays stored for a node that can read it.
    assert {:ok, %{total: 1, entries: [entry]}} = Queue.dead(inst, limit: 10)
    assert entry.reason == ":unsupported_envelope_version"
    assert entry.envelope.source_id == "load"
    assert entry.envelope.seq == 7
    assert {:ok, ^hook} = Ankusa.Store.get(inst, :hooks, Ankusa.Store.Keys.hook(7))
  end

  test "an orphaned dead key is logged and not counted in total" do
    config = config(roles: [:edge])
    inst = config.instance
    start_supervised!({Ankusa.Instance, config})

    now = System.system_time(:millisecond)

    :ok =
      Ankusa.Store.write(
        inst,
        dead_index_ops(inst, 99, 0xFFFE, now, "load", "evt_orphan"),
        sync: true
      )

    assert {:ok, %{total: 0, entries: []}} = Queue.dead(inst, limit: 10)
  end
end
