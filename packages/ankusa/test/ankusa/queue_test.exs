defmodule Ankusa.QueueTest do
  @moduledoc """
  The queue contract: seqs are dense and never reused, a commit is refused when
  the store is down, and the commit span measures real commits only.
  """
  use ExUnit.Case, async: true

  import Ankusa.TestHelpers

  alias Ankusa.{Envelope, Queue}
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

    assert_receive {:telemetry, [:ankusa, :commit, :stop], measurements, metadata}
    assert measurements.batch_size == 1
    assert measurements.bytes > 0
    assert is_integer(measurements.duration)
    assert metadata.instance == inst

    :ok = Supervisor.terminate_child(Ankusa.via(inst, :instance), {Ankusa.Store, inst})
    assert {:error, _} = Queue.enqueue(inst, [%{envelope: envelope(2), sinks: [{Log, []}]}])

    # The span emits :exception, never :stop, so the commit series counts only
    # real commits.
    assert_receive {:telemetry, [:ankusa, :commit, :exception], _measurements, _metadata}
    refute_receive {:telemetry, [:ankusa, :commit, :stop], _, _}, 200
  end
end
