defmodule Ankusa.Intake.ReconcilerTest do
  @moduledoc """
  The live reconciler (ADR 0002, sections 3 and 5): half-persisted hooks are
  carried to completion, never undone; the fleet dedupe claim is held until both
  halves are confirmed.
  """
  use ExUnit.Case, async: false

  @moduletag capture_log: true

  import Ankusa.Test.Dual
  import Ankusa.TestHelpers, only: [request: 2]

  alias Ankusa.Edge.Ingest
  alias Ankusa.Intake.{Memory, Pending, Reconciler}
  alias Ankusa.Queue.Deliveries.Counters
  alias Ankusa.Test.Dual.FleetStub

  defp fleet_config(overrides \\ []) do
    table = :ets.new(:fleet_stub, [:public, :set])

    config =
      start_dual(
        [fleet: %{store: {FleetStub, table: table}, timeout_ms: 20}, dedupe: %{scope: :fleet}] ++
          overrides
      )

    {config, table}
  end

  defp table(config), do: Pending.table(config.instance)

  defp restart_store(config) do
    sup = Ankusa.via(config.instance, :instance)
    {:ok, _pid} = Supervisor.restart_child(sup, {Ankusa.Store, config.instance})
  end

  defp stop_store(config) do
    sup = Ankusa.via(config.instance, :instance)
    :ok = Supervisor.terminate_child(sup, {Ankusa.Store, config.instance})
  end

  test "completes the log half after the intake returns" do
    config = start_dual()
    test = self()

    :telemetry.attach(
      "reconciled-#{inspect(config.instance)}",
      [:ankusa, :intake, :reconciled],
      fn _event, _m, meta, _ -> send(test, {:reconciled, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach("reconciled-#{inspect(config.instance)}") end)

    Memory.fail(config.instance, :unavailable)
    assert {:error, :intake_unavailable} = ingest(config, "evt-1")
    assert Memory.records(config.instance) == []
    assert pending(config) == 1

    Memory.heal(config.instance)
    assert drained(config)
    assert [%{id: id}] = Memory.records(config.instance)
    assert [^id] = hook_ids(config)
    assert_receive {:reconciled, %{half: :log}}
  end

  test "completes the local half once the store write succeeds" do
    config = start_dual()
    assert {:ok, _} = ingest(config, "warm")
    assert drained(config)

    stop_store(config)
    # no dedupe key: nothing reads the store before the commit, so the log half
    # confirms and the local write fails
    assert {:error, :store_unavailable} =
             Ingest.ingest(config.instance, request("load", ~s({"k":"solo"})))

    restart_store(config)

    assert drained(config)
    assert length(hook_ids(config)) == 2
  end

  test "does not reapply a local half that already landed" do
    config = start_dual()
    assert {:ok, env} = ingest(config, "evt-1")
    assert drained(config)

    dedupe = fn ->
      {:ok, [n]} = Counters.read_field(config.instance, [0], :dedupe)
      n
    end

    before = dedupe.()

    # a carried entry whose batch's probe seq is in the store: the first write
    # landed after all, so its ops (a counter delta that would show) are not applied
    carry_local_missing(config, env.seq, 77, Counters.node_ops(dedupe: 5))

    assert {:ok, [77], [_done]} =
             GenServer.call(Ankusa.via(config.instance, :queue_writer), {:complete_local, [77]})

    assert dedupe.() == before

    # and one whose probe seq is absent is applied, once
    carry_local_missing(config, env.seq + 1_000, 78, Counters.node_ops(dedupe: 5))

    assert {:ok, [78], [_done]} =
             GenServer.call(Ankusa.via(config.instance, :queue_writer), {:complete_local, [78]})

    assert dedupe.() == before + 5
  end

  test "a stuck entry past reconcile_deadline_ms releases the claim and raises the gauge" do
    {config, fleet} = fleet_config(intake: %{reconcile_deadline_ms: 1_000})
    Memory.fail(config.instance, :unavailable)

    assert {:error, :intake_unavailable} = ingest(config, "evt-1")
    assert [{:pending, _}] = for({_k, v} <- :ets.tab2list(fleet), match?({:pending, _}, v), do: v)
    assert ops(fleet) == []

    assert wait_until(fn -> Pending.meta(table(config), :stuck) == 1 end, 4_000)
    assert wait_until(fn -> length(ops(fleet)) == 1 end)
    assert [{:release, %{key: "evt-1"}}] = ops(fleet)

    # it is still pending and still retried: nothing is dropped
    assert pending(config) == 1
    Memory.heal(config.instance)
    assert drained(config)
    assert Pending.meta(table(config), :stuck) == 0
    assert [{:release, _}] = ops(fleet)
  end

  test "permanent class goes straight to stuck and is not dropped" do
    config = start_dual()
    Memory.fail(config.instance, :permanent)

    assert {:error, :intake_unavailable} = ingest(config, "evt-1")
    assert wait_until(fn -> Pending.meta(table(config), :stuck) == 1 end)
    assert pending(config) == 1
  end

  test "hold returns completed when the reconciler won the race" do
    config = start_dual()
    claim = %{tenant: "default", source_id: "load", key: "k", hook_id: "x"}

    assert Reconciler.hold(config.instance, "no-such-id", claim) == :completed

    Memory.fail(config.instance, :unavailable)
    assert {:error, :intake_unavailable} = ingest(config, "evt-1")
    [id] = hook_ids(config)

    assert Reconciler.hold(config.instance, id, claim) == :held
    Memory.heal(config.instance)
    assert drained(config)
    assert Reconciler.hold(config.instance, id, claim) == :completed
  end

  test "claims promote only when both halves are confirmed" do
    {config, fleet} = fleet_config()
    assert {:ok, _} = ingest(config, "warm")
    assert drained(config)
    # the claim of the confirmed hook is promoted by the fleet's settle process
    assert wait_until(fn -> length(ops(fleet)) == 1 end)
    before = ops(fleet)

    Memory.fail(config.instance, :unavailable)
    assert {:error, :intake_unavailable} = ingest(config, "evt-1")
    [_warm, id] = hook_ids(config)

    Process.sleep(300)
    # the log half is missing: neither promoted nor released
    assert ops(fleet) == before

    Memory.heal(config.instance)
    assert wait_until(fn -> length(ops(fleet)) == length(before) + 1 end)
    assert [{:promote, %{key: "evt-1"}, ^id}] = Enum.drop(ops(fleet), length(before))
  end

  # A carried entry for `seq` whose local half is missing: its batch's ops, with
  # `seq` as the probe the reconciler reads first.
  defp carry_local_missing(config, seq, batch_id, ops) do
    t = table(config)
    now = System.monotonic_time(:millisecond)
    Pending.enter(t, [{seq, "fake-#{seq}", 10, nil, %{}}], batch_id, now)

    Pending.carry(
      t,
      %{local: :missing, log: :ok, position: 0},
      [{seq, "bin", %{}}],
      %{
        id: batch_id,
        ops: ops,
        probe_seq: seq,
        seqs: [seq],
        at: System.system_time(:millisecond)
      },
      now
    )
  end
end
