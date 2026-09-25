defmodule Ankusa.DedupStore.RaTest do
  @moduledoc """
  `Ankusa.DedupStore.Ra` against a real single-node Ra cluster.

  The point of this store is that the ledger outlives the dispatcher, so the
  test that matters is the restart one: stop the member (and its system), start
  it again, and the decisions hold. Everything else is the shared rule of
  `Ankusa.DedupStore.decide/4`, checked here through the replicated path.
  """

  use ExUnit.Case, async: false

  alias Ankusa.DedupStore
  alias Ankusa.WAL.Ra

  @ttl_ms 60_000

  setup do
    instance = unique_instance()
    :ok = start_wal(instance)
    on_exit(fn -> stop_wal(instance) end)

    %{instance: instance, store: store(instance)}
  end

  describe "record/6" do
    test "the first copy is delivered and a later copy is dropped", %{
      instance: instance,
      store: store
    } do
      now = System.system_time(:millisecond)

      assert :deliver = DedupStore.record(store, scope(), "evt_1", 5, now, @ttl_ms)
      # A copy with a higher seq is a retry of an event whose earlier copy has
      # already gone through.
      assert :drop = DedupStore.record(store, scope(), "evt_1", 7, now + 1, @ttl_ms)
      # A copy with a lower seq is a different record: it has not been delivered,
      # so it is delivered — and becomes the earliest copy known.
      assert :deliver = DedupStore.record(store, scope(), "evt_1", 3, now + 2, @ttl_ms)
      assert :drop = DedupStore.record(store, scope(), "evt_1", 6, now + 3, @ttl_ms)

      # The ledger is in the cluster, not in this process.
      assert %{dedup_keys: 1} = overview(instance)
    end

    test "the same record re-read after a crash is delivered, not dropped", %{
      store: store
    } do
      now = System.system_time(:millisecond)

      assert :deliver = DedupStore.record(store, scope(), "evt_2", 7, now, @ttl_ms)
      # The dispatcher crashed and re-read its own record: same seq, so this is
      # the copy that went through, not a second one.
      assert :deliver = DedupStore.record(store, scope(), "evt_2", 7, now + 1, @ttl_ms)
      # A later copy is the duplicate.
      assert :drop = DedupStore.record(store, scope(), "evt_2", 9, now + 2, @ttl_ms)
      # An earlier one is a record that never went through.
      assert :deliver = DedupStore.record(store, scope(), "evt_2", 6, now + 3, @ttl_ms)
    end

    test "a copy outside the window is not a duplicate", %{store: store} do
      now = System.system_time(:millisecond)

      assert :deliver = DedupStore.record(store, scope(), "evt_3", 1, now, @ttl_ms)
      # Measured between the two records' commit times, not against a clock at
      # read time: one millisecond past the window is outside it, so this is not
      # the first copy's duplicate.
      assert :deliver = DedupStore.record(store, scope(), "evt_3", 2, now + @ttl_ms + 1, @ttl_ms)
      # That copy is now the ledger's entry, and a later one inside its window is
      # a duplicate again.
      assert :drop = DedupStore.record(store, scope(), "evt_3", 3, now + @ttl_ms + 2, @ttl_ms)
    end

    test "the scope separates two tenants' identical keys", %{store: store} do
      now = System.system_time(:millisecond)

      # Same key, same seq, different tenant: neither is the other's duplicate.
      assert :deliver = DedupStore.record(store, {"t1", "stripe"}, "evt_4", 1, now, @ttl_ms)
      assert :deliver = DedupStore.record(store, {"t2", "stripe"}, "evt_4", 1, now, @ttl_ms)
      # ...and each tenant's own ledger entry is there.
      assert :drop = DedupStore.record(store, {"t1", "stripe"}, "evt_4", 2, now + 1, @ttl_ms)
      assert :drop = DedupStore.record(store, {"t2", "stripe"}, "evt_4", 2, now + 1, @ttl_ms)
    end

    test "an unreachable cluster says so instead of guessing", %{} do
      # Neither `:deliver` nor `:drop`: an unrecorded delivery would leave the
      # *next* copy of this event nothing to be compared against, and dropping
      # it would risk losing a first delivery. Dispatch leaves the record
      # undecided and comes back to it. Short timeout, nobody home.
      store = DedupStore.Ra.new(members: [{:ankusa_wal_nowhere, :nowhere@nohost}], timeout_ms: 50)

      assert {:error, _reason} = DedupStore.record(store, scope(), "evt_5", 1, 0, @ttl_ms)
    end
  end

  describe "durability" do
    test "the ledger survives losing and restarting the member", %{
      instance: instance,
      store: store
    } do
      now = System.system_time(:millisecond)

      assert :deliver = DedupStore.record(store, scope(), "evt_6", 4, now, @ttl_ms)
      assert :drop = DedupStore.record(store, scope(), "evt_6", 5, now + 1, @ttl_ms)

      # A dispatcher that dies loses nothing: the ledger is not in it.
      :ok = stop_wal(instance)
      :ok = start_wal(instance)

      restarted = store(instance)
      # The event's earliest copy is still on record, so a later copy is still a
      # duplicate — that is the whole reason this store exists.
      assert :drop = DedupStore.record(restarted, scope(), "evt_6", 6, now + 2, @ttl_ms)
      assert :deliver = DedupStore.record(restarted, scope(), "evt_6", 2, now + 3, @ttl_ms)
    end
  end

  describe "Ankusa.Dispatch.Receiver" do
    test "a receiver built on the store drops the second copy", %{store: store} do
      receiver =
        Ankusa.Dispatch.Receiver.new(0,
          store: {Ankusa.DedupStore.Ra, members: store.members},
          ttl_ms: @ttl_ms
        )

      assert receiver.store.__struct__ == Ankusa.DedupStore.Ra

      source = %Ankusa.Source{
        id: "stripe",
        tenant_id: "default",
        dedup_key: {Ankusa.DedupKey.Rules, json: ["id"]}
      }

      first = envelope("evt_7", 1)
      copy = envelope("evt_7", 2)

      assert {:ok, false} = Ankusa.Dispatch.Receiver.decide(receiver, source, first)
      assert {:ok, true} = Ankusa.Dispatch.Receiver.decide(receiver, source, copy)
    end
  end

  # ── cluster ───────────────────────────────────────────────────────────────

  defp store(instance) do
    DedupStore.Ra.new(members: [{:"ankusa_wal_#{instance}", node()}])
  end

  defp scope, do: {"default", "stripe"}

  defp overview(instance) do
    cluster = :"ankusa_wal_#{instance}"
    {:ok, overview} = Ra.remote_aux([{cluster, node()}], :overview, timeout: 5_000)
    overview
  end

  defp start_wal(instance) do
    config =
      Ankusa.Config.new(
        instance: instance,
        data_dir: Path.join(System.tmp_dir!(), "ankusa_dedup_ra_test"),
        wal: {Ra, members: [{:"ankusa_wal_#{instance}", node()}]}
      )

    {:ok, _pid} = Ra.start_link(instance: instance, config: config)
    :ok
  end

  defp stop_wal(instance) do
    case Ankusa.whereis(instance, :wal) do
      nil ->
        :ok

      pid ->
        try do
          GenServer.stop(pid)
        catch
          :exit, _ -> :ok
        end
    end

    system = :"ankusa_ra_#{instance}"
    cluster = :"ankusa_wal_#{instance}"

    _ = :ra.stop_server(system, {cluster, node()})
    _ = :ra_system.stop(system)
    :ok
  end

  defp unique_instance do
    suffix = :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)
    :"dedup_ra_#{System.unique_integer([:positive])}#{suffix}"
  end

  defp envelope(id, seq) do
    %Ankusa.Envelope{
      id: Ankusa.UUIDv7.generate(),
      source_id: "stripe",
      tenant_id: "default",
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/webhooks/stripe",
      headers: [],
      body: ~s({"id":"#{id}"}),
      seq: seq,
      committed_at: System.system_time(:millisecond)
    }
  end
end
