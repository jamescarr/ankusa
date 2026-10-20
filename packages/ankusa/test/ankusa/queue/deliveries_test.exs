defmodule Ankusa.Queue.DeliveriesTest do
  @moduledoc """
  The tenant key layout (ADR 0001): tenant ordinals, the per-tenant counters
  and the active-tenant markers every transition has to keep exact.

  The two property tests drive random sequences of every transition builder
  against a live store and check the invariants after each sequence: the
  counters equal a fold of the index families, and no due row is without its
  marker.
  """

  use ExUnit.Case, async: false
  use ExUnitProperties

  import Ankusa.TestHelpers

  alias Ankusa.{Envelope, Queue}
  alias Ankusa.Queue.{Archive, Deliveries, Tenants}
  alias Ankusa.Queue.Deliveries.Counters
  alias Ankusa.Sink.Log
  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @moduletag capture_log: true

  @sinks [{Log, []}, {Log, []}]
  @unresolved 0xFFFF

  defp start_instance(overrides \\ []) do
    config =
      test_config(
        Keyword.merge(
          [
            roles: [:edge, :storage],
            storage: %{interval_ms: 0},
            source_store:
              {Ankusa.SourceStore.Static,
               sources: %{"load" => [verifier: {Ankusa.Verifier.None, []}, sinks: @sinks]}}
          ],
          overrides
        )
      )

    start_supervised!({Ankusa.Instance, config})
    config.instance
  end

  defp envelope(tenant) do
    %Envelope{
      id: Ankusa.UUIDv7.generate(),
      source_id: "load",
      tenant_id: tenant,
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/webhooks/load",
      headers: [],
      body: ~s({"n":1}),
      size: 7
    }
  end

  defp enqueue(inst, tenant, n \\ 1) do
    entries = for _ <- 1..n, do: %{envelope: envelope(tenant), sinks: @sinks}
    {:ok, results} = Queue.enqueue(inst, entries)
    results
  end

  # ── reading the index ────────────────────────────────────────────────────

  defp fold(inst, family, fun) do
    %{lo: lo, hi: hi} = Keys.family(family)

    {:ok, rows} =
      Store.fold(inst, family, {lo, hi}, [], fn k, v, acc -> {:cont, [fun.(k, v) | acc]} end)

    Enum.reverse(rows)
  end

  defp due_rows(inst) do
    fold(inst, :due, fn key, <<size::32, kind::8>> ->
      {ord, at, seq, sink} = Keys.decode_due(key)
      %{key: key, ord: ord, at: at, seq: seq, sink: sink, size: size, kind: kind}
    end)
  end

  defp inflight_rows(inst) do
    fold(inst, :inflight, fn key, <<size::32>> ->
      {ord, seq, sink} = Keys.decode_inflight(key)
      %{ord: ord, seq: seq, sink: sink, size: size}
    end)
  end

  defp dead_hits(inst) do
    {:ok, hits, _last, _done, _scanned} =
      Deliveries.dead_page(inst, {<<?x>>, <<?y>>}, %{}, 1_000, 10_000)

    hits
  end

  defp assert_counters_equal_fold(inst) do
    assert stored_counters(inst) == folded_counters(inst)
  end

  defp assert_no_due_row_without_marker(inst) do
    for %{ord: ord, at: at} <- due_rows(inst) do
      assert {:ok, _} = Store.get(inst, :index, Keys.active(div(at, 1000), ord)),
             "due row of tenant #{ord} at #{at} has no marker"
    end
  end

  # ── transitions ──────────────────────────────────────────────────────────

  defp pick([], _i), do: nil
  defp pick(list, i), do: Enum.at(list, rem(i, length(list)))

  defp write!(inst, ops), do: :ok = Store.write(inst, Deliveries.coalesce(ops), sync: true)

  defp row!(inst, seq, sink) do
    {:ok, row} = Deliveries.row(inst, seq, sink)
    row
  end

  defp apply_op(inst, {:ingest, t}), do: enqueue(inst, "t#{t}", 2)

  # A row imported from a 0.3 data dir: the sink was never recorded.
  defp apply_op(inst, {:legacy, t}) do
    ord = tenant_ord!(inst, "t#{t}")
    seq = 10_000_000 + System.unique_integer([:positive])
    env = envelope("t#{t}")
    bin = Envelope.to_binary(env)

    row =
      Deliveries.encode_row(%{
        module: nil,
        state: :pending,
        attempts: 0,
        at: 0,
        error: nil,
        size: byte_size(bin),
        source_id: "load"
      })

    write!(inst, [
      {:put, :hooks, Keys.hook(seq), bin}
      | Deliveries.new_ops(
          ord,
          System.system_time(:millisecond),
          seq,
          @unresolved,
          byte_size(bin),
          row
        )
    ])
  end

  defp apply_op(inst, {:claim, i}) do
    if due = pick(due_rows(inst), i), do: :ok = Deliveries.claim(inst, [due])
  end

  defp apply_op(inst, {:retry, i}) do
    if f = pick(inflight_rows(inst), i) do
      row = row!(inst, f.seq, f.sink)
      at = System.system_time(:millisecond) + if(rem(i, 2) == 0, do: -100, else: 3_600_000)
      write!(inst, Deliveries.retry_ops(f.ord, f.seq, f.sink, %{row | attempts: 1}, at, "x"))
    end
  end

  defp apply_op(inst, {:deliver, i}) do
    if f = pick(inflight_rows(inst), i),
      do: write!(inst, Deliveries.delivered_ops(f.ord, f.seq, f.sink, f.size))
  end

  defp apply_op(inst, {:dead, i}) do
    if f = pick(inflight_rows(inst), i) do
      row = row!(inst, f.seq, f.sink)
      now = System.system_time(:millisecond)
      write!(inst, Deliveries.dead_ops(:inflight, f.ord, f.seq, f.sink, row, now, "x", nil))
    end
  end

  defp apply_op(inst, {:dead_due, i}) do
    if d = pick(due_rows(inst), i) do
      row = row!(inst, d.seq, d.sink)
      now = System.system_time(:millisecond)

      write!(
        inst,
        Deliveries.dead_ops({:due, d.key, d.kind}, d.ord, d.seq, d.sink, row, now, "x", nil)
      )
    end
  end

  defp apply_op(inst, {:orphan, i}) do
    if f = pick(inflight_rows(inst), i),
      do: write!(inst, Deliveries.orphan_ops(:inflight, f.ord, f.seq, f.sink, f.size))
  end

  defp apply_op(inst, {:expand, i}) do
    case pick(Enum.filter(inflight_rows(inst), &(&1.sink == @unresolved)), i) do
      nil ->
        :ok

      f ->
        now = System.system_time(:millisecond)

        write!(
          inst,
          Deliveries.expand_ops(
            f.ord,
            f.seq,
            @unresolved,
            f.size,
            "load",
            @sinks,
            MapSet.new(),
            now
          )
        )
    end
  end

  defp apply_op(inst, {:revive, i}) do
    if hit = pick(dead_hits(inst), i) do
      {:ok, ops, _revived} =
        Deliveries.revive_ops(inst, [hit], System.system_time(:millisecond), "replay")

      write!(inst, ops)
    end
  end

  defp apply_op(inst, :recover),
    do: {:ok, _} = Deliveries.recover_inflight(inst, System.system_time(:millisecond))

  defp apply_op(inst, {:purge_pending, t}),
    do: {:ok, _} = Queue.purge_tenant(inst, "t#{t}", [:pending])

  defp apply_op(inst, {:purge_dead, t}), do: {:ok, _} = Queue.purge_tenant(inst, "t#{t}", [:dead])

  defp apply_op(inst, :archive_settle) do
    case Archive.pending(inst, 0, 1_099_511_627_776) do
      {:ok, [{seq, _size} | _], _more} -> :ok = Archive.archived(inst, nil, [seq])
      _ -> :ok
    end
  end

  defp apply_op(inst, :archive_drop) do
    case Archive.pending(inst, 0, 1_099_511_627_776) do
      {:ok, [{seq, _size} | _], _more} -> :ok = Archive.archived(inst, nil, [], [seq])
      _ -> :ok
    end
  end

  # What the Pipeline does after a visit, as of a time long after every row.
  defp apply_op(inst, :sweep) do
    now = System.system_time(:millisecond) + 5_000_000
    {:ok, tenants} = Deliveries.active_tenants(inst, now, 4_096)

    for {ord, buckets} <- tenants,
        do: {:ok, _drained?} = Deliveries.sweep_markers(inst, ord, buckets, now)
  end

  defp ops_gen do
    index = integer(0..99)
    tenant = integer(0..2)

    list_of(
      one_of([
        tuple({constant(:ingest), tenant}),
        tuple({constant(:legacy), tenant}),
        tuple({constant(:claim), index}),
        tuple({constant(:retry), index}),
        tuple({constant(:deliver), index}),
        tuple({constant(:dead), index}),
        tuple({constant(:dead_due), index}),
        tuple({constant(:orphan), index}),
        tuple({constant(:expand), index}),
        tuple({constant(:revive), index}),
        tuple({constant(:purge_pending), tenant}),
        tuple({constant(:purge_dead), tenant}),
        constant(:recover),
        constant(:archive_settle),
        constant(:archive_drop),
        constant(:sweep)
      ]),
      max_length: 30
    )
  end

  # ── the properties ───────────────────────────────────────────────────────

  describe "counters" do
    test "counters equal a fold after any sequence of transitions" do
      inst = start_instance()

      check all(ops <- ops_gen(), max_runs: 40) do
        Enum.each(ops, &apply_op(inst, &1))
        assert_counters_equal_fold(inst)
      end
    end

    test "concurrent writers do not lose counts" do
      inst = start_instance()
      ord = tenant_ord!(inst, "busy")

      bytes_per_row = byte_size(Envelope.to_binary(%{envelope("busy") | seq: nil}))

      direct =
        for _ <- 1..4 do
          Task.async(fn ->
            for _ <- 1..200 do
              :ok = Store.write(inst, Counters.ops([{ord, :pending, 1}, {ord, :bytes, 5}]))
            end
          end)
        end

      ingest = for _ <- 1..4, do: Task.async(fn -> enqueue(inst, "busy", 25) end)
      Task.await_many(direct ++ ingest, 30_000)

      # 800 direct merges of one row and 5 bytes, and 4 x 25 hooks of two sinks.
      assert {:ok, %{pending: 1_000, bytes: bytes}} = Queue.pending_for_tenant(inst, "busy")
      assert bytes == 5 * 800 + 200 * bytes_per_row

      assert {:ok, %{pending: 1_000}} = Counters.read(inst, 0)
    end

    test "pending_for_tenant, dead_for_tenant and purge_tenant touch one tenant" do
      inst = start_instance(roles: [:edge])
      enqueue(inst, "acme", 3)
      enqueue(inst, "globex", 2)

      # Two of acme's rows die; the other four are pending.
      for _ <- 1..2 do
        [row | _] = due_rows(inst) |> Enum.filter(&(&1.ord == tenant_ord!(inst, "acme")))
        :ok = Deliveries.claim(inst, [row])
        [f] = Enum.filter(inflight_rows(inst), &(&1.seq == row.seq and &1.sink == row.sink))
        r = row!(inst, f.seq, f.sink)

        write!(
          inst,
          Deliveries.dead_ops(
            :inflight,
            f.ord,
            f.seq,
            f.sink,
            r,
            System.system_time(:millisecond),
            "x",
            nil
          )
        )
      end

      assert {:ok, %{pending: 4, scheduled: 0, inflight: 0, bytes: acme_bytes}} =
               Queue.pending_for_tenant(inst, "acme")

      assert acme_bytes > 0
      assert {:ok, %{dead: 2}} = Queue.dead_for_tenant(inst, "acme")
      assert {:ok, %{pending: 4}} = Queue.pending_for_tenant(inst, "globex")
      assert {:ok, %{dead: 0}} = Queue.dead_for_tenant(inst, "globex")

      assert {:ok, %{pending: 0, scheduled: 0, inflight: 0, bytes: 0}} =
               Queue.pending_for_tenant(inst, "nobody")

      assert {:ok, %{pending: 4, dead: 2}} = Queue.purge_tenant(inst, "acme", [:pending, :dead])

      assert {:ok, %{pending: 0, dead: 0}} = Queue.purge_tenant(inst, "acme", [:pending, :dead])
      assert {:ok, %{pending: 0, inflight: 0, bytes: 0}} = Queue.pending_for_tenant(inst, "acme")
      assert {:ok, %{dead: 0}} = Queue.dead_for_tenant(inst, "acme")

      # globex is untouched, and so is the node total: globex's rows only.
      assert {:ok, %{pending: 4}} = Queue.pending_for_tenant(inst, "globex")
      assert Enum.all?(due_rows(inst), &(&1.ord == tenant_ord!(inst, "globex")))
      assert {:ok, %{total: 0}} = Queue.dead(inst, limit: 10)
      assert_counters_equal_fold(inst)

      # Every hook nobody owes is reclaimed: only globex's remain.
      assert length(stored_ids(inst)) == 2
    end
  end

  describe "tenant ordinals" do
    test "tenant ordinal is assigned once and never reused" do
      config =
        test_config(roles: [:edge], source_store: {Ankusa.SourceStore.Static, sources: %{}})

      put_config(config)
      inst = config.instance
      start_supervised!({Ankusa.Instance, config})

      enqueue_tenants = fn tenants ->
        {:ok, results} =
          Queue.enqueue(inst, for(t <- tenants, do: %{envelope: envelope(t), sinks: @sinks}))

        results
      end

      enqueue_tenants.(["a", "b", "a", "c"])
      assert {:ok, 1} = Tenants.ord(inst, "a")
      assert {:ok, 2} = Tenants.ord(inst, "b")
      assert {:ok, 3} = Tenants.ord(inst, "c")
      assert :not_found = Tenants.ord(inst, "d")
      assert {:ok, "b"} = Tenants.name(inst, 2)
      assert {:ok, 3} = Tenants.count(inst)

      # A hook with no tenant is the `default` tenant, one ordinal like any other.
      enqueue_tenants.([nil, "default"])
      assert {:ok, 4} = Tenants.ord(inst, "default")
      assert {:ok, 4} = Tenants.count(inst)

      # Every row of a tenant goes; its ordinal does not come back.
      assert {:ok, _} = Queue.purge_tenant(inst, "b", [:pending, :dead])

      stop_supervised!({Ankusa.Instance, inst})
      start_supervised!({Ankusa.Instance, config})

      enqueue_tenants.(["d", "b"])
      assert {:ok, 5} = Tenants.ord(inst, "d")
      assert {:ok, 2} = Tenants.ord(inst, "b")
      assert {:ok, 1} = Tenants.ord(inst, "a")
    end

    test "tenants.max refuses a new tenant with tenant_limit" do
      inst = start_instance(tenants: %{max: 2})

      assert [{:committed, _}, {:committed, _}] =
               Queue.enqueue(
                 inst,
                 for(t <- ["a", "b"], do: %{envelope: envelope(t), sinks: @sinks})
               )
               |> elem(1)

      before = stored_ids(inst)

      # One batch: the known tenant commits, the new one is refused in its place.
      entries = for t <- ["c", "a"], do: %{envelope: envelope(t), sinks: @sinks}
      assert {:ok, [{:error, :tenant_limit}, {:committed, _}]} = Queue.enqueue(inst, entries)
      assert :not_found = Tenants.ord(inst, "c")
      assert length(stored_ids(inst)) == length(before) + 1

      # Through the edge: the refusal is a value the router turns into a 503.
      req = request("load", "x") |> Map.put(:tenant_id, "c")
      assert {:error, :tenant_limit} = Ankusa.Edge.Ingest.ingest(inst, req)
      assert_counters_equal_fold(inst)
    end

    test "an existing tenant is never refused at the cap" do
      inst = start_instance(tenants: %{max: 1})

      assert {:ok, [{:committed, _}]} =
               Queue.enqueue(inst, [%{envelope: envelope("only"), sinks: @sinks}])

      for _ <- 1..3 do
        assert {:ok, [{:committed, _}]} =
                 Queue.enqueue(inst, [%{envelope: envelope("only"), sinks: @sinks}])
      end

      assert {:ok, [{:error, :tenant_limit}]} =
               Queue.enqueue(inst, [%{envelope: envelope("another"), sinks: @sinks}])

      # A call with the caller's own ops (a quarantine release) is refused whole.
      assert {:error, :tenant_limit} =
               Queue.release(inst, [%{envelope: envelope("another"), sinks: @sinks}], [
                 {:put, :default, "m:probe", "x"}
               ])

      assert :not_found = Store.get(inst, :default, "m:probe")
    end
  end

  describe "the active-tenant markers" do
    test "no due row without a marker" do
      inst = start_instance()

      check all(ops <- ops_gen(), max_runs: 40) do
        Enum.each(ops, &apply_op(inst, &1))
        assert_no_due_row_without_marker(inst)
      end
    end

    test "active_tenants returns only tenants with due rows" do
      inst = start_instance()
      now = System.system_time(:millisecond)

      enqueue(inst, "due")
      enqueue(inst, "later")
      enqueue(inst, "claimed")

      due = tenant_ord!(inst, "due")
      later = tenant_ord!(inst, "later")
      claimed = tenant_ord!(inst, "claimed")

      # `later`'s rows are retries an hour out, `claimed`'s are all claimed.
      for row <- Enum.filter(due_rows(inst), &(&1.ord in [later, claimed])) do
        :ok = Deliveries.claim(inst, [row])

        if row.ord == later do
          r = row!(inst, row.seq, row.sink)
          write!(inst, Deliveries.retry_ops(row.ord, row.seq, row.sink, r, now + 3_600_000, "x"))
        end
      end

      # A claimed row leaves its marker until a visit removes it: sweep as the
      # Pipeline does after one.
      tick = now + 5_000

      for ord <- [later, claimed] do
        {:ok, active} = Deliveries.active_tenants(inst, tick, 100)
        {^ord, buckets} = List.keyfind(active, ord, 0)
        assert {:ok, _drained?} = Deliveries.sweep_markers(inst, ord, buckets, tick)
      end

      assert {:ok, [{^due, [_ | _]}]} = Deliveries.active_tenants(inst, tick, 100)

      # The earliest due time is the live tenant's, not the retry's.
      assert {:ok, at} = Deliveries.next_due_at(inst)
      assert at <= now + 1_000

      # Past the retry's time the second tenant is active too; the claimed one never.
      assert {:ok, active} = Deliveries.active_tenants(inst, now + 3_700_000, 100)
      assert active |> Enum.map(&elem(&1, 0)) |> Enum.sort() == Enum.sort([due, later])
    end

    test "cleanup does not lose a marker to a concurrent enqueue" do
      inst = start_instance()
      ord = tenant_ord!(inst, "acme")
      now = System.system_time(:millisecond)
      old = now - 10_000

      # A tenant whose only marker is from an earlier second and has no row.
      write!(inst, [{:put, :index, Keys.active(div(old, 1000), ord), <<>>}])
      seq = 99

      commit = fn ->
        write!(inst, Deliveries.due_ops(ord, old + 5, seq, 0, 10, 0))
      end

      # The writer's batch lands between the cleanup's read and its delete, and
      # between its delete and its verify: either way the marker is there at the end.
      for step <- [:after_read, :after_delete] do
        write!(inst, [{:delete, :index, Keys.due(ord, old + 5, seq, 0)}])
        write!(inst, [{:put, :index, Keys.active(div(old, 1000), ord), <<>>}])

        assert {:ok, _} =
                 Deliveries.sweep_markers(inst, ord, [div(old, 1000)], now, [{step, commit}])

        assert {:ok, _} = Store.get(inst, :index, Keys.active(div(old, 1000), ord)),
               "marker lost to a commit #{step}"

        assert [%{seq: ^seq}] = due_rows(inst)
      end

      # And with nothing slipping in, a stale marker is removed.
      write!(inst, [{:delete, :index, Keys.due(ord, old + 5, seq, 0)}])
      write!(inst, [{:put, :index, Keys.active(div(old, 1000), ord), <<>>}])
      assert {:ok, true} = Deliveries.sweep_markers(inst, ord, [div(old, 1000)], now)
      assert :not_found = Store.get(inst, :index, Keys.active(div(old, 1000), ord))
    end
  end

  # ── DUR-2.b: retention ───────────────────────────────────────────────────

  # Dead-letter every due row at `at` (its time of death), with its hook, as the
  # dispatch pipeline does.
  defp kill_all(inst, at \\ System.system_time(:millisecond)) do
    for {d, i} <- Enum.with_index(due_rows(inst)) do
      {:ok, bin} = Store.get(inst, :hooks, Keys.hook(d.seq))
      {:ok, env} = Envelope.from_binary(bin)
      row = row!(inst, d.seq, d.sink)

      write!(
        inst,
        Deliveries.dead_ops({:due, d.key, d.kind}, d.ord, d.seq, d.sink, row, at + i, "x", env)
      )
    end
  end

  defp listen(event) do
    handler = {:b35, event, make_ref()}
    pid = self()

    :telemetry.attach(
      handler,
      event,
      fn _event, measurements, meta, _ -> send(pid, {event, measurements, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  @day_ms 86_400_000

  describe "dlq reaper" do
    alias Ankusa.Queue.DlqReaper
    alias Ankusa.Storage.Compactor

    test "dlq reaper: a DLQ over max_rows shrinks by archived rows only, with a counter, an audit row and an event" do
      inst = start_instance(queue: %{dlq: %{max_rows: 3}})
      enqueue(inst, "acme", 3)
      kill_all(inst)
      assert {:ok, %{dead: 6, dead_bytes: bytes}} = Counters.read(inst, 0)
      assert bytes > 0

      # The compactor archives the hooks: they are now recoverable from a segment.
      assert {:ok, 1} = Compactor.tick(inst)
      listen([:ankusa, :dlq, :evicted])
      start_supervised!({DlqReaper, instance: inst})

      assert {:ok, %{evicted: 3, full: false}} = DlqReaper.reap(inst)
      assert {:ok, %{dead: 3}} = Counters.read(inst, 0)
      assert_counters_equal_fold(inst)
      refute DlqReaper.full?(inst)

      assert_receive {[:ankusa, :dlq, :evicted], %{count: 3},
                      %{reason: "max_rows", tenant_id: "acme", ids: ids, seqs: seqs}}

      assert length(ids) == 3 and length(seqs) == 3

      # An evicted hook is archived and findable, whether or not it was reclaimed.
      for id <- Enum.uniq(ids),
          do: assert({:ok, %Envelope{id: ^id}} = Ankusa.Storage.fetch(inst, id))

      # One batched audit row, not one per hook.
      assert {:ok, %{entries: [row]}} = Ankusa.Audit.list(inst, action: "dlq.evict")
      assert row.actor == "system"
      assert %{"count" => 3, "reason" => "max_rows", "seqs" => [_, _]} = row.detail
    end

    test "dlq reaper: rows past retention_days go, newer ones stay, and the byte counter follows" do
      inst = start_instance(queue: %{dlq: %{retention_days: 1}})
      enqueue(inst, "acme", 2)
      kill_all(inst, System.system_time(:millisecond) - 3 * @day_ms)
      enqueue(inst, "acme", 1)
      kill_all(inst)
      assert {:ok, 1} = Compactor.tick(inst)
      start_supervised!({DlqReaper, instance: inst})

      assert {:ok, %{evicted: 4, full: false}} = DlqReaper.reap(inst)
      assert {:ok, %{dead: 2, dead_bytes: kept}} = Counters.read(inst, 0)
      assert kept > 0
      assert_counters_equal_fold(inst)

      assert {:ok, %{entries: [%{detail: %{"reason" => "retention"}}]}} =
               Ankusa.Audit.list(inst, action: "dlq.evict")
    end

    test "dlq reaper never evicts an unarchived hook" do
      # No :storage role: nothing is ever archived, so nothing is recoverable.
      inst = start_instance(roles: [:edge], queue: %{dlq: %{max_rows: 2, retention_days: 1}})
      enqueue(inst, "acme", 3)
      kill_all(inst, System.system_time(:millisecond) - 3 * @day_ms)
      hooks = stored_ids(inst)
      start_supervised!({DlqReaper, instance: inst})
      refute DlqReaper.full?(inst)

      # Over the ceiling and past retention, yet nothing goes: the pressure shows.
      assert {:ok, %{evicted: 0, full: true}} = DlqReaper.reap(inst)
      assert DlqReaper.full?(inst)
      assert {:ok, %{dead: 6}} = Counters.read(inst, 0)
      assert stored_ids(inst) == hooks
      assert_counters_equal_fold(inst)

      # ...and the edge refuses new hooks with a Retry-After instead.
      conn = route_through_edge(%{instance: inst}, request("load", ~s({"n":1})))
      assert conn.status == 503
      assert Plug.Conn.get_resp_header(conn, "retry-after") == ["60"]
      assert JSON.decode!(conn.resp_body) == %{"error" => "dlq_full"}

      # An operator's purge is bound by the same rule.
      assert {:ok, %{deleted: 0, unarchived: 6}} = DlqReaper.evict(inst, %{}, 100)
      assert {:ok, %{dead: 6}} = Counters.read(inst, 0)
    end

    test "dlq reaper: a purge deletes archived rows of the filter's tenant and source only" do
      inst = start_instance()
      enqueue(inst, "acme", 2)
      enqueue(inst, "globex", 1)
      kill_all(inst)
      assert {:ok, 1} = Compactor.tick(inst)
      start_supervised!({DlqReaper, instance: inst})

      filter = %{tenants: ["acme"], source_id: "load"}

      assert {:ok, %{deleted: 4, unarchived: 0, bytes: bytes}} =
               DlqReaper.evict(inst, filter, 100, audit: %Ankusa.Audit.Context{actor: "ops"})

      assert bytes > 0
      assert {:ok, %{dead: 0}} = Queue.dead_for_tenant(inst, "acme")
      assert {:ok, %{dead: 2}} = Queue.dead_for_tenant(inst, "globex")
      assert_counters_equal_fold(inst)

      assert {:ok, %{entries: [%{actor: "ops", tenant: "acme"}]}} =
               Ankusa.Audit.list(inst, action: "dlq.purge")
    end
  end

  describe "quarantine retention" do
    alias Ankusa.Edge.Quarantine

    defp hold(inst, age_ms) do
      env = %{envelope("acme") | received_at: System.system_time(:millisecond) - age_ms}
      assert :ok = Quarantine.put(inst, env, :bad_signature)
      env
    end

    test "quarantine retention: entries past retention_days are deleted, with an audit row" do
      inst = start_instance(quarantine: %{retention_days: 1})
      old = hold(inst, 3 * @day_ms)
      older = hold(inst, 5 * @day_ms)
      fresh = hold(inst, 1_000)
      assert {:ok, %{bytes: before_bytes}} = Quarantine.held(inst)
      listen([:ankusa, :quarantine, :pruned])

      assert {:ok, %{deleted: 2, bytes: freed, more: false}} = Quarantine.prune(inst)
      assert freed > 0
      assert_receive {[:ankusa, :quarantine, :pruned], %{count: 2}, _}

      assert {:ok, entries} = Quarantine.recent(inst, 10)
      assert Enum.map(entries, & &1.id) == [fresh.id]
      refute Enum.any?(entries, &(&1.id in [old.id, older.id]))
      assert {:ok, %{bytes: after_bytes, entries: 1}} = Quarantine.held(inst)
      assert after_bytes == before_bytes - freed

      assert {:ok, %{entries: [%{detail: %{"deleted" => 2}}]}} =
               Ankusa.Audit.list(inst, action: "quarantine.retention")
    end

    test "quarantine retention: an entry a live replay job can still reach stays until it ends" do
      inst = start_instance(roles: [:edge, :dispatch], quarantine: %{retention_days: 1})
      for age <- [3, 4, 5], do: hold(inst, age * @day_ms)

      # A slow job over the whole pen: it releases one entry a second.
      assert {:ok, :created, job} =
               Ankusa.Replay.start(inst, %{kind: :quarantine, rate: 1})

      assert {:ok, %{deleted: 0}} = Quarantine.prune(inst)

      assert {:ok, _} = Ankusa.Replay.update(inst, job.id, %{state: :cancelled})
      assert {:ok, %{deleted: deleted}} = Quarantine.prune(inst)
      assert deleted >= 1
    end
  end

  describe "catalogue retention" do
    alias Ankusa.Storage.Compactor

    # A hook whose id says it was received `age_ms` ago, with no sinks (so it is
    # archived and then reclaimed) or with the two test sinks (so it stays owed).
    defp old_hook(inst, age_ms, sinks) do
      id = Ankusa.UUIDv7.generate(System.system_time(:millisecond) - age_ms)
      env = %{envelope("acme") | id: id}
      {:ok, [{:committed, _}]} = Queue.enqueue(inst, [%{envelope: env, sinks: sinks}])
      id
    end

    test "catalogue retention: a segment's row goes once its newest hook is past retention, and fetch stops finding it" do
      inst = start_instance(storage: %{catalogue_retention_days: 1})
      old = old_hook(inst, 3 * @day_ms, [])
      assert {:ok, 1} = Compactor.tick(inst)
      fresh = old_hook(inst, 1_000, [])
      assert {:ok, 1} = Compactor.tick(inst)
      assert {:ok, %Envelope{}} = Ankusa.Storage.fetch(inst, old)
      listen([:ankusa, :archive, :pruned])

      assert {:ok, %{segments: 1, legacy: 0, objects: 0}} = Compactor.prune_catalogue(inst)
      assert_receive {[:ankusa, :archive, :pruned], %{segments: 1}, _}
      assert Ankusa.Storage.fetch(inst, old) == :error
      assert {:ok, %Envelope{}} = Ankusa.Storage.fetch(inst, fresh)
    end

    test "catalogue retention: archive.retention_days also deletes the segment objects" do
      inst =
        start_instance(storage: %{catalogue_retention_days: 30}, archive: %{retention_days: 1})

      old_hook(inst, 3 * @day_ms, [])
      assert {:ok, 1} = Compactor.tick(inst)
      # the segment, its index and the node-day's manifest
      assert {:ok, [_, _, _] = keys} = Ankusa.BlobStore.list(inst, :segments, "archive/v1/")

      assert {:ok, %{segments: 1, objects: 2}} = Compactor.prune_catalogue(inst)
      # the manifest goes with the last segment it summarises
      assert {:ok, []} = Ankusa.BlobStore.list(inst, :segments, "archive/v1/")
      assert length(keys) == 3
    end

    test "catalogue retention: a segment holding a hook that is still owed, or one a live archive replay reads, stays" do
      inst = start_instance(storage: %{catalogue_retention_days: 1})
      owed = old_hook(inst, 3 * @day_ms, @sinks)
      assert {:ok, 1} = Compactor.tick(inst)
      assert {:ok, %{segments: 0}} = Compactor.prune_catalogue(inst)
      assert {:ok, %Envelope{}} = Ankusa.Storage.fetch(inst, owed)

      # Nobody owes it any more: the row is free to go.
      assert {:ok, _} = Queue.purge_tenant(inst, "acme", [:pending])
      assert {:ok, %{segments: 1}} = Compactor.prune_catalogue(inst)
      assert Ankusa.Storage.fetch(inst, owed) == :error
    end

    test "catalogue retention: a live archive replay job keeps the segments it can still read" do
      inst =
        start_instance(
          roles: [:edge, :dispatch, :storage],
          storage: %{catalogue_retention_days: 1}
        )

      for _ <- 1..3, do: old_hook(inst, 3 * @day_ms, [])
      assert {:ok, 1} = Compactor.tick(inst)
      now = System.system_time(:millisecond)

      assert {:ok, :created, job} =
               Ankusa.Replay.start(inst, %{
                 kind: :archive,
                 from: now - 4 * @day_ms,
                 to: now - 2 * @day_ms,
                 rate: 1
               })

      assert {:ok, %{segments: 0}} = Compactor.prune_catalogue(inst)
      assert {:ok, _} = Ankusa.Replay.update(inst, job.id, %{state: :cancelled})
      assert {:ok, %{segments: 1}} = Compactor.prune_catalogue(inst)
    end

    test "catalogue retention: a 0.3 import's location rows go with their hook's age" do
      inst = start_instance(storage: %{catalogue_retention_days: 1})
      old = Ankusa.UUIDv7.generate(System.system_time(:millisecond) - 3 * @day_ms)
      fresh = Ankusa.UUIDv7.generate()
      location = :erlang.term_to_binary({"seg/x", 0, 10, 1})

      :ok =
        Store.write(
          inst,
          [
            {:put, :archive, Keys.legacy_location(old), location},
            {:put, :archive, Keys.legacy_location(fresh), location}
          ],
          sync: true
        )

      assert {:ok, %{legacy: 1}} = Compactor.prune_catalogue(inst)
      assert Archive.legacy_location(inst, old) == :error
      assert {:ok, _} = Archive.legacy_location(inst, fresh)
    end
  end

  # DUR-2.c: the sink-down drill. A sink that never answers for `days` days, with
  # `hooks` hooks a day: every hook is dead-lettered on its day, archived, and
  # carries a claim pack that dates from that day and is leased to it. The
  # retention machinery then runs as it does on its ticks: the compactor, the DLQ
  # reaper, the catalogue prune and the claim sweeper, each in its own process (the
  # Writer is none of them). What the node still holds must be the retention
  # window's worth, whatever the length of the outage.
  #
  # The rate is scaled down: `DRILL_HOOKS` (default 20) hooks a day for
  # `DRILL_DAYS` (default 6) days, with a two-day DLQ and claim retention.
  describe "dlq reaper: the sink-down drill" do
    alias Ankusa.ClaimCheck
    alias Ankusa.ClaimCheck.{Ref, Sweeper}
    alias Ankusa.Queue.DlqReaper
    alias Ankusa.Storage.Compactor

    defp drill_env(name, default), do: String.to_integer(System.get_env(name, default))

    defp sink_down(days, hooks) do
      inst =
        start_instance(
          queue: %{dlq: %{retention_days: 2}},
          storage: %{interval_ms: 0, catalogue_retention_days: 2},
          archive: %{retention_days: 2},
          claim_check: %{retention_days: 2, redeem_grace_ms: 500}
        )

      start_supervised!({DlqReaper, instance: inst}, id: {:reaper, inst})
      now = System.system_time(:millisecond)

      for age <- (days - 1)..0//-1 do
        at = now - age * @day_ms

        entries =
          for _ <- 1..hooks do
            env = %{envelope("acme") | id: Ankusa.UUIDv7.generate(at), received_at: at}
            %{envelope: env, sinks: @sinks}
          end

        {:ok, committed} = Queue.enqueue(inst, entries)
        {:ok, ord} = Ankusa.Queue.Tenants.ord(inst, "acme")

        lease_ops =
          for {:committed, env} <- committed, reduce: [] do
            ops ->
              pack_id = Ref.pack_id(at, :crypto.strong_rand_bytes(8))
              item = %{id: env.id, body: String.duplicate("x", 200)}
              {:ok, claims} = ClaimCheck.check_in(inst, "acme", [item], pack_id: pack_id)
              Deliveries.claim_ops(env.seq, ord, claims[env.id]) ++ ops
          end

        write!(inst, lease_ops)
        kill_all(inst, at)
        {:ok, _segments} = Compactor.tick(inst)
      end

      inst
    end

    defp reap_all(inst) do
      assert {:ok, %{more: more}} = DlqReaper.reap(inst)
      if more, do: reap_all(inst)
    end

    defp sweep_all(inst, opts) do
      report = Sweeper.report(inst, opts)

      if report.leases_pruned + report.packs > 0 or report.backlog > 0 or
           report.partitions_left > 0,
         do: sweep_all(inst, opts),
         else: report
    end

    defp footprint(inst) do
      root = Ankusa.Config.path(Ankusa.config(inst), "segments")
      files = root |> Path.join("**/*") |> Path.wildcard() |> Enum.filter(&File.regular?/1)
      {packs, segments} = Enum.split_with(files, &String.contains?(&1, "/claims/"))
      size = fn paths -> paths |> Enum.map(&File.stat!(&1).size) |> Enum.sum() end
      {:ok, hooks} = Queue.hooks(inst, 0, 1_000_000)
      {:ok, counters} = Counters.read(inst, 0)

      %{
        hooks: length(hooks),
        dead: counters.dead,
        leases: counters.claim_leases,
        packs: length(packs),
        pack_bytes: size.(packs),
        segments: length(segments),
        segment_bytes: size.(segments)
      }
    end

    # The retention tick, in the order the processes run: the reaper evicts the
    # dead rows whose hooks are archived, the catalogue prune drops the segments
    # nobody needs, the sweeper clears the leases whose grace is over and the
    # packs they no longer hold.
    defp retention_tick(inst) do
      reap_all(inst)
      assert {:ok, _pruned} = Compactor.prune_catalogue(inst)
      sweep_all(inst, now: System.system_time(:millisecond) + 1_000)
    end

    @tag timeout: 600_000
    test "dlq reaper: a sink down for longer than retention keeps the node's footprint flat" do
      hooks = drill_env("DRILL_HOOKS", "20")
      days = max(drill_env("DRILL_DAYS", "6"), 4)

      # The outage is as long as the retention window plus one day...
      short = sink_down(3, hooks)
      retention_tick(short)
      window = footprint(short)

      # ...or much longer: the same node holds the same things afterwards.
      long = sink_down(days, hooks)
      assert footprint(long).dead == 2 * hooks * days

      # Before the reaper runs, every dead hook's pack is leased: the ones whose day
      # is past the date cutoff survive the sweeper.
      kept = Sweeper.report(long, now: System.system_time(:millisecond) + 1_000)
      assert kept.packs == 0
      assert kept.kept >= hooks * (days - 3)

      retention_tick(long)
      counted = [:hooks, :dead, :leases, :packs, :segments]
      grown = footprint(long)
      assert Map.take(grown, counted) == Map.take(window, counted)
      assert_counters_equal_fold(long)
      assert_counters_equal_fold(short)
      assert grown.pack_bytes <= window.pack_bytes * 1.1
      assert grown.segment_bytes <= window.segment_bytes * 1.1

      # Two days of rows are left: ages 0 and 1, two sinks each; their packs and
      # leases, and the day-2 packs, whose directory is not past the cutoff yet.
      assert window.hooks == 2 * hooks
      assert window.dead == 4 * hooks
      assert window.leases == 2 * hooks
      assert window.packs == 3 * hooks
    end

    test "the sweeps run in processes of their own, never in the writer" do
      inst = start_instance(claim_check: %{retention_days: 2})
      start_supervised!({DlqReaper, instance: inst})

      writer = Ankusa.whereis(inst, :queue_writer)
      sweeper = Ankusa.whereis(inst, :claim_check_sweeper)
      reaper = Ankusa.whereis(inst, :dlq_reaper)
      assert is_pid(writer) and is_pid(sweeper) and is_pid(reaper)
      assert length(Enum.uniq([writer, sweeper, reaper])) == 3

      # The Writer applies a chunk of expired dedupe keys the DedupeSweeper found; it
      # holds no claim, DLQ or file retention code.
      source = File.read!(Path.expand("../../../lib/ankusa/queue/writer.ex", __DIR__))
      refute source =~ ~r/ClaimCheck|DlqReaper|File\.rm/
    end
  end

  describe "the layout" do
    test "only Deliveries builds a ?D, ?F, ?X or ?A key" do
      lib = Path.expand("../../../lib", __DIR__)

      allowed = [
        Path.join(lib, "ankusa/queue/deliveries.ex"),
        Path.join(lib, "ankusa/store/keys.ex")
      ]

      constructor = ~r/Keys\.(due|inflight|dead_by_tenant|active)\(|<<\?[DFXA]\b/

      offenders =
        for file <- Path.wildcard(Path.join(lib, "**/*.ex")),
            file not in allowed,
            line <- File.read!(file) |> String.split("\n"),
            not String.starts_with?(String.trim(line), "#"),
            Regex.match?(constructor, line),
            do: "#{Path.relative_to(file, lib)}: #{String.trim(line)}"

      assert offenders == []
    end
  end
end
