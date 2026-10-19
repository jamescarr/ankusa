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
    {:ok, rows} = Store.fold(inst, family, {lo, hi}, [], fn k, v, acc -> {:cont, [fun.(k, v) | acc]} end)
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
      | Deliveries.new_ops(ord, System.system_time(:millisecond), seq, @unresolved, byte_size(bin), row)
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
          Deliveries.expand_ops(f.ord, f.seq, @unresolved, f.size, "load", @sinks, MapSet.new(), now)
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
          Deliveries.dead_ops(:inflight, f.ord, f.seq, f.sink, r, System.system_time(:millisecond), "x", nil)
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
      config = test_config(roles: [:edge], source_store: {Ankusa.SourceStore.Static, sources: %{}})
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
               Queue.enqueue(inst, for(t <- ["a", "b"], do: %{envelope: envelope(t), sinks: @sinks})) |> elem(1)

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

      assert {:ok, [{:committed, _}]} = Queue.enqueue(inst, [%{envelope: envelope("only"), sinks: @sinks}])

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

  describe "the layout" do
    test "only Deliveries builds a ?D, ?F, ?X or ?A key" do
      lib = Path.expand("../../../lib", __DIR__)
      allowed = [Path.join(lib, "ankusa/queue/deliveries.ex"), Path.join(lib, "ankusa/store/keys.ex")]
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
