defmodule Ankusa.Dispatch.OrderingTest do
  @moduledoc """
  Opt-in ordering lanes (ADR 0004, `DEL-1.b`): a sink entry with `ordered: true`
  delivers the hooks of one lane (`{tenant, source, sink, key}`) one at a time in
  `seq` order, whatever retries, restarts and other lanes do.

  The end-to-end tests run the real `Ankusa.Dispatch.Pipeline` against a sink that
  tells the test what it was handed and when; the last two work on the store's
  index families directly, with the Pipeline's own op builders and
  `Deliveries.Lanes.expand/3`, to check the invariant every transition has to keep
  (*every `?O` member has an `?H` or a `?P`; every `?H` names exactly one row in
  `?D` or `?F`*) and the race the invariant exists for.
  """

  use ExUnit.Case, async: false
  use ExUnitProperties

  import Ankusa.TestHelpers

  alias Ankusa.{Envelope, Ordering, Queue}
  alias Ankusa.Edge.Ingest
  alias Ankusa.Queue.Deliveries
  alias Ankusa.Queue.Deliveries.{Counters, Lanes}
  alias Ankusa.Sink.Log
  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @moduletag capture_log: true

  # What the sink does is decided by the hook's body:
  #
  #   "poison"       a permanent error (dead-letter), unless the hook is a replay
  #   "slow-…"       sleeps `:slow_ms`
  #   "retry-…"      asks for a pause of an hour: the head waits out a retry
  #   "hold-…"       blocks while `:agent` holds `:hold`
  #   a body in `:fail_once` fails its first attempt
  #
  # and tells `:pid` `{:attempt, body, attempt, ordering_key, lane}` for every call
  # and `{:ok, body}` for every success. `:concurrent` is an `:atomics` of two
  # cells: the attempts running now, and the most that ever ran at once.
  defmodule LaneSink do
    @moduledoc false
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, ctx, opts) do
      pid = Keyword.fetch!(opts, :pid)
      body = env.body
      concurrent = Keyword.get(opts, :concurrent)
      if concurrent, do: enter(concurrent)

      send(pid, {:attempt, body, ctx.attempt, ctx[:ordering_key], ctx[:lane]})
      result = behave(body, ctx, opts)

      if concurrent, do: :atomics.sub(concurrent, 1, 1)
      if result == :ok, do: send(pid, {:ok, body})
      result
    end

    defp enter(ref) do
      now = :atomics.add_get(ref, 1, 1)
      if now > :atomics.get(ref, 2), do: :atomics.put(ref, 2, now)
    end

    defp behave("poison", ctx, _opts) do
      if ctx[:replay_id], do: :ok, else: {:error, {:permanent, :poison}}
    end

    defp behave("slow-" <> _, _ctx, opts) do
      Process.sleep(Keyword.fetch!(opts, :slow_ms))
      :ok
    end

    defp behave("retry-" <> _, _ctx, _opts), do: {:error, {:retry_after, 3_600_000, :later}}

    defp behave("hold-" <> _ = body, _ctx, opts) do
      send(Keyword.fetch!(opts, :pid), {:held, body})
      hold(Keyword.fetch!(opts, :agent))
    end

    defp behave(body, ctx, opts) do
      if body in Keyword.get(opts, :fail_once, []) and ctx.attempt == 1 do
        {:error, :boom}
      else
        Process.sleep(Keyword.get(opts, :delay_ms, 0))
        :ok
      end
    end

    defp hold(agent) do
      if Agent.get(agent, & &1) == :hold do
        Process.sleep(5)
        hold(agent)
      else
        :ok
      end
    end
  end

  defp retry, do: {Ankusa.RetryPolicy.Exponential, base_ms: 0, max_attempts: 10, jitter: false}

  # A source whose one sink is ordered; `key` is the `ordering_key` option.
  defp ordered(sink_opts, key \\ {:header, "x-key"}) do
    opts = if key, do: [ordered: true, ordering_key: key], else: [ordered: true]

    [verifier: {Ankusa.Verifier.None, []}, sinks: [{LaneSink, sink_opts ++ opts}]]
  end

  defp config(sources, overrides \\ []) do
    test_config(
      Keyword.merge(
        [
          roles: [:edge, :dispatch],
          source_store: {Ankusa.SourceStore.Static, sources: sources},
          dispatch: %{retry: retry()}
        ],
        overrides
      )
    )
  end

  defp ingest!(inst, source_id, body, headers, tenant \\ nil) do
    request =
      %{source_id: source_id, method: "POST", path: "/w", headers: headers, body: body}
      |> then(&if(tenant, do: Map.put(&1, :tenant_id, tenant), else: &1))

    {:ok, env} = Ingest.ingest(inst, request)
    env
  end

  defp key(value), do: [{"x-key", value}]

  # `n` messages `{:ok, body}`, in the order the sink succeeded.
  defp oks(n, timeout \\ 30_000) do
    for _ <- 1..n do
      receive do
        {:ok, body} -> body
      after
        timeout -> flunk("fewer than #{n} deliveries succeeded")
      end
    end
  end

  defp attempts do
    receive do
      {:attempt, body, attempt, _key, _lane} -> [{body, attempt} | attempts()]
    after
      0 -> []
    end
  end

  defp wait_until(fun, timeout \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait(fun, deadline)
  end

  defp do_wait(fun, deadline) do
    cond do
      fun.() -> :ok
      System.monotonic_time(:millisecond) > deadline -> flunk("condition not met in time")
      true -> Process.sleep(10) && do_wait(fun, deadline)
    end
  end

  defp dispatch_state(inst), do: :sys.get_state(Ankusa.whereis(inst, :dispatch))

  defp counters(inst, ord \\ 0) do
    {:ok, counters} = Counters.read(inst, ord)
    counters
  end

  describe "an ordered lane" do
    test "ordered lane: 100 hooks for one key, a mid-stream retry, arrive in order one at a time" do
      concurrent = :atomics.new(2, signed: false)

      config =
        config(%{
          "s" => ordered(pid: self(), concurrent: concurrent, fail_once: ["50"], delay_ms: 1)
        })

      start_supervised!({Ankusa.Instance, config})
      inst = config.instance

      for n <- 1..100, do: ingest!(inst, "s", Integer.to_string(n), key("customer-1"))

      assert oks(100) == for(n <- 1..100, do: Integer.to_string(n))

      # The failed attempt blocked its lane and nothing else: it is retried before
      # anything behind it ran.
      expected =
        for n <- 1..100, attempt <- if(n == 50, do: [1, 2], else: [1]) do
          {Integer.to_string(n), attempt}
        end

      assert attempts() == expected

      # One attempt of the lane in flight at a time.
      assert :atomics.get(concurrent, 2) == 1

      wait_until(fn -> counters(inst).lanes == 0 end)
      assert %{lanes: 0, lanes_blocked: 0, lane_members: 0, pending: 0, inflight: 0} = counters(inst)
      assert stored_counters(inst) == folded_counters(inst)
    end

    test "the sink gets the lane's key and id; a hook's lane is per tenant" do
      config = config(%{"s" => ordered(pid: self())})
      start_supervised!({Ankusa.Instance, config})
      inst = config.instance

      ingest!(inst, "s", "a", key("customer-1"), "acme")
      ingest!(inst, "s", "b", key("customer-1"), "globex")

      for _ <- 1..2 do
        assert_receive {:attempt, body, 1, "customer-1", lane}, 5_000
        assert body in ["a", "b"]
        assert lane == Base.encode16(Ordering.lane_id(0, "s", "customer-1"), case: :lower)
      end
    end
  end

  describe "other keys concurrent" do
    test "other keys concurrent: a slow lane holds no slot, window room or process" do
      config = config(%{"s" => ordered(pid: self(), slow_ms: 1_500, delay_ms: 0)})
      start_supervised!({Ankusa.Instance, config})
      inst = config.instance

      for n <- 1..3, do: ingest!(inst, "s", "slow-#{n}", key("slow"))
      # The same key under another tenant is another lane.
      ingest!(inst, "s", "slow-other", key("slow"), "globex")
      for n <- 1..5, do: ingest!(inst, "s", "fast-#{n}", key("fast"))

      # Every fast hook is delivered long before the first slow one is done.
      fast = oks(5, 1_000)
      assert fast == for(n <- 1..5, do: "fast-#{n}")

      # Two slow heads run (one per tenant); the two hooks behind the first are
      # members: in the store, and in no slot, window or queue.
      # The fast lane's head has been released by now; the two slow heads run.
      wait_until(fn -> counters(inst).lanes == 2 end)
      state = dispatch_state(inst)
      assert state.claimed == 2
      assert map_size(state.running) == 2
      assert state.queued == 0
      assert %{lanes: 2, lane_members: 2} = counters(inst)

      # Then each slow lane runs on, in order.
      slow = oks(4)
      assert Enum.filter(slow, &(&1 != "slow-other")) == ["slow-1", "slow-2", "slow-3"]
      assert "slow-other" in slow
    end
  end

  describe "a keyless lane" do
    test "keyless lane: with no ordering_key every hook of the sink is in one lane" do
      config = config(%{"plain" => ordered([pid: self()], nil)})
      start_supervised!({Ankusa.Instance, config})
      inst = config.instance

      for n <- 1..20, do: ingest!(inst, "plain", "#{n}", [])

      assert oks(20) == for(n <- 1..20, do: "#{n}")

      # The default group the queue adapters use: `tenant/source`.
      assert {:attempt, "1", 1, "default/plain", _lane} =
               List.keyfind(attempts_with_keys(), "1", 1)
    end

    test "a hook with no key (a configured header absent) goes to the keyless lane, in order" do
      config = config(%{"s" => ordered(pid: self())})
      start_supervised!({Ankusa.Instance, config})
      inst = config.instance

      for n <- 1..10, do: ingest!(inst, "s", "#{n}", [])

      assert oks(10) == for(n <- 1..10, do: "#{n}")
    end

    test "key missing counter: one event per hook whose configured key yielded nothing" do
      handler = "ordering-missing-#{System.unique_integer([:positive])}"
      test = self()

      :telemetry.attach(
        handler,
        [:ankusa, :dispatch, :lane_key_missing],
        fn _event, _measurements, metadata, _config -> send(test, {:missing, metadata}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      config = config(%{"s" => ordered(pid: self())})
      start_supervised!({Ankusa.Instance, config})
      inst = config.instance

      ingest!(inst, "s", "a", [])
      ingest!(inst, "s", "b", key("k"))
      ingest!(inst, "s", "c", [{"x-other", "v"}])
      oks(3)

      assert_received {:missing, %{instance: ^inst, sink: LaneSink}}
      assert_received {:missing, %{instance: ^inst, sink: LaneSink}}
      refute_received {:missing, _}
    end
  end

  describe "a dead head" do
    test "dead head releases lane: a poison hook does not hold its key forever" do
      config = config(%{"s" => ordered(pid: self())})
      start_supervised!({Ankusa.Instance, config})
      inst = config.instance

      ingest!(inst, "s", "poison", key("k"))
      for n <- 1..3, do: ingest!(inst, "s", "after-#{n}", key("k"))

      assert oks(3) == ["after-1", "after-2", "after-3"]

      wait_until(fn -> counters(inst).lanes == 0 end)
      assert {:ok, %{total: 1, entries: [%{envelope: %{body: "poison"}}]}} =
               Queue.dead(inst, limit: 10)

      assert stored_counters(inst) == folded_counters(inst)
    end
  end

  describe "replay" do
    test "replay respects lanes: a revived hook re-enters as a member, ahead of later hooks" do
      agent = start_supervised!({Agent, fn -> :hold end})
      config = config(%{"s" => ordered(pid: self(), agent: agent)})
      start_supervised!({Ankusa.Instance, config})
      inst = config.instance

      ingest!(inst, "s", "poison", key("k"))
      wait_until(fn -> match?({:ok, %{total: 1}}, Queue.dead(inst, limit: 10)) end)
      wait_until(fn -> counters(inst).lanes == 0 end)

      # The lane is busy with a held head, and a later hook waits behind it.
      ingest!(inst, "s", "hold-1", key("k"))
      assert_receive {:held, "hold-1"}, 5_000
      ingest!(inst, "s", "later", key("k"))
      wait_until(fn -> counters(inst).lane_members == 1 end)

      {:ok, [hit], _last, _exhausted, _scanned} =
        Deliveries.dead_page(inst, {<<?x>>, <<?y>>}, %{}, 10, 100)

      {:ok, ops, 1} =
        Deliveries.revive_ops(inst, [hit], System.system_time(:millisecond), "replay-1")

      refute Enum.any?(ops, &match?({:put, :index, <<?D, _::binary>>, _}, &1))
      :ok = Store.write(inst, Deliveries.coalesce(ops), sync: true)

      # Never beside the head: a member, with the later hook, lowest seq first.
      assert %{lane_members: 2, lanes: 1} = counters(inst)

      Agent.update(agent, fn _ -> :open end)

      assert oks(3) == ["hold-1", "poison", "later"]
      assert stored_counters(inst) == folded_counters(inst)
    end
  end

  describe "restart" do
    test "survives restart: a busy lane, a retrying head and their members" do
      agent = start_supervised!({Agent, fn -> :hold end})
      config = config(%{"s" => ordered(pid: self(), agent: agent)})
      start_supervised!({Ankusa.Instance, config}, id: :first)
      inst = config.instance

      # Lane k: a head that is claimed and never finishes, two members behind it.
      ingest!(inst, "s", "hold-1", key("k"))
      assert_receive {:held, "hold-1"}, 5_000
      for n <- 2..3, do: ingest!(inst, "s", "k-#{n}", key("k"))
      # Lane j: a head waiting out a retry, two members behind it.
      ingest!(inst, "s", "retry-1", key("j"))
      assert_receive {:attempt, "retry-1", 1, _key, _lane}, 5_000
      for n <- 2..3, do: ingest!(inst, "s", "j-#{n}", key("j"))

      wait_until(fn -> match?(%{lanes: 2, lanes_blocked: 1, lane_members: 4}, counters(inst)) end)
      :ok
      stop_supervised!(:first)

      # What the store holds now is lane state: heads, members, markers.
      Agent.update(agent, fn _ -> :open end)
      start_supervised!({Ankusa.Instance, config}, id: :second)

      # The claimed head goes back as a first attempt, before any later hook: a
      # duplicate of that hook (at-least-once), in order.
      assert oks(3) == ["hold-1", "k-2", "k-3"]
      assert_received {:attempt, "hold-1", 1, _key, _lane}

      # The retrying head is still waiting out its retry, with its members behind.
      refute_receive {:ok, _body}, 200

      wait_until(fn -> counters(inst).lanes == 1 end)
      assert %{lanes: 1, lanes_blocked: 1, lane_members: 2} = counters(inst)
      assert stored_counters(inst) == folded_counters(inst)
    end

    test "survives restart: members written with no dispatch running are delivered in order" do
      config =
        config(%{"s" => ordered(pid: self())}, roles: [:edge])

      start_supervised!({Ankusa.Instance, config}, id: :edge)
      inst = config.instance

      for n <- 1..5, do: ingest!(inst, "s", "#{n}", key("k"))
      assert %{lane_members: 5, lanes: 0} = counters(inst)
      :ok
      stop_supervised!(:edge)

      start_supervised!({Ankusa.Instance, %{config | roles: [:edge, :dispatch]}}, id: :dispatch)

      assert oks(5) == ["1", "2", "3", "4", "5"]
    end
  end

  describe "the lane invariant" do
    # What the Pipeline does for one visit of a tenant.
    defp sweep_all!(inst) do
      {:ok, next} = Ankusa.Queue.Tenants.next_ord(inst)

      for ord <- 1..(next - 1)//1 do
        {:ok, ops, _more} = Lanes.sweep(inst, ord, System.system_time(:millisecond) + 1, 100)
        write!(inst, ops)
      end
    end

    defp write!(inst, ops) do
      {:ok, ops} = Lanes.expand(inst, ops, System.system_time(:millisecond))
      :ok = Store.write(inst, Deliveries.coalesce(ops), sync: true)
    end

    defp start_store do
      config =
        test_config(
          roles: [:edge],
          source_store: {Ankusa.SourceStore.Static, sources: %{}}
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
        path: "/w",
        headers: [],
        body: ~s({"n":1}),
        size: 7
      }
    end

    defp lane_ingest(inst, tenant, key) do
      lane = Ordering.lane_id(0, "load", key)
      entry = %{envelope: envelope(tenant), sinks: [{Log, []}], lanes: [{lane, key}]}
      {:ok, [{:committed, env}]} = Queue.enqueue(inst, [entry])
      env
    end

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

    defp pick([], _i), do: nil
    defp pick(list, i), do: Enum.at(list, rem(i, length(list)))

    defp row!(inst, seq, sink) do
      {:ok, row} = Deliveries.row(inst, seq, sink)
      row
    end

    defp apply_op(inst, {:ingest, t, k}), do: lane_ingest(inst, "t#{t}", "k#{k}")
    defp apply_op(inst, :sweep), do: sweep_all!(inst)

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
      if f = pick(inflight_rows(inst), i) do
        row = row!(inst, f.seq, f.sink)
        write!(inst, Deliveries.delivered_ops(f.ord, f.seq, f.sink, f.size, row))
      end
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

    defp apply_op(inst, {:dead_member, i}) do
      members = fold(inst, :lane_members, fn key, <<size::32>> -> {key, size} end)

      with {key, _size} <- pick(members, i) do
        {ord, _lane, seq, sink} = Keys.decode_lane_member(key)
        row = row!(inst, seq, sink)
        now = System.system_time(:millisecond)

        write!(
          inst,
          Deliveries.dead_ops({:member, key}, ord, seq, sink, row, now, "x", nil)
        )
      end
    end

    defp apply_op(inst, {:orphan, i}) do
      if f = pick(inflight_rows(inst), i) do
        row = row!(inst, f.seq, f.sink)
        write!(inst, Deliveries.orphan_ops(:inflight, f.ord, f.seq, f.sink, f.size, row))
      end
    end

    defp apply_op(inst, {:revive, i}) do
      {:ok, hits, _last, _done, _scanned} =
        Deliveries.dead_page(inst, {<<?x>>, <<?y>>}, %{}, 1_000, 10_000)

      if hit = pick(hits, i) do
        {:ok, ops, _revived} =
          Deliveries.revive_ops(inst, [hit], System.system_time(:millisecond), "replay")

        write!(inst, ops)
      end
    end

    defp apply_op(inst, :recover),
      do: {:ok, _} = Deliveries.recover_inflight(inst, System.system_time(:millisecond))

    defp apply_op(inst, {:purge, t, :pending}),
      do: {:ok, _} = Queue.purge_tenant(inst, "t#{t}", [:pending])

    defp apply_op(inst, {:purge, t, :all}),
      do: {:ok, _} = Queue.purge_tenant(inst, "t#{t}", [:pending, :inflight, :dead])

    defp ops_gen do
      index = integer(0..99)
      tenant = integer(0..1)
      key = integer(0..2)

      list_of(
        one_of([
          tuple({constant(:ingest), tenant, key}),
          tuple({constant(:ingest), tenant, key}),
          tuple({constant(:ingest), tenant, key}),
          constant(:sweep),
          constant(:sweep),
          tuple({constant(:claim), index}),
          tuple({constant(:claim), index}),
          tuple({constant(:retry), index}),
          tuple({constant(:deliver), index}),
          tuple({constant(:deliver), index}),
          tuple({constant(:dead), index}),
          tuple({constant(:dead_due), index}),
          tuple({constant(:dead_member), index}),
          tuple({constant(:orphan), index}),
          tuple({constant(:revive), index}),
          constant(:recover),
          tuple({constant(:purge), tenant, member_of([:pending, :all])})
        ]),
        max_length: 40
      )
    end

    # Every `?O` has an `?H` or a `?P`; every `?H` names exactly one row of `?D` or
    # `?F`, and that row names the lane; a laned due row has its head; counters
    # equal a fold.
    defp assert_lane_invariants(inst) do
      heads = fold(inst, :lane_heads, fn key, value -> {Keys.decode_lane_head(key), value} end)
      markers = fold(inst, :lane_markers, fn key, _value -> Keys.decode_lane_marker(key) end)
      held = MapSet.new(heads, fn {lane, _value} -> lane end)
      marked = MapSet.new(markers)

      for {ord, lane, seq, sink} <- fold(inst, :lane_members, fn key, _v -> Keys.decode_lane_member(key) end) do
        assert MapSet.member?(held, {ord, lane}) or MapSet.member?(marked, {ord, lane}),
               "member #{seq}/#{sink} of a lane with neither a head nor a marker"
      end

      due = due_rows(inst)
      claimed = inflight_rows(inst)

      for {{ord, lane}, <<seq::64, sink::16>>} <- heads do
        in_due = Enum.count(due, &(&1.ord == ord and &1.seq == seq and &1.sink == sink))
        in_flight = Enum.count(claimed, &(&1.ord == ord and &1.seq == seq and &1.sink == sink))
        assert in_due + in_flight == 1, "head #{seq}/#{sink} is in #{in_due + in_flight} places"

        assert %{lane: ^lane, state: :pending} = row!(inst, seq, sink)
      end

      laned_due = Enum.count(due, &(&1.kind in [2, 3]))

      laned_claimed =
        Enum.count(claimed, &match?(%{lane: _}, row!(inst, &1.seq, &1.sink)))

      assert length(heads) == laned_due + laned_claimed
      assert Enum.all?(due, fn d -> (d.kind in [2, 3]) == Map.has_key?(row!(inst, d.seq, d.sink), :lane) end)

      assert stored_counters(inst) == folded_counters(inst)
    end

    property "lane invariant: random interleavings of ingest, claim, retry, delivered, dead, revive and purge" do
      inst = start_store()

      check all(ops <- ops_gen(), max_runs: 40) do
        Enum.each(ops, fn op ->
          apply_op(inst, op)
          assert_lane_invariants(inst)
        end)

        # And the lanes are live: after a sweep every lane with a member has a head.
        sweep_all!(inst)
        assert_lane_invariants(inst)

        held =
          inst
          |> fold(:lane_heads, fn key, _value -> Keys.decode_lane_head(key) end)
          |> MapSet.new()

        for {ord, lane, _seq, _sink} <-
              fold(inst, :lane_members, fn key, _v -> Keys.decode_lane_member(key) end) do
          assert MapSet.member?(held, {ord, lane}), "a lane with members and no head"
        end
      end
    end

    test "racing writer: a member committed between a release's read and its write is promoted by the sweep" do
      inst = start_store()
      first = lane_ingest(inst, "acme", "k")
      ord = tenant_ord!(inst, "acme")
      lane = Ordering.lane_id(0, "load", "k")

      sweep_all!(inst)
      [head] = due_rows(inst)
      assert head.seq == first.seq
      :ok = Deliveries.claim(inst, [head])
      row = row!(inst, head.seq, head.sink)

      # The release reads the lane now: no member behind the head.
      ops = Deliveries.delivered_ops(ord, head.seq, head.sink, head.size, row)
      assert {:ok, expanded} = Lanes.expand(inst, ops, System.system_time(:millisecond))
      assert {:delete, :index, Keys.lane_head(ord, lane)} in expanded

      # A writer commits a member (and its marker) before the release is written.
      second = lane_ingest(inst, "acme", "k")
      :ok = Store.write(inst, Deliveries.coalesce(expanded), sync: true)

      # The release took the head away; the writer's marker survived it.
      assert [{^ord, ^lane}] = fold(inst, :lane_markers, fn k, _v -> Keys.decode_lane_marker(k) end)
      assert due_rows(inst) == []
      assert_lane_invariants(inst)

      # The sweep finds a member with no head, and promotes it.
      sweep_all!(inst)
      second_seq = second.seq
      assert [%{seq: ^second_seq, kind: 2}] = due_rows(inst)
      assert %{lanes: 1, lane_members: 0} = counters(inst)
      assert_lane_invariants(inst)
    end
  end

  # `{:attempt, ...}` messages, as the sink sent them, for tests that read the
  # ordering key too.
  defp attempts_with_keys do
    receive do
      {:attempt, body, attempt, key, lane} ->
        [{:attempt, body, attempt, key, lane} | attempts_with_keys()]
    after
      0 -> []
    end
  end
end
