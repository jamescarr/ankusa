defmodule Ankusa.Edge.FleetTest do
  @moduledoc """
  Fleet-scoped admission (`Ankusa.Edge.Fleet`) against a stub fleet store: real
  edge instances, real POSTs through the router, and the store a test can slow,
  kill and inspect. The Redis implementation of the same behaviours is proven in
  `ankusa_redis`; this file owns what the core promises regardless of the store —
  one call per hook, the dedupe claim being two-phase, a failing or slow store
  never refusing a hook, and the breaker.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Edge.{Fleet, Router}

  defmodule StubStore do
    @moduledoc false
    # State lives in a public ETS table the test owns (`table:` in the store's
    # options), so two instances in one VM share one "fleet". `:mode` is `:ok`,
    # `:down` (every call errors at once) or `{:slow, ms}` (a call sleeps its whole
    # budget, then errors, as a Redis that answers late does).

    @behaviour Ankusa.RateLimiter.Store
    @behaviour Ankusa.Dedupe.Store

    def child_spec(opts),
      do: %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}

    @impl Ankusa.RateLimiter.Store
    def start_link(_opts), do: :ignore

    defp table(instance) do
      {_mod, opts} = Ankusa.config(instance).fleet.store
      Keyword.fetch!(opts, :table)
    end

    @impl Ankusa.RateLimiter.Store
    def hit(instance, charges, claim, timeout_ms) do
      t = table(instance)
      :ets.update_counter(t, :hits, 1, {:hits, 0})
      :ets.insert(t, {:last_charges, charges})

      case :ets.lookup(t, :mode) do
        [{:mode, :down}] ->
          {:error, :econnrefused}

        [{:mode, {:slow, ms}}] ->
          Process.sleep(min(ms, timeout_ms))
          {:error, :timeout}

        _ ->
          claim(t, claim)
      end
    end

    defp claim(_t, nil), do: :ok

    defp claim(t, claim) do
      key = {claim.tenant, claim.source_id, claim.key}

      if :ets.insert_new(t, {key, {:pending, claim.node, claim.hook_id}}) do
        :ok
      else
        case :ets.lookup(t, key) do
          [{_key, {:committed, id}}] -> {:duplicate, id}
          _pending -> {:pending, 1_000}
        end
      end
    end

    @impl Ankusa.RateLimiter.Store
    def reset(_instance, _tenant, _timeout_ms), do: :ok

    @impl Ankusa.Dedupe.Store
    def settle(instance, ops, _timeout_ms) do
      t = table(instance)

      if :ets.member(t, :hold_settle) do
        {:error, :held}
      else
        Enum.each(ops, &apply_op(t, &1))
        :ok
      end
    end

    defp apply_op(t, {:promote, claim, hook_id}) do
      key = {claim.tenant, claim.source_id, claim.key}
      own = {:pending, claim.node, claim.hook_id}

      case :ets.lookup(t, key) do
        [] -> :ets.insert(t, {key, {:committed, hook_id}})
        [{_key, ^own}] -> :ets.insert(t, {key, {:committed, hook_id}})
        _other -> :ok
      end
    end

    defp apply_op(t, {:release, claim}) do
      key = {claim.tenant, claim.source_id, claim.key}
      :ets.match_delete(t, {key, {:pending, claim.node, claim.hook_id}})
    end
  end

  defmodule FailingSink do
    @moduledoc false
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, _opts), do: {:error, :nope}
  end

  defp new_table do
    table = :"fleet_stub_#{System.unique_integer([:positive])}"
    :ets.new(table, [:named_table, :public, :set])
    table
  end

  defp hits(table) do
    case :ets.lookup(table, :hits) do
      [{:hits, n}] -> n
      [] -> 0
    end
  end

  defp start_edge(table, overrides \\ [], sinks \\ [{Ankusa.Sink.Log, []}]) do
    config =
      test_config(
        [
          roles: [:edge],
          fleet: %{store: {StubStore, table: table}, timeout_ms: 20},
          dedupe: %{scope: :fleet},
          source_store:
            {Ankusa.SourceStore.Static,
             sources: %{
               "demo" => [
                 verifier: {Ankusa.Verifier.None, []},
                 sinks: sinks,
                 dedupe: %{header: "x-event-id"}
               ]
             }}
        ] ++ overrides
      )

    start_supervised!({Ankusa.Instance, config}, id: config.instance)
    config
  end

  defp post(config, event_id \\ nil) do
    headers = if event_id, do: [{"x-event-id", event_id}], else: []

    conn =
      Enum.reduce(headers, Plug.Test.conn(:post, "/webhooks/demo", "{}"), fn {k, v}, c ->
        Plug.Conn.put_req_header(c, k, v)
      end)

    Router.call(conn, Router.init(instance: config.instance))
  end

  # The claim outcomes are settled by a process every few milliseconds.
  defp settled(table, key, expected) do
    deadline = System.monotonic_time(:millisecond) + 2_000

    Stream.repeatedly(fn ->
      Process.sleep(10)
      :ets.lookup(table, key)
    end)
    |> Enum.find(fn rows ->
      rows == expected or System.monotonic_time(:millisecond) > deadline
    end)
  end

  describe "fleet dedupe" do
    test "one store call carries the quota and the claim, and the claim is promoted after the commit" do
      table = new_table()

      config =
        start_edge(table, rate_limits: %{scope: :fleet, default: %{rate: 1000, burst: 1000}})

      conn = post(config, "evt-1")
      assert conn.status == 201
      %{"id" => id} = JSON.decode!(conn.resp_body)

      assert hits(table) == 1
      assert [{:hooks, "default", _inc, _cap}] = :ets.lookup_element(table, :last_charges, 2)

      key = {"default", "demo", "evt-1"}
      assert [{^key, {:committed, ^id}}] = settled(table, key, [{key, {:committed, id}}])
      assert stored_ids(config.instance) == [id]
    end

    test "a key another node committed is a duplicate carrying that node's hook id, and stores nothing" do
      table = new_table()
      config = start_edge(table)
      :ets.insert(table, {{"default", "demo", "evt-2"}, {:committed, "hook-on-node-b"}})

      conn = post(config, "evt-2")

      assert conn.status == 201

      assert %{"id" => "hook-on-node-b", "duplicate" => true} = JSON.decode!(conn.resp_body)
      assert stored_ids(config.instance) == []
    end

    test "a key another node has claimed but not committed is a 409 with Retry-After, never a duplicate 2xx" do
      table = new_table()
      config = start_edge(table)
      :ets.insert(table, {{"default", "demo", "evt-3"}, {:pending, :other@node, "hook-x"}})

      conn = post(config, "evt-3")

      assert conn.status == 409
      assert JSON.decode!(conn.resp_body) == %{"error" => "dedupe_pending"}
      assert Plug.Conn.get_resp_header(conn, "retry-after") == ["1"]
      assert stored_ids(config.instance) == []
    end

    test "a commit that fails after the claim releases it, so the retry on another node stores the hook" do
      table = new_table()

      # Node A acks only after its sink confirms (`wal: :none`) and its sink
      # refuses: the claim was taken, nothing was stored.
      a = start_edge(table, [wal: :none], [{FailingSink, []}])
      b = start_edge(table)

      assert post(a, "evt-4").status == 503

      assert [] = settled(table, {"default", "demo", "evt-4"}, [])
      conn = post(b, "evt-4")

      assert conn.status == 201
      refute Map.has_key?(JSON.decode!(conn.resp_body), "duplicate")
      assert length(stored_ids(b.instance)) == 1
    end

    test "a release that never arrives leaves the key pending: the retry is a 409, then stores once it expires" do
      table = new_table()
      :ets.insert(table, {:hold_settle, true})

      a = start_edge(table, [wal: :none], [{FailingSink, []}])
      b = start_edge(table)

      assert post(a, "evt-5").status == 503
      Process.sleep(100)

      # The claim is still there (node A's release was dropped) ...
      assert post(b, "evt-5").status == 409
      assert stored_ids(b.instance) == []

      # ... until its TTL runs out, which the stub models by deleting it.
      :ets.delete(table, {"default", "demo", "evt-5"})
      assert post(b, "evt-5").status == 201
      assert length(stored_ids(b.instance)) == 1
    end

    test "a hook without a dedupe key makes no claim" do
      table = new_table()
      config = start_edge(table)

      assert post(config).status == 201
      assert hits(table) == 0
    end
  end

  describe "fleet quota" do
    test "a tenant that has no limit costs no store call" do
      table = new_table()
      config = start_edge(table, rate_limits: %{scope: :fleet}, dedupe: %{scope: :node})

      assert post(config).status == 201
      assert hits(table) == 0
    end

    test "the charges are the limit in GCRA microseconds, bytes included" do
      table = new_table()

      config =
        start_edge(table,
          dedupe: %{scope: :node},
          rate_limits: %{
            scope: :fleet,
            default: %{rate: 10, burst: 20, bytes_per_second: 1_000, burst_bytes: 2_000}
          }
        )

      assert post(config).status == 201

      assert [{:hooks, "default", 100_000, 2_000_000}, {:bytes, "default", 2_000, 2_000_000}] =
               :ets.lookup_element(table, :last_charges, 2)
    end
  end

  describe "a store that cannot answer" do
    test "never refuses a hook: the node's own limiter and dedupe answer, and the gauge says so" do
      table = new_table()
      :ets.insert(table, {:mode, :down})

      config =
        start_edge(table,
          rate_limits: %{scope: :fleet, default: %{rate: 0.001, burst: 2}}
        )

      :telemetry.attach_many(
        "fleet-down-#{config.instance}",
        [[:ankusa, :fleet, :degraded], [:ankusa, :fleet, :fallback]],
        fn event, measurements, meta, pid -> send(pid, {event, measurements, meta}) end,
        self()
      )

      on_exit(fn -> :telemetry.detach("fleet-down-#{config.instance}") end)

      # Node scope: the third hook is over the node's own 2-hook burst (a 429,
      # the limit working), and none of them is a 5xx.
      assert [201, 201, 429] == for(n <- 1..3, do: post(config, "evt-down-#{n}").status)

      assert length(stored_ids(config.instance)) == 2
      assert_receive {[:ankusa, :fleet, :fallback], _, %{kind: :rate_limit, reason: :error}}

      send(Ankusa.whereis(config.instance, :fleet), :tick)

      assert_receive {[:ankusa, :fleet, :degraded], %{degraded: 1}, %{kind: :rate_limit}}
      assert_receive {[:ankusa, :fleet, :degraded], %{degraded: 1}, %{kind: :dedupe}}
    end

    test "the node's own dedupe index still collapses a retry while the store is down" do
      table = new_table()
      :ets.insert(table, {:mode, :down})
      config = start_edge(table)

      assert post(config, "evt-local").status == 201
      assert %{"duplicate" => true} = JSON.decode!(post(config, "evt-local").resp_body)
      assert length(stored_ids(config.instance)) == 1
    end

    test "a store 500 ms slow holds each hook for the budget, not 500 ms, and the breaker opens" do
      table = new_table()
      :ets.insert(table, {:mode, {:slow, 500}})

      config =
        start_edge(table,
          dedupe: %{scope: :node},
          rate_limits: %{scope: :fleet, default: %{rate: 1_000, burst: 1_000}},
          fleet: %{
            store: {StubStore, table: table},
            timeout_ms: 20,
            breaker_min_calls: 5,
            breaker_window_ms: 10_000,
            breaker_open_ms: 400
          }
        )

      :telemetry.attach(
        "fleet-breaker-#{config.instance}",
        [:ankusa, :fleet, :breaker],
        fn _event, _measurements, meta, pid -> send(pid, {:breaker, meta.state}) end,
        self()
      )

      on_exit(fn -> :telemetry.detach("fleet-breaker-#{config.instance}") end)

      timings =
        for _ <- 1..40 do
          {us, conn} = :timer.tc(fn -> post(config) end)
          assert conn.status == 201
          div(us, 1000)
        end

      # Five calls spent the 20 ms budget, which tripped the breaker; the other
      # thirty-five never left the node.
      assert hits(table) == 5
      assert Fleet.state(config.instance) == :open
      assert_received {:breaker, :open}
      assert Enum.max(timings) < 200
      p99 = timings |> Enum.sort() |> Enum.at(floor(length(timings) * 0.99))
      assert p99 < 200

      # Half-open: after `breaker_open_ms` one probe goes through; the store has
      # recovered, so the breaker closes and the calls resume.
      Process.sleep(450)
      :ets.insert(table, {:mode, :ok})

      assert post(config).status == 201
      assert_receive {:breaker, :closed}
      assert Fleet.state(config.instance) == :closed
      assert hits(table) == 6

      assert post(config).status == 201
      assert hits(table) == 7
    end

    test "a probe that fails keeps the breaker open" do
      table = new_table()
      :ets.insert(table, {:mode, :down})

      config =
        start_edge(table,
          dedupe: %{scope: :node},
          rate_limits: %{scope: :fleet, default: %{rate: 1_000, burst: 1_000}},
          fleet: %{
            store: {StubStore, table: table},
            timeout_ms: 20,
            breaker_min_calls: 3,
            breaker_open_ms: 100
          }
        )

      for _ <- 1..3, do: assert(post(config).status == 201)
      assert Fleet.state(config.instance) == :open
      assert hits(table) == 3

      Process.sleep(150)
      assert post(config).status == 201
      assert hits(table) == 4
      assert Fleet.state(config.instance) == :open

      # Right after a failed probe the breaker is shut again for the full period.
      assert post(config).status == 201
      assert hits(table) == 4
    end
  end

  test "with both scopes at :node there is no fleet process and no store call" do
    config =
      test_config(
        roles: [:edge],
        source_store:
          {Ankusa.SourceStore.Static,
           sources: %{"demo" => [verifier: {Ankusa.Verifier.None, []}]}}
      )

    start_supervised!({Ankusa.Instance, config})

    assert Ankusa.whereis(config.instance, :fleet) == nil
    assert post(config).status == 201
  end
end
