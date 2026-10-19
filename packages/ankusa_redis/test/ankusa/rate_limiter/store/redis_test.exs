defmodule Ankusa.RateLimiter.Store.RedisTest do
  @moduledoc """
  The fleet store against a real Redis: two instances in one VM sharing one
  namespace stand in for two edge nodes, real POSTs go through the router, and the
  node's own store is the witness for "this hook was stored".

  Needs the package's compose Redis (`docker compose up -d --wait`); `REDIS_URL`
  overrides the default `redis://localhost:6399`.
  """

  use ExUnit.Case, async: false

  alias Ankusa.Edge.{Fleet, Router}

  @url System.get_env("REDIS_URL", "redis://localhost:6399")

  defmodule FailingSink do
    @moduledoc false
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, _opts), do: {:error, :nope}
  end

  defp build_config(opts) do
    instance = Keyword.fetch!(opts, :instance)
    unique = "#{System.unique_integer([:positive])}_#{System.os_time(:microsecond)}"
    dir = Path.join(System.tmp_dir!(), "ankusa_#{instance}_#{unique}")
    ExUnit.Callbacks.on_exit(fn -> File.rm_rf(dir) end)

    opts
    |> Keyword.put_new(:data_dir, dir)
    |> Keyword.put_new(:port, 0)
    |> Ankusa.Config.new()
  end

  # A node: an edge with the fleet scopes on, sharing `ns` (and so one bucket per
  # tenant and one dedupe window) with every other node started on it.
  defp start_node(ns, opts \\ []) do
    {url, opts} = Keyword.pop(opts, :url, @url)
    {fleet, opts} = Keyword.pop(opts, :fleet, %{})
    {sinks, opts} = Keyword.pop(opts, :sinks, [{Ankusa.Sink.Log, []}])

    {rate_limits, opts} =
      Keyword.pop(opts, :rate_limits, %{default: %{rate: 1_000, burst: 1_000}})

    config =
      build_config(
        [
          instance: :"fleet_#{System.unique_integer([:positive])}",
          roles: [:edge],
          fleet:
            Map.merge(
              %{
                store: {Ankusa.RateLimiter.Store.Redis, url: url, namespace: ns, pool_size: 2},
                timeout_ms: 50
              },
              fleet
            ),
          rate_limits: Map.put(rate_limits, :scope, :fleet),
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
        ] ++ opts
      )

    start_supervised!({Ankusa.Instance, config}, id: config.instance)
    config.instance
  end

  defp post(instance, event_id \\ nil) do
    conn = Plug.Test.conn(:post, "/webhooks/demo", "{}")
    conn = if event_id, do: Plug.Conn.put_req_header(conn, "x-event-id", event_id), else: conn
    Router.call(conn, Router.init(instance: instance))
  end

  defp stored(instance) do
    {:ok, hooks} = Ankusa.Queue.hooks(instance, 0, 100_000)
    Enum.map(hooks, & &1.id)
  end

  defp namespace do
    ns = "ankusa:fleet:test:#{System.unique_integer([:positive])}"

    on_exit(fn ->
      {:ok, conn} = Redix.start_link(@url)
      {:ok, keys} = Redix.command(conn, ["KEYS", ns <> ":*"])
      if keys != [], do: {:ok, _} = Redix.command(conn, ["DEL" | keys])
      Redix.stop(conn)
    end)

    ns
  end

  defp redis do
    {:ok, conn} = Redix.start_link(@url)
    on_exit(fn -> if Process.alive?(conn), do: Redix.stop(conn) end)
    conn
  end

  defp eventually(fun, deadline_ms \\ 3_000) do
    deadline = System.monotonic_time(:millisecond) + deadline_ms
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    cond do
      fun.() -> :ok
      System.monotonic_time(:millisecond) > deadline -> flunk("condition was still false")
      true -> Process.sleep(20) && do_eventually(fun, deadline)
    end
  end

  # A TCP forwarder in front of Redis. Killing it makes Redis unreachable for the
  # node pointed at it (the listener closes, open connections drop); listening
  # again on the same port brings it back. `delay` is how long the forwarder holds
  # every reply, in milliseconds, readable and settable from the test.
  defp start_forwarder(port \\ 0, delay \\ :atomics.new(1, signed: false)) do
    parent = self()

    pid =
      spawn(fn ->
        {:ok, listen} =
          :gen_tcp.listen(port, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])

        {:ok, bound} = :inet.port(listen)
        send(parent, {:forwarder, self(), bound})
        forward_loop(listen, delay)
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)

    receive do
      {:forwarder, ^pid, bound} -> {pid, bound, delay}
    after
      2_000 -> flunk("the forwarder did not start")
    end
  end

  defp forward_loop(listen, delay) do
    {:ok, client} = :gen_tcp.accept(listen)
    %URI{host: host, port: port} = URI.parse(@url)
    {:ok, upstream} = :gen_tcp.connect(String.to_charlist(host), port, [:binary, active: false])

    spawn_link(fn -> pipe(client, upstream, fn -> 0 end) end)
    spawn_link(fn -> pipe(upstream, client, fn -> :atomics.get(delay, 1) end) end)

    forward_loop(listen, delay)
  end

  defp pipe(from, to, delay) do
    with {:ok, data} <- :gen_tcp.recv(from, 0),
         _ <- Process.sleep(delay.()),
         :ok <- :gen_tcp.send(to, data) do
      pipe(from, to, delay)
    else
      _closed ->
        :gen_tcp.close(from)
        :gen_tcp.close(to)
    end
  end

  describe "fleet limiter" do
    test "two nodes enforce one tenant's 10 rps combined" do
      ns = namespace()
      limit = %{default: %{rate: 10, burst: 10}}
      a = start_node(ns, rate_limits: limit)
      b = start_node(ns, rate_limits: limit)

      started = System.monotonic_time(:millisecond)
      statuses = for n <- 1..40, do: post(if(rem(n, 2) == 0, do: a, else: b)).status
      elapsed_s = (System.monotonic_time(:millisecond) - started) / 1000

      admitted = Enum.count(statuses, &(&1 == 201))

      assert Enum.all?(statuses, &(&1 in [201, 429]))

      # The burst of 10, plus what 10 rps refilled while the loop ran. Per-node
      # buckets would have admitted 20 at once (two bursts of 10).
      assert admitted >= 10
      assert admitted <= 10 + ceil(elapsed_s * 10) + 1
      assert admitted < 20
      assert length(stored(a)) + length(stored(b)) == admitted
      assert Enum.any?(statuses, &(&1 == 429))
    end

    test "the byte bucket is shared too" do
      ns = namespace()
      limit = %{default: %{rate: 1_000, burst: 1_000, bytes_per_second: 10, burst_bytes: 4}}
      a = start_node(ns, rate_limits: limit)
      b = start_node(ns, rate_limits: limit)

      # Each "{}" is 2 bytes: a 4-byte burst holds two of them, fleet-wide.
      assert [201, 201, 429] == Enum.map([a, b, a], &post(&1).status)
    end

    test "a denial carries Retry-After and stores nothing" do
      ns = namespace()
      node = start_node(ns, rate_limits: %{default: %{rate: 0.001, burst: 1}})

      assert post(node).status == 201
      denied = post(node)

      assert denied.status == 429
      assert JSON.decode!(denied.resp_body) == %{"error" => "rate_limited"}
      assert [retry] = Plug.Conn.get_resp_header(denied, "retry-after")
      assert String.to_integer(retry) >= 1
      assert length(stored(node)) == 1
    end

    test "an override change resets the tenant's fleet bucket" do
      ns = namespace()
      node = start_node(ns, rate_limits: %{default: %{rate: 0.001, burst: 1}})

      assert post(node).status == 201
      assert post(node).status == 429

      assert {:ok, _} =
               Ankusa.Edge.RateLimiter.put_override(node, "default", %{
                 "rate" => 0.001,
                 "burst" => 5
               })

      assert post(node).status == 201
    end
  end

  describe "fleet dedupe" do
    test "the same dedupe key posted to each node yields one hook" do
      ns = namespace()
      a = start_node(ns)
      b = start_node(ns)

      first = post(a, "evt-1")
      assert first.status == 201
      %{"id" => id} = JSON.decode!(first.resp_body)

      # The claim is promoted off the request path; until it is, node B is told to
      # retry (409), never that the hook is a duplicate it cannot name.
      second =
        Enum.find_value(1..100, fn _ ->
          conn = post(b, "evt-1")

          if conn.status == 409 do
            Process.sleep(20)
            nil
          else
            conn
          end
        end)

      assert second.status == 201
      assert %{"id" => ^id, "duplicate" => true} = JSON.decode!(second.resp_body)

      assert stored(a) == [id]
      assert stored(b) == []
    end

    test "the same key under another source or tenant is another event" do
      ns = namespace()
      a = start_node(ns)

      assert post(a, "evt-1").status == 201
      assert post(a, "evt-2").status == 201
      assert length(stored(a)) == 2
    end

    test "a claim whose commit failed is released: the retry on the other node stores the hook" do
      ns = namespace()
      a = start_node(ns, wal: :none, sinks: [{FailingSink, []}])
      b = start_node(ns)
      conn = redis()

      assert post(a, "evt-fail").status == 503

      # Node A's release is delivered in the background.
      eventually(fn -> Redix.command!(conn, ["KEYS", ns <> ":dd:*"]) == [] end)

      retry = post(b, "evt-fail")
      assert retry.status == 201
      refute Map.has_key?(JSON.decode!(retry.resp_body), "duplicate")
      assert length(stored(b)) == 1
    end

    test "a claim still pending is a 409 on the other node, and nothing is stored, until it expires" do
      ns = namespace()
      b = start_node(ns, fleet: %{pending_ttl_ms: 400})
      conn = redis()

      # What a node that claimed the key and died before settling leaves behind.
      key = ns <> ":dd:{default}:4:demo:evt-pending"
      Redix.command!(conn, ["SET", key, "pending:dead@node:hook-x", "PX", "400"])

      blocked = post(b, "evt-pending")
      assert blocked.status == 409
      assert JSON.decode!(blocked.resp_body) == %{"error" => "dedupe_pending"}
      assert [retry] = Plug.Conn.get_resp_header(blocked, "retry-after")
      assert String.to_integer(retry) >= 1
      assert stored(b) == []

      Process.sleep(450)

      assert post(b, "evt-pending").status == 201
      assert length(stored(b)) == 1
    end

    test "a promoted key is committed for the source's TTL" do
      ns = namespace()
      a = start_node(ns)
      conn = redis()

      %{"id" => id} = JSON.decode!(post(a, "evt-ttl").resp_body)
      key = ns <> ":dd:{default}:4:demo:evt-ttl"

      eventually(fn -> Redix.command!(conn, ["GET", key]) == "committed:" <> id end)
      assert Redix.command!(conn, ["PTTL", key]) > 259_000_000 - 5_000
    end
  end

  describe "scripts" do
    test "a flushed script cache is answered with EVAL inside the same call" do
      ns = namespace()
      a = start_node(ns)
      conn = redis()

      assert post(a, "evt-a").status == 201
      assert Redix.command!(conn, ["SCRIPT", "FLUSH"]) == "OK"

      assert post(a, "evt-b").status == 201
      assert length(stored(a)) == 2
      eventually(fn -> Redix.command!(conn, ["KEYS", ns <> ":dd:*"]) |> length() == 2 end)
    end
  end

  describe "degrades without redis" do
    test "an unreachable Redis at boot, and one that goes away, never refuse a hook" do
      ns = namespace()
      {forwarder, port, _delay} = start_forwarder()
      url = "redis://127.0.0.1:#{port}"

      node =
        start_node(ns,
          url: url,
          rate_limits: %{default: %{rate: 0.001, burst: 3}},
          fleet: %{breaker_open_ms: 300, breaker_min_calls: 3}
        )

      :telemetry.attach(
        "fleet-gauge-#{node}",
        [:ankusa, :fleet, :degraded],
        fn _event, measurements, meta, pid ->
          send(pid, {:degraded, meta.kind, measurements.degraded})
        end,
        self()
      )

      on_exit(fn -> :telemetry.detach("fleet-gauge-#{node}") end)

      assert post(node, "evt-up-1").status == 201
      refute Fleet.degraded?(node)

      Process.exit(forwarder, :kill)
      Process.sleep(50)

      # Redis is gone: node scope answers. Its own limiter holds the 3-hook burst
      # (the one 201 above spent a fleet token, not a node one), and none of the
      # answers is a 5xx.
      statuses = for n <- 1..8, do: post(node, "evt-down-#{n}").status
      assert Enum.all?(statuses, &(&1 in [201, 429]))
      assert Enum.count(statuses, &(&1 == 201)) == 3

      assert Fleet.degraded?(node)
      assert Fleet.state(node) == :open

      send(Ankusa.whereis(node, :fleet), :tick)
      assert_receive {:degraded, :rate_limit, 1}
      assert_receive {:degraded, :dedupe, 1}

      # Redis comes back on the same port: the half-open probe closes the breaker.
      {_pid, ^port, _delay} = start_forwarder(port)
      Process.sleep(350)

      eventually(fn ->
        post(node, "evt-back-#{System.unique_integer([:positive])}")
        Fleet.state(node) == :closed
      end)

      send(Ankusa.whereis(node, :fleet), :tick)
      assert_receive {:degraded, :rate_limit, 0}
    end

    test "a Redis that answers in 500 ms holds an ack for the budget, not for 500 ms, and the breaker opens" do
      ns = namespace()
      {_pid, port, delay} = start_forwarder()
      :atomics.put(delay, 1, 500)

      node =
        start_node(ns,
          url: "redis://127.0.0.1:#{port}",
          fleet: %{timeout_ms: 20, breaker_min_calls: 10, breaker_open_ms: 60_000}
        )

      timings =
        for n <- 1..200 do
          {us, conn} = :timer.tc(fn -> post(node, "evt-slow-#{n}") end)
          assert conn.status == 201
          us / 1000
        end

      sorted = Enum.sort(timings)
      p99 = Enum.at(sorted, floor(length(sorted) * 0.99))

      # Ten calls spent their 20 ms budget; the breaker then answered the rest from
      # the node with no call at all. Waiting out Redis would have been 500 ms each.
      assert Fleet.state(node) == :open
      assert Fleet.degraded?(node)
      assert p99 < 150, "ack p99 was #{p99} ms"
      assert Enum.sum(timings) < 3_000
      assert length(stored(node)) == 200
    end
  end
end
