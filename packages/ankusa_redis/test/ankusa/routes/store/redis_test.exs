defmodule Ankusa.Routes.Store.RedisTest do
  @moduledoc """
  The Redis route store against a real Redis: two instances in one VM sharing one
  namespace stand in for two edge nodes.

  Needs the package's compose Redis (`docker compose up -d --wait`); `REDIS_URL`
  overrides the default `redis://localhost:6399`.
  """

  use ExUnit.Case, async: false

  alias Ankusa.Routes
  alias Ankusa.Routes.Store.Redis
  alias Ankusa.Routes.Store.Redis.State

  @namespace "ankusa:routes:test"
  @url System.get_env("REDIS_URL", "redis://localhost:6399")

  # A node's config, built here rather than with core's test helpers: those live
  # in `ankusa`'s test/support, which a dependency does not compile.
  defp build_config(opts) do
    instance = Keyword.fetch!(opts, :instance)
    dir = Path.join(System.tmp_dir!(), "ankusa_#{instance}_#{System.unique_integer([:positive])}")
    ExUnit.Callbacks.on_exit(fn -> File.rm_rf(dir) end)

    opts
    |> Keyword.put_new(:data_dir, dir)
    |> Keyword.put_new(:port, 0)
    |> Ankusa.Config.new()
  end

  defp store(tick_ms), do: {Redis, [url: @url, namespace: @namespace, tick_ms: tick_ms]}

  # Both nodes point at the same namespace; only the tick interval differs, so
  # each test can say whether it is proving pub/sub or the tick.
  defp start_node(opts \\ []) do
    {tick_ms, routes} = Keyword.pop(opts, :tick_ms, 60_000)
    instance = :"redis#{System.unique_integer([:positive])}"

    config =
      build_config(
        instance: instance,
        roles: [:edge],
        routes:
          routes
          |> Keyword.merge(enabled: true, admin: [token: "test-token", port: 0])
          |> Keyword.put(:store, store(tick_ms))
      )

    start_supervised!({Ankusa.Instance, config})
    config
  end

  defp route(id, path, attrs \\ %{}) do
    {:ok, route} =
      Ankusa.Routes.Route.from_attrs(Map.merge(%{"id" => id, "path" => path}, attrs))

    route
  end

  # Polling, because the assertion is "this arrives without a tick" — a sleep
  # long enough to be safe would also be long enough for the 60s tick to be
  # irrelevant, so a deadline is the honest way to express it.
  defp eventually(fun, deadline_ms \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + deadline_ms
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) > deadline do
        flunk("condition was still false after the deadline")
      else
        Process.sleep(20)
        do_eventually(fun, deadline)
      end
    end
  end

  setup do
    {:ok, conn} = Redix.start_link(@url)
    {:ok, _} = Redix.command(conn, ["FLUSHDB"])
    on_exit(fn -> if Process.alive?(conn), do: Redix.stop(conn) end)

    %{conn: conn}
  end

  test "definitions round-trip through Redis via the context" do
    config = start_node()

    assert Routes.ip_rules(config.instance) == %{default: :allow, rules: []}

    assert {:ok, %{id: "a"}} =
             Routes.create(config.instance, %{"id" => "a", "path" => "/hooks/a"})

    assert {:ok, %{id: "a", path: "/hooks/a"}} = Routes.get(config.instance, "a")
    assert {:ok, %{routes: [%{id: "a"}]}} = Routes.list(config.instance)

    assert {:ok, %{path: "/hooks/moved"}} =
             Routes.replace(config.instance, "a", %{"path" => "/hooks/moved"})

    assert {:ok, %{enabled: false}} = Routes.update(config.instance, "a", %{"enabled" => false})
    assert :ok = Routes.delete(config.instance, "a")
    assert {:error, :not_found} = Routes.get(config.instance, "a")
    assert {:error, :not_found} = Routes.delete(config.instance, "a")
  end

  test "the seed loads on a namespace's first boot, and never again" do
    first = start_node(seed: [%{"id" => "s", "path" => "/hooks/s"}])

    assert {:ok, %{id: "s"}} = Routes.get(first.instance, "s")
    assert :ok = Routes.delete(first.instance, "s")

    # A second node on the same namespace reads what is in Redis, so the deleted
    # route stays deleted: seeding is a first-boot event, not a boot event.
    second = start_node()
    assert {:error, :not_found} = Routes.get(second.instance, "s")
    assert Routes.snapshot(second.instance).by_id == %{}
  end

  test "the cap is enforced against the shared hash" do
    config = start_node(max_routes: 1)

    assert {:ok, _} = Routes.create(config.instance, %{"id" => "a", "path" => "/hooks/a"})

    assert {:error, :too_many_routes} =
             Routes.create(config.instance, %{"id" => "b", "path" => "/hooks/b"})

    assert {:ok, %{routes: [%{id: "a"}]}} = Routes.list(config.instance)
  end

  test "a write on one node reaches another over pub/sub, without a tick" do
    # The tick is an hour away on both nodes: only the broadcast can explain B
    # seeing the change.
    node_a = start_node(tick_ms: 3_600_000, seed: [%{"id" => "s", "path" => "/hooks/s"}])
    node_b = start_node(tick_ms: 3_600_000)

    assert Map.keys(Routes.snapshot(node_b.instance).by_id) == ["s"]

    assert {:ok, _} = Routes.create(node_a.instance, %{"id" => "n", "path" => "/hooks/n"})

    eventually(fn -> Map.has_key?(Routes.snapshot(node_b.instance).by_id, "n") end)

    # The mirror is complete, not just appended to: the decision and the
    # definition both have to be there.
    assert {:ok, %{id: "n", path: "/hooks/n"}} = Routes.get(node_b.instance, "n")

    assert Routes.authorize(
             node_b.instance,
             "POST",
             ["hooks", "n"],
             elem(Ankusa.Net.parse("1.2.3.4"), 1)
           ) == {:ok, "n"}

    # ... and a delete travels the same way.
    assert :ok = Routes.delete(node_a.instance, "n")
    eventually(fn -> not Map.has_key?(Routes.snapshot(node_b.instance).by_id, "n") end)
  end

  test "a global rule change reaches another node over pub/sub" do
    node_a = start_node(tick_ms: 3_600_000)
    node_b = start_node(tick_ms: 3_600_000)

    assert {:ok, _} =
             Routes.put_ip_rules(node_a.instance, %{
               "default" => "deny",
               "rules" => [%{"action" => "allow", "cidr" => "10.0.0.0/8"}]
             })

    eventually(fn -> Routes.ip_rules(node_b.instance).default == :deny end)
    assert [%{action: :allow}] = Routes.ip_rules(node_b.instance).rules
  end

  test "the tick catches a change that was never broadcast", %{conn: conn} do
    start_node(seed: [%{"id" => "s", "path" => "/hooks/s"}])
    node_b = start_node(tick_ms: 100)

    # A raw write, as if a script or another deployment had edited the shared
    # hash — no PUBLISH, so only the tick can notice.
    {:ok, _} = Redix.command(conn, ["HSET", "#{@namespace}:routes", "x", encoded_route("x")])
    {:ok, _} = Redix.command(conn, ["INCR", "#{@namespace}:version"])

    eventually(fn -> Map.has_key?(Routes.snapshot(node_b.instance).by_id, "x") end, 3_000)
    assert {:ok, %{id: "x"}} = Routes.get(node_b.instance, "x")
  end

  test "a Redis error on a write is store_unavailable, and does not touch the mirror", %{
    conn: conn
  } do
    config = start_node()

    assert {:ok, _} = Routes.create(config.instance, %{"id" => "a", "path" => "/hooks/a"})
    before = Routes.snapshot(config.instance)

    # Every hash write now fails with WRONGTYPE. A disconnection arrives as the
    # same `{:error, %Redix.Error{} | %Redix.ConnectionError{}}` from Redix, so
    # this pins the mapping without racing a real outage.
    {:ok, _} = Redix.command(conn, ["DEL", "#{@namespace}:routes"])
    {:ok, _} = Redix.command(conn, ["SET", "#{@namespace}:routes", "not-a-hash"])

    assert {:error, :store_unavailable} =
             Routes.create(config.instance, %{"id" => "b", "path" => "/hooks/b"})

    snapshot = Routes.snapshot(config.instance)
    assert snapshot.version == before.version
    assert Map.keys(snapshot.by_id) == ["a"]

    assert Routes.authorize(config.instance, "POST", ["hooks", "a"], {32, 0x01020304}) ==
             {:ok, "a"}
  end

  test "booting against a Redis that is not there fails loudly" do
    # The supervisor is linked to this process, so trapping the exit is what
    # lets the failure be inspected instead of killing the test.
    Process.flag(:trap_exit, true)
    port = dead_port()
    instance = :"redis#{System.unique_integer([:positive])}"

    config =
      build_config(
        instance: instance,
        roles: [:edge],
        routes: [
          enabled: true,
          admin: [token: "test-token", port: 0],
          store: {Redis, [url: "redis://127.0.0.1:#{port}", namespace: @namespace]}
        ]
      )

    assert {:error, {:shutdown, {:failed_to_start_child, Redix, reason}}} =
             Redis.start_link(instance: instance, config: config)

    assert %Redix.ConnectionError{reason: :econnrefused} = reason
  end

  test "a stored definition that no longer parses stops the node rather than loading garbage", %{
    conn: conn
  } do
    Process.flag(:trap_exit, true)
    config = start_node(seed: [%{"id" => "s", "path" => "/hooks/s"}])
    instance = config.instance

    {:ok, _} = Redix.command(conn, ["HSET", "#{@namespace}:routes", "bad", "{\"path\":1}"])
    {:ok, _} = Redix.command(conn, ["INCR", "#{@namespace}:version"])

    # The running node keeps serving, and reports the reload failure.
    assert {:ok, _} = Routes.get(instance, "s")

    # A fresh boot refuses it.
    fresh = :"redis#{System.unique_integer([:positive])}"

    config =
      build_config(
        instance: fresh,
        roles: [:edge],
        routes: [enabled: true, admin: [token: "test-token", port: 0], store: store(60_000)]
      )

    assert {:error, {:shutdown, {:failed_to_start_child, State, reason}}} =
             Redis.start_link(instance: fresh, config: config)

    assert {:invalid_stored_route, "inserted_at", _message} = reason
  end

  test "the state process is addressed like the ETS store's" do
    config = start_node()

    assert is_pid(Ankusa.whereis(config.instance, :routes_store))
    assert State.snapshot(config.instance) == Routes.snapshot(config.instance)
    assert is_pid(Ankusa.whereis(config.instance, :routes_redis))
    assert is_pid(Ankusa.whereis(config.instance, :routes_redis_pubsub))
  end

  defp encoded_route(id) do
    JSON.encode!(Ankusa.Routes.Route.to_json(route(id, "/hooks/#{id}")))
  end

  # A port nothing listens on: Redix's `sync_connect` is what turns that into a
  # boot failure instead of a store that starts and then denies everything.
  defp dead_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end
end
