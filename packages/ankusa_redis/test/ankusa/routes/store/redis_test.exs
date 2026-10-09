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
    unique = "#{System.unique_integer([:positive])}_#{System.os_time(:microsecond)}"
    dir = Path.join(System.tmp_dir!(), "ankusa_#{instance}_#{unique}")
    ExUnit.Callbacks.on_exit(fn -> File.rm_rf(dir) end)

    opts
    |> Keyword.put_new(:data_dir, dir)
    |> Keyword.put_new(:port, 0)
    |> Ankusa.Config.new()
  end

  defp store(tick_ms, extra \\ []),
    do: {Redis, [url: @url, namespace: @namespace, tick_ms: tick_ms] ++ extra}

  # Both nodes point at the same namespace; only the tick interval differs, so
  # each test can say whether it is proving pub/sub or the tick.
  defp start_node(opts \\ []) do
    {tick_ms, opts} = Keyword.pop(opts, :tick_ms, 60_000)
    {store_opts, routes} = Keyword.split(opts, [:redis_timeout_ms])
    instance = :"redis#{System.unique_integer([:positive])}"

    config =
      build_config(
        instance: instance,
        roles: [:edge],
        routes:
          routes
          |> Keyword.merge(enabled: true, admin: [port: 0])
          |> Keyword.put(:store, store(tick_ms, store_opts))
      )

    start_supervised!({Ankusa.Instance, config})
    config
  end

  defp route(id, path, attrs \\ %{}) do
    {:ok, route} =
      Ankusa.Routes.Route.from_attrs(Map.merge(%{"id" => id, "path" => path}, attrs))

    route
  end

  # The ids in a node's route table, sorted.
  defp route_ids(instance) do
    case Routes.meta(instance) do
      nil -> []
      meta -> instance |> Ankusa.Routes.Snapshot.routes(meta) |> Enum.map(& &1.id) |> Enum.sort()
    end
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

  # Freeze both nodes, start one write on each, and release them only once each is
  # waiting on its own node. Both writers have then validated against the table as
  # it was before EITHER write — the interleaving two real nodes get when their
  # mirrors lag — and no part of that depends on timing. Redis then serves
  # whichever script arrives first; the other is the one under test.
  defp collide(node_a, fun_a, node_b, fun_b) do
    stores = for node <- [node_a, node_b], do: Ankusa.whereis(node.instance, :routes_store)
    Enum.each(stores, &:sys.suspend/1)

    tasks = [Task.async(fun_a), Task.async(fun_b)]

    # Only calls are counted: a pub/sub nudge from an earlier write may land in a
    # frozen node's mailbox, and it is not the writer we are waiting for.
    for store <- stores do
      eventually(fn -> queued_calls(store) == 1 end, 5_000)
    end

    Enum.each(stores, &:sys.resume/1)
    Task.await_many(tasks, 30_000)
  end

  defp queued_calls(pid) do
    {:messages, messages} = Process.info(pid, :messages)
    Enum.count(messages, &match?({:"$gen_call", _from, _request}, &1))
  end

  # Only this suite's own three keys are ever deleted, never the database: a
  # developer's Redis, or one a CI job shares between packages, may hold data
  # that is not ours to flush.
  @keys ~w(routes ip_rules version)

  defp clean_namespace(conn) do
    {:ok, _deleted} = Redix.command(conn, ["DEL" | Enum.map(@keys, &"#{@namespace}:#{&1}")])
    :ok
  end

  setup do
    {:ok, conn} = Redix.start_link(@url)
    clean_namespace(conn)

    on_exit(fn ->
      {:ok, conn} = Redix.start_link(@url)
      clean_namespace(conn)
      Redix.stop(conn)
    end)

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
    assert route_ids(second.instance) == []
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

    assert route_ids(node_b.instance) == ["s"]

    assert {:ok, _} = Routes.create(node_a.instance, %{"id" => "n", "path" => "/hooks/n"})

    eventually(fn -> "n" in route_ids(node_b.instance) end)

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
    eventually(fn -> "n" not in route_ids(node_b.instance) end)
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

    eventually(fn -> "x" in route_ids(node_b.instance) end, 3_000)
    assert {:ok, %{id: "x"}} = Routes.get(node_b.instance, "x")
  end

  test "a Redis error on a write is store_unavailable, and does not touch the mirror", %{
    conn: conn
  } do
    config = start_node()

    assert {:ok, _} = Routes.create(config.instance, %{"id" => "a", "path" => "/hooks/a"})
    before = Routes.meta(config.instance)

    # Every hash write now fails with WRONGTYPE. A disconnection arrives as the
    # same `{:error, %Redix.Error{} | %Redix.ConnectionError{}}` from Redix, so
    # this pins the mapping without racing a real outage.
    {:ok, _} = Redix.command(conn, ["DEL", "#{@namespace}:routes"])
    {:ok, _} = Redix.command(conn, ["SET", "#{@namespace}:routes", "not-a-hash"])

    assert {:error, :store_unavailable} =
             Routes.create(config.instance, %{"id" => "b", "path" => "/hooks/b"})

    meta = Routes.meta(config.instance)
    assert meta.version == before.version
    assert route_ids(config.instance) == ["a"]

    assert Routes.authorize(config.instance, "POST", ["hooks", "a"], {1, 2, 3, 4}) ==
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
          admin: [port: 0],
          store: {Redis, [url: "redis://127.0.0.1:#{port}", namespace: @namespace]}
        ]
      )

    assert {:error, {:shutdown, {:failed_to_start_child, Redix, reason}}} =
             Redis.start_link(instance: instance, config: config)

    assert %Redix.ConnectionError{reason: :econnrefused} = reason
  end

  test "a store restarted while Redis is unreachable boots from the last table and reloads when Redis answers",
       %{conn: conn} do
    node = start_node(tick_ms: 100, redis_timeout_ms: 300)
    instance = node.instance
    edge = Ankusa.whereis(instance, :edge)

    assert {:ok, _} = Routes.create(instance, %{"id" => "api", "path" => "/hooks/api"})
    version = Routes.meta(instance).version

    {_id, sup, _type, _modules} =
      edge
      |> Supervisor.which_children()
      |> Enum.find(&match?({Redis, _pid, _type, _modules}, &1))

    # The store and its connections go down together; Redis then stops
    # answering for 1.5s, so the restarted store cannot load anything.
    members = [sup | sup |> Supervisor.which_children() |> Enum.map(&elem(&1, 1))]
    refs = Enum.map(members, &Process.monitor/1)
    {:ok, "OK"} = Redix.command(conn, ["CLIENT", "PAUSE", "1500", "ALL"])

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        Process.exit(sup, :kill)
        for ref <- refs, do: assert_receive({:DOWN, ^ref, :process, _pid, _reason}, 5_000)
        _ = :sys.get_state(edge)
      end)

    # Still paused: the new store is up, on the table the old one left, and said so.
    assert log =~ "serving the last published route table"
    assert is_pid(Ankusa.whereis(instance, :routes_store))
    assert "api" in route_ids(instance)
    assert Routes.meta(instance).version == version
    assert Routes.authorize(instance, "POST", ["hooks", "api"], {1, 2, 3, 4}) == {:ok, "api"}

    # Once Redis answers, the subscription is confirmed and writes work again.
    eventually(
      fn ->
        {:ok, [_channel, subscribers]} = Redix.command(conn, ["PUBSUB", "NUMSUB", @namespace])
        subscribers >= 1
      end,
      5_000
    )

    eventually(
      fn ->
        match?({:ok, _}, Routes.create(instance, %{"id" => "after", "path" => "/hooks/after"}))
      end,
      5_000
    )

    assert "after" in route_ids(instance)
    assert "api" in route_ids(instance)
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
        routes: [enabled: true, admin: [port: 0], store: store(60_000)]
      )

    assert {:error, {:shutdown, {:failed_to_start_child, State, reason}}} =
             Redis.start_link(instance: fresh, config: config)

    assert {:invalid_stored_route, "inserted_at", _message} = reason
  end

  test "the state process is addressed like the ETS store's" do
    config = start_node()

    assert is_pid(Ankusa.whereis(config.instance, :routes_store))
    assert is_pid(Ankusa.whereis(config.instance, :routes_redis))
    assert is_pid(Ankusa.whereis(config.instance, :routes_redis_pubsub))
  end

  test "a node that has booted is already subscribed", %{conn: conn} do
    start_node()

    # Redis itself says so, the instant the node is up: the subscription was
    # confirmed before the node loaded anything, not merely requested. A node that
    # loaded first could miss a write made before its subscription was live.
    assert {:ok, [@namespace, 1]} = Redix.command(conn, ["PUBSUB", "NUMSUB", @namespace])
  end

  test "a node whose mirror lags cannot overwrite what another node already wrote" do
    node_a = start_node(tick_ms: 3_600_000)
    node_b = start_node(tick_ms: 3_600_000)
    pid_a = Ankusa.whereis(node_a.instance, :routes_store)

    # A is frozen while its write is queued, so B's write and broadcast land AFTER
    # A's request: when A wakes, its mirror is one write behind Redis at the very
    # moment it writes. That is the window a lagging pub/sub round trip leaves.
    :sys.suspend(pid_a)

    writer =
      Task.async(fn ->
        Routes.create(node_a.instance, %{"id" => "from-a", "path" => "/hooks/a"})
      end)

    # The writer's call, specifically: this process also receives pub/sub messages
    # and ticks, and a stray one must not release the wait before A's write is
    # queued (`queued_calls/1` counts only `GenServer.call`s).
    eventually(fn -> queued_calls(pid_a) >= 1 end)

    assert {:ok, _} = Routes.create(node_b.instance, %{"id" => "from-b", "path" => "/hooks/b"})
    :sys.resume(pid_a)

    assert {:ok, %{id: "from-a"}} = Task.await(writer)

    # Neither write was lost. A took Redis's table instead of applying its own
    # change to a copy that did not have B's, and B hears about A's in turn.
    assert route_ids(node_a.instance) == ["from-a", "from-b"]

    eventually(fn -> route_ids(node_b.instance) == ["from-a", "from-b"] end)
  end

  test "two nodes creating the same id: exactly one wins", %{conn: conn} do
    node_a = start_node(tick_ms: 3_600_000)
    node_b = start_node(tick_ms: 3_600_000)

    for round <- 1..3 do
      id = "same#{round}"

      results =
        collide(
          node_a,
          fn -> Routes.create(node_a.instance, %{"id" => id, "path" => "/hooks/a#{round}"}) end,
          node_b,
          fn -> Routes.create(node_b.instance, %{"id" => id, "path" => "/hooks/b#{round}"}) end
        )

      assert Enum.count(results, &match?({:ok, %{id: ^id}}, &1)) == 1
      assert Enum.count(results, &(&1 == {:error, {:conflict, id}})) == 1
    end

    assert {:ok, 3} = Redix.command(conn, ["HLEN", "#{@namespace}:routes"])
  end

  test "two nodes creating routes for one path and method: exactly one wins", %{conn: conn} do
    node_a = start_node(tick_ms: 3_600_000)
    node_b = start_node(tick_ms: 3_600_000)

    results =
      collide(
        node_a,
        fn -> Routes.create(node_a.instance, %{"id" => "a", "path" => "/hooks/x"}) end,
        node_b,
        fn -> Routes.create(node_b.instance, %{"id" => "b", "path" => "/hooks/x"}) end
      )

    assert Enum.count(results, &match?({:ok, _}, &1)) == 1
    assert Enum.count(results, &match?({:error, {:conflict, _}}, &1)) == 1
    assert {:ok, 1} = Redix.command(conn, ["HLEN", "#{@namespace}:routes"])
  end

  test "two nodes racing for the last slot: exactly one create succeeds", %{conn: conn} do
    seed = [%{"id" => "s", "path" => "/hooks/s"}]
    node_a = start_node(max_routes: 2, tick_ms: 3_600_000, seed: seed)
    node_b = start_node(max_routes: 2, tick_ms: 3_600_000)

    results =
      collide(
        node_a,
        fn -> Routes.create(node_a.instance, %{"id" => "x", "path" => "/hooks/x"}) end,
        node_b,
        fn -> Routes.create(node_b.instance, %{"id" => "y", "path" => "/hooks/y"}) end
      )

    assert Enum.count(results, &match?({:ok, _}, &1)) == 1
    assert Enum.count(results, &(&1 == {:error, :too_many_routes})) == 1
    assert {:ok, 2} = Redix.command(conn, ["HLEN", "#{@namespace}:routes"])
  end

  test "a delete whose version bump fails deletes nothing", %{conn: conn} do
    config = start_node(seed: [%{"id" => "s", "path" => "/hooks/s"}])

    # A version key of the wrong type makes the bump fail.
    {:ok, _} = Redix.command(conn, ["DEL", "#{@namespace}:version"])
    {:ok, _} = Redix.command(conn, ["HSET", "#{@namespace}:version", "field", "value"])

    assert {:error, :store_unavailable} = Routes.delete(config.instance, "s")

    # Still there in Redis: no half-applied delete for another node to miss, with
    # the version unmoved so that nothing would ever tell it to reload.
    assert {:ok, 1} = Redix.command(conn, ["HEXISTS", "#{@namespace}:routes", "s"])
  end

  test "deleting a route that is not there is not_found and moves nothing", %{conn: conn} do
    config = start_node(seed: [%{"id" => "s", "path" => "/hooks/s"}])
    {:ok, version} = Redix.command(conn, ["GET", "#{@namespace}:version"])

    assert {:error, :not_found} = Routes.delete(config.instance, "nope")
    assert {:ok, ^version} = Redix.command(conn, ["GET", "#{@namespace}:version"])
  end

  test "a Redis restored to an older version is followed down, not ignored", %{conn: conn} do
    node = start_node(tick_ms: 100, seed: [%{"id" => "s", "path" => "/hooks/s"}])
    assert {:ok, _} = Routes.create(node.instance, %{"id" => "a", "path" => "/hooks/a"})
    assert {:ok, _} = Routes.create(node.instance, %{"id" => "b", "path" => "/hooks/b"})
    assert Routes.meta(node.instance).version == 3

    # A restore from an older backup: a different table at a LOWER version.
    {:ok, _} = Redix.command(conn, ["DEL", "#{@namespace}:routes"])
    {:ok, _} = Redix.command(conn, ["HSET", "#{@namespace}:routes", "old", encoded_route("old")])
    {:ok, _} = Redix.command(conn, ["SET", "#{@namespace}:version", "1"])

    eventually(fn -> route_ids(node.instance) == ["old"] end, 3_000)
    assert Routes.meta(node.instance).version == 1
  end

  test "a namespace emptied under a node keeps its last table until a version reappears", %{
    conn: conn
  } do
    node = start_node(tick_ms: 50)
    assert {:ok, _} = Routes.create(node.instance, %{"id" => "a", "path" => "/hooks/a"})
    assert {:ok, _} = Routes.create(node.instance, %{"id" => "b", "path" => "/hooks/b"})
    before = Routes.meta(node.instance)

    {:ok, _} = Redix.command(conn, ["FLUSHDB"])
    # Several ticks, none of which may empty the table.
    Process.sleep(300)

    assert route_ids(node.instance) == ["a", "b"]
    assert Routes.meta(node.instance).epoch == before.epoch
    assert Routes.authorize(node.instance, "POST", ["hooks", "a"], {1, 2, 3, 4}) == {:ok, "a"}

    # A deliberate restore (a version and a definition) is followed.
    {:ok, _} = Redix.command(conn, ["HSET", "#{@namespace}:routes", "c", encoded_route("c")])
    {:ok, _} = Redix.command(conn, ["SET", "#{@namespace}:version", "1"])
    eventually(fn -> route_ids(node.instance) == ["c"] end, 3_000)
  end

  test "a node booting with a different seed leaves an existing namespace alone" do
    first = start_node(seed: [%{"id" => "s", "path" => "/hooks/s"}])
    assert {:ok, _} = Routes.create(first.instance, %{"id" => "n", "path" => "/hooks/n"})
    assert :ok = Routes.delete(first.instance, "s")

    second = start_node(seed: [%{"id" => "t", "path" => "/hooks/t"}])

    # Neither the new seed's route nor the deleted one appears: the namespace
    # already existed, so the seed was not applied.
    assert route_ids(second.instance) == ["n"]
  end

  test "a pub/sub connection that dies is replaced and subscribed again" do
    node_a = start_node(tick_ms: 3_600_000)
    node_b = start_node(tick_ms: 3_600_000)

    old_state = Ankusa.whereis(node_a.instance, :routes_store)
    old_pubsub = Ankusa.whereis(node_a.instance, :routes_redis_pubsub)
    Process.exit(old_pubsub, :kill)

    # Under :rest_for_one the state process goes with it and comes back
    # subscribed. A store that stayed up would be deaf to broadcasts from here on.
    eventually(
      fn ->
        state = Ankusa.whereis(node_a.instance, :routes_store)
        pubsub = Ankusa.whereis(node_a.instance, :routes_redis_pubsub)
        is_pid(state) and is_pid(pubsub) and state != old_state and pubsub != old_pubsub
      end,
      5_000
    )

    # The tick is an hour away: only a live subscription can bring this to A.
    assert {:ok, _} = Routes.create(node_b.instance, %{"id" => "n", "path" => "/hooks/n"})
    eventually(fn -> "n" in route_ids(node_a.instance) end)
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
