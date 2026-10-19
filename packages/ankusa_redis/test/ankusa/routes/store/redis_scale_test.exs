defmodule Ankusa.Routes.Store.RedisScaleTest do
  @moduledoc """
  The Redis route store loading a table that is many `HSCAN` pages long: boot,
  the incremental write path, a reload a peer's write forces, and the two
  limits a load answers to (`load_timeout_ms`, `routes.memory_budget_bytes`).

  Needs the package's compose Redis (`docker compose up -d --wait`); `REDIS_URL`
  overrides the default `redis://localhost:6399`. Each test has its own
  namespace, so it never touches another suite's keys.
  """

  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Ankusa.Routes
  alias Ankusa.Routes.{Route, Snapshot}
  alias Ankusa.Routes.Store.Redis

  @url System.get_env("REDIS_URL", "redis://localhost:6399")
  # Six pages at the scan's COUNT of 1000, and then some.
  @count 6_500

  setup do
    namespace = "ankusa:routes:scale:#{System.unique_integer([:positive])}"
    {:ok, conn} = Redix.start_link(@url)

    on_exit(fn ->
      {:ok, conn} = Redix.start_link(@url)
      {:ok, _} = Redix.command(conn, ["DEL" | keys(namespace)])
      Redix.stop(conn)
    end)

    %{namespace: namespace, conn: conn}
  end

  defp keys(namespace), do: Enum.map(~w(routes ip_rules version), &"#{namespace}:#{&1}")

  # What a fleet that has been creating routes for a while leaves in Redis:
  # `count` routes under `count / 500` first segments, version 7.
  defp preload(conn, namespace, count) do
    1..count
    |> Stream.map(fn i ->
      {:ok, route} =
        Route.from_attrs(%{"id" => "r#{i}", "path" => "/p#{rem(i, 500)}/r#{div(i, 500)}"})

      ["HSET", "#{namespace}:routes", route.id, JSON.encode!(Route.to_stored_json(route))]
    end)
    |> Stream.chunk_every(1_000)
    |> Enum.each(&Redix.pipeline!(conn, &1))

    Redix.command!(conn, ["SET", "#{namespace}:version", "7"])
  end

  defp node_config(namespace, store_opts, routes_opts) do
    instance = :"redis_scale#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "ankusa_#{instance}_#{System.os_time(:microsecond)}")
    on_exit(fn -> File.rm_rf(dir) end)

    Ankusa.Config.new(
      instance: instance,
      roles: [:edge],
      data_dir: dir,
      port: 0,
      routes:
        Keyword.merge(
          [
            enabled: true,
            admin: [port: 0],
            store:
              {Redis,
               Keyword.merge([url: @url, namespace: namespace, tick_ms: 3_600_000], store_opts)}
          ],
          routes_opts
        )
    )
  end

  defp start_node(namespace, store_opts \\ [], routes_opts \\ []) do
    config = node_config(namespace, store_opts, routes_opts)
    start_supervised!({Ankusa.Instance, config}, id: config.instance)
    config.instance
  end

  defp eventually(fun, deadline_ms \\ 5_000),
    do: wait_until(fun, System.monotonic_time(:millisecond) + deadline_ms)

  defp wait_until(fun, deadline) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("condition was still false after the deadline")

      true ->
        Process.sleep(20)
        wait_until(fun, deadline)
    end
  end

  defp authorize(instance, path) do
    segments = String.split(path, "/", trim: true)
    {:ok, ip} = Ankusa.Net.parse("203.0.113.9")
    {decision, _cached} = Routes.authorize_path(instance, "POST", segments, path, ip)
    decision
  end

  test "a table many pages long loads whole and serves every route", %{
    namespace: namespace,
    conn: conn
  } do
    preload(conn, namespace, @count)
    instance = start_node(namespace)

    meta = Routes.meta(instance)
    assert meta.count == @count
    assert meta.version == 7

    for i <- [1, 500, 501, 3_250, @count] do
      assert authorize(instance, "/p#{rem(i, 500)}/r#{div(i, 500)}") == {:ok, "r#{i}"}
    end

    assert authorize(instance, "/p1/missing") == {:reject, :no_route}

    assert {:ok, %{routes: page, next_cursor: cursor}} = Routes.list(instance, limit: 5)
    assert length(page) == 5
    assert is_binary(cursor)
  end

  test "a create after the load changes one row, not the whole generation", %{
    namespace: namespace,
    conn: conn
  } do
    preload(conn, namespace, @count)
    instance = start_node(namespace)
    before = Routes.meta(instance)

    assert {:ok, %Route{id: "fresh"}} =
             Routes.create(instance, %{"id" => "fresh", "path" => "/fresh/hook"})

    after_create = Routes.meta(instance)
    assert after_create.gen == before.gen
    assert after_create.count == @count + 1
    assert after_create.version == 8
    assert authorize(instance, "/fresh/hook") == {:ok, "fresh"}

    # The conflict index is kept in step by the same incremental write.
    assert {:error, {:conflict, "fresh"}} =
             Routes.create(instance, %{"id" => "dup", "path" => "/fresh/hook"})

    assert :ok = Routes.delete(instance, "fresh")
    assert authorize(instance, "/fresh/hook") == {:reject, :no_route}
    assert Routes.meta(instance).gen == before.gen
  end

  test "a peer's write reloads the whole table through the scan", %{
    namespace: namespace,
    conn: conn
  } do
    preload(conn, namespace, @count)
    a = start_node(namespace)
    b = start_node(namespace)
    gen_before = Routes.meta(a).gen

    assert {:ok, _} = Routes.create(b, %{"id" => "from-b", "path" => "/from/b"})

    eventually(fn -> authorize(a, "/from/b") == {:ok, "from-b"} end)
    assert Routes.meta(a).count == @count + 1
    assert Routes.meta(a).version == Routes.meta(b).version
    assert Routes.meta(a).gen != gen_before
  end

  test "a load past load_timeout_ms ends instead of walking on", %{
    namespace: namespace,
    conn: conn
  } do
    preload(conn, namespace, @count)
    config = node_config(namespace, [load_timeout_ms: 1], [])

    capture_log(fn ->
      assert {:error, _reason} = start_supervised({Ankusa.Instance, config}, id: config.instance)
    end)
  end

  test "a table that does not fit routes.memory_budget_bytes is refused at boot", %{
    namespace: namespace,
    conn: conn
  } do
    preload(conn, namespace, @count)
    # The cap and the budget agree (config validation passes); the 6 500 routes
    # already in Redis are far past what the budget holds.
    config =
      node_config(namespace, [],
        max_routes: 100,
        memory_budget_bytes: Snapshot.estimated_bytes(100)
      )

    capture_log(fn ->
      assert {:error, _reason} = start_supervised({Ankusa.Instance, config}, id: config.instance)
    end)
  end

  test "the store's timeouts are positive integers" do
    assert Ankusa.Redis.Options.timeouts([], "routes.store") == {5_000, 300_000}

    assert Ankusa.Redis.Options.timeouts([timeout_ms: 100, load_timeout_ms: 900], "x") ==
             {100, 900}

    assert_raise ArgumentError, ~r/routes\.store timeout_ms must be a positive integer/, fn ->
      Ankusa.Redis.Options.timeouts([timeout_ms: 0], "routes.store")
    end

    assert_raise ArgumentError, ~r/load_timeout_ms must be a positive integer/, fn ->
      Ankusa.Redis.Options.timeouts([load_timeout_ms: "soon"], "routes.store")
    end
  end
end
