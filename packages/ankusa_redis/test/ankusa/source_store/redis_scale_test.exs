defmodule Ankusa.SourceStore.RedisScaleTest do
  @moduledoc """
  The Redis source store loading a hash many `HSCAN` pages long, and the limits
  a load answers to. Each test has its own namespace.

  Needs the package's compose Redis (`docker compose up -d --wait`); `REDIS_URL`
  overrides the default `redis://localhost:6399`.
  """

  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Ankusa.{Source, SourceStore}
  alias Ankusa.SourceStore.Redis

  @url System.get_env("REDIS_URL", "redis://localhost:6399")
  # Four pages at the scan's COUNT of 1000, and then some.
  @count 4_200

  setup do
    namespace = "ankusa:sources:scale:#{System.unique_integer([:positive])}"
    {:ok, conn} = Redix.start_link(@url)

    on_exit(fn ->
      {:ok, conn} = Redix.start_link(@url)
      {:ok, _} = Redix.command(conn, ["DEL", "#{namespace}:sources", "#{namespace}:version"])
      Redix.stop(conn)
    end)

    %{namespace: namespace, conn: conn}
  end

  defp decoder(_source_id, %{"sinks" => [_ | _]}), do: [sinks: [{Ankusa.Sink.Log, []}]]
  defp decoder(_source_id, _spec), do: raise(ArgumentError, "sinks must be a non-empty list")

  defp preload(conn, namespace, count) do
    1..count
    |> Stream.map(fn i ->
      entry = %{
        "tenant" => "t#{i}",
        "name" => "hook",
        "spec" => %{
          "verify" => %{"type" => "hmac", "secret" => "s3cr3t", "signature_header" => "X-Sig"},
          "sinks" => [%{"type" => "log"}]
        }
      }

      ["HSET", "#{namespace}:sources", "t#{i}.hook", JSON.encode!(entry)]
    end)
    |> Stream.chunk_every(1_000)
    |> Enum.each(&Redix.pipeline!(conn, &1))

    Redix.command!(conn, ["SET", "#{namespace}:version", "3"])
  end

  defp node_config(namespace, store_opts, extra \\ []) do
    instance = :"redis_src_scale#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "ankusa_#{instance}_#{System.os_time(:microsecond)}")
    on_exit(fn -> File.rm_rf(dir) end)

    opts =
      Keyword.merge(
        [url: @url, namespace: namespace, tick_ms: 3_600_000, decoder: &decoder/2],
        store_opts
      )

    Ankusa.Config.new(
      Keyword.merge(
        [
          instance: instance,
          roles: [:edge],
          data_dir: dir,
          port: 0,
          source_store: {Redis, opts}
        ],
        extra
      )
    )
  end

  test "a hash many pages long loads whole, and a write after it is one row", %{
    namespace: namespace,
    conn: conn
  } do
    preload(conn, namespace, @count)
    config = node_config(namespace, [])
    start_supervised!({Ankusa.Instance, config}, id: config.instance)
    instance = config.instance

    assert {:ok, %Source{id: "t1.hook", tenant_id: "t1"}} = SourceStore.fetch(instance, "t1.hook")
    assert {:ok, %Source{}} = SourceStore.fetch(instance, "t#{@count}.hook")
    assert {:ok, %Source{}} = SourceStore.fetch(instance, "t2001.hook")
    assert SourceStore.fetch(instance, "t#{@count + 1}.hook") == :error
    assert length(SourceStore.list(instance)) == @count

    assert {:ok, _stored} =
             SourceStore.put(
               instance,
               "fresh",
               "hook",
               %{"sinks" => [%{"type" => "log"}]},
               :create
             )

    assert {:ok, %Source{id: "fresh.hook"}} = SourceStore.fetch(instance, "fresh.hook")
    assert length(SourceStore.list(instance)) == @count + 1

    assert :ok = SourceStore.delete(instance, "t7", "hook")
    assert SourceStore.fetch(instance, "t7.hook") == :error
    assert length(SourceStore.list(instance)) == @count
  end

  test "rewrap walks the same pages", %{namespace: namespace, conn: conn} do
    preload(conn, namespace, @count)
    kek = Base.encode64(:binary.copy(<<7>>, 32))
    config = node_config(namespace, [], secrets: [kek: kek])
    start_supervised!({Ankusa.Instance, config}, id: config.instance)

    # Every entry was stored in plaintext: the first run seals each exactly
    # once (an entry a page returns twice is planned once), the second finds
    # them all current.
    assert {:ok, %{rewrapped: @count, current: 0}} = Redis.rewrap(config.instance)
    assert {:ok, %{rewrapped: 0, current: @count}} = Redis.rewrap(config.instance)
  end

  test "a load past load_timeout_ms ends instead of walking on", %{
    namespace: namespace,
    conn: conn
  } do
    preload(conn, namespace, @count)
    config = node_config(namespace, load_timeout_ms: 1)

    capture_log(fn ->
      assert {:error, _reason} = start_supervised({Ankusa.Instance, config}, id: config.instance)
    end)
  end

  test "the store's timeouts are positive integers" do
    config = node_config("ankusa:sources:scale:unused", timeout_ms: 0)

    capture_log(fn ->
      assert {:error, _reason} = start_supervised({Ankusa.Instance, config}, id: config.instance)
    end)
  end
end
