defmodule Ankusa.BlobStore.PoolTest do
  @moduledoc """
  The connection pool the instance starts for each HTTP object store's endpoint
  (`Ankusa.BlobStore.Pool`): which stores get one, how it is sized, how requests
  reach it, and that `pool_size` and `pool_timeout` bound a real endpoint's
  concurrency.
  """

  use ExUnit.Case, async: true

  alias Ankusa.BlobStore.{GCS, LocalFS, Options, Pool, S3}
  alias Ankusa.Config

  # An endpoint that takes 400 ms to answer.
  defmodule SlowPlug do
    @moduledoc false
    @behaviour Plug
    @impl true
    def init(opts), do: opts
    @impl true
    def call(conn, _opts) do
      Process.sleep(400)
      Plug.Conn.send_resp(conn, 200, "ok")
    end
  end

  defp s3(endpoint, extra \\ []) do
    {S3,
     [
       bucket: "b",
       region: "us-east-1",
       endpoint: endpoint,
       access_key_id: "AKID",
       secret_access_key: "secret"
     ] ++ extra}
  end

  defp config(storage_store, claim_stores) do
    inst = :"pool#{System.unique_integer([:positive])}"
    dir = Ankusa.TestHelpers.unique_data_dir(inst)
    on_exit(fn -> File.rm_rf(dir) end)

    config = Config.new(instance: inst, data_dir: dir, roles: [])

    %{
      config
      | storage: %{config.storage | blob_store: storage_store},
        claim_check: Map.put(config.claim_check, :blob_stores, claim_stores)
    }
  end

  describe "child_specs/1" do
    test "one pool per distinct endpoint, sized by the largest store that shares it" do
      config =
        config(
          s3("http://pool-a.test", pool_size: 7, pool_count: 2),
          [
            s3("http://pool-a.test", pool_size: 9),
            {GCS, bucket: "b", endpoint: "http://pool-b.test"},
            {LocalFS, []}
          ]
        )

      assert [a, b] = Enum.sort_by(Pool.child_specs(config), &elem(&1.id, 2))
      assert a.id == {Pool, config.instance, "http://pool-a.test"}
      assert b.id == {Pool, config.instance, "http://pool-b.test"}

      assert %{default: pool_a} = pools(a)
      assert pool_a[:size] == 9
      assert pool_a[:count] == 2

      # The defaults, for the store that sets nothing.
      assert %{default: pool_b} = pools(b)
      assert pool_b[:size] == Options.default(:pool_size)
      assert pool_b[:count] == Options.default(:pool_count)
    end

    test "a store with its own transport (finch: or plug:) gets no pool" do
      config =
        config(
          s3("http://pool-own.test", req_options: [finch: Some.Finch]),
          [s3("http://pool-plug.test", req_options: [plug: {Req.Test, __MODULE__}])]
        )

      assert [] = Pool.child_specs(config)
    end

    test "https endpoints verify the peer; http ones carry no TLS options" do
      config =
        config(
          s3("https://pool-tls.test", req_options: [connect_options: [protocols: [:http1]]]),
          [s3("http://pool-plain.test")]
        )

      pools = Map.new(Pool.child_specs(config), &{elem(&1.id, 2), pools(&1)[:default]})

      tls = pools["https://pool-tls.test"][:conn_opts][:transport_opts]
      assert tls[:verify] == :verify_peer
      assert tls[:versions] == [:"tlsv1.3", :"tlsv1.2"]
      assert pools["https://pool-tls.test"][:conn_opts][:protocols] == [:http1]

      refute Keyword.has_key?(
               pools["http://pool-plain.test"][:conn_opts][:transport_opts],
               :verify
             )
    end
  end

  describe "req_options/3" do
    test "adds the pool and its checkout timeout once it is running" do
      config = config(s3("http://pool-req.test", pool_timeout: 123), [])
      opts = elem(config.storage.blob_store, 1)

      assert Pool.req_options(config.instance, "http://pool-req.test", opts) == []

      Enum.each(Pool.child_specs(config), &start_supervised!/1)

      req = Pool.req_options(config.instance, "http://pool-req.test", opts)

      assert req[:finch] == [
               name: Pool.name(config.instance, "http://pool-req.test"),
               pool_timeout: 123
             ]
    end

    test "leaves a store's own transport alone, and drops the per-request connection options" do
      config = config(s3("http://pool-keep.test"), [])
      Enum.each(Pool.child_specs(config), &start_supervised!/1)
      endpoint = "http://pool-keep.test"

      own = [req_options: [finch: [name: Mine], pool_timeout: 5]]

      assert Pool.req_options(config.instance, endpoint, own) == [
               finch: [name: Mine],
               pool_timeout: 5
             ]

      plug = [req_options: [plug: {Req.Test, __MODULE__}]]
      assert Pool.req_options(config.instance, endpoint, plug) == [plug: {Req.Test, __MODULE__}]

      tuned = [req_options: [connect_options: [protocols: [:http1]], cacertfile: "ca.pem"]]
      req = Pool.req_options(config.instance, endpoint, tuned)
      refute Keyword.has_key?(req, :connect_options)
      refute Keyword.has_key?(req, :cacertfile)
      assert req[:finch][:name] == Pool.name(config.instance, endpoint)
    end
  end

  test "pool_size and pool_timeout bound the concurrency of a real endpoint" do
    {:ok, server} = Bandit.start_link(plug: SlowPlug, port: 0, ip: :loopback, startup_log: false)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    endpoint = "http://127.0.0.1:#{port}"

    store = s3(endpoint, pool_size: 1, pool_timeout: 100, retries: 0)
    config = config(store, [])
    Enum.each(Pool.child_specs(config), &start_supervised!/1)
    {S3, opts} = store

    results =
      1..2
      |> Task.async_stream(fn _ -> S3.get(config.instance, "k", opts) end, timeout: 10_000)
      |> Enum.map(fn {:ok, result} -> result end)

    # One connection: one request is answered, the other cannot check one out in 100 ms.
    assert Enum.count(results, &match?({:ok, "ok"}, &1)) == 1
    assert [{:error, reason}] = Enum.reject(results, &match?({:ok, _}, &1))
    refute match?({:status, _, _}, reason)
  end

  describe "Options.validate!/3" do
    test "accepts the defaults, and a store that is not an HTTP adapter" do
      assert :ok = Options.validate!(S3, [bucket: "b"], "storage.blob_store")
      assert :ok = Options.validate!(LocalFS, [op_deadline_ms: -1], "storage.blob_store")
    end

    test "names the store and the key of a value out of range" do
      for {key, value} <- [
            timeout_ms: 0,
            upload_timeout_ms: 999,
            op_deadline_ms: 3_600_001,
            multipart_threshold_bytes: 0,
            multipart_part_bytes: 1_048_576,
            retries: 11,
            breaker_failures: -1,
            breaker_open_ms: 99,
            breaker_slow_ms: 9,
            pool_size: 0,
            pool_count: 65,
            pool_timeout: "5s"
          ] do
        assert_raise ArgumentError,
                     ~r/claim_check.blob_stores\[0\]: #{key} must be an integer/,
                     fn ->
                       Options.validate!(S3, [{key, value}], "claim_check.blob_stores[0]")
                     end
      end
    end

    test "a part size is a multiple of 256 KiB" do
      assert_raise ArgumentError, ~r/multiple of 262144/, fn ->
        Options.validate!(S3, [multipart_part_bytes: 5 * 1_048_576 + 1], "storage.blob_store")
      end
    end

    test "BlobStore.validate_config!/1 checks every store the config names" do
      config = config(s3("http://opt.test"), [s3("http://opt.test", op_deadline_ms: 0)])

      assert_raise ArgumentError, ~r/claim_check.blob_stores\[0\]: op_deadline_ms/, fn ->
        Ankusa.BlobStore.validate_config!(config)
      end

      assert :ok = Ankusa.BlobStore.validate_config!(config(s3("http://opt.test"), []))
    end

    test "with_deadline/1 takes the sooner of the store's own and the caller's" do
      now = System.monotonic_time(:millisecond)

      own = Options.with_deadline(op_deadline_ms: 5_000)
      assert own[:deadline] in (now + 4_900)..(now + 5_100)

      soon = Options.with_deadline(op_deadline_ms: 5_000, deadline: now + 100)
      assert soon[:deadline] == now + 100

      late = Options.with_deadline(op_deadline_ms: 5_000, deadline: now + 60_000)
      assert late[:deadline] < now + 6_000
    end
  end

  defp pools(%{start: {Finch, :start_link, [opts]}}), do: opts[:pools]
end
