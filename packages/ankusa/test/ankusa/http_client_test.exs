defmodule Ankusa.HttpClientTest do
  @moduledoc """
  `Ankusa.HttpClient` against a real socket: what Finch's connection pool
  does when it is exhausted, which no `Req.Test` plug reaches.
  """

  # `capture_io(:stderr, ...)` is VM-wide, and the owned pools are one per node.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Ankusa.HttpClient
  alias Ankusa.HttpClient.Pools

  defmodule SlowPlug do
    @behaviour Plug
    import Plug.Conn

    @impl true
    def init(opts), do: opts

    @impl true
    def call(conn, _opts) do
      Process.sleep(300)
      send_resp(conn, 200, "ok")
    end
  end

  test "an exhausted connection pool is a transport error, never a raise" do
    port = Ankusa.TestHelpers.free_port()
    start_supervised!({Bandit, plug: SlowPlug, scheme: :http, ip: {127, 0, 0, 1}, port: port})
    url = "http://127.0.0.1:#{port}/"

    # A pool of 5 connections and a 20 ms checkout timeout: 70 requests that each
    # hold one for 300 ms leave most of them without one.
    results =
      1..70
      |> Task.async_stream(
        fn _ ->
          HttpClient.request(:get, url, [], nil, 5_000, pool: [pool_size: 5, pool_timeout: 20])
        end,
        max_concurrency: 70,
        timeout: 10_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &match?({:ok, 200, "ok"}, &1)) >= 5

    errors = Enum.reject(results, &match?({:ok, 200, _}, &1))
    assert errors != []
    assert Enum.all?(errors, &match?({:error, %RuntimeError{}}, &1))
  end

  describe "TLS verification" do
    defmodule Ok do
      @moduledoc false
      @behaviour Plug
      import Plug.Conn

      @impl true
      def init(opts), do: opts

      @impl true
      def call(conn, _opts), do: send_resp(conn, 200, "ok")
    end

    @insecure [connect_options: [transport_opts: [verify: :verify_none]]]

    # An https listener whose certificate (SAN `localhost`) chains to a private CA
    # nobody trusts by default; `ca` is that CA's PEM bundle.
    setup do
      %{client_config: client, server_config: server} =
        :public_key.pkix_test_data(%{
          # sha256 + P-256: what a TLS 1.3 handshake accepts (the test data's
          # default digest, sha1, gets `no_suitable_signature_algorithm`).
          server_chain: %{
            root: [digest: :sha256, key: {:namedCurve, :secp256r1}],
            peer: [
              digest: :sha256,
              key: {:namedCurve, :secp256r1},
              extensions: [{:Extension, {2, 5, 29, 17}, false, [{:dNSName, ~c"localhost"}]}]
            ]
          },
          client_chain: %{root: [], peer: []}
        })

      dir = Path.join(System.tmp_dir!(), "ankusa-tls-#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      {key_type, key_der} = server[:key]
      cert = Path.join(dir, "cert.pem")
      key = Path.join(dir, "key.pem")
      ca = Path.join(dir, "ca.pem")

      File.write!(cert, :public_key.pem_encode([{:Certificate, server[:cert], :not_encrypted}]))
      File.write!(key, :public_key.pem_encode([{key_type, key_der, :not_encrypted}]))

      File.write!(
        ca,
        :public_key.pem_encode(
          for der <- client[:cacerts], do: {:Certificate, der, :not_encrypted}
        )
      )

      port = Ankusa.TestHelpers.free_port()

      start_supervised!(
        {Bandit,
         plug: Ok, scheme: :https, ip: {127, 0, 0, 1}, port: port, certfile: cert, keyfile: key}
      )

      %{port: port, ca: ca}
    end

    test "a certificate the system does not trust is refused", %{port: port} do
      assert {:error, reason} =
               HttpClient.request(:get, "https://localhost:#{port}/", [], nil, 5_000)

      assert inspect(reason) =~ "unknown_ca"
    end

    test "cacertfile names a private CA to verify against", %{port: port, ca: ca} do
      assert {:ok, 200, "ok"} =
               HttpClient.request(:get, "https://localhost:#{port}/", [], nil, 5_000,
                 cacertfile: ca
               )
    end

    test "the hostname is verified too: the right CA is not enough", %{port: port, ca: ca} do
      assert {:error, reason} =
               HttpClient.request(:get, "https://127.0.0.1:#{port}/", [], nil, 5_000,
                 cacertfile: ca
               )

      assert inspect(reason) =~ "handshake_failure" or inspect(reason) =~ "hostname" or
               inspect(reason) =~ "bad_cert"
    end

    test "rejects verify_none", %{port: port} do
      assert_raise ArgumentError, ~r/verify_none/, fn ->
        HttpClient.request(:get, "https://localhost:#{port}/", [], nil, 5_000, @insecure)
      end

      assert_raise ArgumentError, ~r/verify_none/, fn -> HttpClient.validate!(@insecure) end
      assert :ok = HttpClient.validate!(@insecure, true)
    end

    test "allow_insecure_tls connects to a certificate nobody vouches for", %{port: port} do
      assert {:ok, 200, "ok"} =
               HttpClient.request(:get, "https://localhost:#{port}/", [], nil, 5_000,
                 allow_insecure_tls: true
               )
    end

    test "a named finch pool owns its connection options", %{port: port, ca: ca} do
      pool = [finch: [name: __MODULE__.OwnPool]]

      assert_raise ArgumentError, ~r/named :finch pool/, fn ->
        HttpClient.validate!(pool, true)
      end

      assert_raise ArgumentError, ~r/named :finch pool/, fn ->
        HttpClient.validate!(pool ++ [connect_options: [timeout: 1]])
      end

      assert_raise ArgumentError, ~r/named :finch pool/, fn ->
        HttpClient.validate!(pool, false, ca)
      end

      assert :ok = HttpClient.validate!(pool)

      # The pool's own conn_opts (here: the private CA) are what the request uses.
      start_supervised!(
        {Finch,
         name: __MODULE__.OwnPool,
         pools: %{default: [conn_opts: [transport_opts: [cacertfile: ca]]]}}
      )

      assert {:ok, 200, "ok"} =
               HttpClient.request(:get, "https://localhost:#{port}/", [], nil, 5_000, pool)
    end
  end

  describe "owned pools" do
    alias Ankusa.Config

    defmodule Pong do
      @moduledoc false
      @behaviour Plug
      import Plug.Conn

      @impl true
      def init(opts), do: opts

      @impl true
      def call(conn, _opts), do: send_resp(conn, 200, "ok")
    end

    # Answers with the HTTP version the connection negotiated.
    defmodule Proto do
      @moduledoc false
      @behaviour Plug
      import Plug.Conn

      @impl true
      def init(opts), do: opts

      @impl true
      def call(conn, _opts), do: send_resp(conn, 200, to_string(get_http_protocol(conn)))
    end

    @doc false
    def forward(_event, measurements, metadata, pid),
      do: send(pid, {:pool, measurements, metadata})

    # An instance whose `egress.pools` is `pools`, so its pools are its own: the
    # node's rows are keyed by instance.
    defp instance(pools \\ %{}) do
      inst = Ankusa.TestHelpers.unique_instance()
      Ankusa.put_config(Config.new(instance: inst, egress: %{pools: pools}))
      on_exit(fn -> :persistent_term.erase({Ankusa, :config, inst}) end)
      inst
    end

    defp listener(plug \\ Pong) do
      port = Ankusa.TestHelpers.free_port()

      start_supervised!(
        Supervisor.child_spec(
          {Bandit, plug: plug, scheme: :http, ip: {127, 0, 0, 1}, port: port, startup_log: false},
          id: {:bandit, port}
        )
      )

      {"http://127.0.0.1:#{port}/", port}
    end

    # Finch pools running for 127.0.0.1:port, whichever instance opened them.
    defp live_pools(port) do
      Pools.finch()
      |> Registry.select([{{{:_, "127.0.0.1", port, :_}, :_, :_}, [], [true]}])
      |> length()
    end

    defp get(url, inst, extra \\ []),
      do: HttpClient.request(:get, url, [], nil, 5_000, [instance: inst] ++ extra)

    defp eventually(fun, tries \\ 100) do
      cond do
        fun.() -> :ok
        tries == 0 -> flunk("condition never held")
        true -> Process.sleep(20) && eventually(fun, tries - 1)
      end
    end

    test "pool reaped: an idle pool is closed by the sweep, one with a request in flight is not" do
      inst = instance(%{pool_max_idle_ms: 50})
      {fast, fast_port} = listener()
      {slow, slow_port} = listener(SlowPlug)

      assert {:ok, 200, "ok"} = get(fast, inst)
      assert Pools.count(inst) == 1
      assert live_pools(fast_port) == 1

      busy = Task.async(fn -> get(slow, inst) end)
      eventually(fn -> Pools.count(inst) == 2 end)

      Process.sleep(80)
      assert :ok = Pools.sweep_now()

      # The idle one is gone, its connections with it; the one serving a request stays.
      assert Pools.count(inst) == 1
      assert live_pools(fast_port) == 0
      assert live_pools(slow_port) == 1
      assert {:ok, 200, "ok"} = Task.await(busy)

      # The next request to the reaped origin opens a pool again.
      assert {:ok, 200, "ok"} = get(fast, inst)
      assert live_pools(fast_port) == 1
    end

    test "http2 negotiates against a TLS listener, and http1 only when the instance says so" do
      %{cert: cert, key: key, ca: ca} = Ankusa.TestHelpers.tls_fixture()
      port = Ankusa.TestHelpers.free_port()

      start_supervised!(
        {Bandit,
         plug: Proto,
         scheme: :https,
         ip: {127, 0, 0, 1},
         port: port,
         certfile: cert,
         keyfile: key,
         startup_log: false}
      )

      url = "https://localhost:#{port}/"

      assert {:ok, 200, "HTTP/2"} = get(url, instance(), cacertfile: ca)
      assert {:ok, 200, "HTTP/1.1"} = get(url, instance(%{protocols: [:http1]}), cacertfile: ca)

      # A sink's own `pool:` wins over the instance.
      assert {:ok, 200, "HTTP/1.1"} =
               get(url, instance(%{protocols: [:http1, :http2]}),
                 cacertfile: ca,
                 pool: [protocols: [:http1]]
               )
    end

    test "max_pools: at the ceiling the least recently used idle pool is closed" do
      inst = instance(%{max_pools: 2})
      [{a, pa}, {b, pb}, {c, pc}] = for _ <- 1..3, do: listener()

      assert {:ok, 200, _} = get(a, inst)
      Process.sleep(5)
      assert {:ok, 200, _} = get(b, inst)
      Process.sleep(5)

      :telemetry.attach(
        "closed-#{inst}",
        [:ankusa, :http_pool, :closed],
        &__MODULE__.forward/4,
        self()
      )

      on_exit(fn -> :telemetry.detach("closed-#{inst}") end)

      assert {:ok, 200, _} = get(c, inst)
      assert Pools.count(inst) == 2
      assert {live_pools(pa), live_pools(pb), live_pools(pc)} == {0, 1, 1}
      assert_received {:pool, %{count: 1}, %{instance: ^inst, reason: :evicted}}
    end

    test "max_pools: with every pool serving a request the next origin is refused, retryably" do
      inst = instance(%{max_pools: 2})
      [{a, _}, {b, _}] = for _ <- 1..2, do: listener(SlowPlug)
      {c, pc} = listener()

      :telemetry.attach(
        "refused-#{inst}",
        [:ankusa, :http_pool, :refused],
        &__MODULE__.forward/4,
        self()
      )

      on_exit(fn -> :telemetry.detach("refused-#{inst}") end)

      busy = for url <- [a, b], do: Task.async(fn -> get(url, inst) end)
      eventually(fn -> Pools.count(inst) == 2 end)

      assert {:error, {:unavailable, :pool_limit}} = get(c, inst)
      assert live_pools(pc) == 0
      assert Pools.count(inst) == 2
      assert_received {:pool, %{count: 1}, %{instance: ^inst, scope: :instance}}

      # The sink layer retries it: a transient failure, not a dead letter.
      assert {:transient, {:unavailable, :pool_limit}} =
               Ankusa.Sink.classify({:unavailable, :pool_limit})

      for task <- busy, do: assert({:ok, 200, "ok"} = Task.await(task))
      assert {:ok, 200, "ok"} = get(c, inst)
    end

    test "max_pools_per_tenant: a tenant at its ceiling closes only its own pools" do
      inst = instance(%{max_pools_per_tenant: 1})
      [{a, pa}, {b, pb}, {c, pc}] = for _ <- 1..3, do: listener()

      assert {:ok, 200, _} = get(a, inst, tenant: "acme")
      assert {:ok, 200, _} = get(c, inst, tenant: "globex")
      Process.sleep(5)
      assert {:ok, 200, _} = get(b, inst, tenant: "acme")

      # acme's first pool made room for its second; globex's is untouched.
      assert {live_pools(pa), live_pools(pb), live_pools(pc)} == {0, 1, 1}
      assert Pools.count(inst) == 2
    end

    test "close_source closes the idle pools a deleted source's deliveries used" do
      inst = instance()
      {url, port} = listener()

      assert {:ok, 200, _} = get(url, inst, tenant: "acme", source: {"acme", "stripe"})
      Pools.close_source(inst, "acme", "other")
      assert :ok = Pools.sweep_now()
      assert live_pools(port) == 1

      Pools.close_source(inst, "acme", "stripe")
      assert :ok = Pools.sweep_now()
      assert Pools.count(inst) == 0
      assert live_pools(port) == 0
    end

    test "a request with a transport of its own opens no owned pool" do
      inst = instance()

      assert {:ok, 200, "mine"} =
               HttpClient.request(:get, "http://never.invalid/", [], nil, 1_000,
                 instance: inst,
                 plug: fn conn -> Plug.Conn.send_resp(conn, 200, "mine") end
               )

      assert Pools.count(inst) == 0
    end

    test "an invalid pool override and a ceiling out of range are refused up front" do
      for bad <- [
            [pool_size: 0],
            [pool_count: 65],
            [pool_timeout: "5s"],
            [protocols: []],
            [size: 1]
          ] do
        assert_raise ArgumentError, fn -> Pools.validate_override!(bad) end
      end

      assert :ok = Pools.validate_override!(pool_size: 4, protocols: [:http2])

      assert_raise ArgumentError, ~r/egress.pools.max_pools must be/, fn ->
        instance(%{max_pools: 0})
      end
    end

    test "boot warns when a pool is smaller than the dispatch concurrency, or can outrun ulimit" do
      small = Config.new(instance: :pools_warn, egress: %{pools: %{pool_size: 2}})

      huge =
        Config.new(
          instance: :pools_warn,
          egress: %{pools: %{pool_size: 1_024, max_pools: 65_536}}
        )

      assert capture_log(fn -> Pools.validate_config!(small) end) =~
               "egress.pools.pool_size * pool_count (2) is below dispatch.concurrency (32)"

      assert capture_log(fn -> Pools.validate_config!(huge) end) =~ "file descriptor limit"
    end
  end
end
