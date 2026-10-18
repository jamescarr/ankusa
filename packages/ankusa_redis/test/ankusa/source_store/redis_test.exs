defmodule Ankusa.SourceStore.RedisTest do
  @moduledoc """
  The Redis source store against a real Redis: two instances in one VM sharing
  one namespace stand in for two edge nodes.

  Needs the package's compose Redis (`docker compose up -d --wait`); `REDIS_URL`
  overrides the default `redis://localhost:6399`.
  """

  use ExUnit.Case, async: false

  alias Ankusa.{Source, SourceStore}
  alias Ankusa.SourceStore.Redis

  @namespace "ankusa:sources:test"
  @url System.get_env("REDIS_URL", "redis://localhost:6399")

  @spec_map %{
    "verify" => %{"type" => "hmac", "secret" => "s3cr3t", "signature_header" => "X-Sig"},
    "sinks" => [%{"type" => "log"}]
  }

  @log_spec %{"sinks" => [%{"type" => "log"}]}

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

  # Both nodes point at the same namespace, with a tick far longer than any
  # deadline below: a change that arrives in time arrived by pub/sub.
  defp node_config(opts, config_opts \\ []) do
    store_opts =
      Keyword.merge(
        [url: @url, namespace: @namespace, tick_ms: 60_000, decoder: &decoder/2],
        opts
      )

    [
      instance: :"redis_src#{System.unique_integer([:positive])}",
      roles: [:edge],
      source_store: {Redis, store_opts}
    ]
    |> Keyword.merge(config_opts)
    |> build_config()
  end

  defp start_node(opts \\ [], config_opts \\ []) do
    config = node_config(opts, config_opts)
    start_supervised!({Ankusa.Instance, config}, id: config.instance)
    config.instance
  end

  defp decoder(_source_id, spec) do
    case Map.get(spec, "sinks") do
      [_ | _] = sinks ->
        [
          sinks: Enum.map(sinks, fn %{"type" => "log"} -> {Ankusa.Sink.Log, []} end),
          trust_url_tenant: Map.get(spec, "trust_url_tenant", false)
        ]

      _ ->
        raise ArgumentError, "sinks must be a non-empty list"
    end
  end

  defp eventually(fun, deadline_ms \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + deadline_ms
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("condition was still false after the deadline")

      true ->
        Process.sleep(20)
        do_eventually(fun, deadline)
    end
  end

  # Only this suite's own keys are deleted, never the database.
  defp clean_namespace(conn) do
    {:ok, _} = Redix.command(conn, ["DEL", "#{@namespace}:sources", "#{@namespace}:version"])
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

  test "a source created on one node is served by the other" do
    a = start_node()
    b = start_node()

    assert {:ok, stored} = SourceStore.put(a, "acme", "billing", @spec_map, :create)

    eventually(fn -> match?({:ok, _}, SourceStore.fetch(b, "acme.billing")) end)

    assert {:ok, %Source{id: "acme.billing", tenant_id: "acme"}} =
             SourceStore.fetch(b, "acme.billing")

    assert SourceStore.get(b, "acme", "billing") == {:ok, stored}
    assert SourceStore.list_tenant(b, "acme") == [stored]
    assert SourceStore.list_tenant(a, "acme") == [stored]
  end

  test "updates reach every node, keep the stored secret, and creates/updates are checked in Redis" do
    a = start_node()
    b = start_node()

    assert {:ok, _} = SourceStore.put(a, "acme", "billing", @spec_map, :create)
    eventually(fn -> match?({:ok, _}, SourceStore.get(b, "acme", "billing")) end)

    # A verify block that keeps its type but leaves the secret out inherits it.
    edit = put_in(@spec_map, ["verify"], %{"type" => "hmac", "signature_header" => "X-New"})
    assert {:ok, updated} = SourceStore.put(b, "acme", "billing", edit, :update)
    assert updated.spec["verify"]["secret"] == "s3cr3t"

    eventually(fn ->
      match?(
        {:ok, %{spec: %{"verify" => %{"signature_header" => "X-New"}}}},
        SourceStore.get(a, "acme", "billing")
      )
    end)

    assert {:ok, %{spec: %{"verify" => %{"secret" => "s3cr3t"}}}} =
             SourceStore.get(a, "acme", "billing")

    assert SourceStore.put(b, "acme", "billing", @log_spec, :create) == {:error, :exists}
    assert SourceStore.put(a, "acme", "missing", @log_spec, :update) == {:error, :not_found}
  end

  test "two nodes creating the same source: exactly one succeeds" do
    a = start_node()
    b = start_node()

    results =
      [a, b]
      |> Enum.map(&Task.async(fn -> SourceStore.put(&1, "acme", "race", @log_spec, :create) end))
      |> Task.await_many()

    assert Enum.count(results, &match?({:ok, _}, &1)) == 1
    assert Enum.count(results, &(&1 == {:error, :exists})) == 1
  end

  test "a delete on one node is a 404 on the other" do
    a = start_node()
    b = start_node()

    assert {:ok, _} = SourceStore.put(a, "acme", "billing", @log_spec, :create)
    eventually(fn -> match?({:ok, _}, SourceStore.fetch(b, "acme.billing")) end)

    assert SourceStore.delete(b, "acme", "billing") == :ok
    eventually(fn -> SourceStore.fetch(a, "acme.billing") == :error end)
    assert SourceStore.get(a, "acme", "billing") == :error
    assert SourceStore.delete(a, "acme", "billing") == {:error, :not_found}
  end

  test "seeds are config-only and read-only" do
    a = start_node(sources: %{"acme.billing" => [sinks: [{Ankusa.Sink.Log, []}]]})

    assert {:ok, %Source{}} = SourceStore.fetch(a, "acme.billing")
    assert SourceStore.list_tenant(a, "acme") == []
    assert {:error, :invalid, message} = SourceStore.put(a, "acme", "billing", @log_spec, :create)
    assert message =~ "seeded"
    assert {:error, :invalid, _} = SourceStore.delete(a, "acme", "billing")
  end

  test "an undecodable spec is rejected before anything reaches Redis", %{conn: conn} do
    a = start_node()

    assert {:error, :invalid, message} =
             SourceStore.put(a, "acme", "bad", %{"sinks" => []}, :create)

    assert message =~ "sinks"
    assert {:ok, 0} = Redix.command(conn, ["HLEN", "#{@namespace}:sources"])
  end

  test "a node that boots after the writes loads them" do
    a = start_node()
    assert {:ok, _} = SourceStore.put(a, "acme", "billing", @log_spec, :create)

    b = start_node()
    assert {:ok, %Source{tenant_id: "acme"}} = SourceStore.fetch(b, "acme.billing")
  end

  test "a restarted state process keeps serving its mirror and stays subscribed" do
    a = start_node()
    b = start_node()
    assert {:ok, _} = SourceStore.put(b, "acme", "billing", @log_spec, :create)
    eventually(fn -> match?({:ok, _}, SourceStore.fetch(a, "acme.billing")) end)

    old = Ankusa.whereis(a, :source_store)
    ref = Process.monitor(old)

    # A reader hammering the mirror across the restart never sees it gone.
    reader =
      Task.async(fn ->
        for _ <- 1..2_000, reduce: :ok do
          :ok -> if match?({:ok, _}, SourceStore.fetch(a, "acme.billing")), do: :ok, else: :miss
          miss -> miss
        end
      end)

    Process.exit(old, :kill)
    assert_receive {:DOWN, ^ref, :process, _, _}
    assert Task.await(reader) == :ok

    eventually(fn ->
      is_pid(Ankusa.whereis(a, :source_store)) and Ankusa.whereis(a, :source_store) != old
    end)

    assert {:ok, _} = SourceStore.put(b, "acme", "later", @log_spec, :create)
    eventually(fn -> match?({:ok, _}, SourceStore.fetch(a, "acme.later")) end)
  end

  test "a first boot against an unreachable Redis fails" do
    config = node_config(url: "redis://127.0.0.1:1")
    Process.flag(:trap_exit, true)

    assert {:error, _reason} = Ankusa.Instance.start_link(config)
  end

  test "no process of the store prints the seeded sources in its status" do
    canary = "s3cr3t-canary"
    inst = start_node(sources: %{"seeded" => [sinks: [{Ankusa.Sink.Log, [token: canary]}]]})
    sup = Ankusa.whereis(inst, :source_store_sup)

    nested =
      for {_id, pid, :supervisor, _modules} <- Supervisor.which_children(sup), do: pid

    for pid <- [sup, Ankusa.whereis(inst, :source_store) | nested] do
      assert is_pid(pid)
      printed = inspect(:sys.get_status(pid), limit: :infinity, printable_limit: :infinity)
      refute printed =~ canary, "#{inspect(pid)} printed the seeded sources in its status"
    end
  end

  test "under a resolver that takes the tenant from the request, an unverified shared source must trust it" do
    config = node_config([], route_resolver: {Ankusa.RouteResolver.TenantPath, []})
    start_supervised!({Ankusa.Instance, config}, id: config.instance)
    inst = config.instance

    assert {:error, :invalid, message} =
             SourceStore.put(inst, "default", "open", @log_spec, :create)

    assert message =~ "trust_url_tenant"

    trusted = Map.put(@log_spec, "trust_url_tenant", true)
    assert {:ok, _} = SourceStore.put(inst, "default", "open", trusted, :create)
    assert {:ok, %Source{trust_url_tenant: true}} = SourceStore.fetch(inst, "default.open")
  end

  test "during an outage the connections restart on the mirror, writes are store_unavailable, and the node catches up" do
    {forwarder, port} = start_forwarder()
    a = start_node(url: "redis://127.0.0.1:#{port}", tick_ms: 100)
    b = start_node()

    assert {:ok, _} = SourceStore.put(a, "acme", "billing", @log_spec, :create)

    # Redis goes away for A only; B keeps writing.
    Process.exit(forwarder, :kill)
    assert {:ok, _} = SourceStore.put(b, "acme", "other", @log_spec, :create)

    # The connections and the state process die together and come back
    # without Redis: no crash loop past the restart limit, the mirror intact.
    sup = Ankusa.whereis(a, :source_store_sup)

    {_id, connections, _type, _modules} =
      sup
      |> Supervisor.which_children()
      |> Enum.find(&match?({Ankusa.SourceStore.Redis.Connections, _, _, _}, &1))

    old_state = Ankusa.whereis(a, :source_store)
    refs = Enum.map([connections, old_state], &Process.monitor/1)
    Process.exit(connections, :kill)
    for ref <- refs, do: assert_receive({:DOWN, ^ref, :process, _, _})

    eventually(
      fn ->
        pid = Ankusa.whereis(a, :source_store)
        is_pid(pid) and pid != old_state
      end,
      5_000
    )

    assert Process.alive?(sup)
    assert {:ok, _} = SourceStore.fetch(a, "acme.billing")

    assert SourceStore.put(a, "acme", "during", @log_spec, :create) ==
             {:error, :store_unavailable}

    assert SourceStore.delete(a, "acme", "billing") == {:error, :store_unavailable}
    assert {:ok, _} = SourceStore.fetch(a, "acme.billing")

    # Redis is back: A loads what it missed, and writes work again.
    start_forwarder(port)
    eventually(fn -> match?({:ok, _}, SourceStore.fetch(a, "acme.other")) end, 15_000)
    assert {:ok, _} = SourceStore.put(a, "acme", "after", @log_spec, :create)
    eventually(fn -> match?({:ok, _}, SourceStore.fetch(b, "acme.after")) end)
  end

  # ── secrets at rest ─────────────────────────────────────────────────────────

  describe "secrets at rest" do
    @kek_a Base.encode64(:binary.copy(<<1>>, 32))
    @kek_b Base.encode64(:binary.copy(<<2>>, 32))

    defp stored_entry(conn, source_id) do
      assert {:ok, entry} = Redix.command(conn, ["HGET", @namespace <> ":sources", source_id])
      entry
    end

    @tag :capture_log
    test "Redis holds no plaintext secret, and every node with the key reads it", %{conn: conn} do
      a = start_node([], secrets: [kek: "k1:" <> @kek_a])
      b = start_node([], secrets: [kek: "k1:" <> @kek_a])

      assert {:ok, stored} = SourceStore.put(a, "acme", "billing", @spec_map, :create)
      assert stored.spec == @spec_map

      entry = stored_entry(conn, "acme.billing")
      refute entry =~ "s3cr3t"
      assert entry =~ "AENC:"
      # identity and structure stay readable
      assert entry =~ "X-Sig"
      assert entry =~ ~s("tenant":"acme")

      eventually(fn -> match?({:ok, _}, SourceStore.get(b, "acme", "billing")) end)
      assert {:ok, %{spec: @spec_map}} = SourceStore.get(b, "acme", "billing")

      # An update that omits the secret merges Redis's decrypted copy, and the
      # stored result is encrypted again.
      edit = put_in(@spec_map, ["verify"], %{"type" => "hmac", "signature_header" => "X-New"})
      assert {:ok, updated} = SourceStore.put(b, "acme", "billing", edit, :update)
      assert updated.spec["verify"]["secret"] == "s3cr3t"
      refute stored_entry(conn, "acme.billing") =~ "s3cr3t"

      eventually(fn ->
        match?(
          {:ok, %{spec: %{"verify" => %{"signature_header" => "X-New"}}}},
          SourceStore.get(a, "acme", "billing")
        )
      end)

      assert {:ok, %{spec: %{"verify" => %{"secret" => "s3cr3t"}}}} =
               SourceStore.get(a, "acme", "billing")
    end

    @tag :capture_log
    test "a node that has no key for an entry refuses its first boot, naming it" do
      a = start_node([], secrets: [kek: "k1:" <> @kek_a])
      assert {:ok, _} = SourceStore.put(a, "acme", "billing", @spec_map, :create)

      Process.flag(:trap_exit, true)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:error, _reason} = Ankusa.Instance.start_link(node_config([]))

          assert {:error, _reason} =
                   Ankusa.Instance.start_link(node_config([], secrets: [kek: "k2:" <> @kek_b]))
        end)

      assert log =~ "source acme.billing cannot be decrypted"
      assert log =~ ~s({:unknown_key, "k1"})
      refute log =~ "s3cr3t"
    end

    @tag :capture_log
    test "a KEK that cannot be resolved refuses the boot" do
      Process.flag(:trap_exit, true)

      config = node_config([], secrets: [kek_provider: {__MODULE__, :failing_provider, []}])
      assert {:error, _reason} = Ankusa.Instance.start_link(config)
    end

    @tag :capture_log
    test "rewrap moves plaintext and previous-key entries to the current key", %{conn: conn} do
      plain = start_node()
      assert {:ok, _} = SourceStore.put(plain, "acme", "billing", @spec_map, :create)
      assert {:ok, _} = SourceStore.put(plain, "acme", "open", @log_spec, :create)
      assert stored_entry(conn, "acme.billing") =~ "s3cr3t"

      # Without a KEK there is nothing to move rows to, and nothing is written.
      assert {:error, {:secrets, _source_id, :no_kek}} = Redis.rewrap(plain)
      assert stored_entry(conn, "acme.billing") =~ "s3cr3t"

      # k1 arrives: the plaintext rows load, and rewrap encrypts the ones with
      # something to hide.
      k1 = start_node([], secrets: [kek: "k1:" <> @kek_a])
      eventually(fn -> match?({:ok, _}, SourceStore.get(k1, "acme", "billing")) end)
      assert Redis.rewrap(k1) == {:ok, %{rewrapped: 1, current: 1}}
      refute stored_entry(conn, "acme.billing") =~ "s3cr3t"
      assert Redis.rewrap(k1) == {:ok, %{rewrapped: 0, current: 2}}

      # Rotation: k2 current with k1 previous reads k1's rows, and rewrap moves them.
      k2 = start_node([], secrets: [kek: "k2:" <> @kek_b, kek_previous: ["k1:" <> @kek_a]])
      eventually(fn -> match?({:ok, _}, SourceStore.get(k2, "acme", "billing")) end)
      assert {:ok, %{spec: @spec_map}} = SourceStore.get(k2, "acme", "billing")
      assert Redis.rewrap(k2) == {:ok, %{rewrapped: 1, current: 1}}
      assert Redis.rewrap(k2) == {:ok, %{rewrapped: 0, current: 2}}

      # k1 can be dropped: a node that has only k2 boots and reads everything.
      k2_only = start_node([], secrets: [kek: "k2:" <> @kek_b])
      assert {:ok, %{spec: @spec_map}} = SourceStore.get(k2_only, "acme", "billing")
      assert {:ok, %{spec: @log_spec}} = SourceStore.get(k2_only, "acme", "open")
    end

    @tag :capture_log
    test "a rewrap tells the other nodes the version moved", %{conn: conn} do
      plain = start_node()
      assert {:ok, _} = SourceStore.put(plain, "acme", "billing", @spec_map, :create)
      {:ok, before} = Redix.command(conn, ["GET", @namespace <> ":version"])

      keyed = start_node([], secrets: [kek: "k1:" <> @kek_a])
      eventually(fn -> match?({:ok, _}, SourceStore.get(keyed, "acme", "billing")) end)
      assert {:ok, %{rewrapped: 1}} = Redis.rewrap(keyed)

      assert {:ok, after_rewrap} = Redix.command(conn, ["GET", @namespace <> ":version"])
      assert String.to_integer(after_rewrap) > String.to_integer(before)

      # a rewrap that wrote nothing leaves the version alone
      assert {:ok, %{rewrapped: 0}} = Redis.rewrap(keyed)
      assert {:ok, ^after_rewrap} = Redix.command(conn, ["GET", @namespace <> ":version"])
    end
  end

  # Called through `secrets.kek_provider`.
  def failing_provider, do: {:error, :kms_down}

  # A TCP forwarder in front of the compose Redis, as in the route store's
  # suite: killing it makes Redis unreachable for the one node pointed at it,
  # and listening again on the same port brings it back.
  defp start_forwarder(port \\ 0) do
    parent = self()

    pid =
      spawn(fn ->
        {:ok, listen} =
          :gen_tcp.listen(port, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])

        {:ok, bound} = :inet.port(listen)
        send(parent, {:forwarder, self(), bound})
        forward_loop(listen)
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)

    receive do
      {:forwarder, ^pid, bound} -> {pid, bound}
    after
      2_000 -> flunk("the forwarder did not start")
    end
  end

  defp forward_loop(listen) do
    {:ok, client} = :gen_tcp.accept(listen)
    %URI{host: host, port: port} = URI.parse(@url)
    {:ok, upstream} = :gen_tcp.connect(String.to_charlist(host), port, [:binary, active: false])

    for {from, to} <- [{client, upstream}, {upstream, client}] do
      spawn_link(fn -> pipe(from, to) end)
    end

    forward_loop(listen)
  end

  defp pipe(from, to) do
    with {:ok, data} <- :gen_tcp.recv(from, 0),
         :ok <- :gen_tcp.send(to, data) do
      pipe(from, to)
    else
      _closed ->
        :gen_tcp.close(from)
        :gen_tcp.close(to)
    end
  end
end
