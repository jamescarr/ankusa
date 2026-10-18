defmodule Ankusa.InstanceTest do
  @moduledoc """
  The instance's failure domains: an optional subtree that keeps crashing is
  restarted with backoff and never takes the edge listener down, and crash
  reports do not print configuration.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Edge.{Batcher, Ingest}
  alias Ankusa.Instance.Isolated

  @canary "s3cr3t-canary"

  defmodule CaptureSink do
    @moduledoc false
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, _ctx, opts) do
      send(Keyword.fetch!(opts, :to), {:delivered, env.id})
      :ok
    end
  end

  # A subtree child that refuses to start while the `Agent` flag says `:fail`.
  defmodule FlakyChild do
    @moduledoc false

    def child_spec(flag),
      do: %{id: __MODULE__, start: {__MODULE__, :start_link, [flag]}}

    def start_link(flag) do
      case Agent.get(flag, & &1) do
        :fail -> {:error, :nope}
        :ok -> Agent.start_link(fn -> :up end)
      end
    end
  end

  @doc false
  def handle_event(event, measurements, metadata, test_pid),
    do: send(test_pid, {:telemetry, event, measurements, metadata})

  defp attach_subtree_events(inst) do
    id = {__MODULE__, inst}

    :telemetry.attach_many(
      id,
      [[:ankusa, :instance, :subtree_down], [:ankusa, :instance, :subtree_up]],
      &__MODULE__.handle_event/4,
      self()
    )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  defp eventually(fun, tries \\ 300) do
    cond do
      fun.() ->
        :ok

      tries == 0 ->
        flunk("condition never became true")

      true ->
        Process.sleep(10)
        eventually(fun, tries - 1)
    end
  end

  # Kill `pid_fun.()` over and over until the subtree gives up (its budget is 3
  # restarts in 5 s, so four kills). After each kill wait for one of the two
  # outcomes: a new process (kill again) or the subtree gone. `nil` in between,
  # while the supervisor is restarting or the manager has not yet handled its
  # supervisor's exit, is neither.
  defp kill_until_down(inst, domain, pid_fun, kills \\ 10)

  defp kill_until_down(_inst, domain, _pid_fun, 0),
    do: flunk("#{domain} subtree never exhausted its restart budget")

  defp kill_until_down(inst, domain, pid_fun, kills) do
    pid = pid_fun.()
    assert is_pid(pid)
    Process.exit(pid, :kill)

    eventually(fn ->
      Isolated.subtree(inst, domain) == nil or replaced?(pid_fun.(), pid)
    end)

    if Isolated.subtree(inst, domain) != nil,
      do: kill_until_down(inst, domain, pid_fun, kills - 1)
  end

  defp replaced?(new, old), do: is_pid(new) and new != old

  @tag :capture_log
  test "a dispatch subtree that exhausts its restart budget leaves the edge up and comes back" do
    config =
      test_config(
        roles: [:edge, :dispatch],
        source_store:
          {Ankusa.SourceStore.Static,
           sources: %{"demo" => [sinks: [{CaptureSink, [to: self()]}]]}}
      )

    inst = config.instance
    attach_subtree_events(inst)
    start_supervised!({Ankusa.Instance, config})

    pids = fn ->
      Enum.map([:edge, {:batcher, 0}, :store, :instance], &Ankusa.whereis(inst, &1))
    end

    before = pids.()
    assert Enum.all?(before, &is_pid/1)

    kill_until_down(inst, :dispatch, fn -> Ankusa.whereis(inst, :dispatch) end)

    assert_receive {:telemetry, [:ankusa, :instance, :subtree_down], %{delay_ms: 1000},
                    %{instance: ^inst, domain: :dispatch}}

    # Dispatch is gone for now; nothing the edge needs went with it.
    assert pids.() == before

    assert {:ok, env} = Ingest.ingest(inst, request("demo", "x"))
    id = env.id

    # ...and the hook is delivered once the subtree is restarted.
    assert_receive {:telemetry, [:ankusa, :instance, :subtree_up], _, %{domain: :dispatch}}, 5_000
    assert_receive {:delivered, ^id}, 5_000
    assert pids.() == before
  end

  @tag :capture_log
  test "a restart that fails is retried with a longer delay, and the manager stays up" do
    inst = unique_instance()
    attach_subtree_events(inst)
    flag = start_supervised!({Agent, fn -> :ok end})

    manager =
      start_supervised!(
        {Isolated,
         instance: inst,
         domain: :flaky,
         children: [{FlakyChild, flag}],
         base_backoff_ms: 50,
         max_backoff_ms: 200}
      )

    # While the flag says `:fail` every restart attempt is refused, so one kill
    # spends the subtree's budget, and every retry after it fails too.
    Agent.update(flag, fn _ -> :fail end)
    sup = Isolated.subtree(inst, :flaky)
    [{_id, child, _type, _modules}] = Supervisor.which_children(sup)
    Process.exit(child, :kill)

    assert_receive {:telemetry, [:ankusa, :instance, :subtree_down], %{delay_ms: 50},
                    %{domain: :flaky}},
                   2_000

    assert_receive {:telemetry, [:ankusa, :instance, :subtree_down], %{delay_ms: 100},
                    %{domain: :flaky}},
                   2_000

    assert Isolated.subtree(inst, :flaky) == nil
    assert Ankusa.whereis(inst, {:isolated, :flaky}) == manager

    Agent.update(flag, fn _ -> :ok end)
    assert_receive {:telemetry, [:ankusa, :instance, :subtree_up], _, %{domain: :flaky}}, 2_000

    assert is_pid(Isolated.subtree(inst, :flaky))
    assert Ankusa.whereis(inst, {:isolated, :flaky}) == manager
  end

  @tag :capture_log
  test "a Registry partition crash rebuilds the instance with every process registered again" do
    config =
      test_config(
        roles: [:edge],
        source_store:
          {Ankusa.SourceStore.Static, sources: %{"demo" => [sinks: [{Ankusa.Sink.Log, []}]]}}
      )

    inst = config.instance
    start_supervised!({Ankusa.Instance, config})

    keys = [:instance, :store, :edge, :batcher_sup, {:batcher, 0}, :queue_writer]
    snapshot = fn -> Map.new(keys, &{&1, Ankusa.whereis(inst, &1)}) end
    before = snapshot.()
    assert Enum.all?(before, fn {_key, pid} -> is_pid(pid) end)

    # The Registry's partition owns every registration. The store and the
    # supervisors trap exits, so they outlive it unregistered unless the
    # instance is rebuilt.
    [{_id, partition, _type, _modules} | _] = Supervisor.which_children(Ankusa.Registry)
    Process.exit(partition, :kill)

    eventually(fn ->
      # `lookup/2` raises while the partition's tables are being recreated.
      try do
        now = snapshot.()
        Enum.all?(now, fn {_key, pid} -> is_pid(pid) end) and now.store != before.store
      rescue
        ArgumentError -> false
      end
    end)

    assert {:ok, _env} = Ingest.ingest(inst, request("demo", "x"))
  end

  @tag :capture_log
  test "a crash while handling a hook or a source write does not log the sinks it carried" do
    sink = {Ankusa.Sink.Log, [token: @canary]}

    config =
      test_config(
        roles: [:edge],
        batcher: %{partitions: 1},
        source_store: {Ankusa.SourceStore.Persistent, sources: %{}}
      )

    inst = config.instance
    start_supervised!({Ankusa.Instance, config})

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        # Break each process's state so the next call it handles raises; the
        # crash report then prints that call as its "Last message".
        batcher = Ankusa.whereis(inst, {:batcher, 0})
        :sys.replace_state(batcher, &%{&1 | count: :broken})
        catch_exit(Batcher.commit(inst, 0, %{envelope: nil, sinks: [sink]}))

        store = Ankusa.whereis(inst, :source_store)
        :sys.replace_state(store, &%{&1 | table: :no_such_table})
        spec = %{"sinks" => [%{"type" => "log", "token" => @canary}]}
        catch_exit(Ankusa.SourceStore.put(inst, "acme", "billing", spec, :create))

        # Both reports are logged before the processes exit; give the handler
        # a moment to write them.
        Process.sleep(200)
      end)

    assert log =~ "Ankusa.Edge.Batcher"
    assert log =~ "Ankusa.SourceStore.Persistent"
    refute log =~ @canary
  end

  test "no process the instance runs prints configuration in its status" do
    sink = {Ankusa.Sink.Log, [token: @canary]}

    config =
      test_config(
        roles: [:edge, :dispatch, :storage],
        claim_check: %{retention_days: 7},
        backup: %{enabled: true},
        lifecycle: %{sinks: [sink]},
        source_store: {Ankusa.SourceStore.Persistent, sources: %{"demo" => [sinks: [sink]]}}
      )

    inst = config.instance
    start_supervised!({Ankusa.Instance, config})

    printed = fn key ->
      inst
      |> Ankusa.whereis(key)
      |> :sys.get_status()
      |> inspect(limit: :infinity, printable_limit: :infinity)
    end

    # The canary is in the state to begin with: the redaction is what hides it.
    raw = inst |> Ankusa.whereis(:dispatch) |> :sys.get_state() |> inspect(limit: :infinity)
    assert raw =~ @canary

    for key <- [
          :dispatch,
          :compactor,
          :claim_check_sweeper,
          :rate_limiter,
          :lifecycle,
          :source_store,
          :replayer,
          :backup,
          {:isolated, :dispatch},
          {:isolated, :backup}
        ] do
      refute printed.(key) =~ @canary, "#{inspect(key)} printed the config in its status"
    end
  end

  test "no supervisor of the instance, nor the instance's parent, prints configuration" do
    sink = {Ankusa.Sink.Log, [token: @canary]}

    config =
      test_config(
        roles: [:edge, :dispatch, :storage],
        claim_check: %{retention_days: 7},
        backup: %{enabled: true},
        lifecycle: %{sinks: [sink]},
        source_store: {Ankusa.SourceStore.Persistent, sources: %{"demo" => [sinks: [sink]]}}
      )

    inst = config.instance

    parent =
      start_supervised!(%{
        id: :instance_parent,
        type: :supervisor,
        start: {Supervisor, :start_link, [[{Ankusa.Instance, config}], [strategy: :one_for_one]]}
      })

    printed = fn pid ->
      pid |> :sys.get_status() |> inspect(limit: :infinity, printable_limit: :infinity)
    end

    supervisors =
      [parent | Enum.map([:instance, :edge, :batcher_sup], &Ankusa.whereis(inst, &1))] ++
        Enum.map([:dispatch, :storage, :backup, :lifecycle], &Isolated.subtree(inst, &1))

    for sup <- supervisors do
      assert is_pid(sup)
      refute printed.(sup) =~ @canary, "#{inspect(sup)} printed the config in its status"
    end
  end

  test "a configured source whose HMAC secret is empty refuses to boot, naming the source" do
    config =
      test_config(
        roles: [:edge],
        source_store:
          {Ankusa.SourceStore.Static,
           sources: %{"gh" => [verifier: {Ankusa.Verifier.Hmac, scheme: :github, secret: ""}]}}
      )

    Process.flag(:trap_exit, true)

    assert {:error, {%ArgumentError{message: message}, _stack}} =
             Ankusa.Instance.start_link(config)

    assert message == "source gh: verifier secret is missing or undecodable"
  end

  describe "listeners" do
    @tls_dir Path.expand("../fixtures/tls", __DIR__)

    defp fixture(name), do: Path.join(@tls_dir, name)

    # One HTTPS GET over `:ssl`, verifying the server against the fixture CA and
    # presenting `client` (`{certfile, keyfile}`) when given. A refused client
    # certificate shows up either as a failed handshake or, under TLS 1.3, as an
    # error on the first read, so both are `{:error, _}`.
    defp tls_get(port, path, client) do
      opts =
        [
          :binary,
          active: false,
          verify: :verify_peer,
          cacertfile: fixture("ca.pem"),
          server_name_indication: ~c"localhost"
        ] ++
          case client do
            nil -> []
            {cert, key} -> [certfile: cert, keyfile: key]
          end

      case :ssl.connect(~c"127.0.0.1", port, opts, 5_000) do
        {:ok, sock} ->
          result =
            with :ok <- :ssl.send(sock, "GET #{path} HTTP/1.1\r\nHost: localhost\r\n\r\n"),
                 do: :ssl.recv(sock, 0, 5_000)

          :ssl.close(sock)
          result

        {:error, _} = error ->
          error
      end
    end

    defp http_get(port, path) do
      Req.get!("http://127.0.0.1:#{port}#{path}", retry: false, decode_body: false)
    end

    defp listening?(port) do
      case :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false]) do
        {:ok, sock} ->
          :gen_tcp.close(sock)
          true

        {:error, _} ->
          false
      end
    end

    @tag :capture_log
    test "tls listener requires client cert" do
      port = free_port()

      config =
        test_config(
          roles: [:edge],
          port: port,
          listener: %{
            tls: [
              certfile: fixture("server.pem"),
              keyfile: fixture("server-key.pem"),
              cacertfile: fixture("ca.pem"),
              verify: :verify_peer
            ]
          }
        )

      start_supervised!({Ankusa.Instance, config})
      eventually(fn -> listening?(port) end)

      # The pinned client: served.
      assert {:ok, "HTTP/1.1 200" <> _} =
               tls_get(port, "/health", {fixture("client.pem"), fixture("client-key.pem")})

      # No certificate at all: refused.
      assert {:error, _} = tls_get(port, "/health", nil)

      # A certificate some other CA signed: refused.
      assert {:error, _} =
               tls_get(port, "/health", {fixture("stranger.pem"), fixture("stranger-key.pem")})

      # And the listener is TLS only: a plain-HTTP client gets no answer.
      assert {:error, _} = Req.get("http://127.0.0.1:#{port}/health", retry: false)
    end

    @tag :capture_log
    test "mtls principal: the client certificate's identity decides what the admin API allows" do
      port = free_port()
      other = free_port()

      admin = fn port, subject_map ->
        %{
          enabled: true,
          port: port,
          tls: [
            certfile: fixture("server.pem"),
            keyfile: fixture("server-key.pem"),
            cacertfile: fixture("ca.pem"),
            verify: :verify_peer
          ],
          auth: %{mode: :mtls, mtls: %{subject_map: subject_map}}
        }
      end

      config =
        test_config(
          roles: [:edge],
          admin: admin.(port, %{"cn:ankusa-test-client" => ["config:read"]})
        )

      start_supervised!({Ankusa.Instance, config}, id: :mapped)
      eventually(fn -> listening?(port) end)

      client = {fixture("client.pem"), fixture("client-key.pem")}
      stranger = {fixture("stranger.pem"), fixture("stranger-key.pem")}

      # The mapped certificate is a principal holding config:read and nothing else.
      assert {:ok, "HTTP/1.1 200" <> _} = tls_get(port, "/v1/config", client)
      assert {:ok, "HTTP/1.1 403" <> forbidden} = tls_get(port, "/v1/dlq", client)
      assert forbidden =~ ~s("scope":"dlq:read")
      assert {:ok, "HTTP/1.1 200" <> _} = tls_get(port, "/health", client)

      # No certificate, or one another CA signed, never gets as far as a request.
      assert {:error, _} = tls_get(port, "/v1/config", nil)
      assert {:error, _} = tls_get(port, "/v1/config", stranger)

      # A certificate the CA vouches for but nobody mapped is 401, not a principal.
      unmapped =
        test_config(roles: [:edge], admin: admin.(other, %{"cn:someone-else" => ["config:read"]}))

      start_supervised!({Ankusa.Instance, unmapped}, id: :unmapped)
      eventually(fn -> listening?(other) end)

      assert {:ok, "HTTP/1.1 401" <> _} = tls_get(other, "/v1/config", client)
      assert {:ok, "HTTP/1.1 200" <> _} = tls_get(other, "/health", client)
    end

    @tag :capture_log
    test "a tls listener without verify_peer serves a client that has no certificate" do
      port = free_port()

      config =
        test_config(
          roles: [:edge],
          port: port,
          listener: %{
            tls: [certfile: fixture("server.pem"), keyfile: fixture("server-key.pem")]
          }
        )

      start_supervised!({Ankusa.Instance, config})
      eventually(fn -> listening?(port) end)

      assert {:ok, "HTTP/1.1 200" <> _} = tls_get(port, "/health", nil)
    end

    @tag :capture_log
    test "admin, route-management, claim-check and metrics listeners take their own tls block" do
      [admin, routes, claim, metrics] = Enum.map(1..4, fn _ -> free_port() end)
      tls = [certfile: fixture("server.pem"), keyfile: fixture("server-key.pem")]

      config =
        test_config(
          roles: [:edge, :claim_check],
          admin: %{enabled: true, port: admin, tls: tls},
          routes: %{enabled: true, admin: %{port: routes, tls: tls}},
          claim_check: %{port: claim, tls: tls},
          metrics: %{enabled: true, port: metrics, tls: tls}
        )

      start_supervised!({Ankusa.Instance, config})

      for {port, path} <- [
            {admin, "/health"},
            {routes, "/health"},
            {claim, "/health"},
            {metrics, "/health"}
          ] do
        eventually(fn -> listening?(port) end)
        assert {:ok, "HTTP/1.1 " <> _} = tls_get(port, path, nil), "port #{port} is not TLS"
      end
    end

    test "metrics listener serves only metrics" do
      port = free_port()
      config = test_config(roles: [:edge], metrics: %{enabled: true, port: port})
      start_supervised!({Ankusa.Instance, config})
      eventually(fn -> listening?(port) end)

      metrics = http_get(port, "/metrics")
      assert metrics.status == 200
      assert [content_type] = Req.Response.get_header(metrics, "content-type")
      assert content_type =~ "text/plain"

      assert http_get(port, "/health").status == 200
      assert http_get(port, "/ready").status == 200

      # None of the operator API is here.
      for path <- ["/v1/config", "/v1/dlq", "/v1/wal", "/v1/quarantine", "/asyncapi.json", "/"] do
        assert http_get(port, path).status == 404, "#{path} is served by the metrics listener"
      end

      assert Req.post!("http://127.0.0.1:#{port}/v1/replays", retry: false, body: "{}").status ==
               404

      assert Req.delete!("http://127.0.0.1:#{port}/v1/quarantine", retry: false).status == 404
    end

    test "the admin listener keeps serving /metrics when the metrics listener is on" do
      [admin, metrics] = [free_port(), free_port()]

      config =
        test_config(
          roles: [:edge],
          admin: %{enabled: true, port: admin},
          metrics: %{enabled: true, port: metrics}
        )

      start_supervised!({Ankusa.Instance, config})
      eventually(fn -> listening?(admin) and listening?(metrics) end)

      assert http_get(admin, "/metrics").status == 200
      assert http_get(admin, "/v1/config").status == 200
      assert http_get(metrics, "/metrics").status == 200
      assert http_get(metrics, "/v1/config").status == 404
    end
  end
end
