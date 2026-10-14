defmodule Ankusa.AuditTest do
  @moduledoc """
  The audit log end to end: what is recorded (mutations and denials, never
  reads), that an unrecordable mutation does not happen, how the log is read,
  shipped to sinks at least once and in order, and pruned without outrunning a
  lagging sink.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Admin.Router
  alias Ankusa.{Audit, Store}
  alias Ankusa.Store.Keys
  alias Ankusa.TestHelpers.TokenTable

  @day 86_400_000
  @scim_token String.duplicate("s", 40)

  @tokens %{
    "admin" => %{"sub" => "u-admin", "preferred_username" => "ada", "groups" => ["Admins"]},
    "acme" => %{"sub" => "u-acme", "groups" => ["AcmeAdmins"]},
    "viewer" => %{"sub" => "u-viewer", "groups" => ["Viewers"]}
  }

  @bindings [
    %{group: "Admins", role: :admin},
    %{group: "AcmeAdmins", role: :admin, tenants: ["acme"]},
    %{group: "Viewers", role: :viewer}
  ]

  # Delivers to the test pid unless its gate still holds failures to hand out.
  defmodule CollectSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, _ctx, opts) do
      failing? =
        Agent.get_and_update(Keyword.fetch!(opts, :gate), fn
          n when n > 0 -> {true, n - 1}
          n -> {false, n}
        end)

      if failing? do
        {:error, :gated}
      else
        send(Keyword.fetch!(opts, :pid), {:shipped, env})
        :ok
      end
    end
  end

  def decoder(_source_id, %{"sinks" => [_ | _] = sinks}),
    do: [sinks: Enum.map(sinks, fn %{"type" => "log"} -> {Ankusa.Sink.Log, []} end)]

  def decoder(_source_id, _spec), do: raise(ArgumentError, "sinks must be a non-empty list")

  def forward_event(_event, _measurements, meta, pid), do: send(pid, {:ship_failed, meta})

  defp start(overrides \\ []) do
    config =
      test_config(
        Keyword.merge(
          [
            roles: [:edge, :dispatch],
            admin: %{enabled: true, port: 0},
            source_store: {Ankusa.SourceStore.Persistent, decoder: &__MODULE__.decoder/2},
            auth: %{
              enabled: true,
              verifier: {TokenTable, tokens: @tokens},
              bindings: @bindings
            },
            audit: %{enabled: true, retention_days: 365, sinks: []}
          ],
          overrides
        )
      )

    start_supervised!({Ankusa.Instance, config})
    config
  end

  defp admin(config, method, path, opts) do
    conn = Plug.Test.conn(method, path, Keyword.get(opts, :body, ""))

    conn =
      Enum.reduce(Keyword.get(opts, :headers, []), conn, fn {k, v}, c ->
        Plug.Conn.put_req_header(c, k, v)
      end)

    conn =
      case Keyword.get(opts, :token) do
        nil -> conn
        token -> Plug.Conn.put_req_header(conn, "authorization", "Bearer " <> token)
      end

    Router.call(conn, Router.init(instance: config.instance))
  end

  defp json(conn), do: JSON.decode!(conn.resp_body)

  defp source_body(name), do: JSON.encode!(%{"name" => name, "sinks" => [%{"type" => "log"}]})

  defp create_source(config, tenant, name, token \\ "admin") do
    admin(config, :post, "/v1/tenants/#{tenant}/sources", token: token, body: source_body(name))
  end

  # Newest first.
  defp entries(config, filters \\ %{}) do
    {:ok, %{entries: entries}} = Audit.list(config.instance, Map.put_new(filters, :limit, 1000))
    entries
  end

  describe "what is recorded" do
    setup do
      %{config: start()}
    end

    test "a mutation leaves exactly one finished entry naming who did what to what", %{
      config: config
    } do
      conn =
        admin(config, :post, "/v1/tenants/acme/sources",
          token: "admin",
          body: source_body("stripe"),
          headers: [{"x-request-id", "req-1"}]
        )

      assert conn.status == 201

      assert [entry] = entries(config)

      assert %{
               "outcome" => "success",
               "status" => 201,
               "action" => "source.create",
               "target" => "acme.stripe",
               "tenant" => "acme",
               "method" => "POST",
               "path" => "/v1/tenants/acme/sources",
               "listener" => "admin",
               "request_id" => "req-1",
               "actor" => %{
                 "type" => "user",
                 "subject" => "u-admin",
                 "username" => "ada",
                 "ip" => "127.0.0.1"
               }
             } = entry

      assert entry["instance"] == to_string(config.instance)
      assert {:ok, _, 0} = DateTime.from_iso8601(entry["time"])
      assert is_integer(entry["at"])
    end

    test "request bodies are never recorded", %{config: config} do
      create_source(config, "acme", "secretive")
      refute JSON.encode!(entries(config)) =~ "sinks"
    end

    test "the entity a replay job creates is its target", %{config: config} do
      conn = admin(config, :post, "/v1/replays", token: "admin", body: ~s({"kind":"dlq"}))
      assert conn.status == 202

      assert [%{"action" => "replay.create", "target" => target}] = entries(config)
      assert target == json(conn)["id"]
    end

    test "reads, health and readiness are not recorded", %{config: config} do
      for path <- ["/v1/config", "/v1/dlq", "/v1/tenants/acme/sources", "/health", "/ready"] do
        admin(config, :get, path, token: "admin")
      end

      assert entries(config) == []
    end

    test "denials are recorded, with whoever was refused", %{config: config} do
      assert create_source(config, "acme", "a", nil).status == 401
      assert create_source(config, "acme", "a", "forged").status == 401
      assert create_source(config, "acme", "a", "viewer").status == 403
      assert admin(config, :get, "/v1/audit", token: "viewer").status == 403

      rows = entries(config)
      assert length(rows) == 4
      assert Enum.all?(rows, &(&1["outcome"] == "denied"))

      assert [
               %{
                 "action" => "audit.list",
                 "status" => 403,
                 "actor" => %{"subject" => "u-viewer"}
               },
               %{"status" => 403, "actor" => %{"subject" => "u-viewer"}},
               %{"status" => 401, "actor" => %{"type" => "anonymous", "subject" => nil}},
               %{"status" => 401, "actor" => %{"type" => "anonymous", "ip" => "127.0.0.1"}}
             ] = rows

      # nothing was created
      assert Ankusa.SourceStore.list_tenant(config.instance, "acme") == []
    end

    test "a handler that refuses is a failure, not a success", %{config: config} do
      conn = admin(config, :post, "/v1/tenants/acme/sources", token: "admin", body: "{}")
      assert conn.status == 400

      assert [%{"outcome" => "failure", "status" => 400, "action" => "source.create"}] =
               entries(config)
    end

    test "a handler that crashes is an error entry, and the response is still sent", %{
      config: config
    } do
      # the limiter is gone: setting a limit exits
      edge = Ankusa.via(config.instance, :edge)
      :ok = Supervisor.terminate_child(edge, {Ankusa.Edge.RateLimiter, config.instance})

      try do
        admin(config, :put, "/v1/tenants/acme/rate-limit",
          token: "admin",
          body: ~s({"rate": 5, "burst": 5})
        )

        flunk("the crash was swallowed")
      catch
        _kind, _reason -> :ok
      end

      assert [
               %{
                 "outcome" => "error",
                 "status" => 500,
                 "action" => "rate_limit.put",
                 "tenant" => "acme"
               }
             ] = entries(config)
    end

    test "with auth off the actor is anonymous but the mutation is still recorded" do
      config = start(auth: %{enabled: false})

      assert admin(config, :post, "/v1/tenants/acme/sources", body: source_body("a")).status ==
               201

      assert [%{"outcome" => "success", "actor" => %{"type" => "anonymous"}}] = entries(config)
    end
  end

  describe "fail closed" do
    test "a mutation whose record cannot be written does not happen" do
      config = start()
      store = {Ankusa.Store, config.instance}
      :ok = Supervisor.terminate_child(Ankusa.via(config.instance, :instance), store)

      conn = create_source(config, "acme", "stripe")

      assert conn.status == 503
      assert json(conn) == %{"error" => "audit_unavailable"}
      assert Plug.Conn.get_resp_header(conn, "retry-after") == ["1"]

      {:ok, _} = Supervisor.restart_child(Ankusa.via(config.instance, :instance), store)
      assert Ankusa.SourceStore.get(config.instance, "acme", "stripe") == :error
      assert entries(config) == []
    end

    test "reads do not depend on the audit store" do
      config = start()
      store = {Ankusa.Store, config.instance}
      :ok = Supervisor.terminate_child(Ankusa.via(config.instance, :instance), store)

      assert admin(config, :get, "/v1/config", token: "admin").status == 200
    end
  end

  describe "reading it" do
    setup do
      config = start()

      for name <- ~w(a b c d e) do
        assert create_source(config, "acme", name).status == 201
        Process.sleep(2)
      end

      assert create_source(config, "globex", "g").status == 201
      assert create_source(config, "acme", "x", "viewer").status == 403
      %{config: config}
    end

    defp page(config, query, token \\ "admin") do
      conn = admin(config, :get, "/v1/audit?" <> query, token: token)
      assert conn.status == 200, conn.resp_body
      json(conn)
    end

    test "pages walk every entry newest first, once", %{config: config} do
      expected = config |> entries() |> Enum.map(& &1["id"])
      assert length(expected) == 7

      first = page(config, "limit=2")
      assert length(first["entries"]) == 2
      assert is_binary(first["next_cursor"])

      walked =
        Stream.unfold(first, fn
          nil -> nil
          page -> {page["entries"], next_page(config, page)}
        end)
        |> Enum.flat_map(& &1)
        |> Enum.map(& &1["id"])

      assert walked == expected
    end

    defp next_page(_config, %{"next_cursor" => nil}), do: nil

    defp next_page(config, %{"next_cursor" => cursor}),
      do: page(config, "limit=2&cursor=#{cursor}")

    test "the last page has no cursor", %{config: config} do
      assert %{"next_cursor" => nil, "entries" => entries} = page(config, "limit=1000")
      assert length(entries) == 7
    end

    test "filters", %{config: config} do
      assert length(page(config, "outcome=denied")["entries"]) == 1
      assert length(page(config, "outcome=success")["entries"]) == 6
      assert length(page(config, "action=source.create")["entries"]) == 7
      assert length(page(config, "actor=ada")["entries"]) == 6
      assert length(page(config, "actor=u-viewer")["entries"]) == 1
      assert length(page(config, "tenant=globex")["entries"]) == 1

      all = entries(config)
      third = Enum.at(all, 3)["at"]
      newer = page(config, "since=#{third}")["entries"]
      assert Enum.all?(newer, &(&1["at"] >= third))
      assert length(newer) == 4

      older = page(config, "until=#{third}")["entries"]
      assert Enum.all?(older, &(&1["at"] <= third))
    end

    test "a tenant's trail holds only that tenant's entries", %{config: config} do
      conn = admin(config, :get, "/v1/tenants/globex/audit", token: "admin")
      assert [%{"target" => "globex.g"}] = json(conn)["entries"]

      conn = admin(config, :get, "/v1/tenants/acme/audit", token: "admin")
      assert length(json(conn)["entries"]) == 6
      assert Enum.all?(json(conn)["entries"], &(&1["tenant"] == "acme"))
    end

    test "a tenant admin reads its own tenant's trail and nobody else's", %{config: config} do
      assert admin(config, :get, "/v1/tenants/acme/audit", token: "acme").status == 200
      assert admin(config, :get, "/v1/tenants/globex/audit", token: "acme").status == 403
      assert admin(config, :get, "/v1/audit", token: "acme").status == 403
    end

    test "bad parameters are an invalid_filter naming the field", %{config: config} do
      for {query, field} <- [
            {"limit=abc", "limit"},
            {"since=-1", "since"},
            {"outcome=bogus", "outcome"},
            {"cursor=not-a-cursor", "cursor"},
            {"since=10&until=5", "until"},
            {"tenant=bad.tenant", "tenant"}
          ] do
        conn = admin(config, :get, "/v1/audit?" <> query, token: "admin")
        assert conn.status == 400, query
        assert json(conn) == %{"error" => "invalid_filter", "field" => field}
      end
    end
  end

  test "the log is 409 when audit is off" do
    config = start(audit: %{enabled: false})
    conn = admin(config, :get, "/v1/audit", token: "admin")
    assert conn.status == 409
    assert json(conn) == %{"error" => "audit_disabled"}
    assert admin(config, :get, "/v1/tenants/acme/audit", token: "admin").status == 409
  end

  test "with audit off nothing is written" do
    config = start(audit: %{enabled: false})
    assert create_source(config, "acme", "a").status == 201
    assert {:ok, %{entries: []}} = Audit.list(config.instance)
  end

  test "the SCIM token never appears in the config the API serves" do
    config =
      start(
        scim: %{enabled: true, token: @scim_token},
        # with SCIM on, a user token must match a provisioned user; a subject
        # binding needs none
        auth: %{
          enabled: true,
          verifier: {TokenTable, tokens: @tokens},
          bindings: [%{subject: "u-admin", role: :admin}]
        }
      )

    conn = admin(config, :get, "/v1/config", token: "admin")

    assert conn.status == 200
    refute conn.resp_body =~ @scim_token
    assert json(conn)["scim"]["token"] == "[REDACTED]"
  end

  describe "shipping" do
    setup do
      {:ok, gate} = Agent.start_link(fn -> 3 end)
      test_pid = self()

      config =
        start(
          audit: %{
            enabled: true,
            retention_days: nil,
            sinks: [{CollectSink, pid: test_pid, gate: gate}]
          },
          dispatch: %{
            retry: {Ankusa.RetryPolicy.Exponential, base_ms: 5, max_ms: 20, jitter: false}
          }
        )

      handler = "audit-ship-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:ankusa, :audit, :ship_failed],
        &__MODULE__.forward_event/4,
        test_pid
      )

      on_exit(fn -> :telemetry.detach(handler) end)
      %{config: config, gate: gate}
    end

    defp tick(config) do
      wait_until(fn -> Ankusa.whereis(config.instance, :audit_shipper) end)
      |> send(:tick)
    end

    defp wait_until(fun, attempts \\ 100) do
      case fun.() do
        nil when attempts > 0 ->
          Process.sleep(20)
          wait_until(fun, attempts - 1)

        value ->
          value
      end
    end

    defp receive_shipped(count) do
      for _ <- 1..count do
        assert_receive {:shipped, env}, 3_000
        env
      end
    end

    test "a sink that fails three times still receives every entry, in order, none skipped", %{
      config: config,
      gate: gate
    } do
      for name <- ~w(a b c), do: assert(create_source(config, "acme", name).status == 201)
      expected = config |> entries() |> Enum.reverse() |> Enum.map(& &1["id"])
      assert length(expected) == 3

      tick(config)
      received = receive_shipped(3)

      assert Enum.map(received, &JSON.decode!(&1.body)["id"]) == expected
      assert Enum.map(received, & &1.id) == expected
      assert Agent.get(gate, & &1) == 0

      assert_receive {:ship_failed, %{sink: sink}}
      assert sink == inspect(CollectSink)
      assert_receive {:ship_failed, _}
      assert_receive {:ship_failed, _}

      [first | _] = received
      assert first.source_id == "ankusa:audit"
      assert first.content_type == "application/cloudevents+json"
      assert first.tenant_id == "acme"

      assert %{
               "specversion" => "1.0",
               "type" => "io.ankusa.audit.source.create",
               "subject" => "acme.a",
               "source" => source,
               "data" => %{"action" => "source.create", "outcome" => "success"}
             } = JSON.decode!(first.body)

      assert source == "urn:ankusa:instance:#{config.instance}"
    end

    test "a restart resumes from the cursor: nothing is shipped twice", %{
      config: config,
      gate: gate
    } do
      Agent.update(gate, fn _ -> 0 end)

      create_source(config, "acme", "a")
      tick(config)
      [shipped] = receive_shipped(1)
      assert shipped.tenant_id == "acme"

      # the cursor is written after the delivery returns: restarting before it
      # is on disk would (correctly) deliver the entry again
      wait_until(fn ->
        case Store.get(config.instance, :default, Keys.meta("audit_cursor:0")) do
          {:ok, _cursor} -> :saved
          _ -> nil
        end
      end)

      sup = Ankusa.via(config.instance, :instance)
      child = {Ankusa.Instance.Isolated, :audit}
      :ok = Supervisor.terminate_child(sup, child)
      {:ok, _} = Supervisor.restart_child(sup, child)

      create_source(config, "acme", "b")
      wait_until(fn -> Ankusa.whereis(config.instance, :audit_shipper) end)
      tick(config)
      [next] = receive_shipped(1)

      assert JSON.decode!(next.body)["subject"] == "acme.b"
      refute_receive {:shipped, _}, 300
    end

    test "a mutation still running is not shipped until it has an outcome", %{
      config: config,
      gate: gate
    } do
      Agent.update(gate, fn _ -> 0 end)

      # a `pending` entry as `begin/1` writes it, 1 s old
      at = System.system_time(:millisecond) - 1_000
      id = Ankusa.UUIDv7.generate()

      pending = %{
        "id" => id,
        "at" => at,
        "time" => DateTime.to_iso8601(DateTime.utc_now()),
        "action" => "source.create",
        "outcome" => "pending",
        "status" => nil,
        "path" => "/v1/tenants/acme/sources",
        "tenant" => "acme",
        "target" => nil,
        "actor" => %{}
      }

      :ok =
        Store.write(
          config.instance,
          [{:put, :default, Keys.audit(at, 0, id), JSON.encode!(pending)}],
          sync: true
        )

      tick(config)
      refute_receive {:shipped, _}, 300

      # an old pending entry is a crashed request's last words: it ships as it is
      old = %{pending | "at" => at - 120_000, "id" => Ankusa.UUIDv7.generate()}

      :ok =
        Store.write(
          config.instance,
          [{:put, :default, Keys.audit(old["at"], 0, old["id"]), JSON.encode!(old)}],
          sync: true
        )

      # it sorts before the young one, and ships alone
      tick(config)
      assert [env] = receive_shipped(1)
      assert JSON.decode!(env.body)["data"]["id"] == old["id"]
      refute_receive {:shipped, _}, 300
    end
  end

  describe "pruning" do
    setup do
      {:ok, gate} = Agent.start_link(fn -> 1_000_000 end)
      sink = {CollectSink, pid: self(), gate: gate}

      config =
        start(audit: %{enabled: true, retention_days: 1, sinks: [sink]})

      %{config: config}
    end

    defp put_entry(config, at) do
      id = Ankusa.UUIDv7.generate()

      entry = %{
        "id" => id,
        "at" => at,
        "time" => at |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601(),
        "action" => "source.create",
        "outcome" => "success",
        "status" => 201,
        "tenant" => nil,
        "target" => nil,
        "actor" => %{}
      }

      key = Keys.audit(at, 0, id)
      :ok = Store.write(config.instance, [{:put, :default, key, JSON.encode!(entry)}], sync: true)
      {key, id}
    end

    defp set_cursor(config, key) do
      value =
        JSON.encode!(%{
          "module" => inspect(CollectSink),
          "key" => Base.url_encode64(key, padding: false)
        })

      :ok =
        Store.write(config.instance, [{:put, :default, Keys.meta("audit_cursor:0"), value}],
          sync: true
        )
    end

    test "old shipped entries go; old entries a sink has not received stay", %{config: config} do
      now = System.system_time(:millisecond)
      {_k1, id1} = put_entry(config, now - 3 * @day)
      {k2, _id2} = put_entry(config, now - 3 * @day + 1_000)
      {_k3, id3} = put_entry(config, now - 2 * @day)
      {_k4, id4} = put_entry(config, now)

      # the sink has everything up to the second entry: the third is past
      # retention but not delivered
      set_cursor(config, k2)
      assert :ok = Ankusa.Audit.Shipper.prune(config.instance)

      assert Enum.map(entries(config), & &1["id"]) == [id4, id3]
      refute id1 in Enum.map(entries(config), & &1["id"])
    end

    test "with no cursor for a sink nothing is pruned", %{config: config} do
      now = System.system_time(:millisecond)
      put_entry(config, now - 3 * @day)
      put_entry(config, now - 2 * @day)

      assert :ok = Ankusa.Audit.Shipper.prune(config.instance)
      assert length(entries(config)) == 2
    end

    test "a cursor written for a different sink module does not count", %{config: config} do
      now = System.system_time(:millisecond)
      {key, _} = put_entry(config, now - 3 * @day)

      value =
        JSON.encode!(%{
          "module" => "Some.Other.Sink",
          "key" => Base.url_encode64(key, padding: false)
        })

      :ok =
        Store.write(config.instance, [{:put, :default, Keys.meta("audit_cursor:0"), value}],
          sync: true
        )

      assert :ok = Ankusa.Audit.Shipper.prune(config.instance)
      assert length(entries(config)) == 1
    end
  end

  describe "pruning with no sinks" do
    test "entries past retention are deleted" do
      config = start(audit: %{enabled: true, retention_days: 1, sinks: []})
      now = System.system_time(:millisecond)

      old = Ankusa.UUIDv7.generate()
      fresh = Ankusa.UUIDv7.generate()

      for {at, id} <- [{now - 3 * @day, old}, {now, fresh}] do
        entry =
          JSON.encode!(%{
            "id" => id,
            "at" => at,
            "action" => "x",
            "outcome" => "success",
            "actor" => %{}
          })

        :ok =
          Store.write(config.instance, [{:put, :default, Keys.audit(at, 0, id), entry}],
            sync: true
          )
      end

      assert :ok = Ankusa.Audit.Shipper.prune(config.instance)
      assert Enum.map(entries(config), & &1["id"]) == [fresh]
    end

    test "retention_days: nil keeps everything" do
      config = start(audit: %{enabled: true, retention_days: nil, sinks: []})
      at = System.system_time(:millisecond) - 900 * @day
      id = Ankusa.UUIDv7.generate()

      entry =
        JSON.encode!(%{
          "id" => id,
          "at" => at,
          "action" => "x",
          "outcome" => "success",
          "actor" => %{}
        })

      :ok =
        Store.write(config.instance, [{:put, :default, Keys.audit(at, 0, id), entry}], sync: true)

      assert :ok = Ankusa.Audit.Shipper.prune(config.instance)
      assert length(entries(config)) == 1
    end
  end

  describe "configuration" do
    test "retention must be nil or at least a day" do
      for bad <- [0, -1, 1.5, "30"] do
        assert_raise ArgumentError, ~r/audit.retention_days/, fn ->
          Audit.validate_config!(Ankusa.Config.new(audit: %{retention_days: bad}))
        end
      end

      assert :ok = Audit.validate_config!(Ankusa.Config.new(audit: %{retention_days: nil}))
    end

    test "sinks must be {module, opts} pairs" do
      assert_raise ArgumentError, ~r/audit.sinks/, fn ->
        Audit.validate_config!(Ankusa.Config.new(audit: %{sinks: [:nope]}))
      end
    end

    test "the reserved source id cannot be a static source" do
      config =
        Ankusa.Config.new(
          source_store: {Ankusa.SourceStore.Static, sources: %{"ankusa:audit" => [sinks: []]}}
        )

      assert_raise ArgumentError, ~r/reserved for audit events/, fn ->
        Audit.validate_config!(config)
      end
    end
  end
end
