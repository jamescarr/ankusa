defmodule Ankusa.Admin.AuditTest do
  @moduledoc """
  The audit trail (`Ankusa.Audit`, SEC-2.a): every change an operator makes
  leaves exactly one row, written with the change, holding no secret, in commit
  order, readable through `GET /v1/audit`.

  The kill test runs `test/support/audit_harness.exs` in a child BEAM: it makes
  every kind of change through the routers and the parent `kill -9`s it on the
  response to the last one, then reopens the store alone.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Admin.Auth.Token
  alias Ankusa.Admin.Router, as: AdminRouter
  alias Ankusa.Audit
  alias Ankusa.Audit.Context
  alias Ankusa.AuditHarness
  alias Ankusa.Principal
  alias Ankusa.Routes.Router, as: RoutesRouter
  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @harness Path.expand("../../support/audit_harness.exs", __DIR__)

  @every_scope Principal.scopes()

  defp token(id, plain, scopes), do: %{id: id, sha256: Token.hash(plain), scopes: scopes}

  defp tokens do
    [
      token("alice", "alice", @every_scope ++ ["tenant:*"]),
      token("acme-ops", "acme-ops", ["audit:read", "sources:write", "tenant:acme"]),
      token("globex-ops", "globex-ops", ["audit:read", "sources:write", "tenant:globex"]),
      token("no-audit", "no-audit", @every_scope -- (["audit:read"] ++ ["tenant:*"]))
    ]
  end

  setup do
    auth = %{mode: :token, tokens: tokens()}

    config =
      test_config(
        roles: [:edge, :dispatch],
        source_store: {Ankusa.SourceStore.Persistent, decoder: &AuditHarness.decoder/2},
        routes: %{enabled: true, admin: %{port: 0, auth: auth}},
        admin: %{enabled: true, port: 0, auth: auth}
      )

    put_config(config)
    start_supervised!({Ankusa.Instance, config})
    %{inst: config.instance}
  end

  defp call(router, inst, method, path, opts) do
    body = Keyword.get(opts, :body)

    conn =
      method
      |> Plug.Test.conn(path, if(body, do: JSON.encode!(body), else: ""))
      |> Plug.Conn.put_req_header("content-type", "application/json")

    conn =
      case Keyword.get(opts, :token, "alice") do
        nil -> conn
        token -> Plug.Conn.put_req_header(conn, "authorization", "Bearer " <> token)
      end

    conn =
      Enum.reduce(Keyword.get(opts, :headers, []), conn, fn {k, v}, conn ->
        Plug.Conn.put_req_header(conn, k, v)
      end)

    router.call(conn, router.init(instance: inst))
  end

  defp admin(inst, method, path, opts \\ []), do: call(AdminRouter, inst, method, path, opts)
  defp routes(inst, method, path, opts \\ []), do: call(RoutesRouter, inst, method, path, opts)
  defp json(conn), do: JSON.decode!(conn.resp_body)

  # Newest first, as the API returns it.
  defp trail(inst, opts \\ []) do
    {:ok, %{entries: entries}} = Audit.list(inst, Keyword.put_new(opts, :limit, 1000))
    entries
  end

  defp pending(inst) do
    %{lo: lo, hi: hi} = Keys.family(:audit_pending)

    {:ok, rows} =
      Store.fold(inst, :audit_pending, {lo, hi}, [], fn key, _v, acc -> {:cont, [key | acc]} end)

    rows
  end

  @source %{
    "verify" => %{"type" => "hmac", "secret" => "hunter2-canary"},
    "sinks" => [%{"type" => "log"}]
  }

  describe "every change leaves exactly one row" do
    test "through every mutating route, in order, with who and where", %{inst: inst} do
      steps = [
        {&admin/4, :post, "/v1/tenants/acme/sources", Map.put(@source, "name", "billing"), 201,
         "source.create"},
        {&admin/4, :put, "/v1/tenants/acme/sources/billing", @source, 200, "source.update"},
        {&admin/4, :put, "/v1/tenants/acme/rate-limit", %{"rate" => 5, "burst" => 10}, 200,
         "rate_limit.put"},
        {&admin/4, :delete, "/v1/tenants/acme/rate-limit", nil, 204, "rate_limit.delete"},
        {&admin/4, :delete, "/v1/quarantine", nil, 200, "quarantine.purge"},
        {&admin/4, :post, "/v1/replays", %{"kind" => "dlq"}, 202, "replay.create"},
        {&routes/4, :post, "/admin/routes", %{"id" => "stripe", "path" => "/hooks/stripe"}, 201,
         "route.create"},
        {&routes/4, :put, "/admin/routes/stripe", %{"path" => "/hooks/stripe"}, 200,
         "route.update"},
        {&routes/4, :patch, "/admin/routes/stripe", %{"enabled" => false}, 200, "route.update"},
        {&routes/4, :put, "/admin/ip-rules", %{"default" => "allow", "rules" => []}, 200,
         "ip_rules.put"},
        {&routes/4, :put, "/admin/routes/other", %{"path" => "/hooks/other"}, 200,
         "route.create"},
        {&routes/4, :delete, "/admin/routes/other", nil, 204, "route.delete"},
        {&admin/4, :delete, "/v1/tenants/acme/sources/billing", nil, 204, "source.delete"}
      ]

      for {call, method, path, body, status, action} <- steps do
        before = length(trail(inst))
        conn = call.(inst, method, path, body: body, headers: [{"x-request-id", "req-1"}])

        assert conn.status == status, "#{method} #{path} answered #{conn.status}"
        assert Plug.Conn.get_resp_header(conn, "x-request-id") == ["req-1"]

        entries = trail(inst)

        assert length(entries) == before + 1,
               "#{method} #{path} wrote #{length(entries) - before} rows"

        assert [%{action: ^action} = row | _] = entries
        assert row.actor == "alice"
        assert row.remote_ip == "127.0.0.1"
        assert row.request_id == "req-1"
        assert row.result == "ok"
      end

      assert pending(inst) == []
    end

    test "a request that changes nothing writes no row", %{inst: inst} do
      assert admin(inst, :post, "/v1/tenants/acme/sources", body: Map.put(@source, "name", "a")).status ==
               201

      assert admin(inst, :post, "/v1/replays", body: %{"kind" => "dlq"}).status == 202

      assert routes(inst, :post, "/admin/routes", body: %{"id" => "r", "path" => "/hooks/r"}).status ==
               201

      count = length(trail(inst))

      rejected = [
        {&admin/4, :post, "/v1/tenants/acme/sources", Map.put(@source, "name", "a"), 409},
        {&admin/4, :put, "/v1/tenants/acme/sources/missing", @source, 404},
        {&admin/4, :delete, "/v1/tenants/acme/sources/missing", nil, 404},
        {&admin/4, :post, "/v1/tenants/acme/sources", %{"name" => "bad name", "sinks" => []},
         400},
        {&admin/4, :delete, "/v1/tenants/acme/rate-limit", nil, 404},
        {&admin/4, :put, "/v1/tenants/acme/rate-limit", %{"rate" => -1}, 400},
        # the same replay again is the job that already runs
        {&admin/4, :post, "/v1/replays", %{"kind" => "dlq"}, 200},
        {&admin/4, :patch, "/v1/replays/nope", %{"state" => "paused"}, 404},
        {&routes/4, :post, "/admin/routes", %{"id" => "r", "path" => "/hooks/r"}, 409},
        {&routes/4, :patch, "/admin/routes/missing", %{"enabled" => false}, 404},
        {&routes/4, :delete, "/admin/routes/missing", nil, 404},
        {&routes/4, :put, "/admin/ip-rules", %{"rules" => []}, 400}
      ]

      for {call, method, path, body, status} <- rejected do
        conn = call.(inst, method, path, body: body)
        assert conn.status == status, "#{method} #{path} answered #{conn.status}"
      end

      assert length(trail(inst)) == count
      assert pending(inst) == []
    end

    test "a source's revision counts its changes and survives its deletion", %{inst: inst} do
      path = "/v1/tenants/acme/sources/billing"

      assert admin(inst, :post, "/v1/tenants/acme/sources",
               body: Map.put(@source, "name", "billing")
             ).status == 201

      assert json(admin(inst, :get, path))["revision"] == 1

      assert json(admin(inst, :put, path, body: @source))["revision"] == 2
      assert admin(inst, :delete, path).status == 204

      conn =
        admin(inst, :post, "/v1/tenants/acme/sources", body: Map.put(@source, "name", "billing"))

      assert json(conn)["revision"] == 4

      revisions = inst |> trail() |> Enum.map(& &1.revision) |> Enum.reverse()
      assert revisions == [1, 2, 3, 4]

      # and after a restart of the node's store the counter is where it was
      stop_supervised!({Ankusa.Instance, inst})
      start_supervised!({Ankusa.Instance, Ankusa.config(inst)})
      assert json(admin(inst, :get, path))["revision"] == 4
    end

    test "a route's revision counts its changes", %{inst: inst} do
      body = %{"id" => "stripe", "path" => "/hooks/stripe"}
      assert json(routes(inst, :post, "/admin/routes", body: body))["revision"] == 1
      assert json(routes(inst, :put, "/admin/routes/stripe", body: body))["revision"] == 2

      assert json(routes(inst, :patch, "/admin/routes/stripe", body: %{"enabled" => false}))[
               "revision"
             ] == 3

      assert json(routes(inst, :get, "/admin/routes/stripe"))["revision"] == 3

      assert routes(inst, :delete, "/admin/routes/stripe").status == 204
      assert json(routes(inst, :post, "/admin/routes", body: body))["revision"] == 1

      # newest first: the re-created route, its deletion (one past the last
      # revision it had), the patch, the replace, the creation
      assert Enum.map(trail(inst), &{&1.action, &1.revision}) == [
               {"route.create", 1},
               {"route.delete", 4},
               {"route.update", 3},
               {"route.update", 2},
               {"route.create", 1}
             ]
    end

    test "the hashes show what changed without holding it", %{inst: inst} do
      assert admin(inst, :post, "/v1/tenants/acme/sources",
               body: Map.put(@source, "name", "billing")
             ).status == 201

      path = "/v1/tenants/acme/sources/billing"
      rotated = put_in(@source, ["verify", "secret"], "rotated-canary")
      assert admin(inst, :put, path, body: rotated).status == 200
      assert admin(inst, :put, path, body: rotated).status == 200
      assert admin(inst, :delete, path).status == 204

      [delete, same, rotate, create] = trail(inst)

      assert create.before_hash == nil
      assert create.after_hash =~ ~r/\A[0-9a-f]{64}\z/
      assert rotate.before_hash == create.after_hash
      assert rotate.after_hash != create.after_hash
      # an identical re-write changes nothing the hash can see
      assert same.before_hash == same.after_hash
      assert delete.before_hash == same.after_hash
      assert delete.after_hash == nil
    end
  end

  describe "no secret in audit" do
    test "no row, stored or served, holds a secret or a spec", %{inst: inst} do
      canary = "hunter2-canary"
      sink = %{"type" => "http", "url" => "https://example.com/hook", "token" => "sink-canary"}
      spec = %{@source | "sinks" => [sink]}

      assert admin(inst, :post, "/v1/tenants/acme/sources",
               body: Map.put(spec, "name", "billing")
             ).status == 201

      assert admin(inst, :put, "/v1/tenants/acme/sources/billing",
               body: put_in(spec, ["verify", "secret"], "rotated-canary")
             ).status == 200

      assert admin(inst, :delete, "/v1/tenants/acme/sources/billing").status == 204

      body = %{
        "id" => "stripe",
        "path" => "/hooks/stripe",
        "metadata" => %{"note" => "route-canary"}
      }

      assert routes(inst, :post, "/admin/routes", body: body).status == 201

      served = admin(inst, :get, "/v1/audit").resp_body
      assert JSON.decode!(served)["entries"] |> length() == 4

      %{lo: lo, hi: hi} = Keys.family(:audit)

      {:ok, stored} =
        Store.fold(inst, :audit, {lo, hi}, [], fn _k, v, acc -> {:cont, [v | acc]} end)

      assert length(stored) == 4

      for secret <- [canary, "rotated-canary", "sink-canary", "route-canary", "example.com"],
          bytes <- [served | stored] do
        refute String.contains?(bytes, secret), "#{secret} reached an audit row"
      end
    end

    test "a hash is keyed: it is not the hash of the value", %{inst: inst} do
      assert admin(inst, :put, "/v1/tenants/acme/rate-limit", body: %{"rate" => 5, "burst" => 10}).status ==
               200

      [row] = trail(inst)

      plain =
        Base.encode16(:crypto.hash(:sha256, :erlang.term_to_binary(%{rate: 5, burst: 10})),
          case: :lower
        )

      assert row.after_hash != plain
      assert Audit.digest(inst, %{rate: 5, burst: 10}) == row.after_hash
    end
  end

  describe "GET /v1/audit" do
    test "filters, pages and orders", %{inst: inst} do
      put = fn tenant, token ->
        admin(inst, :put, "/v1/tenants/#{tenant}/rate-limit",
          token: token,
          body: %{"rate" => 1, "burst" => 1}
        )
      end

      assert put.("acme", "alice").status == 200
      Process.sleep(15)
      middle = System.system_time(:millisecond)
      Process.sleep(15)
      assert put.("globex", "alice").status == 200
      assert admin(inst, :delete, "/v1/tenants/acme/rate-limit", token: "acme-ops").status == 403

      assert admin(inst, :post, "/v1/tenants/acme/sources",
               token: "acme-ops",
               body: Map.put(@source, "name", "billing")
             ).status == 201

      assert routes(inst, :put, "/admin/ip-rules", body: %{"default" => "allow", "rules" => []}).status ==
               200

      list = fn query -> json(admin(inst, :get, "/v1/audit" <> query)) end
      actions = fn query -> for e <- list.(query)["entries"], do: e["action"] end

      # newest first
      assert actions.("") == ["ip_rules.put", "source.create", "rate_limit.put", "rate_limit.put"]
      assert list.("")["next_cursor"] == nil

      assert actions.("?action=rate_limit.put") == ["rate_limit.put", "rate_limit.put"]
      assert actions.("?actor=acme-ops") == ["source.create"]
      assert actions.("?actor=nobody") == []
      assert actions.("?tenant=globex") == ["rate_limit.put"]
      assert actions.("?tenant=acme") == ["source.create", "rate_limit.put"]
      assert actions.("?since=#{middle}") == ["ip_rules.put", "source.create", "rate_limit.put"]
      assert actions.("?until=#{middle}") == ["rate_limit.put"]
      assert actions.("?since=#{middle}&until=#{middle}") == []
      assert actions.("?action=rate_limit.put&tenant=acme") == ["rate_limit.put"]

      first = list.("?limit=3")
      assert length(first["entries"]) == 3
      assert is_binary(first["next_cursor"])
      second = list.("?limit=3&cursor=#{first["next_cursor"]}")
      assert length(second["entries"]) == 1
      assert second["next_cursor"] == nil

      paged = for e <- first["entries"] ++ second["entries"], do: e["seq"]
      assert paged == Enum.sort(paged, :desc)
      assert paged == for(e <- list.("")["entries"], do: e["seq"])

      row = hd(list.("?action=source.create")["entries"])

      assert %{
               "actor" => "acme-ops",
               "remote_ip" => "127.0.0.1",
               "action" => "source.create",
               "tenant" => "acme",
               "resource" => "acme.billing",
               "before_hash" => nil,
               "result" => "ok",
               "revision" => 1
             } = row

      assert row["after_hash"] =~ ~r/\A[0-9a-f]{64}\z/
      assert {:ok, _, _} = DateTime.from_iso8601(row["at"])
      assert is_binary(row["request_id"])
    end

    test "bad parameters are invalid_filter", %{inst: inst} do
      for query <- ["?since=x", "?until=-1", "?limit=x", "?cursor=0", "?cursor=x", "?actor[]=a"] do
        conn = admin(inst, :get, "/v1/audit" <> query)
        assert conn.status == 400, "#{query} answered #{conn.status}"
        assert %{"error" => "invalid_filter"} = json(conn)
      end
    end

    test "401 without a credential, 403 without audit:read", %{inst: inst} do
      assert admin(inst, :get, "/v1/audit", token: nil).status == 401
      assert admin(inst, :get, "/v1/audit", token: "wrong").status == 401

      conn = admin(inst, :get, "/v1/audit", token: "no-audit")
      assert conn.status == 403
      assert %{"error" => "forbidden", "scope" => "audit:read"} = json(conn)
    end

    test "a tenant-scoped reader sees its tenant's rows only", %{inst: inst} do
      body = fn name -> Map.put(@source, "name", name) end

      assert admin(inst, :post, "/v1/tenants/acme/sources", token: "acme-ops", body: body.("a")).status ==
               201

      assert admin(inst, :post, "/v1/tenants/globex/sources",
               token: "globex-ops",
               body: body.("g")
             ).status == 201

      assert routes(inst, :put, "/admin/ip-rules", body: %{"default" => "allow", "rules" => []}).status ==
               200

      assert admin(inst, :post, "/v1/replays", body: %{"kind" => "dlq"}).status == 202

      assert [%{"tenant" => "acme"}] =
               json(admin(inst, :get, "/v1/audit", token: "acme-ops"))["entries"]

      assert [%{"tenant" => "globex"}] =
               json(admin(inst, :get, "/v1/audit", token: "globex-ops"))["entries"]

      assert length(json(admin(inst, :get, "/v1/audit"))["entries"]) == 4

      # naming another tenant is a 403 like any other tenant resource
      conn = admin(inst, :get, "/v1/audit?tenant=globex", token: "acme-ops")
      assert conn.status == 403
      assert %{"error" => "forbidden", "scope" => "tenant:globex"} = json(conn)
      assert admin(inst, :get, "/v1/audit?tenant=acme", token: "acme-ops").status == 200
    end
  end

  describe "request context" do
    test "a request id that is not a plain token is replaced, and the id is answered", %{
      inst: inst
    } do
      for bad <- ["has space", String.duplicate("a", 129), "semi;colon"] do
        conn =
          admin(inst, :put, "/v1/tenants/acme/rate-limit",
            body: %{"rate" => 1, "burst" => 1},
            headers: [{"x-request-id", bad}]
          )

        assert [id] = Plug.Conn.get_resp_header(conn, "x-request-id")
        assert id != bad
        assert hd(trail(inst)).request_id == id
      end
    end

    test "X-Forwarded-For is believed only from a trusted proxy", %{inst: inst} do
      put = fn inst ->
        admin(inst, :put, "/v1/tenants/acme/rate-limit",
          body: %{"rate" => 1, "burst" => 1},
          headers: [{"x-forwarded-for", "203.0.113.9"}]
        )
      end

      assert put.(inst).status == 200
      assert hd(trail(inst)).remote_ip == "127.0.0.1"

      config =
        test_config(
          roles: [:edge],
          routes: %{enabled: true, trusted_proxies: ["127.0.0.0/8"], admin: %{port: 0}},
          admin: %{enabled: true, port: 0}
        )

      put_config(config)
      start_supervised!({Ankusa.Instance, config}, id: :proxied)

      assert admin(config.instance, :put, "/v1/tenants/acme/rate-limit",
               token: nil,
               body: %{"rate" => 1, "burst" => 1},
               headers: [{"x-forwarded-for", "203.0.113.9"}]
             ).status == 200

      assert hd(trail(config.instance)).remote_ip == "203.0.113.9"
    end

    test "a change made from code is the system's", %{inst: inst} do
      assert {:ok, _} = Ankusa.SourceStore.put(inst, "acme", "coded", @source, :create)
      assert %{actor: "system", remote_ip: nil, request_id: nil} = hd(trail(inst))

      context = %Context{actor: "deploy-bot", remote_ip: "10.1.2.3", request_id: "ci-9"}

      assert {:ok, _} =
               Ankusa.Edge.RateLimiter.put_override(inst, "acme", %{"rate" => 1, "burst" => 1},
                 audit: context
               )

      assert %{actor: "deploy-bot", remote_ip: "10.1.2.3", request_id: "ci-9"} = hd(trail(inst))
    end
  end

  describe "audit row survives kill" do
    test "every change is in the trail after kill -9 on the last response" do
      data_dir = unique_data_dir(:audit_harness)
      on_exit(fn -> File.rm_rf(data_dir) end)
      config = AuditHarness.config(data_dir)

      port =
        Port.open({:spawn_executable, System.find_executable("elixir")}, [
          :binary,
          :exit_status,
          {:line, 4096},
          args: [@harness, "serve"],
          env: [
            {~c"ERL_LIBS", String.to_charlist(Mix.Project.build_path() <> "/lib")},
            {~c"DATA_DIR", String.to_charlist(data_dir)}
          ]
        ])

      on_exit(fn -> kill_if_alive(port) end)
      {:os_pid, os_pid} = Port.info(port, :os_pid)

      steps = await_ready(port, [])
      assert length(steps) == 13
      assert Enum.all?(steps, &(&1 in 200..204)), "a step failed: #{inspect(steps)}"

      # SIGKILL on the response to the last change: no clean shutdown, no flush
      # beyond what each change's own synced batch put on disk.
      {_, 0} = System.cmd("kill", ["-9", Integer.to_string(os_pid)])
      assert_receive {^port, {:exit_status, 137}}, 10_000

      put_config(config)
      start_supervised!({Store, instance: config.instance})

      rows = config.instance |> trail() |> Enum.reverse()
      assert Enum.map(rows, & &1.action) == AuditHarness.expected_actions()
      assert Enum.all?(rows, &(&1.result == "ok"))

      seqs = Enum.map(rows, & &1.seq)
      assert seqs == Enum.sort(seqs)
      assert pending(config.instance) == []

      %{lo: lo, hi: hi} = Keys.family(:audit)

      {:ok, stored} =
        Store.fold(config.instance, :audit, {lo, hi}, [], fn _k, v, acc -> {:cont, [v | acc]} end)

      for secret <- [AuditHarness.canary(), AuditHarness.rotated()], bytes <- stored do
        refute String.contains?(bytes, secret)
      end
    end

    defp await_ready(port, steps) do
      receive do
        {^port, {:data, {:eol, "STEP " <> rest}}} ->
          [_n, _method, _path, status] = String.split(rest, " ")
          await_ready(port, steps ++ [String.to_integer(status)])

        {^port, {:data, {:eol, "READY"}}} ->
          steps

        {^port, {:data, {:eol, _other}}} ->
          await_ready(port, steps)

        {^port, {:exit_status, status}} ->
          flunk("the harness exited with #{status} after #{length(steps)} steps")
      after
        90_000 -> flunk("the harness was not ready after #{length(steps)} steps")
      end
    end

    defp kill_if_alive(port) do
      case Port.info(port, :os_pid) do
        {:os_pid, os_pid} ->
          System.cmd("kill", ["-9", Integer.to_string(os_pid)], stderr_to_stdout: true)
          :ok

        nil ->
          :ok
      end
    end
  end

  describe "the audit process" do
    setup do
      config = test_config(roles: [:edge], audit: %{retention_days: 1})
      put_config(config)
      start_supervised!({Store, instance: config.instance})
      %{audit: config.instance}
    end

    defp start_audit(inst), do: start_supervised!({Audit, instance: inst})

    @attrs %{action: "test.change", tenant: "acme", resource: "thing"}

    test "rows are committed in sequence, whoever commits them", %{audit: inst} do
      start_audit(inst)

      1..50
      |> Task.async_stream(
        fn n -> Audit.commit(inst, Context.system(), %{@attrs | resource: "r#{n}"}, []) end,
        max_concurrency: 16
      )
      |> Enum.each(&assert({:ok, :ok} = &1))

      rows = inst |> trail() |> Enum.reverse()
      assert Enum.map(rows, & &1.seq) == Enum.to_list(1..50)
      assert Enum.map(rows, & &1.at) == Enum.sort(Enum.map(rows, & &1.at))
    end

    test "a change's ops and its row are written together", %{audit: inst} do
      start_audit(inst)
      assert :ok = Audit.commit(inst, Context.system(), @attrs, [{:put, :default, "k", "v"}])
      assert Store.get(inst, :default, "k") == {:ok, "v"}
      assert [%{seq: 1, action: "test.change"}] = trail(inst)
      assert Store.get(inst, :default, Keys.audit_seq()) == {:ok, <<1::64>>}
    end

    test "a pending row a crashed node left is recorded as interrupted at the next boot", %{
      audit: inst
    } do
      start_audit(inst)
      attrs = Map.merge(@attrs, %{before_hash: "b", after_hash: "a"})
      assert {:ok, _pending} = Audit.begin(inst, Context.system(), attrs)
      assert length(pending(inst)) == 1
      assert trail(inst) == []

      stop_supervised!({Audit, inst})
      start_audit(inst)

      assert pending(inst) == []

      assert [%{action: "test.change", result: "interrupted", before_hash: "b", after_hash: "a"}] =
               trail(inst)
    end

    test "a caller that dies between begin and finish leaves an interrupted row", %{audit: inst} do
      start_audit(inst)
      parent = self()

      spawn(fn ->
        {:ok, _} = Audit.begin(inst, Context.system(), @attrs)
        send(parent, :began)
      end)

      assert_receive :began
      assert eventually(fn -> match?([%{result: "interrupted"}], trail(inst)) end)
      assert pending(inst) == []
    end

    test "record/4 finishes with the outcome, discards a no-op and survives a raise", %{
      audit: inst
    } do
      start_audit(inst)
      ctx = Context.system()

      assert :changed =
               Audit.record(inst, ctx, @attrs, fn -> {:changed, %{result: "ok", revision: 7}} end)

      assert :nothing = Audit.record(inst, ctx, @attrs, fn -> {:nothing, :discard} end)
      assert :refused = Audit.record(inst, ctx, @attrs, fn -> {:refused, %{result: "failed"}} end)

      assert_raise RuntimeError, "boom", fn ->
        Audit.record(inst, ctx, @attrs, fn -> raise "boom" end)
      end

      assert [%{result: "interrupted"}, %{result: "failed"}, %{result: "ok", revision: 7}] =
               trail(inst)

      assert pending(inst) == []
    end

    test "a change is refused when its audit row cannot be written", %{audit: inst} do
      assert {:error, :store_unavailable} = Audit.commit(inst, Context.system(), @attrs, [])
      assert {:error, :store_unavailable} = Audit.begin(inst, Context.system(), @attrs)

      assert {:error, :store_unavailable} =
               Audit.record(inst, Context.system(), @attrs, fn -> flunk("ran") end)
    end

    test "rows older than audit.retention_days are pruned, oldest first", %{audit: inst} do
      old = System.system_time(:millisecond) - 3 * 86_400_000

      seeded =
        for seq <- 1..5 do
          row = %{seq: seq, at: old, action: "old", actor: "x", tenant: nil, resource: nil}
          {:put, :audit, Keys.audit(seq, old), <<"AA", 1>> <> :erlang.term_to_binary(row)}
        end

      :ok =
        Store.write(
          inst,
          seeded ++
            [
              {:put, :audit, Keys.lifecycle_entity(1), JSON.encode!(%{"old" => true})},
              {:put, :default, Keys.audit_seq(), <<5::64>>}
            ],
          sync: true
        )

      start_audit(inst)
      assert :ok = Audit.commit(inst, Context.system(), @attrs, [])
      assert length(trail(inst)) == 6

      handler = {:audit_pruned, self()}

      :telemetry.attach(
        handler,
        [:ankusa, :audit, :pruned],
        fn _event, %{count: count}, _meta, pid -> send(pid, {:pruned, count}) end,
        self()
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      assert {:ok, 5} = Audit.prune(inst)
      assert_receive {:pruned, 5}
      assert [%{action: "test.change", seq: 6}] = trail(inst)
      assert {:ok, 0} = Audit.prune(inst)
      # The lifecycle entity beside a pruned row goes with it.
      assert Audit.entity(inst, 1) == :not_found
    end

    test "a change with an entity is flagged for publication and keeps it beside the row", %{
      audit: inst
    } do
      start_audit(inst)

      assert :ok =
               Audit.commit(inst, Context.system(), Map.put(@attrs, :entity, %{"k" => "v"}), [])

      assert [%{publish: true} = row] = trail(inst)
      refute Map.has_key?(row, :entity)
      assert Audit.entity(inst, row.seq) == {:ok, %{"k" => "v"}}

      assert :ok = Audit.commit(inst, Context.system(), @attrs, [])
      assert [%{publish: false, seq: 2}, _first] = trail(inst)
      assert Audit.entity(inst, 2) == :not_found
    end

    test "every committed row is a [:ankusa, :audit] event", %{audit: inst} do
      start_audit(inst)
      handler = {:audit_event, self()}

      :telemetry.attach(
        handler,
        [:ankusa, :audit],
        fn _event, measurements, meta, pid -> send(pid, {:audit, measurements, meta}) end,
        self()
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      assert :ok = Audit.commit(inst, %Context{actor: "ops"}, @attrs, [])

      assert_receive {:audit, %{count: 1},
                      %{
                        instance: ^inst,
                        action: "test.change",
                        actor: "ops",
                        tenant: "acme",
                        result: "ok"
                      }}
    end

    test "a row a newer release wrote reads, and an unknown version is an error", %{audit: inst} do
      start_audit(inst)
      newer = %{seq: 10, at: System.system_time(:millisecond), action: "future", extra: "field"}

      :ok =
        Store.write(
          inst,
          [
            {:put, :audit, Keys.audit(10, newer.at), <<"AA", 1>> <> :erlang.term_to_binary(newer)}
          ],
          sync: true
        )

      assert [%{action: "future", extra: "field"}] = trail(inst)

      :ok =
        Store.write(
          inst,
          [
            {:put, :audit, Keys.audit(11, newer.at), <<"AA", 9>> <> :erlang.term_to_binary(newer)}
          ],
          sync: true
        )

      assert {:error, {:corrupt_audit_row, 11, :unsupported_audit_row_version}} = Audit.list(inst)
    end

    defp eventually(fun, tries \\ 100) do
      cond do
        fun.() -> true
        tries == 0 -> false
        true -> Process.sleep(20) && eventually(fun, tries - 1)
      end
    end
  end

  describe "retention and the lifecycle outbox" do
    setup do
      config =
        test_config(
          roles: [:edge],
          audit: %{retention_days: 1},
          lifecycle: %{sinks: [{Ankusa.Sink.Log, []}]}
        )

      put_config(config)
      start_supervised!({Store, instance: config.instance})
      %{audit: config.instance}
    end

    defp seed_old_rows(inst, seqs) do
      old = System.system_time(:millisecond) - 3 * 86_400_000

      ops =
        Enum.flat_map(seqs, fn seq ->
          row = %{seq: seq, at: old, action: "old", actor: "x", tenant: nil, resource: nil}

          [
            {:put, :audit, Keys.audit(seq, old), <<"AA", 1>> <> :erlang.term_to_binary(row)},
            {:put, :audit, Keys.lifecycle_entity(seq), JSON.encode!(%{"seq" => seq})}
          ]
        end)

      :ok =
        Store.write(
          inst,
          ops ++ [{:put, :default, Keys.audit_seq(), <<Enum.max(seqs)::64>>}],
          sync: true
        )
    end

    test "a lifecycle sink that never confirms keeps every row at or above the cursor", %{
      audit: inst
    } do
      seed_old_rows(inst, 1..5)

      # The cursor is where the slowest lifecycle sink stands: it has dealt
      # with rows 1 and 2 and never confirms the rest.
      :ok = Store.write(inst, [{:put, :default, Keys.lifecycle_cursor(), <<3::64>>}], sync: true)
      start_audit(inst)

      handler = {:audit_retention, self()}

      :telemetry.attach(
        handler,
        [:ankusa, :audit, :retention],
        fn _event, %{blocked_by_cursor: held}, _meta, pid -> send(pid, {:retention, held}) end,
        self()
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      assert {:ok, 2} = Audit.prune(inst)
      assert_receive {:retention, 3}

      for seq <- 3..5 do
        assert {:ok, %{"seq" => ^seq}} = Audit.entity(inst, seq)
      end

      assert Audit.entity(inst, 1) == :not_found
      assert Audit.entity(inst, 2) == :not_found
      assert {:ok, rows} = Audit.page(inst, 0, 10)
      assert Enum.map(rows, &elem(&1, 0)) == [3, 4, 5]

      # Prune again with the sink still down: nothing more goes, and the gauge holds.
      assert {:ok, 0} = Audit.prune(inst)
      assert_receive {:retention, 3}

      # The sink catches up: retention proceeds and the gauge clears.
      :ok = Store.write(inst, [{:put, :default, Keys.lifecycle_cursor(), <<6::64>>}], sync: true)
      assert {:ok, 3} = Audit.prune(inst)
      assert_receive {:retention, 0}
      assert {:ok, []} = Audit.page(inst, 0, 10)
    end

    test "with no lifecycle cursor written yet retention is the date alone", %{audit: inst} do
      seed_old_rows(inst, 1..3)
      start_audit(inst)

      assert {:ok, 3} = Audit.prune(inst)
    end
  end

  describe "outbox ceiling" do
    @ops %Context{actor: "ops"}
    @event %{action: "source.create", tenant: "acme", resource: "s", entity: %{"id" => "s"}}

    defp start_trail(audit) do
      config =
        test_config(
          roles: [:edge],
          audit: audit,
          lifecycle: %{sinks: [{Ankusa.Sink.Log, []}]}
        )

      put_config(config)
      inst = config.instance
      start_supervised!({Store, instance: inst})

      # The publisher's first boot writes its cursor at the last seq: what the
      # trail holds then is history, and the sink stands at 0.
      :ok = Store.write(inst, [{:put, :default, Keys.lifecycle_cursor(), <<0::64>>}], sync: true)
      start_supervised!({Audit, instance: inst})
      inst
    end

    defp cursor!(inst, seq),
      do:
        :ok =
          Store.write(inst, [{:put, :default, Keys.lifecycle_cursor(), <<seq::64>>}], sync: true)

    test "outbox ceiling: a lifecycle sink that never confirms stops new mutations and loses no row" do
      inst = start_trail(%{outbox_max_backlog: 3})

      for n <- 1..3, do: assert(:ok = Audit.commit(inst, @ops, %{@event | resource: "s#{n}"}, []))
      assert %{outbox_backlog: 3} = Audit.stats(inst)
      assert Audit.refusing?(inst)

      # The change's own ops never run, and nothing is dropped to make room.
      assert {:error, :audit_backlog} =
               Audit.commit(inst, @ops, @event, [{:put, :default, "k", "v"}])

      assert {:error, :audit_backlog} = Audit.begin(inst, @ops, @event)
      assert Store.get(inst, :default, "k") == :not_found
      assert {:ok, rows} = Audit.page(inst, 0, 10)
      assert Enum.map(rows, &elem(&1, 0)) == [1, 2, 3]
      for seq <- 1..3, do: assert({:ok, _} = Audit.entity(inst, seq))

      # What the node does itself (a restore, a sweep's own row) still goes through.
      assert :ok = Audit.commit(inst, Context.system(), @event, [])

      # The sink catches up: new changes are accepted again.
      cursor!(inst, 4)
      refute Audit.refusing?(inst)
      assert :ok = Audit.commit(inst, @ops, @event, [])
    end

    test "a trail over audit.max_rows is pruned oldest first, never past the cursor, else refuses" do
      inst = start_trail(%{max_rows: 3, outbox_max_backlog: 1_000})

      for n <- 1..4, do: assert(:ok = Audit.commit(inst, @ops, %{@event | resource: "s#{n}"}, []))
      assert %{rows: 4} = Audit.stats(inst)

      # Over the ceiling with the sink at 0: every row is unpublished, so none may go.
      assert {:error, :audit_backlog} = Audit.commit(inst, @ops, @event, [])
      assert {:ok, rows} = Audit.page(inst, 0, 10)
      assert Enum.map(rows, &elem(&1, 0)) == [1, 2, 3, 4]

      # The sink confirmed rows 1 and 2: the oldest goes, with its entity, and the
      # change is accepted.
      cursor!(inst, 3)
      assert :ok = Audit.commit(inst, @ops, @event, [])
      assert {:ok, rows} = Audit.page(inst, 0, 10)
      assert Enum.map(rows, &elem(&1, 0)) == [2, 3, 4, 5]
      assert Audit.entity(inst, 1) == :not_found
      assert %{rows: 4} = Audit.stats(inst)

      # The counters survive a restart and match the trail.
      stop_supervised!({Audit, inst})
      start_supervised!({Audit, instance: inst})
      assert %{rows: 4} = Audit.stats(inst)
    end
  end

  test "audit.retention_days must be a positive integer" do
    for bad <- [0, -1, 1.5, "365", nil] do
      assert_raise ArgumentError, ~r/audit\.retention_days must be an integer >= 1/, fn ->
        Ankusa.Config.new(audit: %{retention_days: bad})
      end
    end

    assert Ankusa.Config.new().audit == %{
             retention_days: 365,
             max_rows: 10_000_000,
             max_bytes: 5_368_709_120,
             outbox_max_backlog: 100_000
           }

    for key <- [:max_rows, :max_bytes, :outbox_max_backlog], bad <- [0, -1, 1.5, "1", nil] do
      assert_raise ArgumentError, ~r/audit\.#{key} must be an integer >= 1/, fn ->
        Ankusa.Config.new(audit: %{key => bad})
      end
    end

    assert_raise ArgumentError, ~r/unknown Ankusa.Config key: audit.days/, fn ->
      Ankusa.Config.new(audit: %{days: 1})
    end
  end
end
