defmodule Ankusa.Routes.RouterTest do
  @moduledoc """
  The management API over `Plug.Test.conn`, plus one test over a real socket:
  the in-process cases pin the contract, and the socket case proves the listener
  binds on its own port — with the capture listener on a different one, serving
  a different surface.

  `async: false` because the socket test binds a port and the API reads the
  instance's `:persistent_term` config.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Admin.Auth.Token
  alias Ankusa.Routes.Router

  defp start(routes_opts, extra_opts \\ []), do: start_routes(routes_opts, extra_opts)

  defp call(config, method, path, opts \\ []) do
    body = Keyword.get(opts, :body)
    conn = Plug.Test.conn(method, path, body)

    conn =
      case Keyword.get(opts, :token) do
        nil -> conn
        token -> Plug.Conn.put_req_header(conn, "authorization", "Bearer " <> token)
      end

    Router.call(conn, Router.init(instance: config.instance))
  end

  defp json(conn), do: JSON.decode!(conn.resp_body)

  defp dry_run(config, request) do
    call(config, :post, "/admin/routes/test", body: JSON.encode!(request))
  end

  describe "route lifecycle" do
    setup do
      config = start([])
      %{config: config}
    end

    test "a duplicate enabled route is a 409 naming the holder", %{config: config} do
      call(config, :post, "/admin/routes", body: ~s({"id":"a","path":"/hooks/x"}))

      response = call(config, :post, "/admin/routes", body: ~s({"id":"b","path":"/hooks/x"}))

      assert response.status == 409
      assert json(response) == %{"error" => "duplicate_route", "conflicting_id" => "a"}
    end

    test "an invalid body is a 400", %{config: config} do
      assert json(call(config, :post, "/admin/routes", body: "not json")) == %{
               "error" => "invalid_body"
             }

      assert call(config, :post, "/admin/routes", body: "[1,2]").status == 400
      assert call(config, :put, "/admin/routes/a", body: "").status == 400
    end

    test "an invalid route names the field", %{config: config} do
      response = call(config, :post, "/admin/routes", body: ~s({"path":"hooks/x"}))

      assert response.status == 400

      assert json(response) == %{
               "error" => "invalid_route",
               "field" => "path",
               "message" => "must start with \"/\""
             }
    end
  end

  describe "listing" do
    setup do
      config = start([])

      for id <- ["a", "b", "c"] do
        call(config, :post, "/admin/routes", body: ~s({"id":"#{id}","path":"/hooks/#{id}"}))
      end

      %{config: config}
    end

    test "returns every route with a cursor when there is another page", %{config: config} do
      all = json(call(config, :get, "/admin/routes"))
      assert Enum.map(all["routes"], & &1["id"]) == ["a", "b", "c"]
      assert all["next_cursor"] == nil

      page = json(call(config, :get, "/admin/routes?limit=2"))
      assert Enum.map(page["routes"], & &1["id"]) == ["a", "b"]
      assert page["next_cursor"] == "b"

      rest = json(call(config, :get, "/admin/routes?limit=2&cursor=b"))
      assert Enum.map(rest["routes"], & &1["id"]) == ["c"]
      assert rest["next_cursor"] == nil
    end

    test "filters by enabled", %{config: config} do
      call(config, :patch, "/admin/routes/b", body: ~s({"enabled":false}))

      enabled = json(call(config, :get, "/admin/routes?enabled=true"))
      assert Enum.map(enabled["routes"], & &1["id"]) == ["a", "c"]

      disabled = json(call(config, :get, "/admin/routes?enabled=false"))
      assert Enum.map(disabled["routes"], & &1["id"]) == ["b"]
    end

    test "rejects a bad query rather than guessing", %{config: config} do
      assert json(call(config, :get, "/admin/routes?limit=abc")) == %{
               "error" => "invalid_query",
               "field" => "limit"
             }

      assert json(call(config, :get, "/admin/routes?enabled=maybe")) == %{
               "error" => "invalid_query",
               "field" => "enabled"
             }
    end
  end

  describe "the route cap" do
    test "PUT past max_routes is a 409 naming the cap" do
      config = start(max_routes: 1, seed: [%{"id" => "s", "path" => "/hooks/s"}])

      response = call(config, :put, "/admin/routes/extra", body: ~s({"path":"/hooks/extra"}))

      assert response.status == 409
      assert json(response) == %{"error" => "too_many_routes", "max_routes" => 1}

      # Replacing the route that is already there still works.
      assert call(config, :put, "/admin/routes/s", body: ~s({"path":"/hooks/s2"})).status == 200
    end
  end

  describe "IP rules" do
    test "a bad rule is a 400 naming the field" do
      config = start([])

      response =
        call(config, :put, "/admin/ip-rules",
          body: ~s({"default":"allow","rules":[{"action":"allow","cidr":"nope"}]})
        )

      assert response.status == 400

      assert json(response) == %{
               "error" => "invalid_ip_rules",
               "field" => "rules",
               "message" => "rule 0: invalid cidr \"nope\""
             }
    end
  end

  describe "the dry run" do
    setup do
      config =
        start(
          ip_rules: [default: :allow, rules: [%{action: :deny, cidr: "10.0.0.0/8"}]],
          seed: [
            %{"id" => "s", "path" => "/hooks/s"},
            %{
              "id" => "pinned",
              "path" => "/hooks/pinned",
              "ip_rules" => [%{"action" => "allow", "cidr" => "192.168.0.0/16"}]
            }
          ]
        )

      %{config: config}
    end

    test "reports each rejection reason with its rule scope", %{config: config} do
      assert json(dry_run(config, %{"method" => "POST", "path" => "/nope", "ip" => "1.2.3.4"})) ==
               %{
                 "decision" => "deny",
                 "reason" => "no_route",
                 "route_id" => nil,
                 "ip_rule" => nil
               }

      assert json(dry_run(config, %{"method" => "GET", "path" => "/hooks/s", "ip" => "1.2.3.4"})) ==
               %{"decision" => "deny", "reason" => "method", "route_id" => nil, "ip_rule" => nil}

      assert json(
               dry_run(config, %{"method" => "POST", "path" => "/hooks/s", "ip" => "10.1.2.3"})
             ) ==
               %{
                 "decision" => "deny",
                 "reason" => "ip_denied",
                 "route_id" => nil,
                 "ip_rule" => %{"action" => "deny", "cidr" => "10.0.0.0/8", "scope" => "global"}
               }

      # The route's own rule decided this one, so the rule comes back with the
      # route scope.
      assert json(
               dry_run(config, %{
                 "method" => "POST",
                 "path" => "/hooks/pinned",
                 "ip" => "192.168.1.1"
               })
             ) ==
               %{
                 "decision" => "allow",
                 "reason" => "matched",
                 "route_id" => "pinned",
                 "ip_rule" => %{
                   "action" => "allow",
                   "cidr" => "192.168.0.0/16",
                   "scope" => "route"
                 }
               }

      # Outside the route's allow list, the denial is the *absence* of a match,
      # so there is no rule to name.
      assert json(
               dry_run(
                 config,
                 %{"method" => "POST", "path" => "/hooks/pinned", "ip" => "203.0.113.7"}
               )
             ) == %{
               "decision" => "deny",
               "reason" => "ip_denied",
               "route_id" => "pinned",
               "ip_rule" => nil
             }
    end

    test "names the field for a bad request", %{config: config} do
      response = dry_run(config, %{"method" => "POST", "path" => "/hooks/s", "ip" => "nope"})

      assert response.status == 400

      assert %{"error" => "invalid_request", "field" => "ip"} = json(response)
    end
  end

  describe "the listener" do
    test "binds its own port" do
      port = free_port()

      # `free_port/0` closes the socket it opened, so the OS can hand the same
      # number out twice; this test is about two different listeners.
      ingest_port = Enum.find(Stream.repeatedly(&free_port/0), &(&1 != port))

      config = start([admin: [port: port]], port: ingest_port)

      assert config.routes.admin.port == port
      assert config.port == ingest_port

      response = Req.get!("http://127.0.0.1:#{port}/health")
      assert response.status == 200
      assert response.body == %{"status" => "ok", "routes" => 0}

      created =
        Req.post!("http://127.0.0.1:#{port}/admin/routes", body: ~s({"id":"a","path":"/hooks/a"}))

      assert created.status == 201
      assert created.body["id"] == "a"

      # The two listeners serve different surfaces, which is the point of the
      # second port: `/health` answers a different body on each, and the
      # management API is not on the ingest port at all — `/admin/routes` there
      # goes through the capture guard, so it answers a 404 and no route with it.
      ingest_health = Req.get!("http://127.0.0.1:#{ingest_port}/health")
      assert ingest_health.status == 200
      assert %{"status" => "ok", "instance" => _} = ingest_health.body
      refute Map.has_key?(ingest_health.body, "routes")

      ingested =
        Req.post!("http://127.0.0.1:#{ingest_port}/admin/routes",
          body: ~s({"id":"b","path":"/hooks/b"})
        )

      assert ingested.status == 404

      listed = Req.get!("http://127.0.0.1:#{port}/admin/routes")
      assert Enum.map(listed.body["routes"], & &1["id"]) == ["a"]
    end
  end

  describe "authentication and scopes" do
    # Every documented endpoint, with whether it reads (`routes:read`) or
    # writes (`routes:write`).
    @endpoints [
      {:get, "/admin/routes", "routes:read"},
      {:post, "/admin/routes", "routes:write"},
      {:post, "/admin/routes/test", "routes:read"},
      {:get, "/admin/routes/a", "routes:read"},
      {:put, "/admin/routes/a", "routes:write"},
      {:patch, "/admin/routes/a", "routes:write"},
      {:delete, "/admin/routes/a", "routes:write"},
      {:get, "/admin/ip-rules", "routes:read"},
      {:put, "/admin/ip-rules", "routes:write"}
    ]

    defp token(id, scopes), do: %{id: id, sha256: Token.hash(id), scopes: scopes}

    setup do
      tokens = [
        token("operator", ["routes:read", "routes:write", "tenant:*"]),
        token("reader", ["routes:read", "tenant:*"]),
        token("writer", ["routes:write", "tenant:*"]),
        token("acme", ["routes:read", "routes:write", "tenant:acme"])
      ]

      %{config: start(admin: [auth: %{mode: :token, tokens: tokens}])}
    end

    test "401 for every endpoint without a credential, and for a wrong token", %{config: config} do
      for {method, path, _scope} <- @endpoints, token <- [nil, "guess"] do
        response = call(config, method, path, token: token, body: "{}")
        assert response.status == 401, "#{method} #{path} answered #{response.status}"

        assert Plug.Conn.get_resp_header(response, "www-authenticate") == [
                 ~s(Bearer realm="ankusa")
               ]
      end

      # Not even a 404 for a path that does not exist.
      assert call(config, :get, "/nowhere").status == 401
      assert call(config, :get, "/nowhere", token: "operator").status == 404
    end

    test "/health needs no credential", %{config: config} do
      assert call(config, :get, "/health").status == 200
    end

    test "403 names the scope a token lacks: reading is not writing", %{config: config} do
      for {method, path, scope} <- @endpoints do
        lacking = if scope == "routes:read", do: "writer", else: "reader"
        response = call(config, method, path, token: lacking, body: "{}")

        assert response.status == 403,
               "#{lacking} on #{method} #{path} answered #{response.status}"

        assert %{"error" => "forbidden", "scope" => ^scope} = json(response)
      end
    end

    test "a token for one tenant is refused: routes belong to no tenant, so tenant:* is required",
         %{config: config} do
      for {method, path, _scope} <- @endpoints do
        response = call(config, method, path, token: "acme", body: "{}")
        assert response.status == 403, "#{method} #{path} answered #{response.status}"
        assert %{"scope" => "tenant:*"} = json(response)
      end
    end

    test "the right scopes get the real answers", %{config: config} do
      created =
        call(config, :post, "/admin/routes",
          token: "operator",
          body: ~s({"id":"a","path":"/hooks/a"})
        )

      assert created.status == 201

      assert call(config, :get, "/admin/routes/a", token: "reader").status == 200
      assert call(config, :get, "/admin/routes", token: "reader").status == 200
      assert call(config, :get, "/admin/ip-rules", token: "reader").status == 200

      # The dry run is a POST that changes nothing: a reader may run it.
      dry =
        call(config, :post, "/admin/routes/test",
          token: "reader",
          body: ~s({"method":"POST","path":"/hooks/a","ip":"203.0.113.7"})
        )

      assert dry.status == 200

      assert call(config, :patch, "/admin/routes/a", token: "writer", body: ~s({"enabled":false})).status ==
               200

      assert call(config, :delete, "/admin/routes/a", token: "writer").status == 204
    end

    test "the tenant surface needs tenant:<t> for its own tenant, and no more", %{
      config: config
    } do
      created =
        call(config, :post, "/v1/tenants/acme/routes",
          token: "acme",
          body: ~s({"id":"a","path":"/hooks/a","source_id":"acme.billing"})
        )

      assert created.status == 201
      assert %{"id" => "a", "tenant_id" => "acme", "source_id" => "acme.billing"} = json(created)

      assert call(config, :get, "/v1/tenants/acme/routes/a", token: "acme").status == 200

      assert %{"routes" => [%{"id" => "a"}]} =
               json(call(config, :get, "/v1/tenants/acme/routes", token: "acme"))

      assert call(config, :put, "/v1/tenants/acme/routes/a",
               token: "acme",
               body: ~s({"path":"/hooks/a2"})
             ).status == 200

      assert call(config, :patch, "/v1/tenants/acme/routes/a",
               token: "acme",
               body: ~s({"enabled":false})
             ).status == 200

      assert call(config, :delete, "/v1/tenants/acme/routes/a", token: "acme").status == 204

      # tenant:* reaches every tenant's surface; reading is not writing there too.
      assert call(config, :get, "/v1/tenants/globex/routes", token: "reader").status == 200

      assert call(config, :post, "/v1/tenants/globex/routes", token: "reader", body: "{}").status ==
               403

      assert call(config, :get, "/v1/tenants/globex/routes", token: "operator").status == 200
    end

    test "cross-tenant route CRUD is 403", %{config: config} do
      for {method, path} <- [
            {:get, "/v1/tenants/globex/routes"},
            {:post, "/v1/tenants/globex/routes"},
            {:get, "/v1/tenants/globex/routes/x"},
            {:put, "/v1/tenants/globex/routes/x"},
            {:patch, "/v1/tenants/globex/routes/x"},
            {:delete, "/v1/tenants/globex/routes/x"}
          ] do
        response = call(config, method, path, token: "acme", body: "{}")
        assert response.status == 403, "#{method} #{path} answered #{response.status}"
        assert %{"error" => "forbidden", "scope" => "tenant:globex"} = json(response)
      end

      # And the operator surface, which spans tenants, stays tenant:* only.
      assert call(config, :get, "/admin/routes", token: "acme").status == 403
    end

    test "a route another tenant owns is a 404 on the tenant surface, and stays untouched",
         %{config: config} do
      operator = fn method, path, body ->
        call(config, method, path, token: "operator", body: body)
      end

      assert operator.(
               :post,
               "/admin/routes",
               ~s({"id":"g","path":"/hooks/g","tenant_id":"globex"})
             ).status == 201

      assert call(config, :get, "/v1/tenants/acme/routes/g", token: "acme").status == 404
      assert call(config, :delete, "/v1/tenants/acme/routes/g", token: "acme").status == 404

      assert call(config, :patch, "/v1/tenants/acme/routes/g",
               token: "acme",
               body: ~s({"enabled":false})
             ).status == 404

      assert %{"routes" => []} =
               json(call(config, :get, "/v1/tenants/acme/routes", token: "acme"))

      # An id or a path the other tenant holds is a 409 that does not say whose.
      for body <- [~s({"id":"g","path":"/hooks/new"}), ~s({"id":"n","path":"/hooks/g"})] do
        response = call(config, :post, "/v1/tenants/acme/routes", token: "acme", body: body)
        assert response.status == 409
        assert json(response) == %{"error" => "duplicate_route"}
      end

      # A body naming another tenant is a 400, not a quiet move.
      response =
        call(config, :post, "/v1/tenants/acme/routes",
          token: "acme",
          body: ~s({"path":"/hooks/n","tenant_id":"globex"})
        )

      assert response.status == 400
      assert %{"error" => "invalid_route", "field" => "tenant_id"} = json(response)

      # The operator still sees it, owned by globex, untouched.
      assert %{"tenant_id" => "globex", "enabled" => true} =
               json(operator.(:get, "/admin/routes/g", nil))
    end

    test "a tenant that is not an identity is a 400 invalid_tenant", %{config: config} do
      for {method, path} <- [
            {:get, "/v1/tenants/a.b/routes"},
            {:get, "/v1/tenants/a.b/routes/x"},
            {:delete, "/v1/tenants/a.b/routes/x"}
          ] do
        response = call(config, method, path, token: "operator")
        assert response.status == 400
        assert json(response) == %{"error" => "invalid_tenant"}
      end
    end
  end
end
