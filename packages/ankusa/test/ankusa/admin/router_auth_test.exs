defmodule Ankusa.Admin.RouterAuthTest do
  @moduledoc """
  Authentication and scopes on every route of `Ankusa.Admin.Router`: one table
  of the routes and the scope each needs, driven four ways (no credential, a
  token lacking exactly that scope, a token for the wrong tenant, and a token
  with everything), so a route added without a scope check fails here.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Admin.Auth.Token
  alias Ankusa.Admin.Router
  alias Ankusa.Principal

  # {method, path, scope, tenancy}: `:open` routes are not about a tenant,
  # `:filtered` ones hold every tenant's rows and are limited to the caller's
  # tenants (`tenant:*` for all of them), `{:tenant, t}` ones belong to tenant
  # `t`.
  @routes [
    {:get, "/metrics", "metrics:read", :open},
    {:get, "/v1/wal", "metrics:read", :open},
    {:get, "/v1/config", "config:read", :open},
    {:get, "/asyncapi.json", "config:read", :open},
    {:get, "/v1/dlq", "dlq:read", :filtered},
    {:post, "/v1/replays", "replays:write", :filtered},
    {:get, "/v1/replays", "replays:read", :filtered},
    {:get, "/v1/replays/nope", "replays:read", :filtered},
    {:patch, "/v1/replays/nope", "replays:write", :filtered},
    {:get, "/v1/quarantine", "quarantine:read", :filtered},
    {:delete, "/v1/quarantine", "quarantine:write", :filtered},
    {:get, "/v1/tenants/acme/sources", "sources:read", {:tenant, "acme"}},
    {:post, "/v1/tenants/acme/sources", "sources:write", {:tenant, "acme"}},
    {:get, "/v1/tenants/acme/sources/billing", "sources:read", {:tenant, "acme"}},
    {:put, "/v1/tenants/acme/sources/billing", "sources:write", {:tenant, "acme"}},
    {:delete, "/v1/tenants/acme/sources/billing", "sources:write", {:tenant, "acme"}},
    {:get, "/v1/rate-limits", "rate_limits:read", :filtered},
    {:get, "/v1/tenants/acme/rate-limit", "rate_limits:read", {:tenant, "acme"}},
    {:put, "/v1/tenants/acme/rate-limit", "rate_limits:write", {:tenant, "acme"}},
    {:delete, "/v1/tenants/acme/rate-limit", "rate_limits:write", {:tenant, "acme"}}
  ]

  @every_scope Principal.scopes()

  defp token(id, plain, scopes), do: %{id: id, sha256: Token.hash(plain), scopes: scopes}

  setup do
    missing_one =
      for scope <- Enum.uniq(for {_m, _p, scope, _t} <- @routes, do: scope) do
        token(
          "without-#{scope}",
          "without-#{scope}",
          List.delete(@every_scope, scope) ++ ["tenant:*"]
        )
      end

    tokens =
      [
        token("operator", "operator", @every_scope ++ ["tenant:*"]),
        token("acme", "acme", @every_scope ++ ["tenant:acme"]),
        token("globex", "globex", @every_scope ++ ["tenant:globex"]),
        token("tenantless", "tenantless", @every_scope),
        token("scopeless", "scopeless", ["tenant:*", "claims:read"]),
        token("replayer", "replayer", ["replays:write", "tenant:*"])
      ] ++ missing_one

    config =
      test_config(
        roles: [:edge, :dispatch],
        admin: %{enabled: true, port: 0, auth: %{mode: :token, tokens: tokens}}
      )

    put_config(config)
    start_supervised!({Ankusa.Instance, config})
    %{inst: config.instance}
  end

  defp call(inst, {method, path, _scope, _tenancy}, token) do
    conn = Plug.Test.conn(method, path, "{}")

    conn =
      if token,
        do: Plug.Conn.put_req_header(conn, "authorization", "Bearer " <> token),
        else: conn

    Router.call(conn, Router.init(instance: inst))
  end

  defp call(inst, method, path, token), do: call(inst, {method, path, nil, nil}, token)

  # A request naming `tenant`: a query parameter, or the body key of a replay
  # start (`POST /v1/replays` reads its tenant from the body).
  defp call_naming(inst, {:post, "/v1/replays", _s, _t}, tenant, token) do
    conn =
      Plug.Test.conn(:post, "/v1/replays", JSON.encode!(%{kind: "dlq", tenant: tenant}))
      |> Plug.Conn.put_req_header("authorization", "Bearer " <> token)

    Router.call(conn, Router.init(instance: inst))
  end

  defp call_naming(inst, {method, path, scope, tenancy}, tenant, token),
    do: call(inst, {method, path <> "?tenant=" <> tenant, scope, tenancy}, token)

  describe "401" do
    test "every route but /health and /ready refuses a request with no credential", %{inst: inst} do
      for {method, path, _scope, _tenancy} = route <- @routes do
        conn = call(inst, route, nil)
        assert conn.status == 401, "#{method} #{path} answered #{conn.status}"
        assert Plug.Conn.get_resp_header(conn, "www-authenticate") == [~s(Bearer realm="ankusa")]
        assert %{"error" => "unauthenticated"} = JSON.decode!(conn.resp_body)
      end
    end

    test "an unknown token is refused the same way, and an unknown path is 401 before it is 404",
         %{inst: inst} do
      for {_m, _p, _s, _t} = route <- @routes,
          do: assert(call(inst, route, "guess").status == 401)

      assert call(inst, :get, "/nope", nil).status == 401
      assert call(inst, :get, "/nope", "operator").status == 404
    end

    test "/health and /ready need no credential", %{inst: inst} do
      assert call(inst, :get, "/health", nil).status == 200
      assert call(inst, :get, "/ready", nil).status in [200, 503]
    end
  end

  describe "403" do
    test "a token without exactly the route's scope is refused, naming the scope", %{inst: inst} do
      for {method, path, scope, _tenancy} = route <- @routes do
        conn = call(inst, route, "without-#{scope}")
        assert conn.status == 403, "#{method} #{path} answered #{conn.status}"
        assert %{"error" => "forbidden", "scope" => ^scope} = JSON.decode!(conn.resp_body)
      end
    end

    test "a token with tenant:* and no operation scope is refused everywhere", %{inst: inst} do
      for {method, path, _scope, _tenancy} = route <- @routes do
        assert call(inst, route, "scopeless").status == 403, "#{method} #{path}"
      end
    end

    test "a filtered route needs some tenant, and a tenant-limited token cannot name another",
         %{inst: inst} do
      for {method, path, _scope, :filtered} = route <- @routes do
        conn = call(inst, route, "tenantless")
        assert conn.status == 403, "#{method} #{path} answered #{conn.status}"
        assert %{"scope" => "tenant:*"} = JSON.decode!(conn.resp_body)

        # Its own tenant gets through to the handler (which may still refuse
        # the empty body), another tenant's is 403, naming the tenant.
        refute call(inst, route, "acme").status in [401, 403], "acme on #{method} #{path}"

        conn = call_naming(inst, route, "globex", "acme")
        assert conn.status == 403, "#{method} #{path} answered #{conn.status}"
        assert %{"scope" => "tenant:globex"} = JSON.decode!(conn.resp_body)

        refute call_naming(inst, route, "acme", "acme").status in [401, 403]
        refute call_naming(inst, route, "globex", "operator").status in [401, 403]
      end
    end
  end

  describe "tenant scope" do
    test "a tenant's token reaches its own tenant's routes and no other tenant's", %{inst: inst} do
      for {method, path, _scope, {:tenant, "acme"}} = route <- @routes do
        refute call(inst, route, "acme").status in [401, 403], "acme on #{method} #{path}"

        conn = call(inst, route, "globex")
        assert conn.status == 403, "globex on #{method} #{path} answered #{conn.status}"
        assert %{"scope" => "tenant:acme"} = JSON.decode!(conn.resp_body)
      end
    end

    test "the same holds for a tenant named only in the URL", %{inst: inst} do
      assert call(inst, :get, "/v1/tenants/acme/sources", "acme").status == 200
      assert call(inst, :get, "/v1/tenants/globex/sources", "acme").status == 403
      assert call(inst, :get, "/v1/tenants/globex/sources/billing", "acme").status == 403
      assert call(inst, :get, "/v1/tenants/globex/rate-limit", "acme").status == 403
      assert call(inst, :get, "/v1/tenants/globex/sources", "operator").status == 200
    end

    test "a tenant that is not even a valid name is 403 to a tenant-limited token, 400 to an operator",
         %{inst: inst} do
      assert call(inst, :get, "/v1/tenants/not%20valid/sources", "acme").status == 403
      assert call(inst, :get, "/v1/tenants/not%20valid/sources", "operator").status == 400
    end
  end

  describe "an operator" do
    test "is refused nowhere", %{inst: inst} do
      for {method, path, _scope, _tenancy} = route <- @routes do
        refute call(inst, route, "operator").status in [401, 403], "#{method} #{path}"
      end
    end
  end

  describe "replays of a store need that store's write scope" do
    defp replay(inst, token, kind) do
      conn =
        Plug.Test.conn(:post, "/v1/replays", JSON.encode!(%{kind: kind}))
        |> Plug.Conn.put_req_header("authorization", "Bearer " <> token)
        |> then(&Router.call(&1, Router.init(instance: inst)))

      {conn.status, JSON.decode!(conn.resp_body)}
    end

    test "a dlq replay needs dlq:write, a quarantine replay quarantine:write", %{inst: inst} do
      assert {403, %{"scope" => "dlq:write"}} = replay(inst, "replayer", "dlq")
      assert {403, %{"scope" => "quarantine:write"}} = replay(inst, "replayer", "quarantine")

      assert {202, %{"id" => _}} = replay(inst, "operator", "dlq")
    end

    test "an archive replay needs replays:write and tenant:* only", %{inst: inst} do
      {status, _body} = replay(inst, "replayer", "archive")
      refute status in [401, 403]
    end
  end

  test "/v1/config shows the auth block with every token hash redacted", %{inst: inst} do
    conn = call(inst, :get, "/v1/config", "operator")
    assert conn.status == 200
    assert %{"admin" => %{"auth" => auth}} = JSON.decode!(conn.resp_body)

    assert auth["mode"] == "token"

    assert %{"id" => "operator", "sha256" => "[REDACTED]", "scopes" => scopes} =
             hd(auth["tokens"])

    assert "tenant:*" in scopes
    refute conn.resp_body =~ Token.hash("operator")
  end
end
