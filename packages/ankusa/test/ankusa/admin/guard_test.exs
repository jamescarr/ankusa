defmodule Ankusa.Admin.GuardTest do
  @moduledoc """
  `Ankusa.Admin.Guard` in front of both listeners: who gets in, what each role
  may do, and that the guard and the router can never disagree about a path.
  The verifier is a table of tokens (`TokenTable`), so these tests are about the
  guard, `Ankusa.Auth` and the role table, not about JWTs.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Admin.Router, as: AdminRouter
  alias Ankusa.Routes.Router, as: RoutesRouter
  alias Ankusa.TestHelpers.TokenTable

  @scim_token String.duplicate("s", 40)

  @tokens %{
    "viewer" => %{"sub" => "u-viewer", "groups" => ["Viewers"]},
    "admin" => %{"sub" => "u-admin", "groups" => ["Admins"]},
    "acme" => %{"sub" => "u-acme", "groups" => ["AcmeAdmins"]},
    "stranger" => %{"sub" => "u-stranger", "groups" => ["Nobody"]},
    "ci" => %{"sub" => "ci"},
    "jane" => %{"sub" => "s-jane", "preferred_username" => "JANE@corp.com"},
    "ghost" => %{"sub" => "s-ghost", "preferred_username" => "ghost@corp.com"}
  }

  @bindings [
    %{group: "Viewers", role: :viewer},
    %{group: "Admins", role: :admin},
    %{group: "AcmeAdmins", role: :admin, tenants: ["acme"]},
    %{subject: "ci", role: :viewer}
  ]

  defp start(overrides \\ []) do
    config =
      test_config(
        Keyword.merge(
          [
            roles: [:edge, :dispatch],
            admin: %{enabled: true, port: 0},
            source_store: {Ankusa.SourceStore.Persistent, decoder: &__MODULE__.decoder/2},
            routes: %{enabled: true, admin: %{port: 0}},
            auth: %{
              enabled: true,
              verifier: {TokenTable, tokens: @tokens},
              bindings: @bindings
            }
          ],
          overrides
        )
      )

    start_supervised!({Ankusa.Instance, config})
    config
  end

  defp call(router, config, method, path, opts) do
    conn = Plug.Test.conn(method, path, Keyword.get(opts, :body, ""))

    conn =
      case Keyword.get(opts, :token) do
        nil -> conn
        token -> Plug.Conn.put_req_header(conn, "authorization", "Bearer " <> token)
      end

    router.call(conn, router.init(instance: config.instance))
  end

  defp admin(config, method, path, opts \\ []), do: call(AdminRouter, config, method, path, opts)

  defp routes(config, method, path, opts \\ []),
    do: call(RoutesRouter, config, method, path, opts)

  defp json(conn), do: JSON.decode!(conn.resp_body)

  defp source_body(name), do: JSON.encode!(%{"name" => name, "sinks" => [%{"type" => "log"}]})

  # Stands in for `AnkusaServer.Config.source_from_map!/2`: the store only cares
  # that the decoder returns source options or raises.
  def decoder(_source_id, %{"sinks" => [_ | _] = sinks}),
    do: [sinks: Enum.map(sinks, fn %{"type" => "log"} -> {Ankusa.Sink.Log, []} end)]

  def decoder(_source_id, _spec), do: raise(ArgumentError, "sinks must be a non-empty list")

  describe "authentication" do
    setup do
      %{config: start()}
    end

    test "no credentials is 401 with a challenge", %{config: config} do
      conn = admin(config, :get, "/v1/config")

      assert conn.status == 401
      assert json(conn) == %{"error" => "unauthenticated"}
      assert Plug.Conn.get_resp_header(conn, "www-authenticate") == [~s(Bearer realm="ankusa")]
    end

    test "a header that is not a bearer token is unauthenticated", %{config: config} do
      conn =
        Plug.Test.conn(:get, "/v1/config")
        |> Plug.Conn.put_req_header("authorization", "Basic dXNlcjpwYXNz")
        |> AdminRouter.call(AdminRouter.init(instance: config.instance))

      assert conn.status == 401
      assert json(conn) == %{"error" => "unauthenticated"}
    end

    test "a token the verifier rejects is 401 invalid_token", %{config: config} do
      conn = admin(config, :get, "/v1/config", token: "forged")

      assert conn.status == 401
      assert json(conn) == %{"error" => "invalid_token"}

      assert Plug.Conn.get_resp_header(conn, "www-authenticate") ==
               [~s(Bearer realm="ankusa", error="invalid_token")]
    end

    test "the bearer scheme is case-insensitive", %{config: config} do
      conn =
        Plug.Test.conn(:get, "/v1/config")
        |> Plug.Conn.put_req_header("authorization", "bEaReR viewer")
        |> AdminRouter.call(AdminRouter.init(instance: config.instance))

      assert conn.status == 200
    end

    test "a verifier that cannot decide is 503, never 401", %{config: config} do
      conn = admin(config, :get, "/v1/config", token: "down")

      assert conn.status == 503
      assert json(conn) == %{"error" => "auth_unavailable"}
      assert Plug.Conn.get_resp_header(conn, "retry-after") == ["1"]
    end

    test "health and readiness need no token on either listener", %{config: config} do
      assert admin(config, :get, "/health").status == 200
      assert admin(config, :get, "/ready").status in [200, 503]
      assert routes(config, :get, "/health").status == 200
    end

    test "metrics need viewer", %{config: config} do
      assert admin(config, :get, "/metrics").status == 401
      assert admin(config, :get, "/metrics", token: "viewer").status == 200
    end

    test "a valid token bound to nothing is forbidden, not unauthenticated", %{config: config} do
      conn = admin(config, :get, "/v1/config", token: "stranger")

      assert conn.status == 403
      assert json(conn) == %{"error" => "forbidden", "required_role" => "viewer"}
    end

    test "a subject binding grants a token with no groups", %{config: config} do
      assert admin(config, :get, "/v1/config", token: "ci").status == 200
    end
  end

  describe "roles" do
    setup do
      %{config: start()}
    end

    test "a viewer reads but does not write", %{config: config} do
      assert admin(config, :get, "/v1/config", token: "viewer").status == 200
      assert admin(config, :get, "/v1/dlq", token: "viewer").status == 200

      conn = admin(config, :post, "/v1/replays", token: "viewer", body: ~s({"kind":"dlq"}))
      assert conn.status == 403
      assert json(conn) == %{"error" => "forbidden", "required_role" => "operator"}
    end

    test "source management needs admin", %{config: config} do
      conn =
        admin(config, :post, "/v1/tenants/acme/sources", token: "viewer", body: source_body("a"))

      assert conn.status == 403
      assert json(conn)["required_role"] == "admin"

      assert admin(config, :get, "/v1/tenants/acme/sources", token: "viewer").status == 200

      assert admin(config, :post, "/v1/tenants/acme/sources",
               token: "admin",
               body: source_body("a")
             ).status == 201
    end

    test "a tenant-scoped admin manages only its tenant", %{config: config} do
      assert admin(config, :post, "/v1/tenants/acme/sources",
               token: "acme",
               body: source_body("stripe")
             ).status == 201

      assert admin(config, :get, "/v1/tenants/acme/sources/stripe", token: "acme").status == 200

      conn =
        admin(config, :post, "/v1/tenants/globex/sources",
          token: "acme",
          body: source_body("stripe")
        )

      assert conn.status == 403

      # reading a tenant it does not hold is refused too
      assert admin(config, :get, "/v1/tenants/globex/sources", token: "acme").status == 403
    end

    test "a tenant-scoped admin has no platform-wide rights", %{config: config} do
      # a rate limit is a platform control even though its path names a tenant
      body = ~s({"rate": 10, "burst": 10})

      assert admin(config, :put, "/v1/tenants/acme/rate-limit", token: "acme", body: body).status ==
               403

      assert admin(config, :put, "/v1/tenants/acme/rate-limit", token: "admin", body: body).status ==
               200

      assert admin(config, :get, "/v1/dlq", token: "acme").status == 403
      assert admin(config, :get, "/v1/config", token: "acme").status == 403
    end

    test "the routes listener: reads for viewers, writes for admins", %{config: config} do
      body = ~s({"id":"a","path":"/hooks/a"})

      assert routes(config, :get, "/admin/routes", token: "viewer").status == 200
      assert routes(config, :post, "/admin/routes", token: "viewer", body: body).status == 403
      assert routes(config, :post, "/admin/routes", token: "admin", body: body).status == 201
      assert routes(config, :post, "/admin/routes", token: "acme", body: body).status == 403
      assert routes(config, :delete, "/admin/routes/a", token: "viewer").status == 403
      assert routes(config, :put, "/admin/ip-rules", token: "viewer", body: "{}").status == 403
    end

    test "a dry run changes nothing, so a viewer may run it", %{config: config} do
      body = ~s({"method":"POST","path":"/hooks/a","ip":"1.2.3.4"})

      assert routes(config, :post, "/admin/routes/test", token: "viewer", body: body).status ==
               200
    end

    test "a percent-encoded path cannot dodge the role of the route it reaches", %{config: config} do
      # The router matches on the decoded path, so `replay%73` is `/v1/replays`.
      conn = admin(config, :post, "/v1/replay%73", token: "viewer", body: ~s({"kind":"dlq"}))
      assert conn.status == 403
      assert json(conn)["required_role"] == "operator"

      conn =
        admin(config, :put, "/v1/tenants/globex/source%73/x", token: "viewer", body: "{}")

      assert conn.status == 403

      conn = routes(config, :post, "/admin/route%73", token: "viewer", body: ~s({"id":"a"}))
      assert conn.status == 403

      conn = admin(config, :get, "/v1/tenants/globex/rate%2Dlimit", token: "acme")
      assert conn.status == 403
    end

    test "a path no route answers is 404 once authenticated, and still needs a token", %{
      config: config
    } do
      assert admin(config, :get, "/v1/nope").status == 401
      assert admin(config, :get, "/v1/nope", token: "viewer").status == 404
      assert admin(config, :delete, "/v1/nope", token: "viewer").status == 404
    end
  end

  describe "auth disabled" do
    test "every route answers without a header, as before auth existed" do
      config = start(auth: %{enabled: false})

      for path <- ["/health", "/metrics", "/v1/config", "/v1/dlq", "/v1/tenants/acme/sources"] do
        conn = admin(config, :get, path)
        assert conn.status == 200, "#{path} answered #{conn.status}"
      end

      assert admin(config, :post, "/v1/tenants/acme/sources", body: source_body("a")).status ==
               201

      assert routes(config, :post, "/admin/routes", body: ~s({"id":"a","path":"/hooks/a"})).status ==
               201
    end
  end

  describe "users from the SCIM directory" do
    setup do
      config =
        start(
          scim: %{enabled: true, token: @scim_token},
          auth: %{
            enabled: true,
            verifier: {TokenTable, tokens: @tokens},
            bindings: @bindings ++ [%{group: "Ops", role: :operator}]
          }
        )

      %{config: config}
    end

    defp provision(config, user_name, attrs \\ %{}) do
      {:ok, user} =
        Ankusa.Directory.create_user(
          config.instance,
          Map.merge(%{"userName" => user_name, "active" => true}, attrs)
        )

      user
    end

    test "a token matching no provisioned user is forbidden", %{config: config} do
      conn = admin(config, :get, "/v1/config", token: "ghost")
      assert conn.status == 403
      assert json(conn) == %{"error" => "user_not_provisioned"}
    end

    test "a deactivated user is refused while their token is still valid", %{config: config} do
      provision(config, "jane@corp.com", %{"active" => false})

      conn = admin(config, :get, "/v1/config", token: "jane")
      assert conn.status == 403
      assert json(conn) == %{"error" => "user_inactive"}
    end

    test "the username claim matches case-insensitively, and groups come from the directory", %{
      config: config
    } do
      user = provision(config, "jane@corp.com")

      # provisioned but in no bound group: authenticated, authorized for nothing
      conn = admin(config, :get, "/v1/config", token: "jane")
      assert conn.status == 403
      assert json(conn)["error"] == "forbidden"

      {:ok, ops} =
        Ankusa.Directory.create_group(config.instance, %{"displayName" => "Ops"}, [user["id"]])

      assert ops["displayName"] == "Ops"

      # Ops is bound to operator: replays yes, source management no
      assert admin(config, :post, "/v1/replays", token: "jane", body: ~s({"kind":"dlq"})).status ==
               202

      assert admin(config, :get, "/v1/config", token: "jane").status == 200

      assert admin(config, :post, "/v1/tenants/acme/sources",
               token: "jane",
               body: source_body("a")
             ).status == 403
    end

    test "leaving the group takes the access with it", %{config: config} do
      user = provision(config, "jane@corp.com")

      {:ok, ops} =
        Ankusa.Directory.create_group(config.instance, %{"displayName" => "Ops"}, [user["id"]])

      assert admin(config, :get, "/v1/dlq", token: "jane").status == 200

      :ok = Ankusa.Directory.remove_members(config.instance, ops["id"], [user["id"]])
      assert admin(config, :get, "/v1/dlq", token: "jane").status == 403
    end

    test "a subject binding works with no directory user", %{config: config} do
      assert admin(config, :get, "/v1/config", token: "ci").status == 200
    end

    test "matching by externalId is exact" do
      config =
        start(
          scim: %{enabled: true, token: @scim_token},
          auth: %{
            enabled: true,
            verifier: {TokenTable, tokens: @tokens},
            user_match: %{claim: "sub", attribute: :external_id},
            bindings: [%{group: "Ops", role: :operator}]
          }
        )

      user = provision(config, "someone@corp.com", %{"externalId" => "s-jane"})

      {:ok, _} =
        Ankusa.Directory.create_group(config.instance, %{"displayName" => "Ops"}, [user["id"]])

      assert admin(config, :get, "/v1/config", token: "jane").status == 200
      assert admin(config, :get, "/v1/config", token: "ghost").status == 403
    end
  end

  describe "the SCIM client" do
    setup do
      config =
        start(
          scim: %{enabled: true, token: @scim_token},
          auth: %{
            enabled: true,
            verifier: {TokenTable, tokens: @tokens},
            bindings: @bindings ++ [%{group: "Ops", role: :operator}]
          }
        )

      %{config: config}
    end

    test "a wrong token is a SCIM 401", %{config: config} do
      for token <- [nil, "short", String.duplicate("x", 40), "viewer"] do
        conn = admin(config, :get, "/scim/v2/Users", token: token)

        assert conn.status == 401
        assert [content_type] = Plug.Conn.get_resp_header(conn, "content-type")
        assert content_type =~ "application/scim+json"

        assert %{
                 "schemas" => ["urn:ietf:params:scim:api:messages:2.0:Error"],
                 "status" => "401"
               } = json(conn)
      end
    end

    test "the SCIM token opens /scim/v2 and nothing else", %{config: config} do
      assert admin(config, :get, "/scim/v2/Users", token: @scim_token).status == 200
      assert admin(config, :get, "/v1/config", token: @scim_token).status == 401
      assert admin(config, :get, "/metrics", token: @scim_token).status == 401
    end

    test "a user token is not a SCIM token", %{config: config} do
      assert admin(config, :post, "/scim/v2/Users",
               token: "admin",
               body: ~s({"userName":"x@corp.com"})
             ).status == 401
    end

    test "an unknown SCIM path is a 404 for the client and a 401 for anyone else", %{
      config: config
    } do
      assert admin(config, :get, "/scim/v2/Nope", token: @scim_token).status == 404
      assert admin(config, :get, "/scim/v2/Nope").status == 401
    end

    test "Entra deactivating a user takes their access away", %{config: config} do
      created =
        admin(config, :post, "/scim/v2/Users",
          token: @scim_token,
          body: ~s({"userName":"jane@corp.com","active":true})
        )

      assert created.status == 201
      id = json(created)["id"]

      group =
        admin(config, :post, "/scim/v2/Groups",
          token: @scim_token,
          body: JSON.encode!(%{"displayName" => "Ops", "members" => [%{"value" => id}]})
        )

      assert group.status == 201
      assert admin(config, :get, "/v1/dlq", token: "jane").status == 200

      patch =
        admin(config, :patch, "/scim/v2/Users/#{id}",
          token: @scim_token,
          body:
            JSON.encode!(%{
              "schemas" => ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
              "Operations" => [%{"op" => "Replace", "path" => "active", "value" => "False"}]
            })
        )

      assert patch.status == 200
      assert json(patch)["active"] == false

      conn = admin(config, :get, "/v1/dlq", token: "jane")
      assert conn.status == 403
      assert json(conn) == %{"error" => "user_inactive"}
    end
  end

  describe "SCIM off" do
    test "/scim/v2 is a plain 404 whatever token it carries" do
      config = start()

      assert admin(config, :get, "/scim/v2/Users").status == 404
      assert admin(config, :get, "/scim/v2/Users", token: @scim_token).status == 404
    end
  end
end
