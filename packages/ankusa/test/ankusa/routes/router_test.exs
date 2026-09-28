defmodule Ankusa.Routes.RouterTest do
  @moduledoc """
  The management API over `Plug.Test.conn`, plus one test over a real socket:
  the in-process cases pin the contract, and the socket case proves the listener
  actually binds.

  `async: false` because the socket test binds a port and the API reads the
  instance's `:persistent_term` config.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Routes.Router

  defp start(routes_opts) do
    {admin, routes_opts} = Keyword.pop(routes_opts, :admin, [])

    config =
      test_config(
        roles: [:edge],
        routes:
          routes_opts
          |> Keyword.merge(enabled: true)
          # Port 0 unless a test asks for a specific one: two instances in one VM
          # would otherwise both want the default and the second would fail to
          # bind.
          |> Keyword.put(:admin, Keyword.merge([port: 0], admin))
      )

    start_supervised!({Ankusa.Instance, config})
    config
  end

  defp call(config, method, path, opts \\ []) do
    body = Keyword.get(opts, :body)
    conn = Plug.Test.conn(method, path, body)
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

    test "POST, GET, PUT, PATCH, DELETE", %{config: config} do
      created = call(config, :post, "/admin/routes", body: ~s({"id":"a","path":"/hooks/a"}))
      assert created.status == 201

      assert %{"id" => "a", "path" => "/hooks/a", "methods" => ["POST"], "enabled" => true} =
               body = json(created)

      assert body["ip_rules"] == []
      assert body["metadata"] == %{}
      assert is_binary(body["inserted_at"])
      assert is_binary(body["updated_at"])

      fetched = call(config, :get, "/admin/routes/a")
      assert fetched.status == 200
      assert json(fetched) == body

      # PUT is idempotent, and re-stating the route keeps its history.
      replaced = call(config, :put, "/admin/routes/a", body: ~s({"path":"/hooks/moved"}))
      assert replaced.status == 200
      assert json(replaced)["path"] == "/hooks/moved"
      assert json(replaced)["inserted_at"] == body["inserted_at"]

      again = call(config, :put, "/admin/routes/a", body: ~s({"path":"/hooks/moved"}))
      assert again.status == 200
      assert json(again)["inserted_at"] == body["inserted_at"]
      assert json(again)["path"] == "/hooks/moved"

      patched = call(config, :patch, "/admin/routes/a", body: ~s({"enabled":false}))
      assert patched.status == 200
      assert json(patched)["enabled"] == false
      assert json(patched)["path"] == "/hooks/moved"
      assert json(patched)["inserted_at"] == body["inserted_at"]

      deleted = call(config, :delete, "/admin/routes/a")
      assert deleted.status == 204
      assert deleted.resp_body == ""

      assert call(config, :get, "/admin/routes/a").status == 404
      assert json(call(config, :delete, "/admin/routes/a")) == %{"error" => "not_found"}
      assert call(config, :get, "/admin/nope").status == 404
    end

    test "PUT creates a route that does not exist", %{config: config} do
      assert call(config, :put, "/admin/routes/fresh", body: ~s({"path":"/hooks/fresh"})).status ==
               200

      assert json(call(config, :get, "/admin/routes/fresh"))["path"] == "/hooks/fresh"
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
    test "GET returns the stored rules and PUT replaces them" do
      config = start(seed: [%{"id" => "s", "path" => "/hooks/s"}])

      assert json(call(config, :get, "/admin/ip-rules")) == %{"default" => "allow", "rules" => []}

      put =
        call(config, :put, "/admin/ip-rules",
          body: ~s({"default":"deny","rules":[{"action":"allow","cidr":"10.0.0.0/8"}]})
        )

      assert put.status == 200

      assert json(put) == %{
               "default" => "deny",
               "rules" => [%{"action" => "allow", "cidr" => "10.0.0.0/8"}]
             }

      assert json(call(config, :get, "/admin/ip-rules")) == json(put)
    end

    test "a bad rule is a 400 naming the field" do
      config = start([])

      response =
        call(config, :put, "/admin/ip-rules",
          body: ~s({"rules":[{"action":"allow","cidr":"nope"}]})
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

    test "reports a match", %{config: config} do
      response = dry_run(config, %{"method" => "POST", "path" => "/hooks/s", "ip" => "1.2.3.4"})

      assert response.status == 200

      assert json(response) == %{
               "decision" => "allow",
               "reason" => "matched",
               "route_id" => "s",
               "ip_rule" => nil
             }
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
    test "binds its own port, unauthenticated by design" do
      port = free_port()

      config = start(admin: [port: port])

      assert config.routes.admin.port == port

      response = Req.get!("http://127.0.0.1:#{port}/health")
      assert response.status == 200
      assert response.body == %{"status" => "ok", "routes" => 0}

      created =
        Req.post!("http://127.0.0.1:#{port}/admin/routes", body: ~s({"id":"a","path":"/hooks/a"}))

      assert created.status == 201
      assert created.body["id"] == "a"

      # The ingest listener is a different port and is untouched by this one.
      assert config.port != port
    end
  end

  # A free port from a closed listener: the same shape the plan's smoke test
  # uses, and the only way to know the port before Bandit takes it.
  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end
end
