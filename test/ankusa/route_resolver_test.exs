defmodule Ankusa.RouteResolverTest do
  use ExUnit.Case, async: false

  import Ankusa.TestHelpers
  alias Ankusa.Edge.Router
  alias Ankusa.{Route, RouteResolver, WAL}

  defp start(resolver, sources) do
    config =
      test_config(
        roles: [:edge],
        route_resolver: resolver,
        source_store: {Ankusa.SourceStore.Static, sources: sources}
      )

    start_supervised!({Ankusa.Instance, config})
    config
  end

  defp post(config, path, body, headers \\ []) do
    Plug.Test.conn(:post, path, body)
    |> then(fn c ->
      Enum.reduce(headers, c, fn {k, v}, c -> Plug.Conn.put_req_header(c, k, v) end)
    end)
    |> Router.call(Router.init(instance: config.instance))
  end

  describe "RouteResolver.Path" do
    test "maps /hooks/:source_id to a route with an unset tenant" do
      conn = Plug.Test.conn(:post, "/hooks/demo", "x")

      assert {:ok, %Route{source_id: "demo", tenant_id: nil}} =
               RouteResolver.Path.resolve(:i, conn, [])
    end

    test "honours a custom prefix" do
      conn = Plug.Test.conn(:post, "/c/demo", "x")

      assert {:ok, %Route{source_id: "demo"}} =
               RouteResolver.Path.resolve(:i, conn, prefix: ["c"])
    end

    test "rejects a path whose segment count doesn't fit the scheme" do
      assert :error = RouteResolver.Path.resolve(:i, Plug.Test.conn(:post, "/hooks/a/b"), [])
      assert :error = RouteResolver.Path.resolve(:i, Plug.Test.conn(:post, "/hooks"), [])
      assert :error = RouteResolver.Path.resolve(:i, Plug.Test.conn(:post, "/other/demo"), [])
    end
  end

  describe "RouteResolver.TenantPath" do
    test "maps /hooks/:tenant/:source to a tenant-scoped route" do
      conn = Plug.Test.conn(:post, "/hooks/acme/stripe", "x")

      assert {:ok, %Route{source_id: "stripe", tenant_id: "acme"}} =
               RouteResolver.TenantPath.resolve(:i, conn, [])
    end

    test "rejects a single-segment path" do
      assert :error = RouteResolver.TenantPath.resolve(:i, Plug.Test.conn(:post, "/hooks/x"), [])
    end
  end

  describe "integration through the edge" do
    test "the URL tenant is threaded onto the envelope and scopes dedup" do
      config =
        start({Ankusa.RouteResolver.TenantPath, []}, %{
          "stripe" => [dedup: {Ankusa.DedupKey.Stripe, []}]
        })

      body = ~s({"id":"evt_1","type":"x"})

      acme_first = post(config, "/hooks/acme/stripe", body)
      globex = post(config, "/hooks/globex/stripe", body)
      acme_again = post(config, "/hooks/acme/stripe", body)

      # same source_id + same event id, but two different tenants → both commit
      assert acme_first.status == 201
      assert globex.status == 201
      # same tenant + same event id → duplicate absorbed, still 2xx
      assert acme_again.status == 200
      assert %{"status" => "duplicate"} = JSON.decode!(acme_again.resp_body)

      envs = WAL.read(config.instance, -1, 10)
      assert length(envs) == 2
      assert Enum.map(envs, & &1.tenant_id) |> Enum.sort() == ["acme", "globex"]
      assert Enum.all?(envs, &(&1.source_id == "stripe"))
    end

    test "a source's own tenant_id is used when the resolver doesn't carry one" do
      config =
        start({Ankusa.RouteResolver.Path, []}, %{
          "demo" => [tenant_id: "customer-42"]
        })

      assert post(config, "/hooks/demo", ~s({"hi":1})).status == 201
      assert [env] = WAL.read(config.instance, -1, 10)
      assert env.tenant_id == "customer-42"
      assert env.source_id == "demo"
    end

    test "an unresolvable URL shape is a 404" do
      config = start({Ankusa.RouteResolver.TenantPath, []}, %{"stripe" => []})
      assert post(config, "/hooks/stripe", "x").status == 404
    end
  end
end
