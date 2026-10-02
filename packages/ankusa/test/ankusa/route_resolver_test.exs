defmodule Ankusa.RouteResolverTest do
  use ExUnit.Case, async: false

  import Ankusa.TestHelpers
  alias Ankusa.Edge.Router
  alias Ankusa.{Route, RouteResolver}

  # A hook is only stored when something is obliged to handle it, so sources
  # without sinks of their own get a log sink (nothing dispatches here, so the
  # delivery row simply stays pending).
  defp start(resolver, sources) do
    sources =
      Map.new(sources, fn {id, opts} ->
        {id, Keyword.put_new(opts, :sinks, [{Ankusa.Sink.Log, []}])}
      end)

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
    test "maps /webhooks/:source_id to a route with an unset tenant" do
      conn = Plug.Test.conn(:post, "/webhooks/demo", "x")

      assert {:ok, %Route{source_id: "demo", tenant_id: nil}} =
               RouteResolver.Path.resolve(:i, conn, [])
    end

    test "honours a custom prefix" do
      conn = Plug.Test.conn(:post, "/c/demo", "x")

      assert {:ok, %Route{source_id: "demo"}} =
               RouteResolver.Path.resolve(:i, conn, prefix: ["c"])
    end

    test "rejects a path whose segment count doesn't fit the scheme" do
      assert :error = RouteResolver.Path.resolve(:i, Plug.Test.conn(:post, "/webhooks/a/b"), [])
      assert :error = RouteResolver.Path.resolve(:i, Plug.Test.conn(:post, "/webhooks"), [])
      assert :error = RouteResolver.Path.resolve(:i, Plug.Test.conn(:post, "/other/demo"), [])
    end
  end

  describe "RouteResolver.TenantPath" do
    test "maps /webhooks/:tenant/:source to a tenant-scoped route" do
      conn = Plug.Test.conn(:post, "/webhooks/acme/stripe", "x")

      assert {:ok, %Route{source_id: "stripe", tenant_id: "acme"}} =
               RouteResolver.TenantPath.resolve(:i, conn, [])
    end

    test "rejects a single-segment path" do
      assert :error =
               RouteResolver.TenantPath.resolve(:i, Plug.Test.conn(:post, "/webhooks/x"), [])
    end
  end

  describe "integration through the edge" do
    test "the URL tenant is threaded onto the envelope" do
      config =
        start({Ankusa.RouteResolver.TenantPath, []}, %{
          "stripe" => []
        })

      body = ~s({"id":"evt_1","type":"x"})

      acme_first = post(config, "/webhooks/acme/stripe", body)
      globex = post(config, "/webhooks/globex/stripe", body)
      acme_again = post(config, "/webhooks/acme/stripe", body)

      assert acme_first.status == 201
      assert globex.status == 201
      assert acme_again.status == 201

      {:ok, envs} = Ankusa.Queue.hooks(config.instance, 0, 10)
      assert length(envs) == 3
      assert Enum.map(envs, & &1.tenant_id) |> Enum.sort() == ["acme", "acme", "globex"]
      assert Enum.all?(envs, &(&1.source_id == "stripe"))
    end

    test "a source's own tenant_id is used when the resolver doesn't carry one" do
      config =
        start({Ankusa.RouteResolver.Path, []}, %{
          "demo" => [tenant_id: "customer-42"]
        })

      assert post(config, "/webhooks/demo", ~s({"hi":1})).status == 201
      assert {:ok, [env]} = Ankusa.Queue.hooks(config.instance, 0, 10)
      assert env.tenant_id == "customer-42"
      assert env.source_id == "demo"
    end

    test "an unresolvable URL shape is a 404" do
      config = start({Ankusa.RouteResolver.TenantPath, []}, %{"stripe" => []})
      assert post(config, "/webhooks/stripe", "x").status == 404
    end

    test "a URL tenant outside the grammar is a 404 and nothing is stored" do
      config = start({Ankusa.RouteResolver.TenantPath, []}, %{"stripe" => []})

      for tenant <- ["ac.me", "ac%2Fme", String.duplicate("a", 65)] do
        assert post(config, "/webhooks/#{tenant}/stripe", "x").status == 404, tenant
      end

      assert stored_ids(config.instance) == []
    end
  end

  test "a source whose static tenant is outside the grammar fails when it is built" do
    assert_raise ArgumentError, ~r/must match \[A-Za-z0-9_-\]\{1,64\}/, fn ->
      Ankusa.Source.new("stripe", tenant_id: "acme corp")
    end
  end
end
