defmodule Ankusa.Edge.RouteGuardTest do
  @moduledoc """
  The guard at the edge, over a real instance and a real WAL.

  The point of every rejection case is the same assertion: **no record**. A
  rejected request must not become a WAL entry, because everything downstream —
  dispatch, delivery, a provider's webhook count — is derived from the WAL.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Edge.Router
  alias Ankusa.Routes
  alias Ankusa.WAL

  defp start(routes_opts) do
    config =
      test_config(
        roles: [:edge],
        source_store:
          {Ankusa.SourceStore.Static,
           sources: %{"demo" => [verifier: {Ankusa.Verifier.None, []}]}},
        routes: Keyword.merge([enabled: true, admin: [port: 0]], routes_opts)
      )

    start_supervised!({Ankusa.Instance, config})
    config
  end

  defp post(config, path, extra_headers \\ []) do
    body = ~s({"hello":"world"})

    conn =
      Plug.Test.conn(:post, path, body)
      |> then(fn conn ->
        Enum.reduce(extra_headers, conn, fn {name, value}, conn ->
          Plug.Conn.put_req_header(conn, name, value)
        end)
      end)

    Router.call(conn, Router.init(instance: config.instance))
  end

  defp records(config), do: WAL.stats(config.instance).records

  describe "routes on" do
    setup do
      config = start(seed: [%{"id" => "demo", "path" => "/webhooks/demo"}])
      %{config: config}
    end

    test "a matching POST is captured and committed", %{config: config} do
      conn = post(config, "/webhooks/demo")

      assert conn.status == 201
      assert %{"status" => "accepted"} = JSON.decode!(conn.resp_body)
      assert records(config) == 1
    end

    test "a non-matching POST is 404 and writes nothing", %{config: config} do
      conn = post(config, "/webhooks/whatever")

      assert conn.status == 404
      assert JSON.decode!(conn.resp_body) == %{"error" => "not_found"}
      assert records(config) == 0
    end

    test "a path that matches a route taking another method is 404 and writes nothing", %{
      config: config
    } do
      # The capture clause is POST-only, so a GET is answered by the router's
      # fallback; an embedder that puts the guard in its own pipeline gets the
      # same 404 from the guard itself (see the direct call below).
      conn =
        Plug.Test.conn(:get, "/webhooks/demo")
        |> Router.call(Router.init(instance: config.instance))

      assert conn.status == 404
      assert records(config) == 0

      {decision, _cached} =
        Ankusa.Routes.authorize_path(
          config.instance,
          "GET",
          ["webhooks", "demo"],
          "/webhooks/demo",
          {32, 0x7F000001}
        )

      assert decision == {:reject, :method}
    end

    test "an encoded slash cannot be smuggled into a segment", %{config: config} do
      # With a wildcard route in place, a decoded `%2F` would otherwise match as
      # "one more segment" and capture a path the sender never had.
      {:ok, _} = Routes.create(config.instance, %{"id" => "w", "path" => "/webhooks/*"})

      conn = post(config, "/webhooks/demo%2Fextra")

      assert conn.status == 404
      assert records(config) == 0
    end

    test "a disabled route rejects even its own path", %{config: config} do
      {:ok, _} = Routes.update(config.instance, "demo", %{"enabled" => false})

      conn = post(config, "/webhooks/demo")

      assert conn.status == 404
      assert records(config) == 0
    end

    test "the route id is assigned for downstream consumers", %{config: config} do
      conn = post(config, "/webhooks/demo")
      assert conn.assigns.ankusa_route == "demo"
    end

    test "telemetry reports the match, the cache hit, and the rejection reasons", %{
      config: config
    } do
      handler = "routes-guard-#{System.unique_integer([:positive])}"
      test_pid = self()

      :telemetry.attach_many(
        handler,
        [[:ankusa, :routes, :match], [:ankusa, :routes, :reject]],
        fn event, _measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      assert post(config, "/webhooks/demo").status == 201

      assert_receive {:telemetry, [:ankusa, :routes, :match],
                      %{instance: instance, route_id: "demo", cached: false}}

      assert instance == config.instance

      assert post(config, "/webhooks/demo").status == 201
      assert_receive {:telemetry, [:ankusa, :routes, :match], %{cached: true}}

      assert post(config, "/webhooks/whatever").status == 404

      assert_receive {:telemetry, [:ankusa, :routes, :reject],
                      %{
                        instance: _instance,
                        reason: :no_route,
                        method: "POST",
                        path: "/webhooks/whatever"
                      }}
    end
  end

  describe "IP rules" do
    test "a globally denied sender is 403 and writes nothing" do
      config =
        start(
          ip_rules: [default: :allow, rules: [%{action: :deny, cidr: "127.0.0.0/8"}]],
          seed: [%{"id" => "demo", "path" => "/webhooks/demo"}]
        )

      conn = post(config, "/webhooks/demo")

      assert conn.status == 403
      assert JSON.decode!(conn.resp_body) == %{"error" => "forbidden"}
      assert records(config) == 0
    end

    test "ip_denied_status: 404 hides the distinction" do
      config =
        start(
          ip_denied_status: 404,
          ip_rules: [default: :allow, rules: [%{action: :deny, cidr: "127.0.0.0/8"}]],
          seed: [%{"id" => "demo", "path" => "/webhooks/demo"}]
        )

      conn = post(config, "/webhooks/demo")

      assert conn.status == 404
      assert JSON.decode!(conn.resp_body) == %{"error" => "not_found"}
      assert records(config) == 0
    end

    test "the peer address is used when no proxy is trusted, whatever the header says" do
      config =
        start(
          ip_rules: [default: :deny, rules: [%{action: :allow, cidr: "127.0.0.0/8"}]],
          seed: [%{"id" => "demo", "path" => "/webhooks/demo"}]
        )

      # A spoofed header must not turn an untrusted peer into an allowed one.
      conn = post(config, "/webhooks/demo", [{"x-forwarded-for", "203.0.113.7"}])

      assert conn.status == 201
      assert records(config) == 1
    end

    test "a trusted proxy's X-Forwarded-For decides" do
      config =
        start(
          trusted_proxies: ["127.0.0.0/8"],
          ip_rules: [default: :deny, rules: [%{action: :allow, cidr: "203.0.113.0/24"}]],
          seed: [%{"id" => "demo", "path" => "/webhooks/demo"}]
        )

      assert post(config, "/webhooks/demo", [{"x-forwarded-for", "203.0.113.7"}]).status == 201
      assert records(config) == 1

      assert post(config, "/webhooks/demo", [{"x-forwarded-for", "198.51.100.7"}]).status == 403
      assert records(config) == 1
    end
  end

  describe "routes off" do
    test "an undeclared path is captured exactly as before" do
      config = start(enabled: false)

      assert Routes.enabled?(config.instance) == false

      # No route is declared anywhere: with routes off the source store is the
      # only gate, and the guard touches neither the snapshot nor the config.
      assert post(config, "/webhooks/demo").status == 201
      assert post(config, "/webhooks/demo").status == 201
      assert records(config) == 2
    end
  end
end
