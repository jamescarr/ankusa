defmodule Ankusa.Gateway.OpenApiTest do
  @moduledoc """
  The gateway's OpenAPI documents declare the security they enforce: bearer
  and mutual-TLS auth on the document, inherited by every operation but the
  probes, each of which opts out; every secured operation documents `401` and
  `403`. And every documented path and method is routed (not the catch-all
  `404`).
  """

  use ExUnit.Case, async: false

  alias Ankusa.BlobStore.LocalFS
  alias Ankusa.Gateway.Router
  alias Ankusa.TestHelpers

  @claims Path.expand("../../../priv/openapi/claim_check.v1.yaml", __DIR__)
  @gateway Path.expand("../../../priv/openapi/gateway.v1.yaml", __DIR__)

  test "the claims document declares bearer and mutual-TLS auth, on every operation but the probes" do
    {:ok, document} = YamlElixir.read_from_file(@claims)
    assert_secured(document, ["/health", "/ready"])
  end

  test "every documented claims path and method is routed" do
    {:ok, document} = YamlElixir.read_from_file(@claims)
    inst = :"openapi#{System.unique_integer([:positive])}"
    Ankusa.put_config(Ankusa.Config.new(instance: inst, roles: []))
    Ankusa.Gateway.put_config(Ankusa.Gateway.Config.new(instance: inst))

    for {path, item} <- document["paths"], {method, %{}} <- item, method in http_methods() do
      concrete = String.replace(path, ~r/\{[^}]+\}/, "x")

      conn =
        method
        |> String.to_existing_atom()
        |> Plug.Test.conn(concrete)
        |> Router.call(Router.init(instance: inst))

      # A claim path of `x/x` is a malformed id (`400`), never the catch-all.
      assert conn.status != 404, "#{method} #{path} is not routed"
    end
  end

  test "the gateway document declares bearer and mutual-TLS auth, on every operation but the probes" do
    {:ok, document} = YamlElixir.read_from_file(@gateway)
    assert_secured(document, ["/health", "/ready"])
  end

  @tag :tmp_dir
  test "every documented gateway path and method is routed", %{tmp_dir: dir} do
    {:ok, document} = YamlElixir.read_from_file(@gateway)

    config = TestHelpers.test_config(roles: [])
    start_supervised!({Ankusa.Instance, config})
    inst = config.instance
    root = Path.join(dir, "objects")

    gateway =
      Ankusa.Gateway.Config.new(
        instance: inst,
        port: 0,
        authz: %{
          authorizers: %{
            "all" => {Ankusa.Gateway.Authz.Rules, %{rules: [%{effect: :allow, actions: ["*"]}]}}
          },
          policies: %{"objects" => %{mode: :first, steps: ["all", "grants"]}}
        },
        spaces: %{"docs" => %{store: {LocalFS, [root: root]}, prefix: "docs/", policy: "objects"}},
        sharing: %{enabled: true, signing_keys: ["0123456789abcdef0123"]}
      )

    start_supervised!({Ankusa.Gateway, gateway})

    for {path, item} <- document["paths"], {method, %{}} <- item, method in http_methods() do
      # The object is put back for every request: DELETE removes it, and a GET
      # or HEAD of a missing key answers the catch-all's `404` too — the space
      # exists, so a `404` here can only be an unrouted method.
      {:ok, _version} = LocalFS.put_object(inst, "docs/a/b.txt", "hello", root: root)

      conn =
        method
        |> String.to_existing_atom()
        |> Plug.Test.conn(concrete(path))
        |> Router.call(Router.init(instance: inst))

      assert conn.status != 404,
             "#{String.upcase(method)} #{path} is not routed: 404 #{conn.resp_body}"
    end
  end

  # Every placeholder the documents use, for a space that exists and an object
  # that can exist. Anything left over becomes `x`, so a new placeholder fails
  # the routed check loudly instead of vanishing.
  defp concrete(path) do
    path
    |> String.replace("{space}", "docs")
    |> String.replace("{key}", "a/b.txt")
    |> String.replace(~r/\{[^}]+\}/, "x")
  end

  defp http_methods, do: ~w(get post put patch delete head)

  defp assert_secured(document, exempt) do
    assert document["security"] == [%{"bearerAuth" => []}, %{"mutualTLS" => []}]

    assert %{
             "bearerAuth" => %{"type" => "http", "scheme" => "bearer"},
             "mutualTLS" => %{"type" => "mutualTLS"}
           } = document["components"]["securitySchemes"]

    for {path, item} <- document["paths"],
        {method, operation} <- item,
        method in http_methods(),
        is_map(operation) do
      where = "#{String.upcase(method)} #{path}"

      if path in exempt do
        assert operation["security"] == [], "#{where} is a probe and must opt out of security"
      else
        refute Map.has_key?(operation, "security"),
               "#{where} must inherit the document's security"

        for status <- ["401", "403"] do
          assert Map.has_key?(operation["responses"], status),
                 "#{where} does not document #{status}"
        end
      end
    end
  end
end
