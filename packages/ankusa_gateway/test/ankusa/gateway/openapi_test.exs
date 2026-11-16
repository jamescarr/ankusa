defmodule Ankusa.Gateway.OpenApiTest do
  @moduledoc """
  The gateway's OpenAPI documents declare the security they enforce: bearer
  and mutual-TLS auth on the document, inherited by every operation but the
  probes, each of which opts out; every secured operation documents `401` and
  `403`. And every documented path and method is routed (not the catch-all
  `404`).
  """

  use ExUnit.Case, async: false

  alias Ankusa.Gateway.Router

  @claims Path.expand("../../../priv/openapi/claim_check.v1.yaml", __DIR__)

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
