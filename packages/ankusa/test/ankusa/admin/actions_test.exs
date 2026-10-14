defmodule Ankusa.Admin.ActionsTest do
  @moduledoc """
  `Ankusa.Admin.Actions` is the table the guard authorizes from, so a request
  it does not know is, by default, merely a `viewer` request that the router
  then answers. These tests keep the table honest against the contract: every
  operation `priv/openapi/admin.v1.yaml` documents must classify to a real
  action, and a write must never classify as a read.
  """

  use ExUnit.Case, async: true

  alias Ankusa.Admin.Actions

  @spec_path Path.expand("../../../priv/openapi/admin.v1.yaml", __DIR__)
  @methods ~w(get put post delete patch)

  defp operations do
    {:ok, document} = YamlElixir.read_from_file(@spec_path)

    for {path, item} <- document["paths"],
        {method, operation} <- item,
        method in @methods do
      listener = if "routes" in operation["tags"], do: :routes, else: :admin
      segments = path |> String.split("/", trim: true) |> Enum.map(&param/1)
      {listener, String.upcase(method), path, segments}
    end
  end

  defp param("{tenant}"), do: "acme"
  defp param("{id}"), do: "x"
  defp param(segment), do: segment

  test "every operation in the OpenAPI document is classified" do
    ops = operations()
    assert length(ops) > 40

    for {listener, method, path, segments} <- ops do
      info = Actions.classify(listener, method, segments)

      refute info.action in ["unknown", "scim.unknown"],
             "#{method} #{path} on the #{listener} listener is documented but classifies as #{info.action}"
    end
  end

  test "a documented write is never classified as a read" do
    for {listener, method, path, segments} <- operations(), method != "GET" do
      info = Actions.classify(listener, method, segments)

      # a dry run is a POST that changes nothing
      unless info.action == "route.test" do
        assert info.mutates,
               "#{method} #{path} classifies as #{info.action}, which does not mutate"
      end
    end
  end

  test "no action that mutates can be done as a viewer, and nothing but health is public" do
    for {listener, method, path, segments} <- operations() do
      info = Actions.classify(listener, method, segments)

      if info.mutates, do: assert(info.role in [:operator, :admin, :scim], "#{method} #{path}")
      if info.role == :public, do: assert(path in ["/health", "/ready"])
    end
  end

  test "SCIM is the SCIM client's, on whichever listener" do
    for listener <- [:admin, :routes],
        path <- [["scim", "v2", "Users"], ["scim", "v2", "Groups", "g1"], ["scim", "v2", "Nope"]] do
      assert Actions.classify(listener, "GET", path).role == :scim
    end

    assert %{action: "scim.group.update", target: "g1", mutates: true} =
             Actions.classify(:admin, "PATCH", ["scim", "v2", "Groups", "g1"])

    assert %{action: "scim.unknown", mutates: true} =
             Actions.classify(:admin, "POST", ["scim", "v2", "Users", "u1", "extra"])
  end

  test "the source routes (not in the document) are tenant-scoped admin actions" do
    assert %{action: "source.create", role: :admin, scope: {:tenant, "acme"}, tenant: "acme"} =
             Actions.classify(:admin, "POST", ["v1", "tenants", "acme", "sources"])

    for {method, action} <- [{"PUT", "source.update"}, {"DELETE", "source.delete"}] do
      assert %{action: ^action, role: :admin, scope: {:tenant, "acme"}, target: "acme.stripe"} =
               Actions.classify(:admin, method, ["v1", "tenants", "acme", "sources", "stripe"])
    end

    assert %{action: "source.get", role: :viewer, mutates: false} =
             Actions.classify(:admin, "GET", ["v1", "tenants", "acme", "sources", "stripe"])
  end

  test "a tenant's rate limit is a platform control: global scope, tenant recorded" do
    assert %{action: "rate_limit.put", scope: :global, tenant: "acme", role: :admin} =
             Actions.classify(:admin, "PUT", ["v1", "tenants", "acme", "rate-limit"])

    assert %{action: "rate_limit.get", scope: {:tenant, "acme"}, role: :viewer} =
             Actions.classify(:admin, "GET", ["v1", "tenants", "acme", "rate-limit"])
  end

  test "the audit log needs admin, a tenant's trail only that tenant" do
    assert %{action: "audit.list", role: :admin, scope: :global, mutates: false} =
             Actions.classify(:admin, "GET", ["v1", "audit"])

    assert %{action: "audit.list", role: :admin, scope: {:tenant, "acme"}} =
             Actions.classify(:admin, "GET", ["v1", "tenants", "acme", "audit"])
  end

  test "a path the router does not know is unknown, and unknown writes are writes" do
    assert %{action: "unknown", role: :viewer, mutates: false} =
             Actions.classify(:admin, "GET", ["v1", "nope"])

    assert %{action: "unknown", mutates: true} =
             Actions.classify(:admin, "DELETE", ["v1", "nope"])

    # the route-management paths are not served by the operator listener
    assert %{action: "unknown"} = Actions.classify(:admin, "POST", ["admin", "routes"])
    assert %{action: "unknown"} = Actions.classify(:routes, "GET", ["v1", "config"])
  end

  test "HEAD is a GET" do
    assert Actions.classify(:admin, "HEAD", ["v1", "config"]) ==
             Actions.classify(:admin, "GET", ["v1", "config"])
  end

  test "the route dry run does not shadow a route id" do
    assert %{action: "route.test", role: :viewer, mutates: false} =
             Actions.classify(:routes, "POST", ["admin", "routes", "test"])

    assert %{action: "route.replace", role: :admin, target: "test"} =
             Actions.classify(:routes, "PUT", ["admin", "routes", "test"])
  end
end
