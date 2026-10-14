defmodule Ankusa.Admin.Actions do
  @moduledoc """
  What a request to the admin or route-management listener *is*: the action
  name that goes into the audit log, the role it needs, and the scope it acts
  on. A pure function of listener, method and path, so `Ankusa.Admin.Guard` can
  authorize a request before the router handles it, with no knowledge of the
  router's handlers.

  `classify/3` returns

      %{
        action: "source.create",
        role: :public | :scim | :viewer | :operator | :admin,
        scope: :global | {:tenant, "acme"},
        tenant: "acme" | nil,
        target: "acme.stripe" | nil,
        mutates: true
      }

    * `role` is the lowest role allowed: `:public` needs no credentials
      (`/health`, `/ready`), `:scim` is the SCIM client's static token.
    * `scope` is what the action touches. A tenant-scoped grant satisfies only
      `{:tenant, t}`; a platform-wide action is `:global`. A tenant's rate limit
      is a platform control (it shapes the shared edge), so changing one is
      `:global` even though the path names a tenant; `tenant` still records it.
    * `target` is the entity named in the path. A create learns its id from the
      handler (`Ankusa.Audit.put_target/2`).
    * `mutates` decides whether the audit log records the request (reads are not
      recorded).

  A path no route answers classifies as `unknown` (`scim.unknown` under
  `/scim/v2`), needing `:viewer` (`:scim`): the router will 404 it, and nobody
  without a credential learns which paths exist. `test/ankusa/admin/actions_test.exs`
  walks `priv/openapi/admin.v1.yaml` and fails when a documented operation falls
  through to `unknown`: a new endpoint is classified here, or the suite fails.
  """

  @type listener :: :admin | :routes
  @type info :: %{
          action: String.t(),
          role: :public | :scim | :viewer | :operator | :admin,
          scope: :global | {:tenant, String.t()},
          tenant: String.t() | nil,
          target: String.t() | nil,
          mutates: boolean()
        }

  @scim_users "Users"
  @scim_groups "Groups"
  @scim_verbs %{
    "GET" => "read",
    "POST" => "create",
    "PUT" => "replace",
    "PATCH" => "update",
    "DELETE" => "delete"
  }

  @doc "Classify a request. `method` is the HTTP method string, `path_info` its path segments."
  @spec classify(listener(), String.t(), [String.t()]) :: info()
  def classify(listener, method, path_info) when listener in [:admin, :routes] do
    method = if method == "HEAD", do: "GET", else: method
    do_classify(listener, method, path_info)
  end

  # ── admin listener ────────────────────────────────────────────────────────

  defp do_classify(:admin, "GET", ["health"]), do: public("health")
  defp do_classify(:admin, "GET", ["ready"]), do: public("health")

  defp do_classify(:admin, "GET", ["metrics"]), do: read("metrics.read")
  defp do_classify(:admin, "GET", ["v1", "wal"]), do: read("wal.read")
  defp do_classify(:admin, "GET", ["v1", "config"]), do: read("config.read")
  defp do_classify(:admin, "GET", ["asyncapi.json"]), do: read("asyncapi.read")

  defp do_classify(:admin, "GET", ["v1", "dlq"]), do: read("dlq.list")
  defp do_classify(:admin, "GET", ["v1", "replays"]), do: read("replay.list")
  defp do_classify(:admin, "GET", ["v1", "replays", id]), do: read("replay.get", id)
  defp do_classify(:admin, "GET", ["v1", "quarantine"]), do: read("quarantine.list")
  defp do_classify(:admin, "GET", ["v1", "rate-limits"]), do: read("rate_limit.list")

  defp do_classify(:admin, "POST", ["v1", "replays"]), do: write("replay.create", :operator)

  defp do_classify(:admin, "PATCH", ["v1", "replays", id]),
    do: write("replay.update", :operator, target: id)

  defp do_classify(:admin, "DELETE", ["v1", "quarantine"]),
    do: write("quarantine.purge", :operator)

  defp do_classify(:admin, "GET", ["v1", "tenants", t, "sources"]),
    do: tenant_read("source.list", t, nil)

  defp do_classify(:admin, "GET", ["v1", "tenants", t, "sources", name]),
    do: tenant_read("source.get", t, "#{t}.#{name}")

  defp do_classify(:admin, "POST", ["v1", "tenants", t, "sources"]),
    do: tenant_write("source.create", :admin, t, nil)

  defp do_classify(:admin, "PUT", ["v1", "tenants", t, "sources", name]),
    do: tenant_write("source.update", :admin, t, "#{t}.#{name}")

  defp do_classify(:admin, "DELETE", ["v1", "tenants", t, "sources", name]),
    do: tenant_write("source.delete", :admin, t, "#{t}.#{name}")

  defp do_classify(:admin, "GET", ["v1", "tenants", t, "rate-limit"]),
    do: tenant_read("rate_limit.get", t, nil)

  defp do_classify(:admin, "PUT", ["v1", "tenants", t, "rate-limit"]),
    do: %{write("rate_limit.put", :admin) | tenant: t}

  defp do_classify(:admin, "DELETE", ["v1", "tenants", t, "rate-limit"]),
    do: %{write("rate_limit.delete", :admin) | tenant: t}

  defp do_classify(:admin, "GET", ["v1", "audit"]), do: read("audit.list", nil, :admin)

  defp do_classify(:admin, "GET", ["v1", "tenants", t, "audit"]),
    do: %{tenant_read("audit.list", t, nil) | role: :admin}

  # ── SCIM (any listener: the prefix decides) ───────────────────────────────

  defp do_classify(_listener, method, ["scim", "v2", @scim_users]),
    do: scim_resource("user", method, nil)

  defp do_classify(_listener, method, ["scim", "v2", @scim_users, id]),
    do: scim_resource("user", method, id)

  defp do_classify(_listener, method, ["scim", "v2", @scim_groups]),
    do: scim_resource("group", method, nil)

  defp do_classify(_listener, method, ["scim", "v2", @scim_groups, id]),
    do: scim_resource("group", method, id)

  defp do_classify(_listener, "GET", ["scim", "v2", discovery])
       when discovery in ["ServiceProviderConfig", "ResourceTypes", "Schemas"],
       do: %{read("scim.discovery") | role: :scim}

  defp do_classify(_listener, "GET", ["scim", "v2", discovery, _id])
       when discovery in ["ResourceTypes", "Schemas"],
       do: %{read("scim.discovery") | role: :scim}

  defp do_classify(_listener, method, ["scim", "v2" | _rest]),
    do: %{unknown(method) | action: "scim.unknown", role: :scim}

  # ── route-management listener ─────────────────────────────────────────────

  defp do_classify(:routes, "GET", ["health"]), do: public("health")

  defp do_classify(:routes, "GET", ["admin", "routes"]), do: read("route.list")
  defp do_classify(:routes, "GET", ["admin", "routes", id]), do: read("route.get", id)
  defp do_classify(:routes, "GET", ["admin", "ip-rules"]), do: read("ip_rules.get")

  # A dry run changes nothing, so a viewer may run it. Before `:id` below, as in
  # the router, so `test` is never read as an id.
  defp do_classify(:routes, "POST", ["admin", "routes", "test"]), do: read("route.test")

  defp do_classify(:routes, "POST", ["admin", "routes"]), do: write("route.create", :admin)

  defp do_classify(:routes, "PUT", ["admin", "routes", id]),
    do: write("route.replace", :admin, target: id)

  defp do_classify(:routes, "PATCH", ["admin", "routes", id]),
    do: write("route.update", :admin, target: id)

  defp do_classify(:routes, "DELETE", ["admin", "routes", id]),
    do: write("route.delete", :admin, target: id)

  defp do_classify(:routes, "PUT", ["admin", "ip-rules"]), do: write("ip_rules.put", :admin)

  defp do_classify(_listener, method, _path), do: unknown(method)

  # ── shapes ────────────────────────────────────────────────────────────────

  defp public(action), do: info(action, :public, false, [])

  defp read(action, target \\ nil, role \\ :viewer),
    do: info(action, role, false, target: target)

  defp write(action, role, opts \\ []), do: info(action, role, true, opts)

  defp tenant_read(action, tenant, target),
    do: info(action, :viewer, false, scope: {:tenant, tenant}, tenant: tenant, target: target)

  defp tenant_write(action, role, tenant, target),
    do: info(action, role, true, scope: {:tenant, tenant}, tenant: tenant, target: target)

  defp unknown(method), do: info("unknown", :viewer, method != "GET", [])

  defp info(action, role, mutates, opts) do
    %{
      action: action,
      role: role,
      scope: Keyword.get(opts, :scope, :global),
      tenant: Keyword.get(opts, :tenant),
      target: Keyword.get(opts, :target),
      mutates: mutates
    }
  end

  defp scim_resource(kind, method, id) do
    case Map.fetch(@scim_verbs, method) do
      {:ok, verb} -> info("scim.#{kind}.#{verb}", :scim, method != "GET", target: id)
      :error -> %{unknown(method) | action: "scim.unknown", role: :scim}
    end
  end
end
