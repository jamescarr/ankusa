defmodule Ankusa.Routes.Router do
  @moduledoc """
  The route-management API: CRUD over route definitions, the global IP rules,
  and the dry run.

  It listens on its own port (`routes.admin.port`), never on the ingest port:
  managing routes is an operator action, and mixing it into the capture surface
  would put a second, easily-forgotten endpoint next to the one that accepts
  provider traffic.

  ## Authentication and scopes

  Every route but `GET /health` needs a credential (`routes.admin.auth`, see
  `Ankusa.Admin.Auth`): `401` without one, `403` without the scope. Reading
  (`GET`, and the dry run `POST /admin/routes/test`) needs `routes:read`; every
  other write needs `routes:write`.

  There are two surfaces over the same definitions. The **operator surface**,
  `/admin/routes` and `/admin/ip-rules`, spans tenants, so its scopes only count
  together with `tenant:*`: a principal limited to some tenants must not edit
  what every tenant's ingest is checked against. The **tenant surface**,
  `/v1/tenants/:tenant/routes`, needs `tenant:<tenant>` (or `tenant:*`) instead,
  and shows and changes only that tenant's routes: one that another tenant owns
  is `404`, a body naming another tenant is `400`, and a principal without the
  tenant is `403`. A tenant that is not `[A-Za-z0-9_-]{1,64}` is
  `400 invalid_tenant`.

  ## Responding

  | Method | Path | Success |
  | --- | --- | --- |
  | `GET` | `/admin/routes?enabled=&limit=&cursor=` | `200` `{"routes": [...], "next_cursor": id \| null}` |
  | `POST` | `/admin/routes` | `201` route |
  | `GET` | `/admin/routes/:id` | `200` route |
  | `PUT` | `/admin/routes/:id` | `200` route (idempotent; creates if absent) |
  | `PATCH` | `/admin/routes/:id` | `200` route |
  | `DELETE` | `/admin/routes/:id` | `204` |
  | `GET` | `/v1/tenants/:tenant/routes?enabled=&limit=&cursor=` | `200` the page of that tenant's routes |
  | `POST` | `/v1/tenants/:tenant/routes` | `201` route owned by the tenant |
  | `GET` | `/v1/tenants/:tenant/routes/:id` | `200` route |
  | `PUT` | `/v1/tenants/:tenant/routes/:id` | `200` route (idempotent; creates if absent) |
  | `PATCH` | `/v1/tenants/:tenant/routes/:id` | `200` route |
  | `DELETE` | `/v1/tenants/:tenant/routes/:id` | `204` |
  | `GET` | `/admin/ip-rules` | `200` `{"default": ..., "rules": [...]}` |
  | `PUT` | `/admin/ip-rules` | `200` the stored rules |
  | `POST` | `/admin/routes/test` | `200` `{"decision": ..., "reason": ..., "route_id": ..., "ip_rule": ...}` |
  | `GET` | `/health` | `200` `{"status": "ok", "routes": n}` |

  Errors are JSON objects with a stable `error` code:
  `invalid_route` (with `field` and `message`), `duplicate_route` (with
  `conflicting_id`, left out on the tenant surface when another tenant holds the
  id or the path), `too_many_routes` (with `max_routes`), `invalid_query`,
  `invalid_tenant`, `invalid_ip_rules`, `invalid_request`, `invalid_body`,
  `not_found`, and `store_unavailable` (a `503`: the backing store could not be
  reached, or a write lost every race against concurrent writers — retry).

  Writes are checked against the table they were validated against: two requests
  creating the same id, or two enabled routes for the same path and method,
  cannot both succeed however they interleave. `PUT /admin/ip-rules` replaces
  the whole rule set, so it requires both `default` and `rules`.

  `POST /admin/routes/test` is the dry run: it answers "what would happen to
  this request" without capturing anything and without touching the decision
  cache, so an operator can validate a route change before it goes live.

  ## Contract

  `priv/openapi/admin.v1.yaml` is the HTTP contract for these endpoints (the
  `routes` tag), and it is the source of truth:
  `test/ankusa/routes/router_openapi_test.exs` builds each documented request
  from the examples in it and asserts the response against the documented
  schema and example, so this module answers what the document says or the suite
  fails. Change one, change the other.
  """

  use Plug.Router, copy_opts_to_assign: :ankusa_opts

  require Logger

  alias Ankusa.Admin.Auth
  alias Ankusa.Audit.Context
  alias Ankusa.Routes
  alias Ankusa.Routes.Route

  # A route definition is a handful of fields; 64 KiB is already an order of
  # magnitude more than the largest legitimate one.
  @max_body 65_536

  plug(Auth, section: :routes_admin)
  plug(Context)
  plug(:match)
  plug(:authorize_scope)
  plug(:dispatch)

  # Reading is `routes:read`, writing is `routes:write`; the dry run is a `POST`
  # that changes nothing. The tenant surface checks the tenant in its path, the
  # operator surface needs `tenant:*`.
  defp authorize_scope(%Plug.Conn{path_info: ["health"]} = conn, _opts), do: conn

  defp authorize_scope(
         %Plug.Conn{path_info: ["v1", "tenants", tenant, "routes" | _rest]} = conn,
         _opts
       ),
       do: Auth.authorize_tenant(conn, required_scope(conn), tenant, & &1)

  defp authorize_scope(%Plug.Conn{} = conn, _opts),
    do: Auth.authorize_global(conn, required_scope(conn), & &1)

  defp required_scope(%Plug.Conn{} = conn) do
    if conn.method in ["GET", "HEAD"] or
         (conn.method == "POST" and conn.path_info == ["admin", "routes", "test"]),
       do: "routes:read",
       else: "routes:write"
  end

  get "/health" do
    instance = instance(conn)
    send_json(conn, 200, %{status: "ok", routes: route_count(instance)})
  end

  # Before `/admin/routes/:id` in source order, so `test` is never read as an id.
  post "/admin/routes/test" do
    instance = instance(conn)

    case read_json(conn) do
      {:ok, body, conn} ->
        case Routes.dry_run(instance, body) do
          {:ok, result} -> send_json(conn, 200, result)
          {:error, {:invalid, field, message}} -> invalid_request(conn, field, message)
        end

      {:error, :body, conn} ->
        invalid_body(conn)
    end
  end

  get "/admin/routes" do
    list_routes(conn, nil)
  end

  post "/admin/routes" do
    create_route(conn, nil)
  end

  get "/admin/routes/:id" do
    get_route(conn, id, nil)
  end

  put "/admin/routes/:id" do
    replace_route(conn, id, nil)
  end

  patch "/admin/routes/:id" do
    update_route(conn, id, nil)
  end

  delete "/admin/routes/:id" do
    delete_route(conn, id, nil)
  end

  # The tenant surface: the same operations over one tenant's routes.
  get "/v1/tenants/:tenant/routes" do
    for_tenant(conn, tenant, &list_routes(&1, tenant))
  end

  post "/v1/tenants/:tenant/routes" do
    for_tenant(conn, tenant, &create_route(&1, tenant))
  end

  get "/v1/tenants/:tenant/routes/:id" do
    for_tenant(conn, tenant, &get_route(&1, id, tenant))
  end

  put "/v1/tenants/:tenant/routes/:id" do
    for_tenant(conn, tenant, &replace_route(&1, id, tenant))
  end

  patch "/v1/tenants/:tenant/routes/:id" do
    for_tenant(conn, tenant, &update_route(&1, id, tenant))
  end

  delete "/v1/tenants/:tenant/routes/:id" do
    for_tenant(conn, tenant, &delete_route(&1, id, tenant))
  end

  get "/admin/ip-rules" do
    send_json(conn, 200, ip_rules_json(Routes.ip_rules(instance(conn))))
  end

  put "/admin/ip-rules" do
    instance = instance(conn)

    case read_json(conn) do
      {:ok, body, conn} ->
        case Routes.put_ip_rules(instance, body, audit: Context.from_conn(conn)) do
          {:ok, ip_rules} ->
            send_json(conn, 200, ip_rules_json(ip_rules))

          {:error, {:invalid, field, message}} ->
            send_json(conn, 400, %{error: "invalid_ip_rules", field: field, message: message})

          {:error, reason} ->
            store_error(conn, reason)
        end

      {:error, :body, conn} ->
        invalid_body(conn)
    end
  end

  match _ do
    send_json(conn, 404, %{error: "not_found"})
  end

  # ── operations, shared by both surfaces ─────────────────────────────────────

  # `tenant` is `nil` on the operator surface and the path's tenant on the
  # tenant surface, where it becomes the `tenant:` option of `Ankusa.Routes`.
  defp for_tenant(conn, tenant, fun) do
    if Ankusa.ClaimCheck.Ref.valid_tenant?(tenant),
      do: fun.(conn),
      else: send_json(conn, 400, %{error: "invalid_tenant"})
  end

  defp scoped(opts, nil), do: opts
  defp scoped(opts, tenant), do: Keyword.put(opts, :tenant, tenant)

  defp list_routes(conn, tenant) do
    instance = instance(conn)

    with {:ok, query} <- parse_query(conn),
         {:ok, page} <- Routes.list(instance, scoped(query, tenant)) do
      send_json(conn, 200, %{
        "routes" => Enum.map(page.routes, &Route.to_json/1),
        "next_cursor" => page.next_cursor
      })
    else
      {:error, field} -> invalid_query(conn, field)
    end
  end

  defp create_route(conn, tenant) do
    instance = instance(conn)

    case read_json(conn) do
      {:ok, body, conn} ->
        opts = scoped([audit: Context.from_conn(conn)], tenant)
        conn |> write(Routes.create(instance, body, opts), 201)

      {:error, :body, conn} ->
        invalid_body(conn)
    end
  end

  defp get_route(conn, id, tenant) do
    case Routes.get(instance(conn), id, scoped([], tenant)) do
      {:ok, route} -> send_json(conn, 200, Route.to_json(route))
      {:error, :not_found} -> not_found(conn)
    end
  end

  defp replace_route(conn, id, tenant) do
    instance = instance(conn)

    case read_json(conn) do
      {:ok, body, conn} ->
        opts = scoped([audit: Context.from_conn(conn)], tenant)
        conn |> write(Routes.replace(instance, id, body, opts), 200)

      {:error, :body, conn} ->
        invalid_body(conn)
    end
  end

  defp update_route(conn, id, tenant) do
    instance = instance(conn)

    case read_json(conn) do
      {:ok, body, conn} ->
        opts = scoped([audit: Context.from_conn(conn)], tenant)
        conn |> write(Routes.update(instance, id, body, opts), 200)

      {:error, :body, conn} ->
        invalid_body(conn)
    end
  end

  defp delete_route(conn, id, tenant) do
    opts = scoped([audit: Context.from_conn(conn)], tenant)

    case Routes.delete(instance(conn), id, opts) do
      :ok -> Plug.Conn.send_resp(conn, 204, "")
      {:error, :not_found} -> not_found(conn)
      {:error, reason} -> store_error(conn, reason)
    end
  end

  # ── writes ──────────────────────────────────────────────────────────────────

  defp write(conn, {:ok, %Route{} = route}, status),
    do: send_json(conn, status, Route.to_json(route))

  defp write(conn, {:error, {:invalid, field, message}}, _status),
    do: send_json(conn, 400, %{error: "invalid_route", field: field, message: message})

  # Another tenant holds it: the answer does not say whose.
  defp write(conn, {:error, {:conflict, nil}}, _status),
    do: send_json(conn, 409, %{error: "duplicate_route"})

  defp write(conn, {:error, {:conflict, conflicting_id}}, _status),
    do: send_json(conn, 409, %{error: "duplicate_route", conflicting_id: conflicting_id})

  defp write(conn, {:error, :not_found}, _status), do: not_found(conn)

  defp write(conn, {:error, :too_many_routes}, _status),
    do: send_json(conn, 409, %{error: "too_many_routes", max_routes: max_routes(conn)})

  defp write(conn, {:error, reason}, _status), do: store_error(conn, reason)

  # ── responses ───────────────────────────────────────────────────────────────

  defp invalid_body(conn), do: send_json(conn, 400, %{error: "invalid_body"})

  defp invalid_query(conn, field),
    do: send_json(conn, 400, %{error: "invalid_query", field: field})

  defp invalid_request(conn, field, message),
    do: send_json(conn, 400, %{error: "invalid_request", field: field, message: message})

  defp not_found(conn), do: send_json(conn, 404, %{error: "not_found"})

  # The caller only learns "retry"; the reason (Redis down, a write that lost
  # every race, an unexpected store error) is the operator's to read in the log.
  defp store_error(conn, reason) do
    Logger.warning("[ankusa] route management write answered 503: #{inspect(reason)}")
    send_json(conn, 503, %{error: "store_unavailable"})
  end

  # ── request parsing ─────────────────────────────────────────────────────────

  defp read_json(conn) do
    case Ankusa.Http.read_body_limited(
           conn,
           @max_body,
           Ankusa.Http.read_opts(Ankusa.config(instance(conn)))
         ) do
      {:ok, body, conn} ->
        case JSON.decode(body) do
          {:ok, decoded} when is_map(decoded) -> {:ok, decoded, conn}
          _not_an_object -> {:error, :body, conn}
        end

      {:too_large, conn} ->
        {:error, :body, conn}

      {:error, _reason, conn} ->
        {:error, :body, conn}
    end
  end

  defp parse_query(conn) do
    params = Plug.Conn.fetch_query_params(conn).query_params

    with {:ok, enabled} <- bool_param(params, "enabled"),
         {:ok, limit} <- int_param(params, "limit"),
         {:ok, cursor} <- string_param(params, "cursor") do
      {:ok, [enabled: enabled, cursor: cursor, limit: limit]}
    end
  end

  defp bool_param(params, key) do
    case Map.fetch(params, key) do
      :error -> {:ok, nil}
      {:ok, "true"} -> {:ok, true}
      {:ok, "false"} -> {:ok, false}
      {:ok, _other} -> {:error, key}
    end
  end

  defp int_param(params, key) do
    case Map.fetch(params, key) do
      :error ->
        {:ok, nil}

      {:ok, value} ->
        case Integer.parse(value) do
          {int, ""} -> {:ok, int}
          _not_an_integer -> {:error, key}
        end
    end
  end

  defp string_param(params, key) do
    case Map.fetch(params, key) do
      :error -> {:ok, nil}
      {:ok, ""} -> {:ok, nil}
      {:ok, value} -> {:ok, value}
    end
  end

  # ── helpers ─────────────────────────────────────────────────────────────────

  defp ip_rules_json(rules), do: Route.rules_json(rules)

  defp route_count(instance), do: Ankusa.Routes.Snapshot.count(instance)

  defp max_routes(%Plug.Conn{} = conn), do: Ankusa.config(instance(conn)).routes.max_routes

  defp instance(%Plug.Conn{} = conn) do
    Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
  end

  defp send_json(conn, status, payload), do: Ankusa.Http.send_json(conn, status, payload)
end
