defmodule Ankusa.Routes.Router do
  @moduledoc """
  The route-management API: CRUD over route definitions, the global IP rules,
  and the dry run.

  It listens on its own port (`routes.admin.port`), never on the ingest port:
  managing routes is an operator action, and mixing it into the capture surface
  would put a second, easily-forgotten endpoint next to the one that accepts
  provider traffic.

  ## No authentication, by design

  Same stance as `Ankusa.Admin.Router`: Ankusa does not manage users, tokens, or
  API keys, and it does not know what auth scheme a deployment wants — mTLS, a
  gateway's own bearer scheme, a network policy, an operator VPN. Baking in one
  scheme (a shared token, say) would be the wrong one for most of them. Front
  this port with whatever your deployment already uses; it carries the same
  blast radius as `Ankusa.Admin.Router`'s `/v1/dlq/replay`, so it deserves the
  same treatment.

  ## Responding

  | Method | Path | Success |
  | --- | --- | --- |
  | `GET` | `/admin/routes?enabled=&limit=&cursor=` | `200` `{"routes": [...], "next_cursor": id \| null}` |
  | `POST` | `/admin/routes` | `201` route |
  | `GET` | `/admin/routes/:id` | `200` route |
  | `PUT` | `/admin/routes/:id` | `200` route (idempotent; creates if absent) |
  | `PATCH` | `/admin/routes/:id` | `200` route |
  | `DELETE` | `/admin/routes/:id` | `204` |
  | `GET` | `/admin/ip-rules` | `200` `{"default": ..., "rules": [...]}` |
  | `PUT` | `/admin/ip-rules` | `200` the stored rules |
  | `POST` | `/admin/routes/test` | `200` `{"decision": ..., "reason": ..., "route_id": ..., "ip_rule": ...}` |
  | `GET` | `/health` | `200` `{"status": "ok", "routes": n}` |

  Errors are JSON objects with a stable `error` code:
  `invalid_route` (with `field` and `message`), `duplicate_route` (with
  `conflicting_id`), `too_many_routes` (with `max_routes`), `invalid_query`,
  `invalid_ip_rules`, `invalid_request`, `invalid_body`, `not_found`, and
  `store_unavailable` (a `503`, meaning the backing store could not be reached
  — retry).

  `POST /admin/routes/test` is the dry run: it answers "what would happen to
  this request" without capturing anything and without touching the decision
  cache, so an operator can validate a route change before it goes live.
  """

  use Plug.Router, copy_opts_to_assign: :ankusa_opts

  alias Ankusa.Routes
  alias Ankusa.Routes.Route

  # A route definition is a handful of fields; 64 KiB is already an order of
  # magnitude more than the largest legitimate one.
  @max_body 65_536

  plug(:match)
  plug(:dispatch)

  get "/health" do
    instance = instance(conn)
    send_json(conn, 200, %{status: "ok", routes: route_count(instance)})
  end

  # Before `/admin/routes/:id` in source order, so `test` is never read as an id.
  post "/admin/routes/test" do
    instance = instance(conn)

    with {:ok, body, conn} <- read_json(conn) do
      case Routes.dry_run(instance, body) do
        {:ok, result} -> send_json(conn, 200, result)
        {:error, {:invalid, field, message}} -> invalid_request(conn, field, message)
      end
    else
      {:error, :body, conn} -> invalid_body(conn)
    end
  end

  get "/admin/routes" do
    instance = instance(conn)

    with {:ok, query} <- parse_query(conn),
         {:ok, page} <- Routes.list(instance, query) do
      send_json(conn, 200, %{
        "routes" => Enum.map(page.routes, &Route.to_json/1),
        "next_cursor" => page.next_cursor
      })
    else
      {:error, field} -> invalid_query(conn, field)
    end
  end

  post "/admin/routes" do
    instance = instance(conn)

    with {:ok, body, conn} <- read_json(conn) do
      conn |> write(Routes.create(instance, body), 201)
    else
      {:error, :body, conn} -> invalid_body(conn)
    end
  end

  get "/admin/routes/:id" do
    instance = instance(conn)

    case Routes.get(instance, id) do
      {:ok, route} -> send_json(conn, 200, Route.to_json(route))
      {:error, :not_found} -> not_found(conn)
    end
  end

  put "/admin/routes/:id" do
    instance = instance(conn)

    with {:ok, body, conn} <- read_json(conn) do
      conn |> write(Routes.replace(instance, id, body), 200)
    else
      {:error, :body, conn} -> invalid_body(conn)
    end
  end

  patch "/admin/routes/:id" do
    instance = instance(conn)

    with {:ok, body, conn} <- read_json(conn) do
      conn |> write(Routes.update(instance, id, body), 200)
    else
      {:error, :body, conn} -> invalid_body(conn)
    end
  end

  delete "/admin/routes/:id" do
    instance = instance(conn)

    case Routes.delete(instance, id) do
      :ok -> Plug.Conn.send_resp(conn, 204, "")
      {:error, :not_found} -> not_found(conn)
      {:error, reason} -> store_error(conn, reason)
    end
  end

  get "/admin/ip-rules" do
    send_json(conn, 200, ip_rules_json(Routes.ip_rules(instance(conn))))
  end

  put "/admin/ip-rules" do
    instance = instance(conn)

    with {:ok, body, conn} <- read_json(conn) do
      case Routes.put_ip_rules(instance, body) do
        {:ok, ip_rules} ->
          send_json(conn, 200, ip_rules_json(ip_rules))

        {:error, {:invalid, field, message}} ->
          send_json(conn, 400, %{error: "invalid_ip_rules", field: field, message: message})

        {:error, reason} ->
          store_error(conn, reason)
      end
    else
      {:error, :body, conn} -> invalid_body(conn)
    end
  end

  match _ do
    send_json(conn, 404, %{error: "not_found"})
  end

  # ── writes ──────────────────────────────────────────────────────────────────

  defp write(conn, {:ok, %Route{} = route}, status),
    do: send_json(conn, status, Route.to_json(route))

  defp write(conn, {:error, {:invalid, field, message}}, _status),
    do: send_json(conn, 400, %{error: "invalid_route", field: field, message: message})

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

  defp store_error(conn, _reason), do: send_json(conn, 503, %{error: "store_unavailable"})

  # ── request parsing ─────────────────────────────────────────────────────────

  defp read_json(conn) do
    case Ankusa.Http.read_body_limited(conn, @max_body) do
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
      {:ok,
       [enabled: enabled, cursor: cursor]
       |> put_present(:limit, limit)}
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

  defp put_present(opts, _key, nil), do: opts
  defp put_present(opts, key, value), do: Keyword.put(opts, key, value)

  # ── helpers ─────────────────────────────────────────────────────────────────

  defp ip_rules_json(rules), do: Route.rules_json(rules)

  defp route_count(instance) do
    case Routes.snapshot(instance) do
      nil -> 0
      snapshot -> map_size(snapshot.by_id)
    end
  end

  defp max_routes(conn), do: config(conn).routes.max_routes

  defp config(%Plug.Conn{} = conn), do: Ankusa.config(instance(conn))

  defp instance(%Plug.Conn{} = conn) do
    Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
  end

  defp send_json(conn, status, payload), do: Ankusa.Http.send_json(conn, status, payload)
end
