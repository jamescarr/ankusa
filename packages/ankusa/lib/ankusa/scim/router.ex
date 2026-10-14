defmodule Ankusa.Scim.Router do
  @moduledoc """
  The SCIM 2.0 endpoint (RFC 7643, 7644) an identity provider (Okta, Microsoft
  Entra) provisions users and groups through, mounted at `/scim/v2` on the
  admin listener (`Ankusa.Admin.Router` forwards to it). It writes the
  `Ankusa.Directory`, which `Ankusa.Auth` reads to turn a token into grants: a
  user the IdP deactivates or deletes stops getting in, whatever their token
  says.

  It does no authentication of its own. `Ankusa.Admin.Guard` ran before the
  forward: `scim.enabled` must be true (else `404`), the request must carry the
  static `scim.token` (else `401`), and every mutation is in the audit log.

  | Method | Path | |
  | --- | --- | --- |
  | `GET` `POST` | `/Users` | list (`filter`, `startIndex`, `count`), create `201` |
  | `GET` `PUT` `PATCH` `DELETE` | `/Users/:id` | `200`, `200`, `200`, `204` |
  | `GET` `POST` | `/Groups` | as users; `excludedAttributes=members` |
  | `GET` `PUT` `PATCH` `DELETE` | `/Groups/:id` | as users |
  | `GET` | `/ServiceProviderConfig`, `/ResourceTypes[/:id]`, `/Schemas[/:id]` | discovery |

  Bodies are capped at 4 MiB and sanitized (`Ankusa.Scim.Resource`); filters are
  `attr eq "value"` (`Ankusa.Scim.Filter`); PATCH is `Ankusa.Scim.Patch`, with
  group membership changes applied atomically (`Ankusa.Directory.add_members/3`)
  so concurrent provisioning runs cannot lose members. Responses are
  `application/scim+json`; `meta.location` is `scim.base_url` (or the request's
  own scheme, host and port) plus the resource path, and `meta.version` the weak
  ETag of the stored version.

  Store failures map to SCIM errors (`Ankusa.Scim.Errors`): a taken `userName`
  or `displayName` is `409 uniqueness`, an unknown group member `400
  invalidValue`, an unreachable directory `503`.
  """

  use Plug.Router

  alias Ankusa.{Audit, Directory}
  alias Ankusa.Scim.{Discovery, Errors, Filter, Patch, Resource}

  @max_body 4_194_304
  @default_count 100
  @max_count 1000
  @list_schema "urn:ietf:params:scim:api:messages:2.0:ListResponse"
  @enterprise Resource.enterprise_schema()

  plug(:match)
  plug(:dispatch)

  # ── discovery ──────────────────────────────────────────────────────────────

  get "/ServiceProviderConfig" do
    send_scim(conn, 200, Discovery.service_provider_config(base(conn)))
  end

  get "/ResourceTypes" do
    send_scim(conn, 200, Discovery.resource_types(base(conn)))
  end

  get "/ResourceTypes/:id" do
    discovery(conn, Discovery.resource_type(base(conn), id), id)
  end

  get "/Schemas" do
    send_scim(conn, 200, Discovery.schemas(base(conn)))
  end

  get "/Schemas/:id" do
    discovery(conn, Discovery.schema(base(conn), id), id)
  end

  # ── users ──────────────────────────────────────────────────────────────────

  get "/Users" do
    list(conn, :user)
  end

  post "/Users" do
    with_body(conn, fn body, conn ->
      with {:ok, user} <- sanitize(Resource.user(body)),
           {:ok, created} <- Directory.create_user(instance(conn), user) do
        conn |> Audit.put_target(created["id"]) |> respond_user(201, created)
      else
        error -> failed(conn, error)
      end
    end)
  end

  get "/Users/:id" do
    conn = Audit.put_target(conn, id)

    case Directory.get_user(instance(conn), id) do
      {:ok, user} -> respond_user(conn, 200, user)
      other -> failed(conn, other)
    end
  end

  put "/Users/:id" do
    conn = Audit.put_target(conn, id)

    with_body(conn, fn body, conn ->
      with {:ok, user} <- sanitize(Resource.user(body)),
           {:ok, replaced} <- Directory.replace_user(instance(conn), id, user, nil) do
        respond_user(conn, 200, replaced)
      else
        error -> failed(conn, error)
      end
    end)
  end

  patch "/Users/:id" do
    conn = Audit.put_target(conn, id)

    with_body(conn, fn body, conn ->
      result =
        Directory.update_user(instance(conn), id, fn user ->
          with {:ok, patched, _members} <- apply_patch(:user, user, body), do: {:ok, patched}
        end)

      case result do
        {:ok, user} -> respond_user(conn, 200, user)
        error -> failed(conn, error)
      end
    end)
  end

  delete "/Users/:id" do
    conn = Audit.put_target(conn, id)

    case Directory.delete_user(instance(conn), id) do
      :ok -> send_resp(conn, 204, "")
      error -> failed(conn, error)
    end
  end

  # ── groups ─────────────────────────────────────────────────────────────────

  get "/Groups" do
    list(conn, :group)
  end

  post "/Groups" do
    with_body(conn, fn body, conn ->
      with {:ok, group, members} <- sanitize(Resource.group(body)),
           {:ok, created} <- Directory.create_group(instance(conn), group, members || []) do
        conn |> Audit.put_target(created["id"]) |> respond_group(201, created)
      else
        error -> failed(conn, error)
      end
    end)
  end

  get "/Groups/:id" do
    conn = Audit.put_target(conn, id)

    case Directory.get_group(instance(conn), id) do
      {:ok, group} -> respond_group(conn, 200, group)
      other -> failed(conn, other)
    end
  end

  # A PUT without `members` leaves the membership alone; with `members` (even
  # an empty list) it replaces it.
  put "/Groups/:id" do
    conn = Audit.put_target(conn, id)

    with_body(conn, fn body, conn ->
      with {:ok, group, members} <- sanitize(Resource.group(body)),
           {:ok, replaced} <-
             Directory.replace_group(instance(conn), id, group, members || :keep, nil) do
        respond_group(conn, 200, replaced)
      else
        error -> failed(conn, error)
      end
    end)
  end

  patch "/Groups/:id" do
    conn = Audit.put_target(conn, id)
    with_body(conn, fn body, conn -> patch_group(conn, id, body) end)
  end

  delete "/Groups/:id" do
    conn = Audit.put_target(conn, id)

    case Directory.delete_group(instance(conn), id) do
      :ok -> send_resp(conn, 204, "")
      error -> failed(conn, error)
    end
  end

  match _ do
    Errors.send(conn, 404, nil, "No such SCIM endpoint")
  end

  # ── group PATCH ────────────────────────────────────────────────────────────

  # The attribute changes (`displayName`, `externalId`) are a versioned
  # read-modify-write of the group; membership changes are applied one by one
  # by the store's atomic add/remove, never by rewriting the member list, so
  # concurrent PATCHes (Entra sends several at once) cannot lose each other's.
  defp patch_group(conn, id, body) do
    instance = instance(conn)

    with {:ok, group} <- fetch(Directory.get_group(instance, id)),
         {:ok, patched, changes} <- apply_patch(:group, group, body),
         :ok <- update_attributes(instance, id, group, patched, body),
         :ok <- apply_changes(instance, id, changes),
         {:ok, updated} <- fetch(Directory.get_group(instance, id)) do
      respond_group(conn, 200, updated)
    else
      error -> failed(conn, error)
    end
  end

  defp update_attributes(_instance, _id, group, group, _body), do: :ok

  defp update_attributes(instance, id, _group, _patched, body) do
    result =
      Directory.update_group(instance, id, fn group ->
        with {:ok, patched, _changes} <- apply_patch(:group, group, body),
             do: {:ok, patched, :keep}
      end)

    with {:ok, _group} <- result, do: :ok
  end

  defp apply_changes(instance, id, changes) do
    Enum.reduce_while(changes, :ok, fn change, :ok ->
      case apply_change(instance, id, change) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp apply_change(instance, id, {:add, ids}), do: Directory.add_members(instance, id, ids)
  defp apply_change(instance, id, {:remove, ids}), do: Directory.remove_members(instance, id, ids)

  defp apply_change(instance, id, {:replace, ids}) do
    with {:ok, _group} <- Directory.update_group(instance, id, &{:ok, &1, ids}), do: :ok
  end

  # `Patch` errors travel through the directory's retry loop as a reason.
  defp apply_patch(type, resource, body) do
    case Patch.apply(type, resource, body) do
      {:ok, _patched, _changes} = ok -> ok
      {:error, scim_type, detail} -> {:error, {:scim, scim_type, detail}}
    end
  end

  # ── listing ────────────────────────────────────────────────────────────────

  defp list(conn, type) do
    params = Plug.Conn.fetch_query_params(conn).query_params
    instance = instance(conn)

    with {:ok, start} <- integer(params, "startIndex", 1),
         {:ok, count} <- integer(params, "count", @default_count),
         {:ok, resources} <- candidates(instance, type, params["filter"]),
         start = max(start, 1),
         count = count |> max(0) |> min(@max_count),
         page = resources |> Enum.drop(start - 1) |> Enum.take(count),
         {:ok, rendered} <- render_all(conn, type, page, params) do
      send_scim(conn, 200, %{
        "schemas" => [@list_schema],
        "totalResults" => length(resources),
        "startIndex" => start,
        "itemsPerPage" => length(rendered),
        "Resources" => rendered
      })
    else
      error -> failed(conn, error)
    end
  end

  defp integer(params, key, default) do
    case Map.fetch(params, key) do
      :error ->
        {:ok, default}

      {:ok, value} ->
        case Integer.parse(value) do
          {int, ""} -> {:ok, int}
          _ -> {:error, {:scim, "invalidValue", "#{key} must be an integer"}}
        end
    end
  end

  defp candidates(instance, :user, nil), do: Directory.list_users(instance)
  defp candidates(instance, :group, nil), do: Directory.list_groups(instance)

  defp candidates(instance, type, filter) do
    with {:ok, {attr, value}} <- parse_filter(type, filter) do
      one(lookup(instance, type, attr, value))
    end
  end

  defp parse_filter(type, filter) do
    case Filter.parse(type, filter) do
      {:ok, parsed} -> {:ok, parsed}
      {:error, detail} -> {:error, {:scim, "invalidFilter", detail}}
    end
  end

  defp lookup(instance, :user, :id, value), do: Directory.get_user(instance, value)
  defp lookup(instance, :user, attr, value), do: Directory.find_user(instance, attr, value)
  defp lookup(instance, :group, :id, value), do: Directory.get_group(instance, value)
  defp lookup(instance, :group, attr, value), do: Directory.find_group(instance, attr, value)

  defp one({:ok, resource}), do: {:ok, [resource]}
  defp one(:not_found), do: {:ok, []}
  defp one({:error, _reason} = error), do: error

  defp render_all(conn, type, page, params) do
    Enum.reduce_while(page, {:ok, []}, fn resource, {:ok, acc} ->
      case render(conn, type, resource, params) do
        {:ok, doc} -> {:cont, {:ok, [doc | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, docs} -> {:ok, Enum.reverse(docs)}
      error -> error
    end
  end

  # ── rendering ──────────────────────────────────────────────────────────────

  defp respond_user(conn, status, user) do
    case render(conn, :user, user, %{}) do
      {:ok, doc} -> conn |> created_location(status, doc) |> send_scim(status, doc)
      error -> failed(conn, error)
    end
  end

  defp respond_group(conn, status, group) do
    params = Plug.Conn.fetch_query_params(conn).query_params

    case render(conn, :group, group, params) do
      {:ok, doc} -> conn |> created_location(status, doc) |> send_scim(status, doc)
      error -> failed(conn, error)
    end
  end

  defp created_location(conn, 201, %{"meta" => %{"location" => location}}),
    do: Plug.Conn.put_resp_header(conn, "location", location)

  defp created_location(conn, _status, _doc), do: conn

  defp render(conn, :user, user, _params) do
    with {:ok, groups} <- Directory.groups_for_user(instance(conn), user["id"]) do
      schemas =
        [Discovery.user_schema()] ++
          if(Map.has_key?(user, @enterprise), do: [@enterprise], else: [])

      doc =
        user
        |> Map.put("schemas", schemas)
        |> Map.put(
          "groups",
          Enum.map(groups, &%{"value" => &1["id"], "display" => &1["displayName"]})
        )
        |> with_meta(conn, "Users")

      {:ok, doc}
    end
  end

  defp render(conn, :group, group, params) do
    doc = group |> Map.put("schemas", [Discovery.group_schema()]) |> with_meta(conn, "Groups")

    if excluded?(params, "members") do
      {:ok, doc}
    else
      with {:ok, members} <- member_refs(instance(conn), group["id"]) do
        {:ok, Map.put(doc, "members", members)}
      end
    end
  end

  defp member_refs(instance, group_id) do
    with {:ok, ids} <- Directory.members(instance, group_id) do
      refs =
        Enum.flat_map(ids, fn id ->
          case Directory.get_user(instance, id) do
            {:ok, user} -> [%{"value" => id, "display" => user["userName"]}]
            _ -> []
          end
        end)

      {:ok, refs}
    end
  end

  defp excluded?(params, attribute) do
    params
    |> Map.get("excludedAttributes", "")
    |> String.split(",", trim: true)
    |> Enum.any?(&(String.trim(&1) == attribute))
  end

  defp with_meta(resource, conn, collection) do
    meta =
      resource["meta"]
      |> Map.put("version", ~s(W/"#{resource["meta"]["version"]}"))
      |> Map.put("location", base(conn) <> "/scim/v2/#{collection}/#{resource["id"]}")

    Map.put(resource, "meta", meta)
  end

  # ── plumbing ───────────────────────────────────────────────────────────────

  defp discovery(conn, {:ok, doc}, _id), do: send_scim(conn, 200, doc)
  defp discovery(conn, :not_found, id), do: Errors.send(conn, 404, nil, "No such resource: #{id}")

  defp sanitize({:ok, _} = ok), do: ok
  defp sanitize({:ok, _, _} = ok), do: ok
  defp sanitize({:error, scim_type, detail}), do: {:error, {:scim, scim_type, detail}}

  defp fetch({:ok, resource}), do: {:ok, resource}
  defp fetch(:not_found), do: {:error, :not_found}
  defp fetch({:error, _reason} = error), do: error

  # Every failure, as a SCIM error response.
  defp failed(conn, :not_found), do: Errors.send_store_error(conn, :not_found)
  defp failed(conn, {:error, {:scim, type, detail}}), do: Errors.send(conn, 400, type, detail)
  defp failed(conn, {:error, reason}), do: Errors.send_store_error(conn, reason)

  defp with_body(conn, fun) do
    case Ankusa.Http.read_body_limited(conn, @max_body) do
      {:ok, body, conn} ->
        case JSON.decode(body) do
          {:ok, %{} = map} -> fun.(map, conn)
          _ -> Errors.send(conn, 400, "invalidSyntax", "The body must be a JSON object")
        end

      {:too_large, conn} ->
        Errors.send(conn, 413, nil, "The body exceeds #{@max_body} bytes")

      {:error, _reason, conn} ->
        Errors.send(conn, 400, "invalidSyntax", "The body could not be read")
    end
  end

  defp send_scim(conn, status, payload) do
    conn
    |> put_resp_content_type(Errors.content_type())
    |> send_resp(status, JSON.encode!(payload))
  end

  defp base(conn) do
    case Ankusa.config(instance(conn)).scim.base_url do
      url when is_binary(url) -> String.trim_trailing(url, "/")
      nil -> "#{conn.scheme}://#{conn.host}:#{conn.port}"
    end
  end

  defp instance(%Plug.Conn{} = conn) do
    Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
  end
end
