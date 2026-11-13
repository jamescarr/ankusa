defmodule Ankusa.Gateway.Sharing do
  @moduledoc """
  The Sharing API v1, while `gateway.sharing.enabled` (else every route is
  `404`):

    * `GET /v1/spaces/{space}/acl?key=<k>` or `?prefix=<p>` —
      `{"owner": <the object's owner or null>, "grants": [...]}`;
    * `POST /v1/spaces/{space}/grants` — `{"resource": {"key": k} | {"prefix": p},
      "subject": s, "actions": [...], "expires_in_s": n | null}`: `201` the grant;
    * `DELETE /v1/spaces/{space}/grants/{id}?key=<k>` or `?prefix=<p>` — `204`
      (also for an id the document does not hold);
    * `POST /v1/spaces/{space}/links` — `{"key": k, "expires_in_s": n}`: `201
      {"id", "expires_at", "path"}`, a share link (`Ankusa.Gateway.Sharing.Link`)
      reading that one object.

  Every route names its resource (an object must exist: `404`) and authorizes
  `share` on it against the space's policy. Nobody hands out what they do not
  hold: a new grant or link also needs each of its actions on the same
  resource (`403 {"error":"forbidden","action":"<action>"}`). Each mutation
  writes an audit record v1 first (`<space.prefix>.ankusa/audit/v1/dt=<UTC
  date>/<ULID>.json`); when that write fails nothing changes and the answer is
  `503 audit_unavailable`. A failed ACL update is followed by a best-effort
  `<action>.failed` record.

  Emits `[:ankusa, :gateway, :share]` `%{}` `%{instance, action: :grant_create
  | :grant_revoke | :link_create, result: :ok | :conflict | :unavailable}`.
  """

  require Logger

  alias Ankusa.Audit.Context
  alias Ankusa.Gateway.{ACL, Authz, Config, Spaces}
  alias Ankusa.Gateway.ACL.Store
  alias Ankusa.Gateway.Authz.Request
  alias Ankusa.Gateway.Sharing.Link
  alias Ankusa.{Http, Telemetry}

  @max_body 65_536
  @max_subject 512
  @max_expires_in_s 31_536_000

  @doc "`GET /v1/spaces/{space}/acl`."
  @spec acl(Plug.Conn.t(), String.t()) :: Plug.Conn.t()
  def acl(conn, space_name) do
    conn = Plug.Conn.fetch_query_params(conn)

    with {:ok, gateway, space} <- space(conn, space_name),
         {:ok, resource} <- query_resource(conn, "invalid_key"),
         {:ok, head} <- existing(conn, gateway, space, resource),
         :ok <- authorize(conn, gateway, space, resource, :share) do
      deadline = System.monotonic_time(:millisecond) + gateway.authz.deadline_ms
      {kind, key} = resource

      case Store.fetch(gateway.instance, space, kind, key, deadline) do
        {:ok, doc} ->
          grants = if live?(doc, head), do: Enum.map(doc.grants, &ACL.encode_grant/1), else: []
          Http.send_json(conn, 200, %{owner: head && head.owner, grants: grants})

        {:error, {:unavailable, reason}} ->
          Ankusa.Gateway.Http.store_error(conn, reason)
      end
    else
      {:error, %Plug.Conn{} = conn} -> conn
    end
  end

  @doc "`POST /v1/spaces/{space}/grants`."
  @spec create_grant(Plug.Conn.t(), String.t()) :: Plug.Conn.t()
  def create_grant(conn, space_name) do
    with {:ok, gateway, space} <- space(conn, space_name),
         {:ok, body, conn} <- body(conn),
         {:ok, grant} <- grant_body(conn, body),
         {:ok, _head} <- existing(conn, gateway, space, grant.resource),
         :ok <- authorize(conn, gateway, space, grant.resource, :share),
         :ok <- holds(conn, gateway, space, grant.resource, grant.actions) do
      now = System.os_time(:second)

      new = %{
        id: ACL.grant_id(grant.resource),
        subject: grant.subject,
        actions: grant.actions,
        created_by: conn.assigns.principal.id,
        created_at: now,
        expires_at: grant.expires_in_s && now + grant.expires_in_s
      }

      mutate(conn, gateway, space, grant.resource, :grant_create, new, fn conn ->
        Http.send_json(conn, 201, ACL.encode_grant(new))
      end)
    else
      {:error, %Plug.Conn{} = conn} -> conn
    end
  end

  @doc "`DELETE /v1/spaces/{space}/grants/{id}`."
  @spec revoke_grant(Plug.Conn.t(), String.t(), String.t()) :: Plug.Conn.t()
  def revoke_grant(conn, space_name, id) do
    conn = Plug.Conn.fetch_query_params(conn)

    with {:ok, gateway, space} <- space(conn, space_name),
         {:ok, resource} <- query_resource(conn, "invalid_grant"),
         :ok <- same_resource(conn, id, resource),
         :ok <- authorize(conn, gateway, space, resource, :share) do
      mutate(conn, gateway, space, resource, :grant_revoke, id, fn conn ->
        Plug.Conn.send_resp(conn, 204, "")
      end)
    else
      {:error, %Plug.Conn{} = conn} -> conn
    end
  end

  @doc "`POST /v1/spaces/{space}/links`."
  @spec create_link(Plug.Conn.t(), String.t()) :: Plug.Conn.t()
  def create_link(conn, space_name) do
    with {:ok, gateway, space} <- space(conn, space_name),
         {:ok, body, conn} <- body(conn),
         {:ok, key, expires_in_s} <- link_body(conn, gateway, body),
         resource = {:object, key},
         {:ok, _head} <- existing(conn, gateway, space, resource),
         :ok <- authorize(conn, gateway, space, resource, :share),
         :ok <- holds(conn, gateway, space, resource, [:read]) do
      now = System.os_time(:second)
      expires_at = now + expires_in_s

      grant = %{
        id: ACL.grant_id(resource),
        subject: "link",
        actions: [:read],
        created_by: conn.assigns.principal.id,
        created_at: now,
        expires_at: expires_at
      }

      mutate(conn, gateway, space, resource, :link_create, grant, fn conn ->
        sig = Link.sign(gateway.sharing.signing_keys, space.name, grant.id, expires_at)

        Http.send_json(conn, 201, %{
          id: grant.id,
          expires_at: expires_at,
          path: Link.path(space.name, key, grant.id, expires_at, sig)
        })
      end)
    else
      {:error, %Plug.Conn{} = conn} -> conn
    end
  end

  # ── the steps ───────────────────────────────────────────────────────────────

  defp space(conn, name) do
    gateway = Ankusa.Gateway.config(instance(conn))

    with true <- gateway.sharing.enabled,
         {:ok, space} <- Spaces.fetch(gateway, name) do
      {:ok, gateway, space}
    else
      _ -> {:error, Http.send_json(conn, 404, %{error: "not_found"})}
    end
  end

  # Exactly one of `?key=` and `?prefix=`.
  defp query_resource(conn, error) do
    case conn.query_params do
      %{"key" => key, "prefix" => _} when is_binary(key) -> bad(conn, error)
      %{"key" => key} -> resource(conn, {:object, key}, error)
      %{"prefix" => prefix} -> resource(conn, {:prefix, prefix}, error)
      _ -> bad(conn, error)
    end
  end

  defp resource(conn, {:object, key}, error) do
    if Spaces.validate_key(key) == :ok, do: {:ok, {:object, key}}, else: bad(conn, error)
  end

  # A grant's prefix is a directory: `""`, or ending in `/`.
  defp resource(conn, {:prefix, prefix}, error) do
    if is_binary(prefix) and Spaces.validate_prefix(prefix) == :ok and
         (prefix == "" or String.ends_with?(prefix, "/")),
       do: {:ok, {:prefix, prefix}},
       else: bad(conn, error)
  end

  defp bad(conn, error), do: {:error, Http.send_json(conn, 400, %{error: error})}

  # An object must exist; a prefix always does. `{:ok, head | nil}`.
  defp existing(_conn, _gateway, _space, {:prefix, _}), do: {:ok, nil}

  defp existing(conn, gateway, space, {:object, key}) do
    {module, _opts} = space.store

    case module.head_object(
           gateway.instance,
           Spaces.object_key(space, key),
           Config.store_opts(gateway, space)
         ) do
      {:ok, meta} ->
        {:ok,
         %{owner: meta.metadata["ankusa-owner"], incarnation: meta.metadata["ankusa-incarnation"]}}

      {:error, reason} ->
        {:error, Ankusa.Gateway.Http.store_error(conn, reason)}
    end
  end

  defp live?(nil, _head), do: false
  defp live?(%{resource: %{kind: :prefix}}, _head), do: true
  defp live?(%{incarnation: incarnation}, %{incarnation: incarnation}), do: true
  defp live?(_doc, _head), do: false

  defp authorize(conn, gateway, space, resource, action) do
    case decide(conn, gateway, space, resource, action) do
      {:allow, _meta} -> :ok
      {:deny, _meta} -> {:error, Http.send_json(conn, 403, %{error: "forbidden"})}
      {:error, _} -> {:error, authz_unavailable(conn)}
    end
  end

  # Every action a new grant carries, on the same resource: the first refused
  # one names itself.
  defp holds(conn, gateway, space, resource, actions) do
    Enum.reduce_while(actions, :ok, fn action, :ok ->
      case decide(conn, gateway, space, resource, action) do
        {:allow, _meta} ->
          {:cont, :ok}

        {:deny, _meta} ->
          {:halt,
           {:error,
            Http.send_json(conn, 403, %{error: "forbidden", action: Atom.to_string(action)})}}

        {:error, _} ->
          {:halt, {:error, authz_unavailable(conn)}}
      end
    end)
  end

  defp decide(conn, gateway, space, {kind, key}, action) do
    resource = %{kind: kind, space: space.name, key: key, tenant: nil}
    Authz.decide(gateway.instance, space.policy, Request.from_conn(conn, action, resource))
  end

  defp authz_unavailable(conn) do
    conn
    |> retry_after()
    |> Http.send_json(503, %{error: "authz_unavailable"})
  end

  defp same_resource(conn, id, {kind, key}) do
    case ACL.parse_grant_id(id) do
      {:ok, ^kind, hash} -> if hash == ACL.hash(key), do: :ok, else: bad(conn, "invalid_grant")
      _ -> bad(conn, "invalid_grant")
    end
  end

  # ── bodies ──────────────────────────────────────────────────────────────────

  defp body(conn) do
    read_opts = Http.read_opts(Ankusa.config(instance(conn)))

    case Http.read_body_limited(conn, @max_body, read_opts) do
      {:ok, body, conn} ->
        case JSON.decode(body) do
          {:ok, %{} = map} -> {:ok, map, conn}
          _ -> {:error, Http.send_json(conn, 400, %{error: "invalid_json"})}
        end

      {:too_large, conn} ->
        {:error, Http.send_json(conn, 413, %{error: "too_large"})}

      {:error, _reason, conn} ->
        {:error, Http.send_json(conn, 408, %{error: "request_timeout"})}
    end
  end

  defp grant_body(conn, body) do
    with {:ok, resource} <- body_resource(body["resource"]),
         {:ok, subject} <- subject(body["subject"]),
         {:ok, actions} <- actions(body["actions"]),
         {:ok, expires_in_s} <- expires_in(body["expires_in_s"]),
         true <- map_size(Map.drop(body, ~w(resource subject actions expires_in_s))) == 0 do
      {:ok, %{resource: resource, subject: subject, actions: actions, expires_in_s: expires_in_s}}
    else
      _ -> bad(conn, "invalid_grant")
    end
  end

  defp body_resource(%{"key" => key} = resource) when map_size(resource) == 1 and is_binary(key),
    do: if(Spaces.validate_key(key) == :ok, do: {:ok, {:object, key}}, else: :error)

  defp body_resource(%{"prefix" => prefix} = resource)
       when map_size(resource) == 1 and is_binary(prefix) do
    if Spaces.validate_prefix(prefix) == :ok and (prefix == "" or String.ends_with?(prefix, "/")),
      do: {:ok, {:prefix, prefix}},
      else: :error
  end

  defp body_resource(_other), do: :error

  defp subject("anyone"), do: {:ok, "anyone"}

  defp subject(subject) when is_binary(subject) and byte_size(subject) <= @max_subject do
    case String.split(subject, ":", parts: 2) do
      [type, value] when type in ["user", "group", "tenant"] and value != "" -> {:ok, subject}
      _ -> :error
    end
  end

  defp subject(_other), do: :error

  defp actions([_ | _] = names) do
    actions = Enum.map(names, &ACL.action/1)

    if Enum.all?(actions, &match?({:ok, _}, &1)),
      do: {:ok, actions |> Enum.map(&elem(&1, 1)) |> Enum.uniq()},
      else: :error
  end

  defp actions(_other), do: :error

  defp expires_in(nil), do: {:ok, nil}
  defp expires_in(n) when is_integer(n) and n in 1..@max_expires_in_s, do: {:ok, n}
  defp expires_in(_other), do: :error

  defp link_body(conn, gateway, %{"key" => key, "expires_in_s" => n} = body)
       when map_size(body) == 2 and is_binary(key) and is_integer(n) do
    if Spaces.validate_key(key) == :ok and n in 1..gateway.sharing.link_max_ttl_s,
      do: {:ok, key, n},
      else: bad(conn, "invalid_link")
  end

  defp link_body(conn, _gateway, _body), do: bad(conn, "invalid_link")

  # ── the mutation: audit first, then the document ────────────────────────────

  defp mutate(conn, gateway, space, resource, action, change, respond) do
    instance = gateway.instance
    {kind, key} = resource

    case audit(conn, gateway, space, resource, action, change) do
      {:ok, ref} ->
        case Store.update(
               instance,
               space,
               kind,
               key,
               &apply_change(&1, space, resource, action, change, gateway)
             ) do
          {:ok, _doc} ->
            if kind == :object, do: Store.invalidate(instance, space, key)
            share_event(instance, action, :ok)
            respond.(conn)

          {:error, reason} ->
            failed(conn, gateway, space, resource, action, ref, reason)
        end

      {:error, _reason} ->
        share_event(instance, action, :unavailable)

        conn
        |> retry_after()
        |> Http.send_json(503, %{error: "audit_unavailable"})
    end
  end

  defp apply_change(doc, space, resource, :grant_revoke, id, _gateway) do
    doc = doc || ACL.new(space.name, resource, nil)
    {:ok, %{doc | grants: Enum.reject(doc.grants, &(&1.id == id))}}
  end

  defp apply_change(doc, space, resource, _create, grant, gateway) do
    doc = doc || ACL.new(space.name, resource, nil)
    now = System.os_time(:second)
    live = Enum.filter(doc.grants, &ACL.live?(&1, now))

    if length(live) >= gateway.sharing.max_grants_per_resource,
      do: {:error, :too_many_grants},
      else: {:ok, %{doc | grants: live ++ [grant]}}
  end

  defp failed(conn, gateway, space, resource, action, ref, reason) do
    result = if reason == :conflict, do: :conflict, else: :unavailable
    share_event(gateway.instance, action, result)
    record_failure(conn, gateway, space, resource, action, ref, reason)

    case reason do
      :too_many_grants -> Http.send_json(conn, 409, %{error: "too_many_grants"})
      :conflict -> Http.send_json(conn, 409, %{error: "conflict"})
      :not_found -> Http.send_json(conn, 404, %{error: "not_found"})
      {:unavailable, why} -> Ankusa.Gateway.Http.store_error(conn, why)
      other -> Ankusa.Gateway.Http.store_error(conn, other)
    end
  end

  # ── audit records v1 ────────────────────────────────────────────────────────

  defp audit(conn, gateway, space, resource, action, change) do
    record = %{
      "action" => action_name(action),
      "grant" => grant_field(change)
    }

    write_record(conn, gateway, space, resource, record)
  end

  defp record_failure(conn, gateway, space, resource, action, ref, reason) do
    record = %{
      "action" => action_name(action) <> ".failed",
      "ref" => ref,
      "reason" => inspect(reason, limit: 20, printable_limit: 512)
    }

    case write_record(conn, gateway, space, resource, record) do
      {:ok, _id} ->
        :ok

      {:error, why} ->
        Logger.warning(
          "[ankusa] gateway: could not write the #{record["action"]} audit record " <>
            "(ref #{ref}): #{inspect(why, limit: 20)}"
        )
    end
  end

  defp write_record(conn, gateway, space, {kind, key}, fields) do
    {module, _opts} = space.store
    context = Context.from_conn(conn)
    id = ACL.ulid()
    now_ms = System.os_time(:millisecond)
    date = now_ms |> DateTime.from_unix!(:millisecond) |> DateTime.to_date() |> Date.to_iso8601()

    record =
      Map.merge(fields, %{
        "v" => 1,
        "id" => id,
        "at" => now_ms,
        "actor" => context.actor,
        "remote_ip" => context.remote_ip,
        "request_id" => context.request_id,
        "space" => space.name,
        "resource" => %{"kind" => Atom.to_string(kind), "key" => key}
      })

    audit_key = space.prefix <> ".ankusa/audit/v1/dt=#{date}/#{id}.json"
    opts = Config.store_opts(gateway, space) ++ [content_type: "application/json", if: :absent]

    case module.put_object(gateway.instance, audit_key, JSON.encode!(record), opts) do
      {:ok, _version} -> {:ok, id}
      {:error, reason} -> {:error, reason}
    end
  end

  defp action_name(:grant_create), do: "grant.create"
  defp action_name(:grant_revoke), do: "grant.revoke"
  defp action_name(:link_create), do: "link.create"

  defp grant_field(id) when is_binary(id), do: %{"id" => id}
  defp grant_field(grant), do: ACL.encode_grant(grant)

  defp share_event(instance, action, result),
    do:
      Telemetry.emit([:gateway, :share], %{}, %{
        instance: instance,
        action: action,
        result: result
      })

  defp retry_after(conn),
    do: Plug.Conn.put_resp_header(conn, "retry-after", Integer.to_string(Enum.random(1..3)))

  defp instance(%Plug.Conn{} = conn),
    do: Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
end
