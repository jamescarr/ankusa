defmodule Ankusa.Admin.Router do
  @moduledoc """
  The operator HTTP API: health, Prometheus metrics, the redacted config, the
  dead-letter queue, and this node's recent quarantine list.

  It exists so operating Ankusa does not require an Elixir shell. Everything
  here had an IEx-only affordance before (`Ankusa.Dispatch.replay/2`,
  `Ankusa.Edge.Quarantine.recent/1`, `:sys.get_state/1`-style poking); this is
  the same surface over HTTP.

  ## No authentication, by design

  Ankusa does not manage users, tokens, or API keys. The only request
  authentication in the framework is provider signature verification on ingest
  (`Ankusa.Edge.Router`), which is webhook semantics, not access control — so
  nothing here reads the `authorization` header. Operators front this port with
  their own proxy, SSO, or network policy, the way Elasticsearch was operated
  before it shipped security. `/v1/config` is still redacted because a proxy may
  let many people read it.

  ## Node-local by design

  A route that needs a role this node does not run returns
  `409 role_not_enabled` rather than an empty success: the DLQ is the dispatch
  node's disk, the quarantine list is the edge node's memory, so a wrong-node
  answer must be distinguishable from "nothing there". Aggregating across nodes
  is the operator's job (scrape every admin port).
  """

  use Plug.Router, copy_opts_to_assign: :ankusa_opts

  alias Ankusa.Config
  alias Ankusa.Admin.Redact

  # A replay body is a filter, not a payload: three optional scalar keys. 64 KiB
  # is already an order of magnitude more than it can legitimately need.
  @max_replay_body 65_536
  @default_limit 100
  @max_limit 1000

  plug(:match)
  plug(:dispatch)

  get "/health" do
    config = config(conn)

    send_json(conn, 200, %{
      status: "ok",
      instance: to_string(config.instance),
      roles: config.roles |> Enum.map(&to_string/1) |> Enum.sort()
    })
  end

  get "/metrics" do
    conn
    # Prometheus's text exposition format version is a parameter, not a
    # charset: the header is written verbatim.
    |> Plug.Conn.put_resp_header("content-type", "text/plain; version=0.0.4")
    |> Plug.Conn.send_resp(200, Ankusa.Metrics.scrape(instance(conn)))
  end

  get "/v1/config" do
    send_json(conn, 200, Redact.config(config(conn)))
  end

  get "/v1/dlq" do
    require_role(conn, :dispatch, &dlq_index/1)
  end

  post "/v1/dlq/replay" do
    require_role(conn, :dispatch, &dlq_replay/1)
  end

  get "/v1/quarantine" do
    require_role(conn, :edge, &quarantine_index/1)
  end

  match _ do
    send_json(conn, 404, %{error: "not_found"})
  end

  # ── routes ──────────────────────────────────────────────────────────────────

  defp dlq_index(conn) do
    params = Plug.Conn.fetch_query_params(conn).query_params

    with {:ok, source_id} <- string_param(params, "source_id"),
         {:ok, since} <- int_param(params, "since", nil),
         {:ok, limit} <- int_param(params, "limit", @default_limit) do
      entries =
        conn
        |> config()
        |> Ankusa.Dispatch.DLQ.entries()
        |> Enum.filter(&matches?(&1, source_id, since))
        # Newest first. The file is append-ordered, so the index breaks ties
        # between entries dead-lettered in the same millisecond — an operator
        # paging the DLQ needs a stable order, and `at` alone isn't one.
        |> Enum.with_index()
        |> Enum.sort_by(fn {%{at: at}, index} -> {at, index} end, :desc)
        |> Enum.map(&elem(&1, 0))

      limited = Enum.take(entries, clamp_limit(limit))

      send_json(conn, 200, %{
        total: length(entries),
        entries: Enum.map(limited, &dlq_entry/1)
      })
    else
      {:error, field} -> invalid_filter(conn, field)
    end
  end

  defp dlq_replay(conn) do
    case Ankusa.Http.read_body_limited(conn, @max_replay_body) do
      {:ok, body, conn} -> replay(conn, body)
      {:too_large, conn} -> invalid_filter(conn, "body")
      {:error, _reason, conn} -> invalid_filter(conn, "body")
    end
  end

  defp quarantine_index(conn) do
    params = Plug.Conn.fetch_query_params(conn).query_params

    with {:ok, limit} <- int_param(params, "limit", @default_limit) do
      entries =
        conn
        |> instance()
        |> Ankusa.Edge.Quarantine.recent()
        |> Enum.take(clamp_limit(limit))
        |> Enum.map(&quarantine_entry/1)

      send_json(conn, 200, %{entries: entries})
    else
      {:error, field} -> invalid_filter(conn, field)
    end
  end

  # ── replay filter ───────────────────────────────────────────────────────────

  # Body rules match `Ankusa.Dispatch.replay/2`'s filter: `source_id` and `id`
  # are exact matches, `since` is an inclusive lower bound on the dead-letter
  # timestamp. An empty body replays everything — the same as `replay/2` with an
  # empty filter, which is the operator's escape hatch for a full drain.
  defp replay(conn, body) do
    with {:ok, filter} <- decode_filter(body),
         {:ok, source_id} <- string_param(filter, "source_id"),
         {:ok, id} <- string_param(filter, "id"),
         {:ok, since} <- int_param(filter, "since", nil) do
      filter =
        %{}
        |> put_present(:source_id, source_id)
        |> put_present(:id, id)
        |> put_present(:since, since)

      replayed = Ankusa.Dispatch.replay(instance(conn), filter)
      send_json(conn, 200, %{replayed: replayed})
    else
      {:error, field} -> invalid_filter(conn, field)
    end
  end

  defp decode_filter(""), do: {:ok, %{}}

  defp decode_filter(body) do
    case JSON.decode(body) do
      {:ok, map} when is_map(map) -> {:ok, map}
      _ -> {:error, "body"}
    end
  end

  defp put_present(filter, _key, nil), do: filter
  defp put_present(filter, key, value), do: Map.put(filter, key, value)

  defp matches?(%{envelope: env, at: at}, source_id, since) do
    (is_nil(source_id) or env.source_id == source_id) and (is_nil(since) or at >= since)
  end

  # Bodies are never returned: an operator triaging the DLQ wants to know what
  # failed and why, and the payloads in there are the customer's.
  defp dlq_entry(%{envelope: env, reason: reason, at: at}) do
    %{
      id: env.id,
      source_id: env.source_id,
      tenant_id: env.tenant_id,
      seq: env.seq,
      received_at: env.received_at,
      dead_lettered_at: at,
      size: env.size,
      content_type: env.content_type,
      reason: inspect(reason)
    }
  end

  defp quarantine_entry(%{id: id, source_id: source_id, received_at: received_at, reason: reason}) do
    %{id: id, source_id: source_id, received_at: received_at, reason: inspect(reason)}
  end

  # ── request bits ────────────────────────────────────────────────────────────

  defp require_role(conn, role, fun) do
    if Config.role?(config(conn), role) do
      fun.(conn)
    else
      send_json(conn, 409, %{error: "role_not_enabled", role: to_string(role)})
    end
  end

  # Absent → `default`. Present but not an integer (a query string always
  # arrives as a binary; a JSON body may carry either) → `{:error, key}`.
  defp int_param(map, key, default) do
    case Map.fetch(map, key) do
      :error ->
        {:ok, default}

      {:ok, value} when is_integer(value) ->
        {:ok, value}

      {:ok, value} when is_binary(value) ->
        case Integer.parse(value) do
          {int, ""} -> {:ok, int}
          _ -> {:error, key}
        end

      {:ok, _other} ->
        {:error, key}
    end
  end

  defp string_param(map, key) do
    case Map.fetch(map, key) do
      :error -> {:ok, nil}
      {:ok, value} when is_binary(value) -> {:ok, value}
      {:ok, _other} -> {:error, key}
    end
  end

  defp clamp_limit(limit), do: limit |> max(0) |> min(@max_limit)

  defp invalid_filter(conn, field),
    do: send_json(conn, 400, %{error: "invalid_filter", field: field})

  defp config(%Plug.Conn{} = conn), do: Ankusa.config(instance(conn))

  defp instance(%Plug.Conn{} = conn) do
    Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
  end

  defp send_json(conn, status, payload), do: Ankusa.Http.send_json(conn, status, payload)
end
