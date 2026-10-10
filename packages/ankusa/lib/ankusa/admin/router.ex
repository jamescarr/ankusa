defmodule Ankusa.Admin.Router do
  @moduledoc """
  The operator HTTP API: health, Prometheus metrics, the redacted config, the
  AsyncAPI document of the channels this instance publishes to, the
  dead-letter queue, replay jobs, and this node's quarantine pen (list and
  purge).

  It exists so operating Ankusa does not require an Elixir shell. Everything
  here had an IEx-only affordance before (`Ankusa.Dispatch.replay/2`,
  `Ankusa.Edge.Quarantine.recent/2`, `:sys.get_state/1`-style poking); this is
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
  node's store, the quarantine list is the edge node's, so a wrong-node
  answer must be distinguishable from "nothing there". `GET /v1/wal` is the
  same kind of answer: it describes this node's own store, and under
  `wal.type: none` there is no queue to describe (`409 wal_disabled`).
  Aggregating across nodes is the operator's job (scrape every admin port).
  """

  use Plug.Router, copy_opts_to_assign: :ankusa_opts

  alias Ankusa.Config
  alias Ankusa.Admin.Redact
  alias Ankusa.Edge.RateLimiter

  # A replay body is a filter, not a payload: three optional scalar keys. 64 KiB
  # is already an order of magnitude more than it can legitimately need.
  @max_replay_body 65_536
  @max_source_body 65_536
  @max_rate_limit_body 65_536
  @default_limit 100
  @max_limit 1000
  # A purge is one synced store batch: big enough to clear a flood in a few
  # calls, small enough that one call never holds the pen for long.
  @default_purge_limit 1000
  @max_purge_limit 10_000

  # Same rule as `Ankusa.SourceStore` (and `ClaimCheck.Ref`): a tenant or source
  # name is a URL path segment and a storage partition, so it must not need
  # encoding. Validated here too so a bad tenant is `invalid_tenant`, distinct
  # from the `invalid_source` a bad name or spec gets.
  @identity_regex ~r/\A[A-Za-z0-9_-]{1,64}\z/

  plug(:match)
  plug(:dispatch)

  get "/health" do
    config = config(conn)

    send_json(conn, 200, %{
      status: "ok",
      instance: to_string(config.instance),
      version: to_string(Application.spec(:ankusa, :vsn)),
      roles: config.roles |> Enum.map(&to_string/1) |> Enum.sort()
    })
  end

  # Readiness, not liveness: 503 while this node's store cannot take a synced
  # write. See `Ankusa.Health`.
  get "/ready" do
    case Ankusa.Health.ready(instance(conn)) do
      {:ok, body} ->
        send_json(conn, 200, body)

      {:error, body} ->
        conn |> Plug.Conn.put_resp_header("retry-after", "1") |> send_json(503, body)
    end
  end

  get "/metrics" do
    conn
    # Prometheus's text exposition format version is a parameter, not a
    # charset: the header is written verbatim.
    |> Plug.Conn.put_resp_header("content-type", "text/plain; version=0.0.4")
    |> Plug.Conn.send_resp(200, Ankusa.Metrics.scrape(instance(conn)))
  end

  get "/v1/wal" do
    config = config(conn)

    case config.wal do
      :none ->
        send_json(conn, 409, %{error: "wal_disabled"})

      _ ->
        send_json(conn, 200, %{
          instance: to_string(config.instance),
          wal: safe_stats(instance(conn))
        })
    end
  end

  get "/v1/config" do
    send_json(conn, 200, Redact.config(config(conn)))
  end

  # No role gate, like `/v1/config`: it is built from config, and the document
  # carries no credentials (`Ankusa.AsyncApi`).
  get "/asyncapi.json" do
    AsyncApiSpex.Plug.RenderSpec.send_spec(conn, Ankusa.AsyncApi.document(instance(conn)))
  end

  get "/v1/dlq" do
    require_role(conn, :dispatch, &dlq_index/1)
  end

  post "/v1/replays" do
    require_role(conn, :dispatch, &replay_create/1)
  end

  get "/v1/replays" do
    require_role(conn, :dispatch, &replay_list/1)
  end

  get "/v1/replays/:id" do
    require_role(conn, :dispatch, &replay_get(&1, id))
  end

  patch "/v1/replays/:id" do
    require_role(conn, :dispatch, &replay_update(&1, id))
  end

  get "/v1/quarantine" do
    require_role(conn, :edge, &quarantine_index/1)
  end

  delete "/v1/quarantine" do
    require_role(conn, :edge, &quarantine_purge/1)
  end

  get "/v1/tenants/:tenant/sources" do
    sources_index(conn, tenant)
  end

  post "/v1/tenants/:tenant/sources" do
    source_create(conn, tenant)
  end

  get "/v1/tenants/:tenant/sources/:name" do
    source_get(conn, tenant, name)
  end

  put "/v1/tenants/:tenant/sources/:name" do
    source_update(conn, tenant, name)
  end

  delete "/v1/tenants/:tenant/sources/:name" do
    source_delete(conn, tenant, name)
  end

  # The limiter is the edge node's own state (`Ankusa.Edge.RateLimiter`), so
  # these are role-gated like the quarantine list, not node-agnostic like
  # source management.
  get "/v1/rate-limits" do
    require_role(conn, :edge, &rate_limits_index/1)
  end

  get "/v1/tenants/:tenant/rate-limit" do
    require_role(conn, :edge, &rate_limit_get(&1, tenant))
  end

  put "/v1/tenants/:tenant/rate-limit" do
    require_role(conn, :edge, &rate_limit_put(&1, tenant))
  end

  delete "/v1/tenants/:tenant/rate-limit" do
    require_role(conn, :edge, &rate_limit_delete(&1, tenant))
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
      case Ankusa.Queue.dead(instance(conn),
             source_id: source_id,
             since: since,
             limit: clamp_limit(limit)
           ) do
        {:ok, %{total: total, entries: entries}} ->
          send_json(conn, 200, %{total: total, entries: Enum.map(entries, &dlq_entry/1)})

        {:error, _reason} ->
          send_json(conn, 503, %{error: "store_unavailable"})
      end
    else
      {:error, field} -> invalid_filter(conn, field)
    end
  end

  defp quarantine_index(conn) do
    params = Plug.Conn.fetch_query_params(conn).query_params

    with {:ok, limit} <- int_param(params, "limit", @default_limit) do
      case Ankusa.Edge.Quarantine.recent(instance(conn), clamp_limit(limit)) do
        {:ok, entries} ->
          send_json(conn, 200, %{entries: Enum.map(entries, &quarantine_entry/1)})

        {:error, _reason} ->
          send_json(conn, 503, %{error: "store_unavailable"})
      end
    else
      {:error, field} -> invalid_filter(conn, field)
    end
  end

  # `since`/`until` are inclusive bounds on `received_at` (ms). No filter at
  # all purges the oldest `limit` entries.
  defp quarantine_purge(conn) do
    params = Plug.Conn.fetch_query_params(conn).query_params

    with {:ok, source_id} <- string_param(params, "source_id"),
         {:ok, id} <- string_param(params, "id"),
         {:ok, since} <- non_neg_param(params, "since"),
         {:ok, until} <- non_neg_param(params, "until"),
         :ok <- if(since && until && until < since, do: {:error, "until"}, else: :ok),
         {:ok, limit} <- int_param(params, "limit", @default_purge_limit) do
      filter = %{source_id: source_id, id: id, since: since, until: until}
      limit = limit |> max(1) |> min(@max_purge_limit)

      case Ankusa.Edge.Quarantine.purge(instance(conn), filter, limit) do
        {:ok, %{deleted: deleted, bytes: bytes}} ->
          send_json(conn, 200, %{deleted: deleted, bytes: bytes})

        {:error, :store_unavailable} ->
          send_json(conn, 503, %{error: "store_unavailable"})
      end
    else
      {:error, field} -> invalid_filter(conn, field)
    end
  end

  defp non_neg_param(params, key) do
    case int_param(params, key, nil) do
      {:ok, value} when is_integer(value) and value < 0 -> {:error, key}
      other -> other
    end
  end

  # ── tenant-scoped sources ───────────────────────────────────────────────────

  # No role gate: source management is not tied to a node role, the same way
  # `/v1/config` isn't. The store resolves from the instance's config, so this
  # works on any node running the admin port.
  defp sources_index(conn, tenant) do
    if valid_identity?(tenant) do
      entries =
        instance(conn)
        |> Ankusa.SourceStore.list_tenant(tenant)
        |> Enum.sort_by(& &1.name)
        |> Enum.map(&Redact.source_entry/1)

      send_json(conn, 200, %{tenant: tenant, entries: entries})
    else
      invalid_tenant(conn)
    end
  end

  defp source_get(conn, tenant, name) do
    cond do
      not valid_identity?(tenant) ->
        invalid_tenant(conn)

      not valid_identity?(name) ->
        invalid_source(conn, invalid_name_message(name))

      true ->
        case Ankusa.SourceStore.get(instance(conn), tenant, name) do
          {:ok, stored} -> send_json(conn, 200, Redact.source_entry(stored))
          :error -> send_json(conn, 404, %{error: "source_not_found"})
        end
    end
  end

  defp source_create(conn, tenant) do
    if valid_identity?(tenant) do
      with_source_body(conn, fn body, conn ->
        name = Map.get(body, "name")
        spec = Map.delete(body, "name")
        put_source(conn, 201, Ankusa.SourceStore.put(instance(conn), tenant, name, spec, :create))
      end)
    else
      invalid_tenant(conn)
    end
  end

  defp source_update(conn, tenant, name) do
    cond do
      not valid_identity?(tenant) ->
        invalid_tenant(conn)

      not valid_identity?(name) ->
        invalid_source(conn, invalid_name_message(name))

      true ->
        with_source_body(conn, fn body, conn ->
          # Identity lives in the URL, so a `name` key smuggled into the body is
          # dropped before validation (the store drops `tenant` too).
          spec = Map.delete(body, "name")

          put_source(
            conn,
            200,
            Ankusa.SourceStore.put(instance(conn), tenant, name, spec, :update)
          )
        end)
    end
  end

  defp source_delete(conn, tenant, name) do
    cond do
      not valid_identity?(tenant) ->
        invalid_tenant(conn)

      not valid_identity?(name) ->
        invalid_source(conn, invalid_name_message(name))

      true ->
        params = Plug.Conn.fetch_query_params(conn).query_params

        case deliveries_param(params) do
          {:ok, mode} -> delete_source(conn, tenant, name, mode)
          {:error, field} -> invalid_filter(conn, field)
        end
    end
  end

  defp deliveries_param(params) do
    case Map.fetch(params, "deliveries") do
      :error -> {:ok, nil}
      {:ok, "dead_letter"} -> {:ok, :dead_letter}
      {:ok, _other} -> {:error, "deliveries"}
    end
  end

  # Rows still queued for a source would be dead-lettered as `source_gone`
  # once it is gone — or delivered to the sinks of a source re-created under
  # the same name. So this node's queue is checked first: a source with rows
  # is refused (`409 source_has_deliveries`) unless the caller chose
  # `?deliveries=dead_letter`, which dead-letters the pending ones now
  # (claimed ones settle on their own). Node-local, like the DLQ. A source
  # this node's store cannot read (missing, or config-only) skips the check:
  # `SourceStore.delete/3` answers for it.
  defp delete_source(conn, tenant, name, mode) do
    instance = instance(conn)
    config = config(conn)
    source_id = Ankusa.SourceStore.Table.source_id(tenant, name)

    with true <- config.wal == :disk and Ankusa.Instance.store?(config),
         {:ok, _stored} <- Ankusa.SourceStore.get(instance, tenant, name) do
      case Ankusa.Queue.pending_for_source(instance, source_id) do
        {:ok, %{pending: 0, inflight: 0}} ->
          delete_and_reply(conn, tenant, name)

        {:ok, counts} when mode == nil ->
          send_json(conn, 409, Map.put(counts, :error, "source_has_deliveries"))

        {:ok, %{pending: 0}} ->
          delete_and_reply(conn, tenant, name)

        {:ok, _counts} ->
          dead_letter_and_delete(conn, config, tenant, name, source_id)

        {:error, _reason} ->
          source_store_unavailable(conn)
      end
    else
      _ -> delete_and_reply(conn, tenant, name)
    end
  end

  defp dead_letter_and_delete(conn, config, tenant, name, source_id) do
    if Config.role?(config, :dispatch) do
      case Ankusa.Dispatch.Pipeline.dead_letter_source(instance(conn), source_id) do
        {:ok, _dead} ->
          delete_and_reply(conn, tenant, name)

        {:error, :unavailable} ->
          conn
          |> Plug.Conn.put_resp_header("retry-after", "1")
          |> send_json(503, %{error: "dispatch_unavailable"})

        {:error, _reason} ->
          source_store_unavailable(conn)
      end
    else
      send_json(conn, 409, %{error: "role_not_enabled", role: "dispatch"})
    end
  end

  defp delete_and_reply(conn, tenant, name) do
    case Ankusa.SourceStore.delete(instance(conn), tenant, name) do
      # 204 carries no body: there is nothing left to describe.
      :ok -> Plug.Conn.send_resp(conn, 204, "")
      {:error, :not_found} -> send_json(conn, 404, %{error: "source_not_found"})
      {:error, :invalid, message} -> invalid_source(conn, message)
      {:error, :store_unavailable} -> source_store_unavailable(conn)
      {:error, :read_only} -> send_json(conn, 409, %{error: "source_store_read_only"})
    end
  end

  # Read the body as a JSON object, then hand it to `fun` along with the conn
  # that has had its body consumed. A body that is empty, oversized, unparseable,
  # or not an object is the same `invalid_source`: all mean "the spec isn't a
  # JSON object".
  defp with_source_body(conn, fun) do
    case Ankusa.Http.read_body_limited(conn, @max_source_body) do
      {:ok, body, conn} ->
        case JSON.decode(body) do
          {:ok, map} when is_map(map) -> fun.(map, conn)
          _ -> invalid_source(conn, "body must be a JSON object")
        end

      {:too_large, conn} ->
        invalid_source(conn, "body must be a JSON object")

      {:error, _reason, conn} ->
        invalid_source(conn, "body must be a JSON object")
    end
  end

  defp put_source(conn, ok_status, result) do
    case result do
      {:ok, stored} -> send_json(conn, ok_status, Redact.source_entry(stored))
      {:error, :invalid, message} -> invalid_source(conn, message)
      {:error, :exists} -> send_json(conn, 409, %{error: "source_exists"})
      {:error, :not_found} -> send_json(conn, 404, %{error: "source_not_found"})
      {:error, :store_unavailable} -> source_store_unavailable(conn)
      {:error, :read_only} -> send_json(conn, 409, %{error: "source_store_read_only"})
    end
  end

  # ── per-tenant rate limits ──────────────────────────────────────────────────

  defp rate_limits_index(conn) do
    %{default: default, tenants: tenants} = RateLimiter.list(instance(conn))

    send_json(conn, 200, %{
      default: limit(default),
      tenants:
        Enum.map(tenants, fn {tenant, limit, source} ->
          rate_limit_entry(tenant, {limit, source})
        end)
    })
  end

  defp rate_limit_get(conn, tenant) do
    if valid_identity?(tenant) do
      send_json(
        conn,
        200,
        rate_limit_entry(tenant, RateLimiter.effective(instance(conn), tenant))
      )
    else
      invalid_tenant(conn)
    end
  end

  defp rate_limit_put(conn, tenant) do
    if valid_identity?(tenant) do
      with_rate_limit_body(conn, fn attrs, conn ->
        case RateLimiter.put_override(instance(conn), tenant, attrs) do
          {:ok, limit} -> send_json(conn, 200, rate_limit_entry(tenant, {limit, :override}))
          {:error, :invalid, message} -> invalid_rate_limit(conn, message)
          {:error, :store_unavailable} -> send_json(conn, 503, %{error: "store_unavailable"})
        end
      end)
    else
      invalid_tenant(conn)
    end
  end

  defp rate_limit_delete(conn, tenant) do
    if valid_identity?(tenant) do
      case RateLimiter.delete_override(instance(conn), tenant) do
        # 204 carries no body: there is nothing left to describe.
        :ok -> Plug.Conn.send_resp(conn, 204, "")
        {:error, :not_found} -> send_json(conn, 404, %{error: "rate_limit_not_found"})
        {:error, :store_unavailable} -> send_json(conn, 503, %{error: "store_unavailable"})
      end
    else
      invalid_tenant(conn)
    end
  end

  defp rate_limit_entry(tenant, {limit, source}) do
    %{
      tenant: tenant,
      rate: limit && limit.rate,
      burst: limit && limit.burst,
      source: Atom.to_string(source)
    }
  end

  defp limit(nil), do: nil
  defp limit(%{rate: rate, burst: burst}), do: %{rate: rate, burst: burst}

  # The body is a limit, not a spec: two required keys. A body that is empty,
  # oversized, unparseable, or not an object is the same `invalid_rate_limit`,
  # whose message is the parser's.
  defp with_rate_limit_body(conn, fun) do
    case Ankusa.Http.read_body_limited(conn, @max_rate_limit_body) do
      {:ok, body, conn} ->
        case JSON.decode(body) do
          {:ok, map} when is_map(map) -> fun.(map, conn)
          _ -> invalid_rate_limit(conn, "body must be a JSON object")
        end

      {:too_large, conn} ->
        invalid_rate_limit(conn, "body must be a JSON object")

      {:error, _reason, conn} ->
        invalid_rate_limit(conn, "body must be a JSON object")
    end
  end

  defp valid_identity?(value), do: is_binary(value) and Regex.match?(@identity_regex, value)

  defp invalid_tenant(conn), do: send_json(conn, 400, %{error: "invalid_tenant"})

  defp invalid_source(conn, message),
    do: send_json(conn, 400, %{error: "invalid_source", message: message})

  # The store could not persist the write (a Redis outage, a failed sync): the
  # request was fine, so the client retries rather than fixes it.
  defp source_store_unavailable(conn) do
    conn
    |> Plug.Conn.put_resp_header("retry-after", "1")
    |> send_json(503, %{error: "store_unavailable"})
  end

  defp invalid_rate_limit(conn, message),
    do: send_json(conn, 400, %{error: "invalid_rate_limit", message: message})

  # Matches `Ankusa.SourceStore`'s own message for a bad identity.
  defp invalid_name_message(name),
    do: "name #{inspect(name)} must match [A-Za-z0-9_-]{1,64}"

  # ── replay jobs ────────────────────────────────────────────────────────────

  @kind_map %{"dlq" => :dlq, "archive" => :archive, "quarantine" => :quarantine}
  @state_map %{"running" => :running, "paused" => :paused, "cancelled" => :cancelled}
  @replay_spec_keys ~w(kind source_id id since until from to sinks rate max_lag_ms)
  @replay_patch_keys ~w(state rate max_lag_ms)

  defp replay_create(conn) do
    with_replay_body(conn, fn map, conn ->
      with {:ok, spec} <- replay_spec(map) do
        case Ankusa.Replay.start(instance(conn), spec) do
          {:ok, :created, job} ->
            send_json(conn, 202, Ankusa.Replay.to_json(job))

          {:ok, :existing, job} ->
            send_json(conn, 200, Ankusa.Replay.to_json(job))

          {:error, {:invalid, field}} ->
            invalid_filter(conn, field)

          {:error, :too_many_replays} ->
            send_json(conn, 409, %{error: "too_many_replays"})

          {:error, {:role_not_enabled, role}} ->
            send_json(conn, 409, %{error: "role_not_enabled", role: to_string(role)})

          {:error, :store_unavailable} ->
            send_json(conn, 503, %{error: "store_unavailable"})
        end
      else
        {:error, field} -> invalid_filter(conn, field)
      end
    end)
  end

  defp replay_list(conn) do
    case Ankusa.Replay.list(instance(conn)) do
      {:ok, jobs} -> send_json(conn, 200, %{replays: Enum.map(jobs, &Ankusa.Replay.to_json/1)})
      {:error, :store_unavailable} -> send_json(conn, 503, %{error: "store_unavailable"})
    end
  end

  defp replay_get(conn, id) do
    case Ankusa.Replay.get(instance(conn), id) do
      {:ok, job} -> send_json(conn, 200, Ankusa.Replay.to_json(job))
      {:error, :not_found} -> send_json(conn, 404, %{error: "replay_not_found"})
      {:error, :store_unavailable} -> send_json(conn, 503, %{error: "store_unavailable"})
    end
  end

  defp replay_update(conn, id) do
    with_replay_body(conn, fn map, conn ->
      with {:ok, patch} <- replay_patch(map) do
        case Ankusa.Replay.update(instance(conn), id, patch) do
          {:ok, job} -> send_json(conn, 200, Ankusa.Replay.to_json(job))
          {:error, :not_found} -> send_json(conn, 404, %{error: "replay_not_found"})
          {:error, :finished} -> send_json(conn, 409, %{error: "replay_finished"})
          {:error, {:invalid, field}} -> invalid_filter(conn, field)
          {:error, :store_unavailable} -> send_json(conn, 503, %{error: "store_unavailable"})
        end
      else
        {:error, field} -> invalid_filter(conn, field)
      end
    end)
  end

  # A replay body is a spec, not a payload: read it as a JSON object with a
  # modest size cap. Unknown keys and malformed bodies are `invalid_filter`.
  defp with_replay_body(conn, fun) do
    case Ankusa.Http.read_body_limited(conn, @max_replay_body) do
      {:ok, body, conn} ->
        case decode_filter(body) do
          {:ok, map} -> fun.(map, conn)
          {:error, field} -> invalid_filter(conn, field)
        end

      {:too_large, conn} ->
        invalid_filter(conn, "body")

      {:error, _reason, conn} ->
        invalid_filter(conn, "body")
    end
  end

  defp replay_spec(map) do
    case unknown_key(map, @replay_spec_keys) do
      nil ->
        with {:ok, kind} <- enum_param(map, "kind", @kind_map),
             {:ok, source_id} <- string_param(map, "source_id"),
             {:ok, id} <- string_param(map, "id"),
             {:ok, since} <- int_param(map, "since", nil),
             {:ok, until} <- int_param(map, "until", nil),
             {:ok, from} <- int_param(map, "from", nil),
             {:ok, to} <- int_param(map, "to", nil),
             {:ok, rate} <- int_param(map, "rate", nil),
             {:ok, max_lag_ms} <- int_param(map, "max_lag_ms", nil),
             {:ok, sinks} <- sinks_param(map) do
          spec =
            %{kind: kind}
            |> put_present(:source_id, source_id)
            |> put_present(:id, id)
            |> put_present(:since, since)
            |> put_present(:until, until)
            |> put_present(:from, from)
            |> put_present(:to, to)
            |> put_present(:rate, rate)
            |> put_present(:max_lag_ms, max_lag_ms)
            |> put_present(:sinks, sinks)

          {:ok, spec}
        end

      key ->
        {:error, key}
    end
  end

  defp replay_patch(map) do
    case unknown_key(map, @replay_patch_keys) do
      nil ->
        with {:ok, state} <- optional_enum(map, "state", @state_map),
             {:ok, rate} <- int_param(map, "rate", nil),
             {:ok, max_lag_ms} <- int_param(map, "max_lag_ms", nil) do
          patch =
            %{}
            |> put_present(:state, state)
            |> put_present(:rate, rate)
            |> put_present(:max_lag_ms, max_lag_ms)

          {:ok, patch}
        end

      key ->
        {:error, key}
    end
  end

  defp unknown_key(map, allowed) do
    Enum.find(Map.keys(map), &(&1 not in allowed))
  end

  defp enum_param(map, key, mapping) do
    case Map.fetch(map, key) do
      :error ->
        {:error, key}

      {:ok, value} ->
        case Map.fetch(mapping, value) do
          {:ok, atom} -> {:ok, atom}
          :error -> {:error, key}
        end
    end
  end

  defp optional_enum(map, key, mapping) do
    case Map.fetch(map, key) do
      :error ->
        {:ok, nil}

      {:ok, value} ->
        case Map.fetch(mapping, value) do
          {:ok, atom} -> {:ok, atom}
          :error -> {:error, key}
        end
    end
  end

  defp sinks_param(map) do
    case Map.fetch(map, "sinks") do
      :error ->
        {:ok, nil}

      {:ok, sinks} when is_list(sinks) ->
        if Enum.all?(sinks, &is_integer/1), do: {:ok, sinks}, else: {:error, "sinks"}

      {:ok, _other} ->
        {:error, "sinks"}
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
      reason: reason
    }
  end

  # `tenant_id` and `size` are absent from entries written before the pen kept
  # them (0.4): `null`.
  defp quarantine_entry(
         %{id: id, source_id: source_id, received_at: received_at, reason: reason} = e
       ) do
    %{
      id: id,
      source_id: source_id,
      tenant_id: Map.get(e, :tenant_id),
      received_at: received_at,
      reason: inspect(reason),
      size: Map.get(e, :size)
    }
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

  # A `:claim_check`-only node with the admin API on runs no store, and the
  # store is node-local anyway: a missing or unreadable one is `%{}`, not a
  # 500.
  defp safe_stats(instance) do
    case Ankusa.Queue.stats(instance) do
      {:ok, stats} -> stats
      {:error, _reason} -> %{}
    end
  rescue
    _ -> %{}
  catch
    :exit, _ -> %{}
  end

  defp invalid_filter(conn, field),
    do: send_json(conn, 400, %{error: "invalid_filter", field: field})

  defp config(%Plug.Conn{} = conn), do: Ankusa.config(instance(conn))

  defp instance(%Plug.Conn{} = conn) do
    Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
  end

  defp send_json(conn, status, payload), do: Ankusa.Http.send_json(conn, status, payload)
end
