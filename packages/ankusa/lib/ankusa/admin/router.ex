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

  ## Authentication and scopes

  Every route but `GET /health` and `GET /ready` needs a credential
  (`admin.auth`, see `Ankusa.Admin.Auth`): `401` without one, `403` when the
  caller's scopes (`Ankusa.Principal`) do not cover the route. Scopes are
  checked before anything else, so a caller without them learns nothing about
  this node's roles or data.

  | Route | Scope |
  | --- | --- |
  | `GET /metrics`, `GET /v1/wal` | `metrics:read` |
  | `GET /v1/config`, `GET /asyncapi.json` | `config:read` |
  | `PUT /v1/log-level` | `config:write` + `tenant:*` |
  | `POST /v1/config/reload` | `config:write` + `tenant:*` |
  | `GET /v1/dlq`, `DELETE /v1/dlq` | `dlq:read` / `dlq:write` + a `tenant` (filtered) |
  | `POST /v1/replays` | `replays:write` + a `tenant` (filtered), and `dlq:write` / `quarantine:write` for those kinds |
  | `GET /v1/replays[/:id]` | `replays:read` + a `tenant` (filtered) |
  | `PATCH /v1/replays/:id` | `replays:write` + a `tenant` (filtered) |
  | `GET /v1/quarantine`, `DELETE /v1/quarantine` | `quarantine:read` / `quarantine:write` + a `tenant` (filtered) |
  | `GET /v1/tenants/:tenant/sources[/:name]` | `sources:read` + `tenant:<tenant>` |
  | `POST/PUT/DELETE /v1/tenants/:tenant/sources[/:name]` | `sources:write` + `tenant:<tenant>` |
  | `GET /v1/rate-limits` | `rate_limits:read` + a `tenant` (filtered) |
  | `GET/PUT/DELETE /v1/tenants/:tenant/rate-limit` | `rate_limits:read` / `rate_limits:write` + `tenant:<tenant>` |
  | `DELETE /v1/tenants/:tenant?confirm=<tenant>`, `GET /v1/tenants/:tenant/erase` | `tenants:write` + `tenant:*` |

  The DLQ, the quarantine pen, replay jobs and the rate-limit listing hold every
  tenant's rows, so they are filtered by tenant (`Auth.authorize_tenants/4`).
  A caller limited to some `tenant:<id>` scopes gets only those tenants' rows,
  whatever it asks: the filter is forced server-side. `?tenant=<id>` (the body
  key `tenant` of `POST /v1/replays`) narrows the answer to one tenant, and
  naming a tenant the caller does not hold is `403`. Only `tenant:*` reads or
  changes them unfiltered. A quarantine purge deletes, and a replay touches,
  only the caller's tenants' rows; a replay job is visible to (and steerable
  by) a limited caller only when it is scoped to tenants that caller holds.
  `/v1/config` is still redacted, because many people may hold `config:read`.

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

  require Logger

  alias Ankusa.Admin.Auth
  alias Ankusa.Admin.Redact
  alias Ankusa.Audit
  alias Ankusa.Audit.Context
  alias Ankusa.Config
  alias Ankusa.Edge.RateLimiter
  alias Ankusa.Principal

  # A replay body is a filter, not a payload: three optional scalar keys. 64 KiB
  # is already an order of magnitude more than it can legitimately need.
  @max_replay_body 65_536
  @max_source_body 65_536
  @max_rate_limit_body 65_536
  # `{"level": "..."}`
  @max_log_level_body 1_024
  @default_limit 100
  @max_limit 1000
  @error_class_map %{
    "permanent" => :permanent,
    "throttled" => :throttled,
    "transient" => :transient
  }
  # A purge is one synced store batch: big enough to clear a flood in a few
  # calls, small enough that one call never holds the pen for long.
  @default_purge_limit 1000
  @max_purge_limit 10_000

  # Same rule as `Ankusa.SourceStore` (and `ClaimCheck.Ref`): a tenant or source
  # name is a URL path segment and a storage partition, so it must not need
  # encoding. Validated here too so a bad tenant is `invalid_tenant`, distinct
  # from the `invalid_source` a bad name or spec gets.
  @identity_regex ~r/\A[A-Za-z0-9_-]{1,64}\z/

  # Before `:match`, so an unknown path is `401` to a caller with no credential
  # rather than a `404` that confirms it is not there.
  plug(Auth, section: :admin)
  plug(Context)
  plug(:match)
  plug(:dispatch)

  get "/health" do
    Ankusa.Metrics.Router.health(conn)
  end

  # Readiness, not liveness: 503 while this node's store cannot take a synced
  # write. See `Ankusa.Health`.
  get "/ready" do
    Ankusa.Metrics.Router.ready(conn)
  end

  # Also served, alone, by the metrics listener (`Ankusa.Metrics.Router`).
  get "/metrics" do
    Auth.authorize(conn, "metrics:read", &Ankusa.Metrics.Router.metrics/1)
  end

  get "/v1/wal" do
    Auth.authorize(conn, "metrics:read", &wal/1)
  end

  get "/v1/config" do
    Auth.authorize(conn, "config:read", fn conn ->
      send_json(conn, 200, Redact.config(config(conn)))
    end)
  end

  # No role gate, like `/v1/config`: it is built from config, and the document
  # carries no credentials (`Ankusa.AsyncApi`).
  get "/asyncapi.json" do
    Auth.authorize(conn, "config:read", fn conn ->
      AsyncApiSpex.Plug.RenderSpec.send_spec(conn, Ankusa.AsyncApi.document(instance(conn)))
    end)
  end

  # The VM's log level, for every tenant's lines: a node-wide change, so it
  # needs `tenant:*` as well, like a tenant erase. Audited like every change.
  put "/v1/log-level" do
    Auth.authorize_global(conn, "config:write", &log_level_put/1)
  end

  # Re-read the config file and apply the keys that may change while running
  # (`Ankusa.Reload`): a node-wide change, like the log level.
  post "/v1/config/reload" do
    Auth.authorize_global(conn, "config:write", &config_reload/1)
  end

  get "/v1/dlq" do
    scoped_role(conn, "dlq:read", :dispatch, &dlq_index/2)
  end

  delete "/v1/dlq" do
    scoped_role(conn, "dlq:write", :dispatch, &dlq_purge/2)
  end

  # The tenant the job is limited to is in the body, so only the scope and
  # "holds some tenant" are checked here; `replay_create/1` checks the rest.
  post "/v1/replays" do
    Auth.authorize_tenants(conn, "replays:write", nil, fn conn, _tenants ->
      require_role(conn, :dispatch, &replay_create/1)
    end)
  end

  get "/v1/replays" do
    scoped_role(conn, "replays:read", :dispatch, &replay_list/2)
  end

  get "/v1/replays/:id" do
    scoped_role(conn, "replays:read", :dispatch, &replay_get(&1, id, &2))
  end

  patch "/v1/replays/:id" do
    scoped_role(conn, "replays:write", :dispatch, &replay_update(&1, id, &2))
  end

  get "/v1/quarantine" do
    scoped_role(conn, "quarantine:read", :edge, &quarantine_index/2)
  end

  delete "/v1/quarantine" do
    scoped_role(conn, "quarantine:write", :edge, &quarantine_purge/2)
  end

  get "/v1/audit" do
    Auth.authorize(conn, "audit:read", &audit_index/1)
  end

  get "/v1/tenants/:tenant/sources" do
    Auth.authorize_tenant(conn, "sources:read", tenant, &sources_index(&1, tenant))
  end

  post "/v1/tenants/:tenant/sources" do
    Auth.authorize_tenant(conn, "sources:write", tenant, &source_create(&1, tenant))
  end

  get "/v1/tenants/:tenant/sources/:name" do
    Auth.authorize_tenant(conn, "sources:read", tenant, &source_get(&1, tenant, name))
  end

  put "/v1/tenants/:tenant/sources/:name" do
    Auth.authorize_tenant(conn, "sources:write", tenant, &source_update(&1, tenant, name))
  end

  delete "/v1/tenants/:tenant/sources/:name" do
    Auth.authorize_tenant(conn, "sources:write", tenant, &source_delete(&1, tenant, name))
  end

  # The limiter is the edge node's own state (`Ankusa.Edge.RateLimiter`), so
  # these are role-gated like the quarantine list, not node-agnostic like
  # source management.
  get "/v1/rate-limits" do
    scoped_role(conn, "rate_limits:read", :edge, &rate_limits_index/2)
  end

  get "/v1/tenants/:tenant/rate-limit" do
    tenant_role(conn, "rate_limits:read", tenant, :edge, &rate_limit_get(&1, tenant))
  end

  put "/v1/tenants/:tenant/rate-limit" do
    tenant_role(conn, "rate_limits:write", tenant, :edge, &rate_limit_put(&1, tenant))
  end

  delete "/v1/tenants/:tenant/rate-limit" do
    tenant_role(conn, "rate_limits:write", tenant, :edge, &rate_limit_delete(&1, tenant))
  end

  # An erase removes the tenant everywhere on this node, so it needs `tenant:*`
  # whoever the tenant is: a token for one tenant cannot erase even its own.
  delete "/v1/tenants/:tenant" do
    Auth.authorize_global(conn, "tenants:write", &tenant_erase(&1, tenant))
  end

  get "/v1/tenants/:tenant/erase" do
    Auth.authorize_global(conn, "tenants:write", &tenant_erase_status(&1, tenant))
  end

  match _ do
    send_json(conn, 404, %{error: "not_found"})
  end

  # ── routes ──────────────────────────────────────────────────────────────────

  defp wal(conn) do
    config = config(conn)

    case config.wal do
      :none ->
        send_json(conn, 409, %{error: "wal_disabled"})

      _ ->
        send_json(
          conn,
          200,
          put_intake(
            %{instance: to_string(config.instance), wal: safe_stats(instance(conn))},
            config
          )
        )
    end
  end

  # `wal: :dual`: where this node is against the intake log.
  defp put_intake(body, %{wal: :dual, instance: instance}),
    do: Map.put(body, :intake, Ankusa.Intake.info(instance))

  defp put_intake(body, _config), do: body

  # A page of the DLQ, newest first. `cursor` is the `next_cursor` of the
  # previous page (opaque to the caller: URL-safe base64 of the last key
  # examined); the listing is bounded (`Ankusa.Queue.dead/2`), so a filtered page
  # can be short with `next_cursor` set, and `total` is `null` unless it is known
  # without a scan.
  defp dlq_index(conn, tenants) do
    params = Plug.Conn.fetch_query_params(conn).query_params

    with {:ok, source_id} <- string_param(params, "source_id"),
         {:ok, since} <- int_param(params, "since", nil),
         {:ok, sink} <- non_neg_param(params, "sink"),
         {:ok, error_class} <- optional_enum(params, "error_class", @error_class_map),
         {:ok, cursor} <- dlq_cursor_param(params),
         {:ok, limit} <- int_param(params, "limit", @default_limit) do
      case Ankusa.Queue.dead(instance(conn),
             source_id: source_id,
             since: since,
             sink: sink,
             error_class: error_class,
             cursor: cursor,
             tenants: tenants,
             limit: clamp_limit(limit)
           ) do
        {:ok, %{total: total, entries: entries, next_cursor: next}} ->
          send_json(conn, 200, %{
            total: total,
            entries: Enum.map(entries, &dlq_entry/1),
            next_cursor: next && Base.url_encode64(next, padding: false)
          })

        {:error, :invalid_cursor} ->
          invalid_filter(conn, "cursor")

        {:error, _reason} ->
          send_json(conn, 503, %{error: "store_unavailable"})
      end
    else
      {:error, field} -> invalid_filter(conn, field)
    end
  end

  defp dlq_cursor_param(params) do
    case string_param(params, "cursor") do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, text} ->
        case Base.url_decode64(text, padding: false) do
          {:ok, cursor} when byte_size(cursor) == 18 -> {:ok, cursor}
          _ -> {:error, "cursor"}
        end

      {:error, _field} = error ->
        error
    end
  end

  # Evicts dead rows for good, the oldest first, under the DLQ reaper's rule: a
  # row whose hook is not archived is skipped (`unarchived`), never deleted.
  # `since`/`until` are inclusive bounds on the dead-letter time (ms). No filter
  # at all evicts the oldest `limit` archived rows of the caller's tenants.
  defp dlq_purge(conn, tenants) do
    params = Plug.Conn.fetch_query_params(conn).query_params

    with {:ok, source_id} <- string_param(params, "source_id"),
         {:ok, since} <- non_neg_param(params, "since"),
         {:ok, until} <- non_neg_param(params, "until"),
         :ok <- if(since && until && until < since, do: {:error, "until"}, else: :ok),
         {:ok, limit} <- int_param(params, "limit", @default_purge_limit) do
      filter =
        Map.merge(
          %{source_id: source_id, since: since, until: until},
          tenant_filter(tenants)
        )

      limit = limit |> max(1) |> min(@max_purge_limit)

      case Ankusa.Queue.DlqReaper.evict(instance(conn), filter, limit,
             audit: Context.from_conn(conn)
           ) do
        {:ok, result} ->
          send_json(conn, 200, Map.take(result, [:deleted, :bytes, :unarchived, :more]))

        {:error, :audit_backlog} ->
          audit_backlog(conn)

        {:error, _reason} ->
          send_json(conn, 503, %{error: "store_unavailable"})
      end
    else
      {:error, field} -> invalid_filter(conn, field)
    end
  end

  # The audit trail (or the lifecycle outbox) is over its ceiling: a mutation is
  # refused rather than an unpublished row dropped (`audit.max_rows`,
  # `audit.max_bytes`, `audit.outbox_max_backlog`).
  defp audit_backlog(conn) do
    conn
    |> Plug.Conn.put_resp_header("retry-after", "60")
    |> send_json(503, %{error: "audit_backlog"})
  end

  # `{"level": "debug"}`: one key, one of `Ankusa.LogLevel.levels/0`. The audit
  # row is committed first and the level changes only once it is durable: a
  # change the trail cannot record is not made (`audit_backlog`, `store_unavailable`).
  defp log_level_put(conn) do
    case Ankusa.Http.read_body_limited(conn, @max_log_level_body, read_opts(conn)) do
      {:ok, body, conn} ->
        with {:ok, %{"level" => name} = map} when map_size(map) == 1 <- JSON.decode(body),
             {:ok, level} <- Ankusa.LogLevel.parse(name) do
          log_level_apply(conn, level)
        else
          _ -> invalid_log_level(conn)
        end

      {:too_large, conn} ->
        invalid_log_level(conn)

      {:error, _reason, conn} ->
        invalid_log_level(conn)
    end
  end

  defp log_level_apply(conn, level) do
    previous = Ankusa.LogLevel.get()

    attrs = %{
      action: "log_level.put",
      resource: "log_level",
      detail: %{level: Atom.to_string(level), previous: Atom.to_string(previous)}
    }

    case Audit.commit(instance(conn), Context.from_conn(conn), attrs, []) do
      :ok ->
        {:ok, _previous} = Ankusa.LogLevel.set(level)
        send_json(conn, 200, %{level: Atom.to_string(level), previous: Atom.to_string(previous)})

      {:error, :audit_backlog} ->
        audit_backlog(conn)

      {:error, :store_unavailable} ->
        send_json(conn, 503, %{error: "store_unavailable"})
    end
  end

  # The loader (`ankusa_server`) re-reads, re-resolves secrets and checks; the
  # change and its one audit row are made in `Ankusa.Reload`.
  defp config_reload(conn) do
    case Ankusa.Reload.run(instance(conn), Context.from_conn(conn)) do
      {:ok, %{keys: keys, sources: sources, kept: kept}} ->
        send_json(conn, 200, %{status: "reloaded", keys: keys, sources: sources, kept: kept})

      {:error, {:not_reloadable, keys}} ->
        send_json(conn, 409, %{
          error: "not_reloadable",
          keys: keys,
          message:
            "these keys changed and cannot be reloaded; restart the node, or revert them: " <>
              Enum.join(keys, ", ")
        })

      {:error, :unsupported} ->
        send_json(conn, 409, %{
          error: "reload_unavailable",
          message: "this instance was not started from a config file that can be re-read"
        })

      {:error, {:invalid_config, message}} ->
        send_json(conn, 422, %{error: "invalid_config", message: message})

      {:error, :audit_backlog} ->
        audit_backlog(conn)

      {:error, :store_unavailable} ->
        send_json(conn, 503, %{error: "store_unavailable"})
    end
  end

  defp invalid_log_level(conn) do
    send_json(conn, 400, %{
      error: "invalid_log_level",
      message:
        ~s(body must be a JSON object with one key, "level": one of ) <>
          Enum.map_join(Ankusa.LogLevel.levels(), ", ", &Atom.to_string/1)
    })
  end

  defp quarantine_index(conn, tenants) do
    params = Plug.Conn.fetch_query_params(conn).query_params

    case int_param(params, "limit", @default_limit) do
      {:ok, limit} ->
        case Ankusa.Edge.Quarantine.recent(
               instance(conn),
               clamp_limit(limit),
               tenant_filter(tenants)
             ) do
          {:ok, entries} ->
            send_json(conn, 200, %{entries: Enum.map(entries, &quarantine_entry/1)})

          {:error, _reason} ->
            send_json(conn, 503, %{error: "store_unavailable"})
        end

      {:error, field} ->
        invalid_filter(conn, field)
    end
  end

  # `since`/`until` are inclusive bounds on `received_at` (ms). No filter at
  # all purges the oldest `limit` entries, of the caller's tenants only.
  defp quarantine_purge(conn, tenants) do
    params = Plug.Conn.fetch_query_params(conn).query_params

    with {:ok, source_id} <- string_param(params, "source_id"),
         {:ok, id} <- string_param(params, "id"),
         {:ok, since} <- non_neg_param(params, "since"),
         {:ok, until} <- non_neg_param(params, "until"),
         :ok <- if(since && until && until < since, do: {:error, "until"}, else: :ok),
         {:ok, limit} <- int_param(params, "limit", @default_purge_limit) do
      filter =
        Map.merge(
          %{source_id: source_id, id: id, since: since, until: until},
          tenant_filter(tenants)
        )

      limit = limit |> max(1) |> min(@max_purge_limit)

      case Ankusa.Edge.Quarantine.purge(instance(conn), filter, limit,
             audit: Context.from_conn(conn)
           ) do
        {:ok, %{deleted: deleted, bytes: bytes}} ->
          send_json(conn, 200, %{deleted: deleted, bytes: bytes})

        {:error, :store_unavailable} ->
          send_json(conn, 503, %{error: "store_unavailable"})
      end
    else
      {:error, field} -> invalid_filter(conn, field)
    end
  end

  defp tenant_filter(nil), do: %{}
  defp tenant_filter(tenants), do: %{tenants: tenants}

  defp non_neg_param(params, key) do
    case int_param(params, key, nil) do
      {:ok, value} when is_integer(value) and value < 0 -> {:error, key}
      other -> other
    end
  end

  # ── the audit trail ─────────────────────────────────────────────────────────

  # Newest first. The trail spans tenants: a reader without `tenant:*` sees the
  # rows of its own tenants and none of the rows that belong to no tenant
  # (routes, replays, the quarantine pen), and asking for another tenant's rows
  # by name is a `403`, like any other tenant resource.
  defp audit_index(conn) do
    params = Plug.Conn.fetch_query_params(conn).query_params
    principal = conn.assigns.principal

    with {:ok, since} <- non_neg_param(params, "since"),
         {:ok, until} <- non_neg_param(params, "until"),
         {:ok, actor} <- string_param(params, "actor"),
         {:ok, action} <- string_param(params, "action"),
         {:ok, tenant} <- string_param(params, "tenant"),
         {:ok, cursor} <- cursor_param(params),
         {:ok, limit} <- int_param(params, "limit", @default_limit) do
      if tenant && not Principal.tenant?(principal, tenant) do
        send_json(conn, 403, %{error: "forbidden", scope: "tenant:#{tenant}"})
      else
        opts = [
          since: since,
          until: until,
          actor: actor,
          action: action,
          tenant: tenant,
          cursor: cursor,
          limit: limit |> max(1) |> min(@max_limit),
          tenants: if(Principal.all_tenants?(principal), do: :all, else: principal.tenants)
        ]

        case Audit.list(instance(conn), opts) do
          {:ok, %{entries: entries, next_cursor: next}} ->
            send_json(conn, 200, %{
              entries: Enum.map(entries, &Audit.to_json/1),
              next_cursor: next
            })

          {:error, reason} ->
            Logger.error("[ankusa] could not read the audit trail: #{inspect(reason)}")
            source_store_unavailable(conn)
        end
      end
    else
      {:error, field} -> invalid_filter(conn, field)
    end
  end

  defp cursor_param(params) do
    case int_param(params, "cursor", nil) do
      {:ok, cursor} when is_integer(cursor) and cursor < 1 -> {:error, "cursor"}
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

        put_source(
          conn,
          201,
          Ankusa.SourceStore.put(instance(conn), tenant, name, spec, :create,
            audit: Context.from_conn(conn)
          )
        )
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
            Ankusa.SourceStore.put(instance(conn), tenant, name, spec, :update,
              audit: Context.from_conn(conn)
            )
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

    with true <- config.wal in [:disk, :dual] and Ankusa.Instance.store?(config),
         {:ok, _stored} <- Ankusa.SourceStore.get(instance, tenant, name) do
      # A source owned by a tenant files its hooks under that tenant alone
      # (`bound_elsewhere?/2` in `Ankusa.Edge.Ingest`); the default tenant's
      # sources are shared, so those walk every tenant's rows.
      scope = if tenant == "default", do: [], else: [tenant: tenant]

      case Ankusa.Queue.pending_for_source(instance, source_id, scope) do
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
    case Ankusa.SourceStore.delete(instance(conn), tenant, name, audit: Context.from_conn(conn)) do
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
    case Ankusa.Http.read_body_limited(conn, @max_source_body, read_opts(conn)) do
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
      {:ok, stored} ->
        send_json(conn, ok_status, Redact.source_entry(stored))

      {:error, :invalid, message} ->
        invalid_source(conn, message)

      # The source would outlive the claim bucket's lifecycle rule: a `422` (the
      # spec is well-formed, the instance's claim-check config refuses it).
      {:error, :retry_exceeds_claim_expiry, message} ->
        send_json(conn, 422, %{error: "retry_exceeds_claim_expiry", message: message})

      {:error, :exists} ->
        send_json(conn, 409, %{error: "source_exists"})

      {:error, :not_found} ->
        send_json(conn, 404, %{error: "source_not_found"})

      {:error, :store_unavailable} ->
        source_store_unavailable(conn)

      {:error, :read_only} ->
        send_json(conn, 409, %{error: "source_store_read_only"})
    end
  end

  # ── tenant erase ────────────────────────────────────────────────────────────

  # `?confirm=<tenant>` is the whole confirmation: the name typed again, so a
  # wrong path in a script is a `400`, not a deleted customer. The job runs in
  # the background (`Ankusa.Tenant`): `202` once it is recorded, `200` when one
  # is already running, and `GET .../erase` says where it is.
  defp tenant_erase(conn, tenant) do
    params = Plug.Conn.fetch_query_params(conn).query_params

    case Ankusa.Tenant.delete(instance(conn), tenant,
           confirm: Map.get(params, "confirm"),
           audit: Context.from_conn(conn)
         ) do
      {:ok, :started} ->
        send_json(conn, 202, %{tenant: tenant, state: "erasing"})

      {:ok, :erasing} ->
        send_json(conn, 200, %{tenant: tenant, state: "erasing"})

      {:error, :invalid_tenant} ->
        invalid_tenant(conn)

      {:error, :confirm} ->
        send_json(conn, 400, %{
          error: "confirm_mismatch",
          message: "confirm must be the tenant being erased"
        })

      {:error, :default_tenant} ->
        send_json(conn, 409, %{error: "default_tenant"})

      {:error, {:static_sources, ids}} ->
        send_json(conn, 409, %{error: "tenant_has_configured_sources", sources: Enum.sort(ids)})

      {:error, {:role_not_enabled, role}} ->
        send_json(conn, 409, %{error: "role_not_enabled", role: to_string(role)})

      {:error, _unavailable} ->
        source_store_unavailable(conn)
    end
  end

  defp tenant_erase_status(conn, tenant) do
    if valid_identity?(tenant) do
      case Ankusa.Tenant.status(instance(conn), tenant) do
        {:ok, %{state: :erasing, stage: stage, started_at: at}} ->
          send_json(conn, 200, %{
            tenant: tenant,
            state: "erasing",
            stage: Atom.to_string(stage),
            started_at: iso8601(at)
          })

        {:ok, %{state: :erased, erased_at: at}} ->
          send_json(conn, 200, %{tenant: tenant, state: "erased", erased_at: iso8601(at)})

        :not_found ->
          send_json(conn, 404, %{error: "erase_not_found"})

        {:error, _reason} ->
          source_store_unavailable(conn)
      end
    else
      invalid_tenant(conn)
    end
  end

  defp iso8601(ms), do: ms |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601()

  # ── per-tenant rate limits ──────────────────────────────────────────────────

  # The node default applies to every tenant and names none; the overrides are
  # per tenant, so a tenant-limited caller sees only its own.
  defp rate_limits_index(conn, tenants) do
    %{default: default, tenants: overrides} = RateLimiter.list(instance(conn))

    send_json(conn, 200, %{
      default: limit(default),
      tenants:
        for {tenant, limit, source} <- overrides, tenants == nil or tenant in tenants do
          rate_limit_entry(tenant, {limit, source})
        end
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
        case RateLimiter.put_override(instance(conn), tenant, attrs,
               audit: Context.from_conn(conn)
             ) do
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
      case RateLimiter.delete_override(instance(conn), tenant, audit: Context.from_conn(conn)) do
        # 204 carries no body: there is nothing left to describe.
        :ok -> Plug.Conn.send_resp(conn, 204, "")
        {:error, :not_found} -> send_json(conn, 404, %{error: "rate_limit_not_found"})
        {:error, :store_unavailable} -> send_json(conn, 503, %{error: "store_unavailable"})
      end
    else
      invalid_tenant(conn)
    end
  end

  # `rate` and `burst` always; the optional keys (`bytes_per_second`,
  # `burst_bytes`, `max_body_bytes`, `max_queue`, `max_queue_bytes`) only when
  # the limit sets them, so a limit that never used them reads as it always did.
  defp rate_limit_entry(tenant, {limit, source}) do
    %{
      tenant: tenant,
      rate: limit && limit.rate,
      burst: limit && limit.burst,
      source: Atom.to_string(source)
    }
    |> Map.merge(Map.drop(limit || %{}, [:rate, :burst]))
  end

  defp limit(nil), do: nil
  defp limit(limit), do: limit

  # The body is a limit, not a spec: two required keys. A body that is empty,
  # oversized, unparseable, or not an object is the same `invalid_rate_limit`,
  # whose message is the parser's.
  defp with_rate_limit_body(conn, fun) do
    case Ankusa.Http.read_body_limited(conn, @max_rate_limit_body, read_opts(conn)) do
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

  @kind_map %{
    "dlq" => :dlq,
    "archive" => :archive,
    "quarantine" => :quarantine,
    "lifecycle" => :lifecycle
  }

  @state_map %{"running" => :running, "paused" => :paused, "cancelled" => :cancelled}

  @replay_spec_keys ~w(kind source_id id tenant since until from to sinks sink error_class job_id rate max_lag_ms)
  @replay_patch_keys ~w(state rate max_lag_ms)

  # The route needed `replays:write` and a tenant the caller holds; the body
  # names the kind and, optionally, one `tenant`. Replaying a DLQ or the
  # quarantine pen is also a write to that store, so those kinds need its
  # scope too. A caller limited to some tenants replays only their rows: the
  # job's `tenants` filter is forced to the tenants it holds (`tenant:*` leaves
  # it unset unless the body names one).
  defp replay_create(conn) do
    with_replay_body(conn, fn map, conn ->
      case replay_spec(map) do
        {:ok, spec} ->
          {requested, spec} = Map.pop(spec, :tenant)

          Auth.authorize_tenants(conn, replay_kind_scopes(spec.kind), requested, fn conn,
                                                                                    tenants ->
            if Enum.all?(tenants || [], &valid_identity?/1),
              do: start_replay(conn, put_present(spec, :tenants, tenants && Enum.sort(tenants))),
              else: invalid_filter(conn, "tenant")
          end)

        {:error, field} ->
          invalid_filter(conn, field)
      end
    end)
  end

  defp replay_kind_scopes(:dlq), do: "dlq:write"
  defp replay_kind_scopes(:quarantine), do: "quarantine:write"
  defp replay_kind_scopes(:archive), do: []
  defp replay_kind_scopes(:lifecycle), do: []

  defp start_replay(conn, spec) do
    case Ankusa.Replay.start(instance(conn), spec, audit: Context.from_conn(conn)) do
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
  end

  defp replay_list(conn, tenants) do
    case Ankusa.Replay.list(instance(conn), tenants) do
      {:ok, jobs} -> send_json(conn, 200, %{replays: Enum.map(jobs, &Ankusa.Replay.to_json/1)})
      {:error, :store_unavailable} -> send_json(conn, 503, %{error: "store_unavailable"})
    end
  end

  defp replay_get(conn, id, tenants) do
    case Ankusa.Replay.get(instance(conn), id, tenants) do
      {:ok, job} -> send_json(conn, 200, Ankusa.Replay.to_json(job))
      {:error, :not_found} -> send_json(conn, 404, %{error: "replay_not_found"})
      {:error, :store_unavailable} -> send_json(conn, 503, %{error: "store_unavailable"})
    end
  end

  defp replay_update(conn, id, tenants) do
    with_replay_body(conn, fn map, conn ->
      case replay_patch(map) do
        {:ok, patch} ->
          case Ankusa.Replay.update(instance(conn), id, patch,
                 tenants: tenants,
                 audit: Context.from_conn(conn)
               ) do
            {:ok, job} -> send_json(conn, 200, Ankusa.Replay.to_json(job))
            {:error, :not_found} -> send_json(conn, 404, %{error: "replay_not_found"})
            {:error, :finished} -> send_json(conn, 409, %{error: "replay_finished"})
            {:error, {:invalid, field}} -> invalid_filter(conn, field)
            {:error, :store_unavailable} -> send_json(conn, 503, %{error: "store_unavailable"})
          end

        {:error, field} ->
          invalid_filter(conn, field)
      end
    end)
  end

  # A replay body is a spec, not a payload: read it as a JSON object with a
  # modest size cap. Unknown keys and malformed bodies are `invalid_filter`.
  defp with_replay_body(conn, fun) do
    case Ankusa.Http.read_body_limited(conn, @max_replay_body, read_opts(conn)) do
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
             {:ok, tenant} <- string_param(map, "tenant"),
             {:ok, since} <- int_param(map, "since", nil),
             {:ok, until} <- int_param(map, "until", nil),
             {:ok, from} <- int_param(map, "from", nil),
             {:ok, to} <- int_param(map, "to", nil),
             {:ok, rate} <- int_param(map, "rate", nil),
             {:ok, max_lag_ms} <- int_param(map, "max_lag_ms", nil),
             {:ok, sinks} <- sinks_param(map),
             {:ok, sink} <- int_param(map, "sink", nil),
             {:ok, error_class} <- optional_enum(map, "error_class", @error_class_map),
             {:ok, job_id} <- string_param(map, "job_id") do
          spec =
            %{kind: kind}
            |> put_present(:source_id, source_id)
            |> put_present(:id, id)
            |> put_present(:tenant, tenant)
            |> put_present(:since, since)
            |> put_present(:until, until)
            |> put_present(:from, from)
            |> put_present(:to, to)
            |> put_present(:rate, rate)
            |> put_present(:max_lag_ms, max_lag_ms)
            |> put_present(:sinks, sinks)
            |> put_present(:sink, sink)
            |> put_present(:error_class, error_class)
            |> put_present(:job_id, job_id)

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
  defp dlq_entry(%{envelope: env, reason: reason, at: at} = entry) do
    %{
      id: env.id,
      source_id: env.source_id,
      tenant_id: env.tenant_id,
      seq: env.seq,
      received_at: env.received_at,
      dead_lettered_at: at,
      size: env.size,
      content_type: env.content_type,
      sink: Map.get(entry, :sink),
      error_class: dlq_error_class(Map.get(entry, :error_class)),
      reason: reason
    }
  end

  defp dlq_error_class(nil), do: nil
  defp dlq_error_class(class), do: to_string(class)

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

  # For a resource that spans tenants (`Auth.authorize_tenants/4`): the
  # `tenant` query parameter narrows it, and a caller limited to some tenants
  # is held to them whatever it asks. Scope first, tenant second, role third,
  # so a caller without the scope learns nothing about which roles this node
  # runs. `fun` gets the tenants the rows must be limited to: `nil` is every
  # tenant.
  defp scoped_role(conn, scope, role, fun) do
    Auth.authorize_tenants(conn, scope, tenant_param(conn), fn conn, tenants ->
      run_scoped(conn, tenants, role, fun)
    end)
  end

  defp run_scoped(conn, tenants, role, fun) do
    if Enum.all?(tenants || [], &valid_identity?/1),
      do: require_role(conn, role, &fun.(&1, tenants)),
      else: invalid_filter(conn, "tenant")
  end

  # Absent is `nil`. Present but not a plain string (`tenant[]=x`) is `""`: no
  # principal holds it, and it is not a tenant name.
  defp tenant_param(conn) do
    case Plug.Conn.fetch_query_params(conn).query_params do
      %{"tenant" => tenant} when is_binary(tenant) -> tenant
      %{"tenant" => _other} -> ""
      _ -> nil
    end
  end

  defp tenant_role(conn, scope, tenant, role, fun),
    do: Auth.authorize_tenant(conn, scope, tenant, &require_role(&1, role, fun))

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

  defp read_opts(%Plug.Conn{} = conn), do: Ankusa.Http.read_opts(config(conn))

  defp config(%Plug.Conn{} = conn), do: Ankusa.config(instance(conn))

  defp instance(%Plug.Conn{} = conn) do
    Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
  end

  defp send_json(conn, status, payload), do: Ankusa.Http.send_json(conn, status, payload)
end
