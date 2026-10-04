defmodule Ankusa.Config do
  @moduledoc """
  The configuration struct passed down the supervision tree at start. No
  `Application.get_env/2` buried in call sites — instance-scoped config falls
  out of this for free.
  """

  defstruct instance: :default,
            data_dir: "./data",
            # which OTP roles boot in this node (ANKUSA_ROLES in production)
            roles: [:edge, :dispatch, :storage],
            # Bandit edge
            port: 4000,
            # {module, opts} implementing Ankusa.RouteResolver (URL scheme → identity)
            route_resolver: {Ankusa.RouteResolver.Path, []},
            max_body_bytes: 8_000_000,
            # {module, opts} implementing Ankusa.SourceStore
            source_store: {Ankusa.SourceStore.Static, sources: %{}},
            # :disk (the RocksDB store: every hook is on disk before the ack) or
            # :none: no log, ack on the sink's confirm. With :none, `new/1` drops
            # :dispatch and :storage from :roles (there is nothing for them to
            # read) and requires :edge to remain.
            wal: :disk,
            # direct mode only: the overall deadline every sink must confirm
            # under. Must stay under the provider's own timeout (GitHub's is 10 s).
            direct_publish_timeout_ms: 8_000,
            # group-commit batcher. The queue writer serializes commits itself,
            # so more partitions only add contention now that a partition commits
            # asynchronously instead of holding the caller's message queue.
            batcher: %{
              partitions: 2,
              max_batch: 256,
              # 0 = commit as soon as the batch fills, no linger: the commit is a
              # Task, so waiting costs a scheduling hop, not head-of-line
              # blocking.
              max_delay_ms: 0,
              max_queue: 10_000
            },
            # dispatch pipeline
            dispatch: %{
              # delivery rows claimed per store scan
              batch: 128,
              # max sink deliveries in flight at once
              concurrency: 32,
              # max claimed, unfinished deliveries...
              max_inflight: 4096,
              # ...and the max sum of their stored hook sizes in bytes
              max_inflight_bytes: 134_217_728,
              retry: {Ankusa.RetryPolicy.Exponential, []}
            },
            # segment compaction
            storage: %{
              blob_store: {Ankusa.BlobStore.LocalFS, []},
              codec: {Ankusa.Codec.Raw, []},
              roll_bytes: 16 * 1024 * 1024,
              roll_ms: 30_000,
              interval_ms: 1_000
            },
            # claim check: payloads too large to ride inline in a queue message
            claim_check: %{
              # :claim_check role only
              port: 4001,
              # target size of one pack object; a body larger than this still
              # gets a pack of its own
              pack_max_bytes: 16 * 1024 * 1024,
              # LocalFS retention only; nil disables the sweeper
              retention_days: nil,
              sweep_interval_ms: 3_600_000
            },
            # operator HTTP API + Prometheus /metrics, unauthenticated; off by
            # default for embedded use
            admin: %{enabled: false, port: 4002},
            # route management: the allowlist guard plus its admin API. Off by
            # default, and off means "capture every POST", as it always has. On
            # means deny-by-default: a request is captured only if it matches an
            # enabled route and passes the IP rules.
            routes: %{
              enabled: false,
              # a hard cap on definitions; nothing is ever evicted
              max_routes: 10_000,
              # {module, opts} implementing Ankusa.Routes.Store
              store: {Ankusa.Routes.Store.ETS, []},
              # the decision cache (see Ankusa.Routes.Cache); gc_interval_ms
              # must stay above ttl_ms, or the adapter's generational sweep can
              # evict an entry before its own TTL expires
              cache: %{
                max_size: 50_000,
                ttl_ms: 30_000,
                negative_ttl_ms: 5_000,
                gc_interval_ms: 60_000
              },
              # CIDRs whose peers may set X-Forwarded-For (see Ankusa.Net.ClientIP)
              trusted_proxies: [],
              # global rules: a floor. A route's own ip_rules replace this list
              ip_rules: %{default: :allow, rules: []},
              # its own listener; unauthenticated by design, same stance as
              # the operator admin API — front it with your own proxy or
              # network policy
              admin: %{port: 4003},
              # 1 in log_sample rejections is logged at :debug (0 = silent)
              log_sample: 100,
              # 403, or 404 for uniformity with :no_route
              ip_denied_status: 403,
              # route attrs (maps or keyword lists) loaded at boot
              seed: []
            },
            # per-tenant ingest rate limits, enforced in this node's memory
            # (see `Ankusa.Edge.RateLimiter`). Precedence: a runtime override
            # (admin API) beats `tenants[tenant]`, which beats `default`.
            # `default: nil` and no tenant entry means unlimited.
            #
            #   %{default: nil | %{rate: number, burst: pos_integer},
            #     tenants: %{tenant_id => %{rate: number, burst: pos_integer}}}
            #
            # `rate` is hooks per second (fractions allowed), `burst` the most
            # hooks admitted back to back.
            rate_limits: %{default: nil, tenants: %{}},
            # the quarantine pen (`Ankusa.Edge.Quarantine`): one token bucket per
            # source (`burst` tokens, `rate` refilled per second) and a cap on the
            # pen's total bytes. A full pen refuses with 503, it never evicts.
            quarantine: %{burst: 100, rate: 20, max_bytes: 1_073_741_824},
            # where lifecycle events (a source or route created/updated/deleted,
            # as CloudEvents) are delivered: `[{module, opts}]` implementing
            # Ankusa.Sink, run through the WAL and dispatch like a hook. `[]`
            # means lifecycle events are off. See `Ankusa.Lifecycle`.
            lifecycle: %{sinks: []}

  @type t :: %__MODULE__{}

  @roles [:edge, :dispatch, :storage, :claim_check]

  @role_names %{
    "edge" => :edge,
    "dispatch" => :dispatch,
    "storage" => :storage,
    "claim_check" => :claim_check
  }

  @doc """
  Parse a comma-separated `ANKUSA_ROLES`-shaped string into role atoms.
  Never calls `String.to_atom/1` — each part must name one of the fixed
  roles (`edge`, `dispatch`, `storage`, `claim_check`).
  """
  @spec parse_roles!(String.t()) :: [atom()]
  def parse_roles!(value) do
    roles =
      value
      |> String.split(",")
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.map(fn name ->
        Map.get(@role_names, name) ||
          raise ArgumentError,
                "unknown Ankusa role #{inspect(name)}; expected one of: edge, dispatch, storage, claim_check"
      end)

    if roles == [] do
      raise ArgumentError, "ANKUSA_ROLES must name at least one role"
    end

    roles
  end

  @doc """
  Build a `%Ankusa.Config{}` from a keyword list, deep-merging the map-valued
  sections (`:batcher`, `:dispatch`, `:storage`, `:claim_check`, `:admin`,
  `:routes`, `:rate_limits`, `:quarantine`, `:lifecycle`) over the defaults.

  `:routes` is nested one level deeper than the rest (`:routes` has its own
  `:cache`, `:ip_rules`, and `:admin` sections), so `put_routes/2` merges those
  too — `routes.cache.max_size` keeps the other cache keys.

  `wal: :none` is normalized last: the roles are settled before `normalize_wal/1`
  drops the queue's readers (`:dispatch`, `:storage`) from them.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    base = %__MODULE__{}

    opts
    |> Enum.reduce(base, fn {k, v}, acc ->
      cond do
        k == :roles ->
          bad = Enum.reject(v, &(&1 in @roles))

          if bad != [] do
            raise ArgumentError, "unknown Ankusa role(s) #{inspect(bad)} in :roles"
          end

          Map.put(acc, k, v)

        k == :routes ->
          put_routes(acc, v)

        k in [
          :batcher,
          :dispatch,
          :storage,
          :claim_check,
          :admin,
          :rate_limits,
          :quarantine,
          :lifecycle
        ] ->
          put_section(acc, k, v)

        Map.has_key?(base, k) ->
          Map.put(acc, k, v)

        true ->
          raise ArgumentError, "unknown Ankusa.Config key: #{inspect(k)}"
      end
    end)
    |> normalize_wal()
  end

  # `:dispatch` and `:storage` read the queue and nothing else, so with no queue
  # they have no work: they are dropped rather than rejected, which lets an
  # existing `roles: [:edge, :dispatch, :storage]` deployment flip
  # `wal.type: none` with no other change. The check is "a role is left", not
  # "`:edge` is left": a `:claim_check`-only node survives, which is a
  # legitimate standalone deployment. The effective roles are visible on the
  # admin API's `GET /health`.
  defp normalize_wal(%__MODULE__{wal: :none} = config) do
    case Enum.reject(config.roles, &(&1 in [:dispatch, :storage])) do
      [] -> raise ArgumentError, "wal: :none requires the :edge role"
      roles -> %{config | roles: roles}
    end
  end

  defp normalize_wal(%__MODULE__{wal: :disk} = config), do: config

  defp normalize_wal(%__MODULE__{wal: {Ankusa.WAL.DiskLog, _}}) do
    raise ArgumentError,
          "wal: {Ankusa.WAL.DiskLog, _} was removed: use wal: :disk (the RocksDB store) or wal: :none"
  end

  defp normalize_wal(%__MODULE__{wal: other}) do
    raise ArgumentError, "wal must be :disk or :none, got #{inspect(other)}"
  end

  defp put_section(acc, k, v) do
    Map.put(acc, k, merge_known!(Map.get(acc, k), v, to_string(k)))
  end

  defp merge_known!(defaults, value, dotted_name) do
    incoming =
      case value do
        v when is_map(v) ->
          Map.new(v)

        v when is_list(v) ->
          if Keyword.keyword?(v),
            do: Map.new(v),
            else:
              raise(
                ArgumentError,
                "Ankusa.Config #{dotted_name} must be a map or keyword list"
              )

        _ ->
          raise ArgumentError, "Ankusa.Config #{dotted_name} must be a map or keyword list"
      end

    Enum.each(Map.keys(incoming), fn k ->
      unless Map.has_key?(defaults, k),
        do: raise(ArgumentError, "unknown Ankusa.Config key: #{dotted_name}.#{k}")
    end)

    Map.merge(defaults, incoming)
  end

  # Sections of :routes that hold their own keys; everything else is a scalar
  # or a list. A single-level merge would let `routes.cache.max_size` replace
  # the whole cache map, silently dropping the TTLs.
  @routes_sections [:cache, :ip_rules, :admin]

  defp put_routes(acc, v) do
    routes = section_map(v, "routes")

    merged =
      Enum.reduce(routes, acc.routes, fn {k, value}, routes ->
        cond do
          k in @routes_sections ->
            Map.put(routes, k, merge_known!(routes[k], value, "routes.#{k}"))

          Map.has_key?(routes, k) ->
            Map.put(routes, k, value)

          true ->
            raise ArgumentError, "unknown Ankusa.Config key: routes.#{k}"
        end
      end)

    %{acc | routes: merged}
  end

  defp section_map(value, key) do
    cond do
      is_map(value) -> Map.new(value)
      Keyword.keyword?(value) -> Map.new(value)
      true -> raise ArgumentError, "Ankusa.Config #{key} must be a map or keyword list"
    end
  end

  @doc "Absolute path for an instance-scoped data sub-directory."
  @spec path(t(), Path.t()) :: Path.t()
  def path(%__MODULE__{data_dir: dir, instance: instance}, sub) do
    Path.join([dir, to_string(instance), sub])
  end

  @doc "Is a role enabled for this node?"
  @spec role?(t(), atom()) :: boolean()
  def role?(%__MODULE__{roles: roles}, role), do: role in roles
end
