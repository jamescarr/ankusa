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
            # limits for every HTTP listener this instance runs (the edge, the admin
            # and route-management APIs, the claim-check gateway, the metrics
            # listener), and the TLS block of the ingest listener (see
            # `Ankusa.Listener`).
            listener: %{
              # nil, or %{certfile, keyfile, cacertfile, verify, versions, ciphers}
              tls: nil,
              # open connections per listener, spread over the acceptors
              max_connections: 20_000,
              # nil = two per scheduler, filled in by `new/1`
              num_acceptors: nil,
              # a connection that sends nothing for this long is closed: it bounds a
              # request head dribbled in byte by byte, and an idle keep-alive
              read_timeout_ms: 10_000,
              # one read of the request body must complete within this
              body_read_timeout_ms: 15_000,
              # the whole body must arrive within this, however it is chunked
              request_deadline_ms: 30_000,
              # a keep-alive connection is closed after this many requests
              max_requests_per_connection: 1_000,
              max_header_count: 64,
              max_header_length: 16_384,
              http2: false
            },
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
              # load shedding: a partition refuses (503 overload) once it holds
              # max_queue records, or max_queue_bytes of bodies, counting both
              # the buffer and the batch being committed
              max_queue: 10_000,
              max_queue_bytes: 268_435_456
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
              # a delivery attempt (the sink call, plus the fallback claim check-in
              # when a pack failed) that has not returned after this is killed and
              # counts as a failed attempt. Keep it above every sink's own timeout
              # (http `timeout_ms` 5 s, nats/redis `publish_timeout_ms` 5 s, kafka
              # produce 5 s, the rabbitmq connection call 15 s).
              attempt_timeout_ms: 30_000,
              # per-sink isolation, keyed by {source_id, sink index, sink module}:
              # at most sink_concurrency attempts of one sink run at once (nil =
              # only the global `concurrency` cap)...
              sink_concurrency: nil,
              # ...and a sink that fails breaker_failures times in a row (0 =
              # never) is skipped for breaker_open_ms, doubling per consecutive
              # open up to breaker_max_open_ms. Rows parked by an open breaker
              # keep their attempt count.
              breaker_failures: 5,
              breaker_open_ms: 30_000,
              breaker_max_open_ms: 300_000,
              retry: {Ankusa.RetryPolicy.Exponential, []}
            },
            # segment compaction
            storage: %{
              blob_store: {Ankusa.BlobStore.LocalFS, []},
              # prepended to every segment key ("" or "a/b/" — segments of
              # fleet nodes sharing one bucket must not collide)
              key_prefix: "",
              codec: {Ankusa.Codec.Raw, []},
              roll_bytes: 16 * 1024 * 1024,
              roll_ms: 30_000,
              interval_ms: 1_000
            },
            # claim check: payloads too large to ride inline in a queue message
            claim_check: %{
              # :claim_check role only
              port: 4001,
              # address the claim gateway binds; "0.0.0.0" exposes it on every interface
              ip: "127.0.0.1",
              # nil (plain HTTP) or a TLS block, shaped like `listener.tls`
              tls: nil,
              # target size of one pack object; a body larger than this still
              # gets a pack of its own
              pack_max_bytes: 16 * 1024 * 1024,
              # LocalFS retention only; nil disables the sweeper
              retention_days: nil,
              sweep_interval_ms: 3_600_000,
              # [{module, opts}, ...] claim packs are written to; nil =
              # [storage.blob_store]. Claim keys are never prefixed, so one
              # gateway reads every node's claims.
              blob_stores: nil,
              # which store a new pack goes to: :ordered = the first, the others
              # only while it is down; :hash = the pack id's random bits pick
              # one, the rest take over when it fails
              placement: :ordered,
              # a store that failed a write or could not answer a read is
              # skipped by writes, and searched last by reads, for this long
              # (ms) before it is tried again
              store_cooldown_ms: 30_000,
              # :flat = claims/tenant=<t>/dt=<d>/<pack>; :sharded adds
              # shard=<c>/ under the tenant (32 prefixes per tenant) — see
              # Ankusa.ClaimCheck.Ref
              key_layout: :flat,
              # ── reads: the gateway, and redeem/3 in-process on a :claim_check node ──
              # one read (the pack index, then the bytes) must finish within this;
              # past it the gateway answers 503 + Retry-After, and the store read
              # that outlived it still completes and fills the cache
              read_deadline_ms: 5_000,
              # per store request while reading a claim (a store's own timeout_ms
              # and retries are for writes)
              read_timeout_ms: 3_000,
              read_retries: 1,
              # per store: the ceiling of the adaptive limit on concurrent store
              # reads from this node; it halves on a throttle or an unreachable
              # answer and grows back by one per round of successes. Under the
              # HTTP client's 50 connections per host, so no read waits for one.
              read_max_inflight: 48,
              # per store: reads waiting for a slot; past this a read is shed at
              # once (0 = never wait)
              read_queue_max: 256,
              # memory for cached pack indexes and claim bodies; 0 = no cache
              read_cache_bytes: 128 * 1024 * 1024,
              # a claim larger than this is served but never cached
              read_cache_max_entry_bytes: 1024 * 1024
            },
            # continuous backup of the store to an object store, and restore into
            # an empty data_dir at boot (see Ankusa.Store.Backup). Off by default.
            backup: %{
              enabled: false,
              # a checkpoint is uploaded this often; a lost host loses at most
              # the hooks acked since the last one
              interval_ms: 60_000,
              # backups kept in the object store (older ones and the files only
              # they referenced are deleted after each successful upload)
              keep: 3,
              # {module, opts} for backup objects; nil = storage.blob_store.
              # Keys go under storage.key_prefix either way: backups are per node.
              blob_store: nil
            },
            # operator HTTP API + Prometheus /metrics, unauthenticated; off by
            # default for embedded use
            admin: %{
              enabled: false,
              port: 4002,
              ip: "127.0.0.1",
              gauge_interval_ms: 15_000,
              # nil (plain HTTP) or a TLS block, shaped like `listener.tls`
              tls: nil
            },
            # a listener that serves only `/metrics`, `/health` and `/ready`
            # (`Ankusa.Metrics.Router`), so a scraper needs no route to the
            # mutating admin API. Off by default; the admin API keeps serving
            # `/metrics` either way. Also starts the metrics reporter and the
            # gauge poller when the admin API is off.
            metrics: %{enabled: false, port: 4004, ip: "127.0.0.1", tls: nil},
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
              admin: %{port: 4003, ip: "127.0.0.1", tls: nil},
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
            lifecycle: %{sinks: []},
            # where outbound HTTP may go (`Ankusa.HttpClient.Egress`): every
            # `Ankusa.Sink.Http` delivery — a source's or a lifecycle sink's — is
            # checked against it on every attempt, and a denied target is a
            # permanent failure. `https` only unless `allow_insecure_http`; an
            # address in `deny_cidrs` is refused unless `allow_cidrs` or
            # `allow_hosts` exempts it; `deny_hosts` names are refused before any
            # lookup. `allow_cidrs` is how a deployment opts in to the private
            # network its sinks live on; `deny_cidrs` replaces the default list.
            egress: %{
              allow_insecure_http: false,
              deny_cidrs: [
                "0.0.0.0/8",
                "127.0.0.0/8",
                "10.0.0.0/8",
                "172.16.0.0/12",
                "192.168.0.0/16",
                "100.64.0.0/10",
                "169.254.0.0/16",
                "::/128",
                "::1/128",
                "fc00::/7",
                "fe80::/10"
              ],
              allow_cidrs: [],
              allow_hosts: [],
              deny_hosts: ["metadata.google.internal"]
            },
            # secrets at rest (`Ankusa.Crypto.Envelope`): the key-encryption key
            # that source specs' secrets (verify secrets, sink credentials) are
            # encrypted under before they are stored. `kek` is 32 bytes, base64
            # (optionally `"<id>:<base64>"`); `kek_provider` is a
            # `{module, fun, args}` returning `{:ok, [kek | previous]}` instead;
            # `kek_previous` keys decrypt but never encrypt, for rotation. With
            # none, specs are stored as plain JSON, as they always were.
            secrets: %{kek: nil, kek_previous: [], kek_provider: nil}

  # `lifecycle.sinks` is spelled out because the struct's default (`[]`) is all
  # a bare `%__MODULE__{}` tells dialyzer about the field.
  @type t :: %__MODULE__{lifecycle: %{sinks: [{module(), keyword()}]}}

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
  sections (`:listener`, `:batcher`, `:dispatch`, `:storage`, `:claim_check`,
  `:backup`, `:admin`, `:metrics`, `:routes`, `:rate_limits`, `:quarantine`,
  `:lifecycle`, `:egress`, `:secrets`) over the defaults. Every `tls` block (`listener.tls`,
  `admin.tls`, `routes.admin.tls`, `claim_check.tls`, `metrics.tls`) is
  validated and normalised by `Ankusa.Listener.normalize_tls!/2`, and
  `listener.num_acceptors: nil` becomes two per scheduler.

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
          :listener,
          :batcher,
          :dispatch,
          :storage,
          :claim_check,
          :backup,
          :admin,
          :metrics,
          :rate_limits,
          :quarantine,
          :lifecycle,
          :egress,
          :secrets
        ] ->
          put_section(acc, k, v)

        Map.has_key?(base, k) ->
          Map.put(acc, k, v)

        true ->
          raise ArgumentError, "unknown Ankusa.Config key: #{inspect(k)}"
      end
    end)
    |> normalize_wal()
    |> validate_listen_ips!()
    |> validate_egress!()
    |> normalize_listeners!()
    |> validate_ranges!()
    |> validate_secrets!()
  end

  defp validate_secrets!(config) do
    :ok = Ankusa.Crypto.Envelope.validate_config!(config)
    config
  end

  @doc "The address a listener binds: a strict IPv4/IPv6 literal string, or an `:inet` address tuple."
  @spec listen_ip!(term(), String.t()) :: :inet.ip_address()
  def listen_ip!(ip, key) when is_binary(ip) do
    case :inet.parse_strict_address(String.to_charlist(ip)) do
      {:ok, addr} -> addr
      {:error, _} -> raise ArgumentError, bad_ip(ip, key)
    end
  end

  def listen_ip!(ip, key) when is_tuple(ip) do
    if :inet.is_ip_address(ip), do: ip, else: raise(ArgumentError, bad_ip(ip, key))
  end

  def listen_ip!(ip, key), do: raise(ArgumentError, bad_ip(ip, key))

  defp bad_ip(ip, key) do
    "#{key} must be an IP address such as \"127.0.0.1\" or \"0.0.0.0\", got: #{inspect(ip)}"
  end

  defp validate_listen_ips!(config) do
    listen_ip!(config.admin.ip, "admin.ip")
    listen_ip!(config.claim_check.ip, "claim_check.ip")
    listen_ip!(config.routes.admin.ip, "routes.admin.ip")
    listen_ip!(config.metrics.ip, "metrics.ip")
    config
  end

  # The policy's own shape. The sinks it governs are checked where sources are
  # built (`Ankusa.HttpClient.Egress.validate_config!/1`, at boot).
  defp validate_egress!(config) do
    Ankusa.HttpClient.Egress.validate_policy!(config.egress)
    config
  end

  # Every `tls` block is checked and normalised here (files exist, `verify_peer`
  # has its CA), and the acceptor count the section leaves open is settled.
  defp normalize_listeners!(config) do
    listener = config.listener

    listener = %{
      listener
      | tls: Ankusa.Listener.normalize_tls!(listener.tls, "listener.tls"),
        num_acceptors: listener.num_acceptors || Ankusa.Listener.default_acceptors()
    }

    %{
      config
      | listener: listener,
        admin: put_tls!(config.admin, "admin.tls"),
        claim_check: put_tls!(config.claim_check, "claim_check.tls"),
        metrics: put_tls!(config.metrics, "metrics.tls"),
        routes: %{config.routes | admin: put_tls!(config.routes.admin, "routes.admin.tls")}
    }
  end

  defp put_tls!(section, key),
    do: %{section | tls: Ankusa.Listener.normalize_tls!(section.tls, key)}

  # Types and ranges of the numeric keys, so `0` partitions or a negative
  # concurrency fail at boot with the key's name instead of misbehaving later.
  # Module-specific rules (dispatch breakers, claim retention, quarantine
  # buckets, routes) stay with their modules' `validate_config!/1`.
  @ports [
    {[:port], "port"},
    {[:admin, :port], "admin.port"},
    {[:claim_check, :port], "claim_check.port"},
    {[:routes, :admin, :port], "routes.admin.port"},
    {[:metrics, :port], "metrics.port"}
  ]

  @positive [
    {[:max_body_bytes], "max_body_bytes"},
    {[:direct_publish_timeout_ms], "direct_publish_timeout_ms"},
    {[:batcher, :partitions], "batcher.partitions"},
    {[:batcher, :max_batch], "batcher.max_batch"},
    {[:batcher, :max_queue], "batcher.max_queue"},
    {[:batcher, :max_queue_bytes], "batcher.max_queue_bytes"},
    {[:dispatch, :batch], "dispatch.batch"},
    {[:dispatch, :concurrency], "dispatch.concurrency"},
    {[:dispatch, :max_inflight], "dispatch.max_inflight"},
    {[:dispatch, :max_inflight_bytes], "dispatch.max_inflight_bytes"},
    {[:storage, :roll_bytes], "storage.roll_bytes"},
    {[:claim_check, :sweep_interval_ms], "claim_check.sweep_interval_ms"},
    {[:claim_check, :store_cooldown_ms], "claim_check.store_cooldown_ms"},
    {[:claim_check, :read_deadline_ms], "claim_check.read_deadline_ms"},
    {[:claim_check, :read_timeout_ms], "claim_check.read_timeout_ms"},
    {[:claim_check, :read_max_inflight], "claim_check.read_max_inflight"},
    {[:claim_check, :read_cache_max_entry_bytes], "claim_check.read_cache_max_entry_bytes"},
    {[:backup, :interval_ms], "backup.interval_ms"},
    {[:backup, :keep], "backup.keep"},
    {[:admin, :gauge_interval_ms], "admin.gauge_interval_ms"},
    {[:routes, :max_routes], "routes.max_routes"},
    {[:listener, :max_connections], "listener.max_connections"},
    {[:listener, :num_acceptors], "listener.num_acceptors"},
    {[:listener, :read_timeout_ms], "listener.read_timeout_ms"},
    {[:listener, :body_read_timeout_ms], "listener.body_read_timeout_ms"},
    {[:listener, :request_deadline_ms], "listener.request_deadline_ms"},
    {[:listener, :max_requests_per_connection], "listener.max_requests_per_connection"},
    {[:listener, :max_header_count], "listener.max_header_count"},
    {[:listener, :max_header_length], "listener.max_header_length"}
  ]

  # `0` means something for these: no linger before a commit, roll at once,
  # a compactor that only runs when asked (`Compactor.tick/1`), a claim read
  # with no retry, one that never waits for a slot, and no claim cache.
  @non_negative [
    {[:batcher, :max_delay_ms], "batcher.max_delay_ms"},
    {[:storage, :roll_ms], "storage.roll_ms"},
    {[:storage, :interval_ms], "storage.interval_ms"},
    {[:claim_check, :read_retries], "claim_check.read_retries"},
    {[:claim_check, :read_queue_max], "claim_check.read_queue_max"},
    {[:claim_check, :read_cache_bytes], "claim_check.read_cache_bytes"}
  ]

  @key_prefix ~r"\A([A-Za-z0-9._-]+/)+\z"

  defp validate_ranges!(config) do
    for {path, key} <- @ports do
      check!(
        config,
        path,
        key,
        "an integer from 0 to 65535",
        &(is_integer(&1) and &1 in 0..65_535)
      )
    end

    for {path, key} <- @positive do
      check!(config, path, key, "a positive integer", &(is_integer(&1) and &1 >= 1))
    end

    for {path, key} <- @non_negative do
      check!(config, path, key, "a non-negative integer", &(is_integer(&1) and &1 >= 0))
    end

    check!(
      config,
      [:claim_check, :retention_days],
      "claim_check.retention_days",
      "nil or an integer >= 1",
      &(&1 == nil or (is_integer(&1) and &1 >= 1))
    )

    check!(
      config,
      [:storage, :key_prefix],
      "storage.key_prefix",
      ~s(\"\" or path segments each ending in "/" such as "node-a/"),
      &(&1 == "" or (is_binary(&1) and Regex.match?(@key_prefix, &1)))
    )

    for {path, key} <- [
          {[:listener, :http2], "listener.http2"},
          {[:metrics, :enabled], "metrics.enabled"}
        ] do
      check!(config, path, key, "true or false", &is_boolean/1)
    end

    config
  end

  defp check!(config, path, key, constraint, valid?) do
    value = get_in(config, Enum.map(path, &Access.key/1))

    unless valid?.(value) do
      raise ArgumentError, "#{key} must be #{constraint}, got #{inspect(value)}"
    end
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
