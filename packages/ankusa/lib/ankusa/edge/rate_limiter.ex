defmodule Ankusa.Edge.RateLimiter do
  @moduledoc """
  Ingest admission, enforced in this node's memory. Two layers, in the order a
  request meets them:

  ## Before the body is read: CPU protection

  Charged by `Ankusa.Edge.Router` after the source is looked up and before a
  byte of the body is read, so none of them depends on verification and a forged
  flood is refused cheaply. They protect this node, they are not a quota.

    * `rate_limits.per_ip` — hooks per second per client address
      (`hit_ip/2`). The address is `Ankusa.Net.ClientIP`'s, with
      `routes.trusted_proxies` deciding whose `X-Forwarded-For` is believed; an
      IPv6 address counts as its `/64`. Addresses hash into a fixed 2^20 buckets
      with a per-boot salt, so the table has a hard ceiling however many
      addresses a flood invents.
    * `rate_limits.node` — hooks per second for the whole node (`hit_node/1`).
    * `rate_limits.sources.default.bytes_per_second` — a body-bytes-per-second
      ceiling per source (`hit_source_bytes/3`), 100 MB/s unless changed. It is
      charged with the declared `Content-Length`; a body with no declared length
      is charged when it has been read.

  A denial is `429` with `Retry-After`; `pre_read/4` says which one answered.

  ## After verification: the tenant's quota

  A limit is `%{rate: number, burst: pos_integer}`: `rate` is hooks per second
  (fractions are allowed — `0.5` means one every two seconds) and `burst` is the
  most hooks admitted back to back. It may also carry:

    * `bytes_per_second`, `burst_bytes` — body bytes, charged with the hook
      (`burst_bytes` defaults to one second's worth and needs
      `bytes_per_second`). A body bigger than `burst_bytes` is admitted only
      into a full bucket and empties it.
    * `max_body_bytes` — the biggest body this tenant may send, at most the
      instance's `max_body_bytes` (`body_limit/2`; the router answers `413`).
    * `max_queue`, `max_queue_bytes` — how many hooks and body bytes the tenant
      may hold in this node's batchers at once, buffered and in flight
      (`acquire_queue/3`, `release_queue/2`). Past either is
      `429 tenant_overload`, never the global `503 overload`, so one tenant
      filling its share cannot starve the rest.

  Limits come from three places, in precedence order (the first one found
  applies *whole*; its keys do not merge with the next's):

    1. a runtime override set through the admin API (`PUT
       /v1/tenants/:tenant/rate-limit`), persisted to this node's `Ankusa.Store`
       and node-local, exactly like `Ankusa.SourceStore.Persistent`'s sources;
    2. `rate_limits.tenants[tenant]`;
    3. `rate_limits.default`.

  No limit at all (`default: nil` with no tenant entry, the default config)
  means unlimited. There is no "unlimited" value — exempt a single tenant from a
  default by giving it a high limit. A tenant-carrying URL scheme
  (`Ankusa.RouteResolver.TenantPath`) lets the sender name the tenant, so
  `validate_config!/1` refuses it without a `default`.

  A hook that clears the hook bucket and is then denied on bytes has spent its
  hook token; a forged request spends none of these, because the tenant charge
  comes after verification.

  ## Enforcement

  One ETS row per bucket holds its GCRA theoretical arrival time, and `hit/3`
  and friends update it with a compare-and-swap (`:ets.select_replace/2`), so
  concurrent requests on one bucket can never overshoot the limit. A denial
  writes nothing. Rows whose TAT has passed are a full bucket — identical to
  having no row — so a periodic sweep deletes them, which also bounds memory
  when the route resolver lets senders mint tenant ids.

  Queue occupancy is two counters per tenant (`:ets.update_counter/4`: charge,
  compare, undo on refusal), held only while records are in flight and deleted
  at zero, so the table is bounded by the batchers' own caps.

  The tables live on the edge node and nowhere else: with the default
  `rate_limits.scope: :node` a fleet of N edge nodes admits up to N × the limit,
  and an override set against one node's admin port is that node's alone.
  `rate_limits.scope: :fleet` moves the tenant's buckets (hooks and bytes) into
  `fleet.store` (`Ankusa.Edge.Fleet`, `fleet_charges/3`), so the fleet admits one
  limit; this node's buckets then only answer when the store cannot. The override
  is still read from this node (set it on every node), and `per_ip`, `node` and
  `sources` stay per node under either scope: they protect this node's CPU, they
  are not a quota. Per-tenant queue limits cover the group-commit batcher
  (`wal: disk`); `wal: none` publishes inside the request and has no queue.
  """

  use GenServer

  require Logger

  alias Ankusa.Audit
  alias Ankusa.Audit.Context
  alias Ankusa.Config
  alias Ankusa.Net
  alias Ankusa.Net.ClientIP
  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @type limit :: %{
          required(:rate) => number(),
          required(:burst) => pos_integer(),
          optional(:bytes_per_second) => number(),
          optional(:burst_bytes) => pos_integer(),
          optional(:max_body_bytes) => pos_integer(),
          optional(:max_queue) => pos_integer(),
          optional(:max_queue_bytes) => pos_integer()
        }
  @type source :: :override | :config | :default
  @type denied :: {:error, {:rate_limited, pos_integer()}}

  @typedoc "What `acquire_queue/3` hands back: pass it to `release_queue/2`. `nil` is nothing counted."
  @type lease :: {String.t(), non_neg_integer()} | nil

  @optional_keys [:bytes_per_second, :burst_bytes, :max_body_bytes, :max_queue, :max_queue_bytes]
  @limit_keys [:rate, :burst | @optional_keys]

  # The per-IP table's ceiling: addresses hash into this many buckets.
  @ip_buckets 1_048_576

  @sweep_ms 60_000

  # A tenant over its queue share clears as its own commits finish.
  @tenant_overload_retry_ms 1_000

  # ── process ─────────────────────────────────────────────────────────────────

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :rate_limiter))
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    config = Ankusa.config(instance)

    overrides =
      :ets.new(overrides_table(instance), [
        :named_table,
        :protected,
        :set,
        read_concurrency: true
      ])

    # What the per-IP limiter reads on every request, once, at boot: the parsed
    # `routes.trusted_proxies` and the salt that makes bucket collisions
    # unchoosable from outside.
    settings =
      :ets.new(settings_table(instance), [
        :named_table,
        :protected,
        :set,
        read_concurrency: true
      ])

    proxies =
      for cidr <- config.routes.trusted_proxies,
          {:ok, parsed} <- [Net.parse_cidr(cidr)],
          do: parsed

    :ets.insert(settings, {:settings, :crypto.strong_rand_bytes(8), proxies})

    # `:public`, because every request process runs `hit/3`'s compare-and-swap
    # against it — that is the whole point of the CAS.
    :ets.new(buckets_table(instance), [
      :named_table,
      :public,
      :set,
      read_concurrency: true,
      write_concurrency: true
    ])

    # `{tenant, queued_hooks, queued_bytes}`, updated by the request processes.
    :ets.new(queue_table(instance), [
      :named_table,
      :public,
      :set,
      read_concurrency: true,
      write_concurrency: true
    ])

    case load_persisted(instance, overrides) do
      :ok ->
        Process.send_after(self(), :sweep, @sweep_ms)
        {:ok, %{instance: instance, config: config}}

      {:error, reason} ->
        {:stop, {:rate_limits_load_failed, reason}}
    end
  end

  # Crash reports print the state; the config is the whole instance config,
  # sink options and all.
  @impl true
  def format_status(%{state: %{config: _} = state} = status),
    do: %{status | state: %{state | config: :redacted}}

  def format_status(status), do: status

  @impl true
  def handle_info(:sweep, state) do
    now = System.monotonic_time()

    # A row with `tat <= now` is a full bucket: dropping it admits exactly what
    # keeping it would.
    :ets.select_delete(buckets_table(state.instance), [
      {{:_, :"$1"}, [{:"=<", :"$1", now}], [true]}
    ])

    Process.send_after(self(), :sweep, @sweep_ms)
    {:noreply, state}
  end

  # ── config validation ───────────────────────────────────────────────────────

  @doc """
  Validate the `rate_limits` section of a config, raising `ArgumentError` on the
  first problem. Called on every node and role at boot, so a typo fails the
  boot rather than the first limited hook.

  A config whose route resolver is `Ankusa.RouteResolver.TenantPath` must set a
  `rate_limits.default`: the URL names the tenant, so without one every tenant
  the config does not list is unlimited.
  """
  @spec validate_config!(Config.t()) :: :ok
  def validate_config!(%Config{rate_limits: rate_limits} = config) do
    %{default: default, tenants: tenants} = rate_limits

    unless is_map(tenants) do
      raise ArgumentError,
            "rate_limits.tenants must be a map of tenant id to limit, got #{inspect(tenants)}"
    end

    Enum.each(tenants, fn {tenant, limit} ->
      unless Ankusa.ClaimCheck.Ref.valid_tenant?(tenant) do
        raise ArgumentError,
              "rate_limits.tenants: tenant id #{inspect(tenant)} must match [A-Za-z0-9_-]{1,64}"
      end

      validate_limit!(limit, "rate_limits.tenants.#{tenant}", config)
    end)

    if default != nil, do: validate_limit!(default, "rate_limits.default", config)

    validate_hook_limit!(rate_limits.per_ip, "rate_limits.per_ip")
    validate_hook_limit!(rate_limits.node, "rate_limits.node")
    validate_sources!(rate_limits.sources)
    validate_tenant_path!(config)

    :ok
  end

  defp validate_limit!(limit, name, config) when is_map(limit) do
    keys = Map.keys(limit)

    if :rate in keys and :burst in keys and Enum.all?(keys, &(&1 in @limit_keys)) do
      with :ok <- check_values(limit.rate, limit.burst),
           :ok <- check_optional(limit, config.max_body_bytes) do
        :ok
      else
        {:error, message} -> raise ArgumentError, "#{name}.#{message}"
      end
    else
      raise ArgumentError, bad_limit(name, limit)
    end
  end

  defp validate_limit!(value, name, _config), do: raise(ArgumentError, bad_limit(name, value))

  defp bad_limit(name, value) do
    optional = Enum.map_join(@optional_keys, ", ", &inspect/1)

    "#{name} must be a map with :rate and :burst and, optionally, #{optional}, " <>
      "got #{inspect(value)}"
  end

  # `per_ip` and `node` are hooks-per-second limits and nothing more.
  defp validate_hook_limit!(nil, _name), do: :ok

  defp validate_hook_limit!(limit, name) when is_map(limit) do
    case Enum.sort(Map.keys(limit)) do
      [:burst, :rate] ->
        case check_values(limit.rate, limit.burst) do
          :ok -> :ok
          {:error, message} -> raise ArgumentError, "#{name}.#{message}"
        end

      _ ->
        raise ArgumentError,
              "#{name} must be a map with exactly :rate and :burst, got #{inspect(limit)}"
    end
  end

  defp validate_hook_limit!(value, name) do
    raise ArgumentError,
          "#{name} must be a map with exactly :rate and :burst, got #{inspect(value)}"
  end

  defp validate_sources!(sources) when is_map(sources) do
    case Map.keys(sources) -- [:default] do
      [] ->
        :ok

      [key | _] ->
        raise ArgumentError,
              "rate_limits.sources: unknown key #{inspect(key)}; only :default is supported"
    end

    case Map.get(sources, :default) do
      nil -> :ok
      limit when is_map(limit) -> validate_source_limit!(limit)
      other -> raise ArgumentError, bad_source_limit(other)
    end
  end

  defp validate_sources!(other) do
    raise ArgumentError, "rate_limits.sources must be a map, got #{inspect(other)}"
  end

  defp validate_source_limit!(limit) do
    keys = Map.keys(limit)

    if :bytes_per_second in keys and Enum.all?(keys, &(&1 in [:bytes_per_second, :burst_bytes])) do
      case check_optional(limit, nil) do
        :ok -> :ok
        {:error, message} -> raise ArgumentError, "rate_limits.sources.default.#{message}"
      end
    else
      raise ArgumentError, bad_source_limit(limit)
    end
  end

  defp bad_source_limit(value) do
    "rate_limits.sources.default must be nil or a map with :bytes_per_second and, " <>
      "optionally, :burst_bytes, got #{inspect(value)}"
  end

  defp validate_tenant_path!(%Config{
         route_resolver: {Ankusa.RouteResolver.TenantPath, _},
         rate_limits: %{default: nil}
       }) do
    raise ArgumentError,
          "rate_limits.default is required with routing: tenant_path " <>
            "(Ankusa.RouteResolver.TenantPath): the URL names the tenant, so any tenant " <>
            "the config does not list would have no limit at all"
  end

  defp validate_tenant_path!(_config), do: :ok

  # Shared by the config validator and the JSON parser, so both accept exactly
  # the same limits.
  defp check_values(rate, burst) do
    cond do
      not (is_number(rate) and rate > 0) ->
        {:error, "rate must be a number greater than 0, got #{inspect(rate)}"}

      not (is_integer(burst) and burst >= 1) ->
        {:error, "burst must be an integer of at least 1, got #{inspect(burst)}"}

      true ->
        :ok
    end
  end

  # The optional keys that are present, in a fixed order. `max_body_bytes` is
  # `nil` for a limit with no instance to compare against.
  defp check_optional(limit, max_body_bytes) do
    Enum.find_value(@optional_keys, :ok, fn key ->
      case Map.fetch(limit, key) do
        {:ok, value} -> optional_error(key, value, limit, max_body_bytes)
        :error -> nil
      end
    end)
  end

  defp optional_error(:bytes_per_second, value, _limit, _max) do
    unless is_number(value) and value > 0,
      do: {:error, "bytes_per_second must be a number greater than 0, got #{inspect(value)}"}
  end

  defp optional_error(:burst_bytes, value, limit, _max) do
    cond do
      not (is_integer(value) and value >= 1) ->
        {:error, "burst_bytes must be an integer of at least 1, got #{inspect(value)}"}

      not Map.has_key?(limit, :bytes_per_second) ->
        {:error, "burst_bytes needs bytes_per_second"}

      true ->
        nil
    end
  end

  defp optional_error(:max_body_bytes, value, _limit, max) do
    unless is_integer(value) and value >= 1 and (max == nil or value <= max) do
      ceiling = if max, do: " and at most the instance's max_body_bytes (#{max})", else: ""

      {:error, "max_body_bytes must be an integer of at least 1#{ceiling}, got #{inspect(value)}"}
    end
  end

  defp optional_error(key, value, _limit, _max) when key in [:max_queue, :max_queue_bytes] do
    unless is_integer(value) and value >= 1,
      do: {:error, "#{key} must be an integer of at least 1, got #{inspect(value)}"}
  end

  # ── limits ──────────────────────────────────────────────────────────────────

  @doc """
  Parse a limit from JSON-shaped attrs (`%{"rate" => …, "burst" => …}` plus any
  of the optional keys, atom keys accepted). `rate` and `burst` are required;
  neither is inferred. `max_body_bytes` is the instance's, the ceiling a
  tenant's `max_body_bytes` may not pass.
  """
  @spec parse_limit(term(), pos_integer()) :: {:ok, limit()} | {:error, :invalid, String.t()}
  def parse_limit(attrs, max_body_bytes) when is_map(attrs) do
    attrs =
      Map.new(attrs, fn
        {key, value} when is_atom(key) -> {Atom.to_string(key), value}
        {key, value} -> {key, value}
      end)

    with :ok <- check_known_keys(attrs),
         {:ok, rate} <- fetch_key(attrs, "rate"),
         {:ok, burst} <- fetch_key(attrs, "burst"),
         :ok <- check_values(rate, burst),
         limit = with_optional(%{rate: rate, burst: burst}, attrs),
         :ok <- check_optional(limit, max_body_bytes) do
      {:ok, limit}
    else
      {:error, message} -> {:error, :invalid, message}
    end
  end

  def parse_limit(_attrs, _max_body_bytes), do: {:error, :invalid, "body must be a JSON object"}

  defp with_optional(limit, attrs) do
    Enum.reduce(@optional_keys, limit, fn key, acc ->
      case Map.fetch(attrs, Atom.to_string(key)) do
        {:ok, value} -> Map.put(acc, key, value)
        :error -> acc
      end
    end)
  end

  defp check_known_keys(attrs) do
    known = Enum.map(@limit_keys, &Atom.to_string/1)

    case attrs |> Map.keys() |> Enum.reject(&(&1 in known)) |> Enum.sort() do
      [] -> :ok
      [key | _] -> {:error, "unknown field #{inspect(key)}"}
    end
  end

  defp fetch_key(attrs, key) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, "#{key} is required"}
    end
  end

  @doc """
  The limit that applies to `tenant_id` on this node, with where it came from.
  `{nil, :none}` means unlimited.
  """
  @spec effective(atom(), String.t()) :: {limit(), source()} | {nil, :none}
  def effective(instance, tenant_id) do
    case :ets.lookup(overrides_table(instance), tenant_id) do
      [{^tenant_id, limit}] ->
        {limit, :override}

      [] ->
        %{default: default, tenants: tenants} = Ankusa.config(instance).rate_limits

        case Map.fetch(tenants, tenant_id) do
          {:ok, limit} -> {limit, :config}
          :error when default == nil -> {nil, :none}
          :error -> {default, :default}
        end
    end
  end

  @doc """
  Every limit this node knows: the config's tenants unioned with the runtime
  overrides, sorted by tenant id, plus the configured default.
  """
  @spec list(atom()) :: %{
          default: limit() | nil,
          tenants: [{String.t(), limit(), :override | :config}]
        }
  def list(instance) do
    %{default: default, tenants: tenants} = Ankusa.config(instance).rate_limits
    overrides = Map.new(:ets.tab2list(overrides_table(instance)))

    entries =
      tenants
      |> Map.merge(overrides)
      |> Enum.map(fn {tenant, limit} ->
        {tenant, limit, if(Map.has_key?(overrides, tenant), do: :override, else: :config)}
      end)
      |> Enum.sort_by(&elem(&1, 0))

    %{default: default, tenants: entries}
  end

  @doc """
  Charge one hook of `bytes` body bytes to `tenant_id`'s buckets. `:ok` admits
  it; a denial carries how long the client should wait, in milliseconds. The
  byte bucket is charged only when the limit has `bytes_per_second`; `bytes`
  defaults to `0` (a hook-count charge only).
  """
  @spec hit(atom(), String.t(), non_neg_integer()) :: :ok | denied()
  def hit(instance, tenant_id, bytes \\ 0) do
    case effective(instance, tenant_id) do
      {nil, :none} ->
        :ok

      {limit, _source} ->
        table = buckets_table(instance)

        with :ok <- charge(table, tenant_id, limit.rate, limit.burst, 1) do
          hit_bytes(table, tenant_id, limit, bytes)
        end
    end
  end

  defp hit_bytes(table, tenant_id, %{bytes_per_second: rate} = limit, bytes) when bytes > 0,
    do: charge(table, {:bytes, tenant_id}, rate, burst_bytes(limit), bytes)

  defp hit_bytes(_table, _tenant_id, _limit, _bytes), do: :ok

  # Without a `burst_bytes`, one second's worth.
  defp burst_bytes(%{burst_bytes: burst}), do: burst
  defp burst_bytes(%{bytes_per_second: rate}), do: max(1, ceil(rate))

  @doc """
  The buckets `hit/3` would charge for one hook of `bytes` body bytes, as
  `Ankusa.RateLimiter.Store` charges (GCRA increment and capacity in
  microseconds) — `[]` when the tenant has no limit. Reads the effective limit
  on this node and charges nothing.
  """
  @spec fleet_charges(atom(), String.t(), non_neg_integer()) ::
          [Ankusa.RateLimiter.Store.charge()]
  def fleet_charges(instance, tenant_id, bytes) do
    case effective(instance, tenant_id) do
      {nil, :none} ->
        []

      {limit, _source} ->
        {increment, capacity} = gcra(1_000_000, limit.rate, limit.burst, 1)
        hooks = {:hooks, tenant_id, increment, capacity}

        case limit do
          %{bytes_per_second: rate} when bytes > 0 ->
            {increment, capacity} = gcra(1_000_000, rate, burst_bytes(limit), bytes)
            [hooks, {:bytes, tenant_id, increment, capacity}]

          _ ->
            [hooks]
        end
    end
  end

  @doc """
  The biggest body `tenant_id` may send, when its limit sets `max_body_bytes`
  (otherwise `nil`: the instance's `max_body_bytes` alone applies).
  """
  @spec body_limit(atom(), String.t()) :: pos_integer() | nil
  def body_limit(instance, tenant_id) do
    case effective(instance, tenant_id) do
      {%{max_body_bytes: max}, _source} -> max
      _ -> nil
    end
  end

  # ── before the body: per IP, per node, per source ───────────────────────────

  @doc """
  The checks that run before a request's body is read: the client's address
  (`rate_limits.per_ip`), then the node (`rate_limits.node`), then the source's
  declared `bytes` (`rate_limits.sources`; `nil` is a body with no declared
  length, charged later with `hit_source_bytes/3`). The first to refuse answers,
  with the scope that did and how long to wait in milliseconds. An address
  refused here has not touched the node's bucket.
  """
  @spec pre_read(atom(), Plug.Conn.t(), String.t(), non_neg_integer() | nil) ::
          :ok | {:error, {:ip | :node | :source, pos_integer()}}
  def pre_read(instance, %Plug.Conn{} = conn, source_id, declared) do
    with :ok <- scoped(hit_ip(instance, conn), :ip),
         :ok <- scoped(hit_node(instance), :node) do
      scoped(hit_source_bytes(instance, source_id, declared || 0), :source)
    end
  end

  defp scoped(:ok, _scope), do: :ok
  defp scoped({:error, {:rate_limited, ms}}, scope), do: {:error, {scope, ms}}

  @doc "Charge one hook to the client address's bucket (`rate_limits.per_ip`)."
  @spec hit_ip(atom(), Plug.Conn.t()) :: :ok | denied()
  def hit_ip(instance, %Plug.Conn{} = conn) do
    case Ankusa.config(instance).rate_limits.per_ip do
      nil ->
        :ok

      %{rate: rate, burst: burst} ->
        [{:settings, salt, proxies}] = :ets.lookup(settings_table(instance), :settings)

        case client_address(conn, proxies) do
          nil ->
            :ok

          ip ->
            bucket = :erlang.phash2({salt, ip_prefix(ip)}, @ip_buckets)
            charge(buckets_table(instance), {:ip, bucket}, rate, burst, 1)
        end
    end
  end

  # A chain this node cannot account for charges the peer's own address (the
  # proxy pays for what it forwards): `ClientIP.resolve/2` with no trusted
  # proxies is the peer. No peer at all is nothing to charge.
  defp client_address(conn, proxies) do
    with :error <- ClientIP.resolve(conn, proxies),
         :error <- ClientIP.resolve(conn, []) do
      nil
    else
      {:ok, ip} -> ip
    end
  end

  # An IPv6 customer holds a /64, not one address.
  defp ip_prefix({a, b, c, d, _e, _f, _g, _h}), do: {:v6, a, b, c, d}
  defp ip_prefix(ip), do: ip

  @doc "Charge one hook to the node's bucket (`rate_limits.node`)."
  @spec hit_node(atom()) :: :ok | denied()
  def hit_node(instance) do
    case Ankusa.config(instance).rate_limits.node do
      nil -> :ok
      %{rate: rate, burst: burst} -> charge(buckets_table(instance), :node, rate, burst, 1)
    end
  end

  @doc """
  Charge `bytes` of body to `source_id`'s bucket
  (`rate_limits.sources.default.bytes_per_second`).
  """
  @spec hit_source_bytes(atom(), String.t(), non_neg_integer()) :: :ok | denied()
  def hit_source_bytes(instance, source_id, bytes) when bytes > 0 do
    case Ankusa.config(instance).rate_limits.sources do
      %{default: %{bytes_per_second: rate} = source} ->
        charge(buckets_table(instance), {:src, source_id}, rate, burst_bytes(source), bytes)

      _ ->
        :ok
    end
  end

  def hit_source_bytes(_instance, _source_id, _bytes), do: :ok

  # ── the tenant's share of the batchers ──────────────────────────────────────

  @doc """
  Count one hook of `bytes` body bytes against `tenant_id`'s queue share, when
  its limit sets `max_queue` or `max_queue_bytes`. Returns a lease to hand back
  to `release_queue/2` once the batcher has answered, or
  `{:error, {:tenant_overload, retry_after_ms}}` with nothing counted.

  As in the batcher itself, a hook bigger than `max_queue_bytes` on its own is
  admitted when the tenant holds nothing else.
  """
  @spec acquire_queue(atom(), String.t(), non_neg_integer()) ::
          {:ok, lease()} | {:error, {:tenant_overload, pos_integer()}}
  def acquire_queue(instance, tenant_id, bytes) do
    case effective(instance, tenant_id) do
      {limit, _source}
      when is_map_key(limit, :max_queue) or is_map_key(limit, :max_queue_bytes) ->
        reserve(queue_table(instance), tenant_id, bytes, limit)

      _ ->
        {:ok, nil}
    end
  end

  # Charge first, compare, undo on refusal: two racers at the edge of the limit
  # see different counts, so exactly one of them is over it.
  defp reserve(table, tenant_id, bytes, limit) do
    [count, held] =
      :ets.update_counter(table, tenant_id, [{2, 1}, {3, bytes}], {tenant_id, 0, 0})

    if count > Map.get(limit, :max_queue, count) or
         (count > 1 and held > Map.get(limit, :max_queue_bytes, held)) do
      unreserve(table, tenant_id, bytes)
      {:error, {:tenant_overload, @tenant_overload_retry_ms}}
    else
      {:ok, {tenant_id, bytes}}
    end
  end

  @doc "Give back what `acquire_queue/3` counted. Safe to call with `nil`."
  @spec release_queue(atom(), lease()) :: :ok
  def release_queue(_instance, nil), do: :ok

  def release_queue(instance, {tenant_id, bytes}),
    do: unreserve(queue_table(instance), tenant_id, bytes)

  # Counters never go below zero (a limiter that restarted while hooks were in
  # flight has no row for them), and a row at zero is deleted. A charge racing
  # the delete recreates the row, since `reserve/4` supplies a default.
  defp unreserve(table, tenant_id, bytes) do
    :ets.update_counter(table, tenant_id, [{2, -1, 0, 0}, {3, -bytes, 0, 0}])
    :ets.select_delete(table, [{{tenant_id, 0, 0}, [], [true]}])
    :ok
  rescue
    # No row, or the table went away with its instance: nothing is counted.
    ArgumentError -> :ok
  end

  # ── buckets ─────────────────────────────────────────────────────────────────

  # `cost` units against a bucket refilling `rate` units per second with room for
  # `burst` of them. A cost bigger than the whole bucket is clamped to it, so it
  # is admitted only into a full bucket (and empties it) rather than never.
  defp charge(table, key, rate, burst, cost) do
    {increment, capacity} =
      gcra(System.convert_time_unit(1, :second, :native), rate, burst, cost)

    hit_bucket(table, key, increment, capacity)
  end

  # The GCRA parameters for `cost` units against a bucket refilling `rate` units
  # a second with room for `burst`, in a clock of `units_per_second`: the cost's
  # increment (clamped to the bucket) and the bucket's capacity.
  defp gcra(units_per_second, rate, burst, cost) do
    interval = max(1, round(units_per_second / rate))
    capacity = interval * burst
    {min(cost * interval, capacity), capacity}
  end

  # GCRA: a bucket is described by its theoretical arrival time. The next charge
  # is `increment` after that, and it is early — denied — if arriving then would
  # put the bucket more than `capacity` ahead of now. Equivalent to a token
  # bucket with `capacity` room, without ever storing tokens.
  #
  # Every update is a compare-and-swap on the row read, so two concurrent hits
  # on one bucket either both count or one retries: they can never both write.
  defp hit_bucket(table, key, increment, capacity) do
    now = System.monotonic_time()

    case :ets.lookup(table, key) do
      [] ->
        # A fresh bucket always admits: `increment <= capacity`.
        if :ets.insert_new(table, {key, now + increment}),
          do: :ok,
          else: hit_bucket(table, key, increment, capacity)

      [{^key, tat} = old] ->
        new_tat = max(tat, now) + increment
        allow_at = new_tat - capacity

        cond do
          now < allow_at ->
            {:error, {:rate_limited, retry_after_ms(allow_at - now)}}

          :ets.select_replace(table, [{old, [], [{:const, {key, new_tat}}]}]) == 1 ->
            :ok

          # Lost the race, or the sweeper deleted the row between the lookup and
          # the swap: re-read and try again.
          true ->
            hit_bucket(table, key, increment, capacity)
        end
    end
  end

  defp retry_after_ms(native) do
    micro = System.convert_time_unit(native, :native, :microsecond)
    max(1, div(micro + 999, 1000))
  end

  # A tenant's buckets: its hooks and its bytes.
  defp reset_buckets(instance, tenant_id) do
    table = buckets_table(instance)
    :ets.delete(table, tenant_id)
    :ets.delete(table, {:bytes, tenant_id})
    Ankusa.Edge.Fleet.reset_tenant(instance, tenant_id)
  end

  # ── overrides ───────────────────────────────────────────────────────────────

  @doc """
  Set `tenant_id`'s runtime override, resetting its buckets on success: without
  the reset a raised limit would stay blocked until the old limit's accumulated
  debt drained. The override replaces the tenant's limit whole, optional keys
  included. Options: `audit:`, the `Ankusa.Audit.Context` of the change.
  """
  @spec put_override(atom(), term(), term(), keyword()) ::
          {:ok, limit()} | {:error, :invalid, String.t()} | {:error, :store_unavailable}
  def put_override(instance, tenant_id, attrs, opts \\ []) do
    if Ankusa.ClaimCheck.Ref.valid_tenant?(tenant_id) do
      case parse_limit(attrs, Ankusa.config(instance).max_body_bytes) do
        {:ok, limit} ->
          GenServer.call(
            Ankusa.via(instance, :rate_limiter),
            {:put, tenant_id, limit, Context.from_opts(opts)}
          )

        {:error, :invalid, message} ->
          {:error, :invalid, message}
      end
    else
      {:error, :invalid, "tenant #{inspect(tenant_id)} must match [A-Za-z0-9_-]{1,64}"}
    end
  end

  @doc """
  Remove `tenant_id`'s runtime override, so the config's limit applies again.
  Resets the buckets too, for the same reason a `put` does. Options: `audit:`.
  """
  @spec delete_override(atom(), term(), keyword()) ::
          :ok | {:error, :not_found} | {:error, :store_unavailable}
  def delete_override(instance, tenant_id, opts \\ []) do
    GenServer.call(
      Ankusa.via(instance, :rate_limiter),
      {:delete, tenant_id, Context.from_opts(opts)}
    )
  end

  @impl true
  def handle_call({:put, tenant_id, limit, context}, _from, state) do
    table = overrides_table(state.instance)

    before =
      case :ets.lookup(table, tenant_id) do
        [{^tenant_id, current}] -> current
        [] -> nil
      end

    case persist_put(state.instance, context, tenant_id, before, limit) do
      :ok ->
        :ets.insert(table, {tenant_id, limit})
        reset_buckets(state.instance, tenant_id)
        {:reply, {:ok, limit}, state}

      {:error, reason} ->
        Logger.error("[ankusa] could not persist rate limits: #{inspect(reason)}")
        {:reply, {:error, :store_unavailable}, state}
    end
  end

  def handle_call({:delete, tenant_id, context}, _from, state) do
    table = overrides_table(state.instance)

    case :ets.lookup(table, tenant_id) do
      [] ->
        {:reply, {:error, :not_found}, state}

      [{^tenant_id, current}] ->
        ops = [{:delete, :default, Keys.rate_limit(tenant_id)}]

        attrs = %{
          action: "rate_limit.delete",
          tenant: tenant_id,
          resource: tenant_id,
          before_hash: Audit.digest(state.instance, current),
          detail: limit_detail(current)
        }

        case Audit.commit(state.instance, context, attrs, ops) do
          :ok ->
            :ets.delete(table, tenant_id)
            reset_buckets(state.instance, tenant_id)
            {:reply, :ok, state}

          {:error, reason} ->
            Logger.error("[ankusa] could not persist rate limits: #{inspect(reason)}")
            {:reply, {:error, :store_unavailable}, state}
        end
    end
  end

  # ── persistence ─────────────────────────────────────────────────────────────

  # One key per override (`r:<tenant>`), in the same synced batch as its audit
  # row: a `PUT` answers "stored".
  defp persist_put(instance, context, tenant_id, before, limit) do
    value = JSON.encode!(limit_detail(limit))

    attrs = %{
      action: "rate_limit.put",
      tenant: tenant_id,
      resource: tenant_id,
      before_hash: Audit.digest(instance, before),
      after_hash: Audit.digest(instance, limit),
      detail: limit_detail(limit)
    }

    Audit.commit(instance, context, attrs, [{:put, :default, Keys.rate_limit(tenant_id), value}])
  end

  # The limit as JSON-shaped attrs: what is stored, audited and re-read.
  defp limit_detail(limit),
    do: Map.new(limit, fn {key, value} -> {Atom.to_string(key), value} end)

  # A store this node cannot read must not boot with silently empty overrides:
  # a tenant's raised or lowered limit would quietly revert to the config's.
  defp load_persisted(instance, table) do
    %{lo: lo, hi: hi} = Keys.family(:rate_limits)
    max_body_bytes = Ankusa.config(instance).max_body_bytes

    result =
      Store.fold(instance, :rate_limits, {lo, hi}, :ok, fn key, value, :ok ->
        load_entry(Keys.decode_rate_limit(key), value, table, max_body_bytes)
        {:cont, :ok}
      end)

    case result do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp load_entry(tenant, value, table, max_body_bytes) do
    result =
      if Ankusa.ClaimCheck.Ref.valid_tenant?(tenant) do
        case JSON.decode(value) do
          {:ok, attrs} -> parse_limit(attrs, max_body_bytes)
          {:error, _} -> {:error, :invalid, "stored override is not valid JSON"}
        end
      else
        {:error, :invalid, "tenant id #{inspect(tenant)} must match [A-Za-z0-9_-]{1,64}"}
      end

    case result do
      {:ok, limit} ->
        :ets.insert(table, {tenant, limit})

      {:error, :invalid, message} ->
        Logger.warning(
          "[ankusa] skipping persisted rate limit for tenant #{inspect(tenant)}: #{message}"
        )
    end
  end

  # ── ETS ─────────────────────────────────────────────────────────────────────

  # The instance name is operator config: one atom per instance, never request input.
  # sobelow_skip ["DOS.BinToAtom"]
  defp overrides_table(instance), do: :"ankusa_rate_limits_#{instance}"

  # The instance name is operator config: one atom per instance, never request input.
  # sobelow_skip ["DOS.BinToAtom"]
  defp buckets_table(instance), do: :"ankusa_rate_buckets_#{instance}"

  # The instance name is operator config: one atom per instance, never request input.
  # sobelow_skip ["DOS.BinToAtom"]
  defp queue_table(instance), do: :"ankusa_rate_queue_#{instance}"

  # The instance name is operator config: one atom per instance, never request input.
  # sobelow_skip ["DOS.BinToAtom"]
  defp settings_table(instance), do: :"ankusa_rate_settings_#{instance}"
end
