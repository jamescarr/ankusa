defmodule Ankusa.Edge.RateLimiter do
  @moduledoc """
  Per-tenant ingest rate limits, enforced in this node's memory.

  A limit is `%{rate: number, burst: pos_integer}`: `rate` is hooks per second
  (fractions are allowed — `0.5` means one every two seconds) and `burst` is the
  most hooks admitted back to back.

  Limits come from three places, in precedence order:

    1. a runtime override set through the admin API (`PUT
       /v1/tenants/:tenant/rate-limit`), persisted to this node's `Ankusa.Store`
       and node-local, exactly like `Ankusa.SourceStore.Persistent`'s sources;
    2. `rate_limits.tenants[tenant]`;
    3. `rate_limits.default`.

  No limit at all (`default: nil` with no tenant entry, the default config)
  means unlimited. There is no "unlimited" value — exempt a single tenant from a
  default by giving it a high limit.

  ## Enforcement

  One ETS row per tenant holds its GCRA theoretical arrival time, and
  `hit/2` updates it with a compare-and-swap (`:ets.select_replace/2`), so
  concurrent hooks on one tenant can never overshoot the limit. A denial writes
  nothing. Rows whose TAT has passed are a full bucket — identical to having no
  row — so a periodic sweep deletes them, which also bounds memory when the
  route resolver lets senders mint tenant ids.

  The tables live on the edge node and nowhere else: a fleet of N edge nodes
  admits up to N × the limit, and an override set against one node's admin port
  is that node's alone.
  """

  use GenServer

  require Logger

  alias Ankusa.Config
  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @type limit :: %{rate: number(), burst: pos_integer()}
  @type source :: :override | :config | :default

  @sweep_ms 60_000

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

    # `:public`, because every request process runs `hit/2`'s compare-and-swap
    # against it — that is the whole point of the CAS.
    :ets.new(buckets_table(instance), [
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
  """
  @spec validate_config!(Config.t()) :: :ok
  def validate_config!(%Config{rate_limits: %{default: default, tenants: tenants}}) do
    unless is_map(tenants) do
      raise ArgumentError,
            "rate_limits.tenants must be a map of tenant id to limit, got #{inspect(tenants)}"
    end

    Enum.each(tenants, fn {tenant, limit} ->
      unless Ankusa.ClaimCheck.Ref.valid_tenant?(tenant) do
        raise ArgumentError,
              "rate_limits.tenants: tenant id #{inspect(tenant)} must match [A-Za-z0-9_-]{1,64}"
      end

      validate_limit!(limit, "rate_limits.tenants.#{tenant}")
    end)

    if default != nil, do: validate_limit!(default, "rate_limits.default")

    :ok
  end

  defp validate_limit!(limit, name) when is_map(limit) do
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

  defp validate_limit!(value, name) do
    raise ArgumentError,
          "#{name} must be a map with exactly :rate and :burst, got #{inspect(value)}"
  end

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

  # ── limits ──────────────────────────────────────────────────────────────────

  @doc """
  Parse a limit from JSON-shaped attrs (`%{"rate" => …, "burst" => …}`, atom
  keys accepted). Both keys are required; neither is inferred.
  """
  @spec parse_limit(term()) :: {:ok, limit()} | {:error, :invalid, String.t()}
  def parse_limit(attrs) when is_map(attrs) do
    attrs =
      Map.new(attrs, fn
        {key, value} when is_atom(key) -> {Atom.to_string(key), value}
        {key, value} -> {key, value}
      end)

    with :ok <- check_known_keys(attrs),
         {:ok, rate} <- fetch_key(attrs, "rate"),
         {:ok, burst} <- fetch_key(attrs, "burst"),
         :ok <- check_values(rate, burst) do
      {:ok, %{rate: rate, burst: burst}}
    else
      {:error, message} -> {:error, :invalid, message}
    end
  end

  def parse_limit(_attrs), do: {:error, :invalid, "body must be a JSON object"}

  defp check_known_keys(attrs) do
    case attrs |> Map.keys() |> Enum.reject(&(&1 in ["rate", "burst"])) |> Enum.sort() do
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
  Charge one hook to `tenant_id`'s bucket. `:ok` admits it; a denial carries how
  long the client should wait, in milliseconds.
  """
  @spec hit(atom(), String.t()) :: :ok | {:error, {:rate_limited, pos_integer()}}
  def hit(instance, tenant_id) do
    case effective(instance, tenant_id) do
      {nil, :none} ->
        :ok

      {%{rate: rate, burst: burst}, _source} ->
        interval = max(1, round(System.convert_time_unit(1, :second, :native) / rate))

        hit_bucket(buckets_table(instance), tenant_id, interval, interval * burst)
    end
  end

  # GCRA: a bucket is described by its theoretical arrival time. The next hook
  # is 1/rate after that, and it is early — denied — if arriving then would put
  # the bucket more than `burst` hooks' worth of time ahead of now. Equivalent
  # to a token bucket with `burst` capacity, without ever storing tokens.
  #
  # Every update is a compare-and-swap on the row read, so two concurrent hits
  # on one tenant either both count or one retries: they can never both write.
  defp hit_bucket(table, tenant_id, interval, capacity) do
    now = System.monotonic_time()

    case :ets.lookup(table, tenant_id) do
      [] ->
        # A fresh bucket always admits: `burst >= 1`.
        if :ets.insert_new(table, {tenant_id, now + interval}),
          do: :ok,
          else: hit_bucket(table, tenant_id, interval, capacity)

      [{^tenant_id, tat} = old] ->
        new_tat = max(tat, now) + interval
        allow_at = new_tat - capacity

        cond do
          now < allow_at ->
            {:error, {:rate_limited, retry_after_ms(allow_at - now)}}

          :ets.select_replace(table, [{old, [], [{:const, {tenant_id, new_tat}}]}]) == 1 ->
            :ok

          # Lost the race, or the sweeper deleted the row between the lookup and
          # the swap: re-read and try again.
          true ->
            hit_bucket(table, tenant_id, interval, capacity)
        end
    end
  end

  defp retry_after_ms(native) do
    micro = System.convert_time_unit(native, :native, :microsecond)
    max(1, div(micro + 999, 1000))
  end

  # ── overrides ───────────────────────────────────────────────────────────────

  @doc """
  Set `tenant_id`'s runtime override, resetting its bucket on success: without
  the reset a raised limit would stay blocked until the old limit's accumulated
  debt drained.
  """
  @spec put_override(atom(), term(), term()) ::
          {:ok, limit()} | {:error, :invalid, String.t()} | {:error, :store_unavailable}
  def put_override(instance, tenant_id, attrs) do
    if Ankusa.ClaimCheck.Ref.valid_tenant?(tenant_id) do
      case parse_limit(attrs) do
        {:ok, limit} ->
          GenServer.call(Ankusa.via(instance, :rate_limiter), {:put, tenant_id, limit})

        {:error, :invalid, message} ->
          {:error, :invalid, message}
      end
    else
      {:error, :invalid, "tenant #{inspect(tenant_id)} must match [A-Za-z0-9_-]{1,64}"}
    end
  end

  @doc """
  Remove `tenant_id`'s runtime override, so the config's limit applies again.
  Resets the bucket too, for the same reason a `put` does.
  """
  @spec delete_override(atom(), term()) ::
          :ok | {:error, :not_found} | {:error, :store_unavailable}
  def delete_override(instance, tenant_id) do
    GenServer.call(Ankusa.via(instance, :rate_limiter), {:delete, tenant_id})
  end

  @impl true
  def handle_call({:put, tenant_id, limit}, _from, state) do
    table = overrides_table(state.instance)

    case persist_put(state.instance, tenant_id, limit) do
      :ok ->
        :ets.insert(table, {tenant_id, limit})
        :ets.delete(buckets_table(state.instance), tenant_id)
        {:reply, {:ok, limit}, state}

      {:error, reason} ->
        Logger.error("[ankusa] could not persist rate limits: #{inspect(reason)}")
        {:reply, {:error, :store_unavailable}, state}
    end
  end

  def handle_call({:delete, tenant_id}, _from, state) do
    table = overrides_table(state.instance)

    case :ets.lookup(table, tenant_id) do
      [] ->
        {:reply, {:error, :not_found}, state}

      [{^tenant_id, _limit}] ->
        ops = [{:delete, :default, Keys.rate_limit(tenant_id)}]

        case Store.write(state.instance, ops, sync: true) do
          :ok ->
            :ets.delete(table, tenant_id)
            :ets.delete(buckets_table(state.instance), tenant_id)
            {:reply, :ok, state}

          {:error, reason} ->
            Logger.error("[ankusa] could not persist rate limits: #{inspect(reason)}")
            {:reply, {:error, :store_unavailable}, state}
        end
    end
  end

  # ── persistence ─────────────────────────────────────────────────────────────

  # One key per override (`r:<tenant>`), synced: a `PUT` answers "stored".
  defp persist_put(instance, tenant_id, %{rate: rate, burst: burst}) do
    value = JSON.encode!(%{"rate" => rate, "burst" => burst})
    Store.write(instance, [{:put, :default, Keys.rate_limit(tenant_id), value}], sync: true)
  end

  # A store this node cannot read must not boot with silently empty overrides:
  # a tenant's raised or lowered limit would quietly revert to the config's.
  defp load_persisted(instance, table) do
    %{lo: lo, hi: hi} = Keys.family(:rate_limits)

    result =
      Store.fold(instance, :rate_limits, {lo, hi}, :ok, fn key, value, :ok ->
        load_entry(Keys.decode_rate_limit(key), value, table)
        {:cont, :ok}
      end)

    case result do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp load_entry(tenant, value, table) do
    result =
      if Ankusa.ClaimCheck.Ref.valid_tenant?(tenant) do
        case JSON.decode(value) do
          {:ok, attrs} -> parse_limit(attrs)
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

  defp overrides_table(instance), do: :"ankusa_rate_limits_#{instance}"

  defp buckets_table(instance), do: :"ankusa_rate_buckets_#{instance}"
end
