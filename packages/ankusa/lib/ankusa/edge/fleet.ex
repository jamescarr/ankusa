defmodule Ankusa.Edge.Fleet do
  @moduledoc """
  Fleet-scoped admission: the tenant quota (`rate_limits.scope: :fleet`) and the
  provider event claim (`dedupe.scope: :fleet`), counted in `fleet.store` so N
  edge nodes enforce one limit and one dedupe window instead of N.

  `admit/3` is the request path's one entry: with both scopes `:node` (the
  default) it is the node's own `Ankusa.Edge.RateLimiter.hit/3` and nothing
  else. With a fleet scope it makes **one** store call for the hook — the
  tenant's charges and the dedupe claim together
  (`c:Ankusa.RateLimiter.Store.hit/4`) — ahead of the queue writer's local dedupe
  check, which still runs.

  ## A limiter outage never refuses a hook

  Every fleet call has a hard budget, `fleet.timeout_ms`, so a *slow* store costs
  at most that per hook, not Redis's own timeout multiplied by the request rate.
  A call that fails or times out is answered from this node alone (its own
  buckets, its writer's dedupe index) and counted
  (`[:ankusa, :fleet, :fallback]`). A breaker watches the call outcomes: when
  `fleet.breaker_failure_rate` of at least `fleet.breaker_min_calls` calls in a
  `fleet.breaker_window_ms` window failed, it opens and every hook is answered
  node-local with no store call at all, for `fleet.breaker_open_ms`; then one
  probe call decides whether it closes. The breaker and its window are atomics:
  the request path takes no lock and sends no message.

  `ankusa_fleet_limiter_degraded{kind}` is `1` while a kind (`rate_limit`,
  `dedupe`) is answered node-local — breaker open, or the last call failed — and
  a warning is logged at most once a minute while it is.

  ## Settling claims

  A claimed event key is *pending* until the hook is durable
  (`Ankusa.Dedupe.Store`). `settle/3` queues the outcome in an ETS table; a
  process drains it in pipelined batches every few milliseconds, so promoting or
  releasing a claim is never on the `2xx`'s critical path and costs one round
  trip per batch, not per hook. The queue holds at most #{50_000} operations; one
  more is dropped and counted (`[:ankusa, :fleet, :settle_dropped]`): a dropped
  promote is a pending key that expires (a possible duplicate later), a dropped
  release is a key that stays pending until `fleet.pending_ttl_ms` (the sender is
  told to retry meanwhile). Neither loses a hook.
  """

  use GenServer

  require Logger

  alias Ankusa.Edge.RateLimiter
  alias Ankusa.{Envelope, Source}

  @type admitted ::
          {:ok, Ankusa.RateLimiter.Store.claim() | nil}
          | {:duplicate, String.t()}
          | {:error, {:rate_limited, pos_integer()} | {:dedupe_pending, pos_integer()}}

  @settle_max 50_000
  @flush_ms 5
  @flush_batch 500
  @tick_ms 1_000
  @log_every_ms 60_000

  # Breaker slots in the `:atomics` array.
  @state 1
  @opened_at 2
  @window_at 3
  @calls 4
  @fails 5
  @last_failed 6

  # ── the request path ────────────────────────────────────────────────────────

  @doc """
  Charge the tenant's quota and claim the hook's event key, in whatever scope the
  config says. `{:ok, claim}` admits the hook (`claim` is `nil` when nothing was
  claimed: pass it to `settle/3` either way); `{:duplicate, hook_id}` is a hook
  another node already stored; `{:error, {:rate_limited, ms}}` and
  `{:error, {:dedupe_pending, ms}}` are refusals the sender retries.
  """
  @spec admit(atom(), Source.t(), Envelope.t()) :: admitted()
  def admit(instance, %Source{} = source, %Envelope{} = env) do
    config = Ankusa.config(instance)
    fleet_rate? = config.rate_limits.scope == :fleet
    claim = claim(config, source, env)

    if fleet_rate? or claim != nil do
      fleet(instance, config, env, fleet_rate?, claim)
    else
      local(instance, env)
    end
  end

  defp local(instance, env) do
    case RateLimiter.hit(instance, env.tenant_id, env.size) do
      :ok -> {:ok, nil}
      {:error, {:rate_limited, _ms}} = limited -> limited
    end
  end

  defp claim(config, source, env) do
    if config.dedupe.scope == :fleet and source.dedupe != nil and is_binary(env.dedupe_key) do
      %{
        tenant: env.tenant_id || "default",
        source_id: env.source_id,
        key: env.dedupe_key,
        hook_id: env.id,
        node: node(),
        ttl_ms: source.dedupe.ttl_ms,
        pending_ttl_ms: config.fleet.pending_ttl_ms
      }
    end
  end

  # The node-scoped half of the limiter is charged first when the quota is not
  # fleet-scoped: a refusal then never leaves a claim behind.
  defp fleet(instance, config, env, fleet_rate?, claim) do
    with :ok <- node_quota(instance, env, fleet_rate?) do
      charges =
        if fleet_rate?, do: RateLimiter.fleet_charges(instance, env.tenant_id, env.size), else: []

      if charges == [] and claim == nil do
        # A fleet quota and this tenant has no limit: nothing to count anywhere.
        {:ok, nil}
      else
        case call(instance, config, charges, claim) do
          :ok -> {:ok, claim}
          {:duplicate, hook_id} -> {:duplicate, hook_id}
          {:pending, ms} -> {:error, {:dedupe_pending, ms}}
          {:rate_limited, ms} -> {:error, {:rate_limited, ms}}
          {:fallback, reason} -> fallback(instance, env, fleet_rate?, claim, reason)
        end
      end
    end
  end

  defp node_quota(_instance, _env, true), do: :ok

  defp node_quota(instance, env, false) do
    case RateLimiter.hit(instance, env.tenant_id, env.size) do
      :ok -> :ok
      {:error, {:rate_limited, _ms}} = limited -> limited
    end
  end

  # The store could not answer in time: this node's own limiter (when the quota
  # was fleet-scoped; the node-scoped one was already charged) and its writer's
  # dedupe index stand in, and no claim exists to settle.
  defp fallback(instance, env, fleet_rate?, claim, reason) do
    for kind <- kinds(fleet_rate?, claim) do
      Ankusa.Telemetry.emit([:fleet, :fallback], %{}, %{
        instance: instance,
        kind: kind,
        reason: reason
      })
    end

    if fleet_rate?, do: local(instance, env), else: {:ok, nil}
  end

  defp kinds(fleet_rate?, claim) do
    if(fleet_rate?, do: [:rate_limit], else: []) ++ if(claim, do: [:dedupe], else: [])
  end

  # One store call under the breaker and the hard budget. `{:fallback, reason}`
  # is "answer from this node".
  defp call(instance, config, charges, claim) do
    {store, _opts} = config.fleet.store

    case gate(instance, config.fleet) do
      {:closed, ref} ->
        outcome = guarded(store, instance, charges, claim, config.fleet.timeout_ms)
        record(instance, ref, config.fleet, outcome)
        reply(outcome)

      {:probe, ref} ->
        outcome = guarded(store, instance, charges, claim, config.fleet.timeout_ms)
        probed(instance, ref, outcome)
        reply(outcome)

      :open ->
        {:fallback, :breaker_open}
    end
  end

  defp reply({:error, _reason}), do: {:fallback, :error}
  defp reply(result), do: result

  defp guarded(store, instance, charges, claim, timeout_ms) do
    store.hit(instance, charges, claim, timeout_ms)
  catch
    _kind, reason -> {:error, {:crashed, reason}}
  end

  # ── the breaker ─────────────────────────────────────────────────────────────

  defp gate(instance, fleet) do
    case :ets.lookup(settings_table(instance), :ref) do
      [{:ref, ref}] ->
        case :atomics.get(ref, @state) do
          0 -> {:closed, ref}
          from -> probe_if_due(instance, ref, fleet, from)
        end

      [] ->
        :open
    end
  rescue
    # The fleet process is not running (restarting, or never started).
    ArgumentError -> :open
  end

  # Open for `breaker_open_ms`, then exactly one caller wins the swap and probes;
  # a probe that never reports (its process died) is retried after another
  # `breaker_open_ms`.
  defp probe_if_due(_instance, ref, fleet, from) do
    now = now_ms()

    if now - :atomics.get(ref, @opened_at) >= fleet.breaker_open_ms and
         :atomics.compare_exchange(ref, @state, from, 2) == :ok do
      :atomics.put(ref, @opened_at, now)
      {:probe, ref}
    else
      :open
    end
  end

  defp record(instance, ref, fleet, outcome) do
    now = now_ms()
    window_at = :atomics.get(ref, @window_at)

    if now - window_at >= fleet.breaker_window_ms and
         :atomics.compare_exchange(ref, @window_at, window_at, now) == :ok do
      :atomics.put(ref, @calls, 0)
      :atomics.put(ref, @fails, 0)
    end

    calls = :atomics.add_get(ref, @calls, 1)

    if match?({:error, _}, outcome) do
      :atomics.put(ref, @last_failed, 1)
      fails = :atomics.add_get(ref, @fails, 1)

      if calls >= fleet.breaker_min_calls and fails / calls >= fleet.breaker_failure_rate and
           :atomics.compare_exchange(ref, @state, 0, 1) == :ok do
        :atomics.put(ref, @opened_at, now)
        breaker(instance, :open)
      end
    else
      :atomics.put(ref, @last_failed, 0)
    end
  end

  defp probed(instance, ref, {:error, _reason}) do
    :atomics.put(ref, @opened_at, now_ms())
    :atomics.put(ref, @state, 1)
    breaker(instance, :open)
  end

  defp probed(instance, ref, _ok) do
    :atomics.put(ref, @calls, 0)
    :atomics.put(ref, @fails, 0)
    :atomics.put(ref, @window_at, now_ms())
    :atomics.put(ref, @last_failed, 0)
    :atomics.put(ref, @state, 0)
    breaker(instance, :closed)
  end

  defp breaker(instance, state),
    do: Ankusa.Telemetry.emit([:fleet, :breaker], %{}, %{instance: instance, state: state})

  @doc "The breaker's state: `:closed`, `:open`, or `:probing` (one call is testing the store)."
  @spec state(atom()) :: :closed | :open | :probing | :down
  def state(instance) do
    case :ets.lookup(settings_table(instance), :ref) do
      [{:ref, ref}] ->
        case :atomics.get(ref, @state) do
          0 -> :closed
          1 -> :open
          2 -> :probing
        end

      [] ->
        :down
    end
  rescue
    ArgumentError -> :down
  end

  @doc "Whether hooks are being answered from this node alone right now."
  @spec degraded?(atom()) :: boolean()
  def degraded?(instance) do
    case :ets.lookup(settings_table(instance), :ref) do
      [{:ref, ref}] -> :atomics.get(ref, @state) != 0 or :atomics.get(ref, @last_failed) == 1
      [] -> true
    end
  rescue
    ArgumentError -> true
  end

  # ── settling ────────────────────────────────────────────────────────────────

  @doc """
  Queue the outcome of the commit that followed `admit/3`'s claim: the hook is
  durable (`{:ok, env}`, or `{:duplicate, env}` — this node's own index knew it)
  and the key is promoted to committed under that hook's id; anything else stored
  nothing, and the claim is released. `nil` (nothing was claimed) is a no-op.
  """
  @spec settle(atom(), Ankusa.RateLimiter.Store.claim() | nil, tuple()) :: :ok
  def settle(_instance, nil, _result), do: :ok

  def settle(instance, claim, {tag, %Envelope{id: id}}) when tag in [:ok, :duplicate],
    do: enqueue(instance, {:promote, claim, id})

  def settle(instance, claim, _result), do: enqueue(instance, {:release, claim})

  defp enqueue(instance, op) do
    table = queue_table(instance)

    if :ets.info(table, :size) >= @settle_max do
      dropped(instance, :queue_full, 1)
    else
      :ets.insert(table, {System.unique_integer([:monotonic]), op})
    end

    :ok
  rescue
    # The fleet process is not running: no table, nothing to settle through.
    ArgumentError -> dropped(instance, :queue_full, 1)
  end

  defp dropped(instance, reason, count) do
    Ankusa.Telemetry.emit([:fleet, :settle_dropped], %{count: count}, %{
      instance: instance,
      reason: reason
    })

    :ok
  end

  @doc "Forget `tenant`'s fleet buckets (its limit changed). Best effort."
  @spec reset_tenant(atom(), String.t()) :: :ok
  def reset_tenant(instance, tenant) do
    config = Ankusa.config(instance)

    if config.rate_limits.scope == :fleet do
      {store, _opts} = config.fleet.store
      _ = store.reset(instance, tenant, config.fleet.timeout_ms * 5)
    end

    :ok
  catch
    _kind, _reason -> :ok
  end

  # ── process ─────────────────────────────────────────────────────────────────

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :fleet))
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)

    ref = :atomics.new(6, signed: true)
    :atomics.put(ref, @window_at, now_ms())

    settings =
      :ets.new(settings_table(instance), [:named_table, :protected, :set, read_concurrency: true])

    :ets.insert(settings, {:ref, ref})

    :ets.new(queue_table(instance), [:named_table, :public, :set, write_concurrency: true])

    Process.send_after(self(), :flush, @flush_ms)
    Process.send_after(self(), :tick, @tick_ms)

    {:ok, %{instance: instance, ref: ref, last_log_at: nil}}
  end

  @impl true
  def handle_info(:flush, state) do
    flush(state)
    Process.send_after(self(), :flush, @flush_ms)
    {:noreply, state}
  end

  def handle_info(:tick, state) do
    Process.send_after(self(), :tick, @tick_ms)
    {:noreply, sample(state)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # One pipelined call for up to `@flush_batch` queued outcomes, only while the
  # breaker is closed: an open breaker means the store is not answering, and its
  # outcomes wait (bounded by `@settle_max`) for it to.
  defp flush(%{instance: instance, ref: ref}) do
    table = queue_table(instance)

    with true <- :atomics.get(ref, @state) == 0,
         {rows, _continuation} <-
           :ets.select(table, [{{:"$1", :"$2"}, [], [{{:"$1", :"$2"}}]}], @flush_batch) do
      %{fleet: %{store: {store, _opts}, timeout_ms: timeout_ms}} = Ankusa.config(instance)
      ops = Enum.map(rows, &elem(&1, 1))

      result =
        try do
          store.settle(instance, ops, timeout_ms * 5)
        catch
          _kind, reason -> {:error, {:crashed, reason}}
        end

      Enum.each(rows, fn {key, _op} -> :ets.delete(table, key) end)
      if result != :ok, do: dropped(instance, :error, length(ops))
    end

    :ok
  end

  # The gauge says what is true now; the log says it once a minute.
  defp sample(%{instance: instance} = state) do
    %{rate_limits: %{scope: rate}, dedupe: %{scope: dedupe}} = Ankusa.config(instance)
    degraded? = degraded?(instance)

    for {kind, :fleet} <- [rate_limit: rate, dedupe: dedupe] do
      Ankusa.Telemetry.emit([:fleet, :degraded], %{degraded: if(degraded?, do: 1, else: 0)}, %{
        instance: instance,
        kind: kind
      })
    end

    now = now_ms()

    if degraded? and (state.last_log_at == nil or now - state.last_log_at >= @log_every_ms) do
      Logger.warning(
        "[ankusa] fleet store is not answering within its budget (breaker #{state(instance)}): " <>
          "limiting and deduping per node until it does"
      )

      %{state | last_log_at: now}
    else
      state
    end
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  # The instance name is operator config: one atom per instance, never request input.
  # sobelow_skip ["DOS.BinToAtom"]
  defp settings_table(instance), do: :"ankusa_fleet_settings_#{instance}"

  # The instance name is operator config: one atom per instance, never request input.
  # sobelow_skip ["DOS.BinToAtom"]
  defp queue_table(instance), do: :"ankusa_fleet_settle_#{instance}"
end
