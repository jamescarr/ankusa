defmodule Ankusa.ClaimCheck.Reader do
  @moduledoc """
  The one process every store read of a `:claim_check` node passes through,
  so the gateway stays fast while an object store throttles or hangs:

    * **Coalescing** — concurrent reads of one key (a pack's index, a claim's
      bytes, one range of them) share one store request, and every caller
      gets its answer.
    * **An adaptive limit** — per store, at most `limit` store requests run
      at once. It starts at `claim_check.read_max_inflight`, halves when the
      store throttles (`429`, `503`) or cannot answer — at most once per
      100 ms, so a wave of requests failing together halves it once — and
      grows by one after `limit` successes, back up to that ceiling (AIMD).
      A missing key is a success; a `403` changes nothing.
    * **A bounded queue** — a read that finds no free slot waits for one, up
      to `claim_check.read_queue_max` reads per store; past that it is shed at
      once with `{:unavailable, :overloaded}`.
    * **A deadline** — every caller passes one (`claim_check.read_deadline_ms`
      after its request started) and gets `{:unavailable, :deadline}` once it
      passes; a queued read whose deadline passed is dropped without a store
      request. A store request that outlives its callers still runs to the
      end and fills `Ankusa.ClaimCheck.Cache`, so the client's retry finds it.
    * **A node-wide request gate** — `admit/2` and `release/1` bound how many
      gateway requests the node serves at once (`claim_check.max_concurrent_reads`),
      cache hits included, so a flood of cheap reads can't starve the node.
      The gateway answers `503` + `Retry-After` past it, before it locates
      anything.

  The limit is per store, not per key prefix. S3 throttles per prefix, and
  `claim_check.key_layout: :sharded` is what spreads a tenant's packs over
  more of them; this bounds what one node sends a store as a whole. Every
  answer is charged to the store a fetch was addressed to: an index fetch
  searches every store (`Ankusa.ClaimCheck.Stores.read_range/6`) but is
  charged to the pack's home (`Ankusa.ClaimCheck.Stores.home/2`).

  Only a `:claim_check` node runs it, beside the gateway. A read anywhere
  else — a dispatch node calling `Ankusa.ClaimCheck.redeem/3` — goes straight
  to the store, bounded only by the store's read timeout.

  Emits `[:ankusa, :claim_check, :read]` once per caller (see `fetch/5`),
  `[:ankusa, :claim_check, :read_limit]` on every change of a store's limit,
  and `[:ankusa, :claim_check, :read_state]` every second per store it has
  read from (see `Ankusa.Telemetry`).
  """

  use GenServer

  alias Ankusa.ClaimCheck.Stores
  alias Ankusa.Telemetry

  @typedoc "What a fetch reads: coalescing is by key."
  @type key ::
          {:index, tenant_id :: String.t(), pack_id :: String.t()}
          | {:bytes, String.t(), String.t(), index :: non_neg_integer()}
          | {:range, String.t(), String.t(), non_neg_integer(), first :: non_neg_integer(),
             length :: pos_integer()}

  @type stats :: %{
          non_neg_integer() => %{
            limit: pos_integer(),
            inflight: non_neg_integer(),
            queued: non_neg_integer()
          }
        }

  # A store's failures within this window of its last halving don't halve it
  # again: they are the same wave.
  @backoff_window_ms 100
  @tick_ms 1_000

  # ── API ─────────────────────────────────────────────────────────────────────

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, instance, name: Ankusa.via(instance, :claim_reader))
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc """
  Run `fun` — one read of `key` from claim store `store` — under the store's
  limit, or join a fetch of `key` already running. `deadline` is an absolute
  `System.monotonic_time(:millisecond)`: past it the caller gets
  `{:unavailable, :deadline}`, whatever the fetch is doing; the fetch it
  started or joined still runs to the end and fills the cache.

  Every caller is counted once on `[:ankusa, :claim_check, :read]`: the
  reader counts the caller whose fetch went to the store (`:store`, when the
  fetch finishes, whether or not that caller is still waiting); every other
  caller counts itself, as `:coalesced` when it got the fetch's answer and
  `:shed` when it got none (the queue was full, its deadline passed, or the
  reader is down).
  """
  @spec fetch(atom(), key(), non_neg_integer(), (-> result), integer()) ::
          result | {:error, {:unavailable, :overloaded | :deadline | {:exit, term()}}}
        when result: {:ok, term()} | {:error, term()}
  def fetch(instance, key, store, fun, deadline) when is_function(fun, 0) do
    started = System.monotonic_time()
    remaining = deadline - System.monotonic_time(:millisecond)

    {result, source} =
      if remaining <= 0 do
        {{:error, {:unavailable, :deadline}}, :shed}
      else
        try do
          case GenServer.call(
                 Ankusa.via(instance, :claim_reader),
                 {:fetch, key, store, fun, deadline},
                 remaining
               ) do
            {:fetched, result} -> {result, nil}
            {:joined, result} -> {result, :coalesced}
            {:shed, result} -> {result, :shed}
          end
        catch
          # The reply, if it comes later, is dropped: the call's alias is gone.
          :exit, {:timeout, _} -> {{:error, {:unavailable, :deadline}}, :shed}
          :exit, reason -> {{:error, {:unavailable, {:exit, reason}}}, :shed}
        end
      end

    if source do
      emit_read(instance, elem(key, 0), source, result, store, System.monotonic_time() - started)
    end

    result
  end

  @doc """
  Count the calling process as one of the gateway's requests in flight, unless
  `max` already are (`:shed`; the caller answers `503` and does not `release/1`).
  `max` is `claim_check.max_concurrent_reads`; `0` admits everything, and so
  does a node whose reader is not running, which has no gate to count at.

  The gate is a set of request processes, not a counter, so a request process
  that is killed before it releases is dropped by the reader's next tick
  instead of leaking a slot for good.
  """
  @spec admit(atom(), non_neg_integer()) :: :ok | :shed
  def admit(_instance, 0), do: :ok

  def admit(instance, max) when is_integer(max) and max > 0 do
    table = gate(instance)
    true = :ets.insert(table, {self()})

    if :ets.info(table, :size) > max do
      :ets.delete(table, self())
      :shed
    else
      :ok
    end
  rescue
    ArgumentError -> :ok
  end

  @doc "Give back the slot `admit/2` counted for the calling process."
  @spec release(atom()) :: :ok
  def release(instance) do
    :ets.delete(gate(instance), self())
    :ok
  rescue
    ArgumentError -> :ok
  end

  defp gate(instance), do: :"ankusa_claim_gate_#{instance}"

  # A request process killed mid-request never released: forget the dead ones.
  defp sweep_gate(instance) do
    table = gate(instance)

    for {pid} <- :ets.tab2list(table), not Process.alive?(pid) do
      :ets.delete(table, pid)
    end

    :ok
  end

  @doc "Each store's limit, store requests running, and reads queued."
  @spec stats(atom()) :: stats()
  def stats(instance), do: GenServer.call(Ankusa.via(instance, :claim_reader), :stats)

  @doc false
  # One `[:ankusa, :claim_check, :read]` event: `source` is `:cache`,
  # `:direct` (no reader running), `:store`, `:coalesced` or `:shed`.
  @spec emit_read(atom(), atom(), atom(), term(), non_neg_integer(), integer()) :: :ok
  def emit_read(instance, kind, source, result, store, duration) do
    Telemetry.emit(
      [:claim_check, :read],
      %{duration: duration},
      %{instance: instance, kind: kind, source: source, result: result_tag(result), store: store}
    )
  end

  defp result_tag({:ok, _value}), do: :ok
  defp result_tag({:error, :not_found}), do: :not_found
  defp result_tag({:error, :forbidden}), do: :forbidden
  defp result_tag({:error, {:unavailable, :overloaded}}), do: :overloaded
  defp result_tag({:error, {:unavailable, :deadline}}), do: :deadline

  defp result_tag({:error, reason}),
    do: if(Stores.throttled?(reason), do: :throttled, else: :unavailable)

  # ── GenServer ───────────────────────────────────────────────────────────────

  @impl true
  def init(instance) do
    config = Ankusa.config(instance)

    # Linked on purpose: its tasks must not outlive the reader.
    {:ok, task_sup} = Task.Supervisor.start_link()
    :ets.new(gate(instance), [:named_table, :public, :set, write_concurrency: true])
    Process.send_after(self(), :tick, @tick_ms)

    {:ok,
     %{
       instance: instance,
       task_sup: task_sup,
       max_inflight: config.claim_check.read_max_inflight,
       queue_max: config.claim_check.read_queue_max,
       # store index => its limit, store requests running, and queue
       stores: %{},
       # task ref => {key, store}
       running: %{},
       # key => [{from, started}] waiting on its running fetch, newest first
       waiting: %{}
     }}
  end

  # A crash report prints the state: hundreds of queued closures and their
  # callers are noise, and a closure's captures are not for a log. Report
  # sizes.
  @impl true
  def format_status(%{state: %{stores: stores} = state} = status) do
    stores = Map.new(stores, fn {index, s} -> {index, %{s | queue: :queue.len(s.queue)}} end)

    %{
      status
      | state: %{
          state
          | stores: stores,
            running: map_size(state.running),
            waiting: map_size(state.waiting)
        }
    }
  end

  def format_status(status), do: status

  @impl true
  def handle_call({:fetch, key, store, fun, deadline}, from, state) do
    now = System.monotonic_time()
    s = store(state, store)

    cond do
      Map.has_key?(state.waiting, key) ->
        {:noreply, join(state, key, from, now)}

      s.inflight < s.limit ->
        {:noreply, start(state, store, key, fun, [{from, now}])}

      :queue.len(s.queue) < state.queue_max ->
        queue = :queue.in({key, fun, from, deadline, now}, s.queue)
        {:noreply, put_store(state, store, %{s | queue: queue})}

      true ->
        {:reply, {:shed, {:error, {:unavailable, :overloaded}}}, put_store(state, store, s)}
    end
  end

  def handle_call(:stats, _from, state) do
    stats =
      Map.new(state.stores, fn {index, s} ->
        {index, %{limit: s.limit, inflight: s.inflight, queued: :queue.len(s.queue)}}
      end)

    {:reply, stats, state}
  end

  @impl true
  def handle_info({ref, result}, %{running: running} = state) when is_map_key(running, ref) do
    Process.demonitor(ref, [:flush])
    {:noreply, finish(state, ref, result)}
  end

  # The fetch raised, exited or was killed.
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{running: running} = state)
      when is_map_key(running, ref) do
    {:noreply, finish(state, ref, {:error, {:unavailable, {:exit, reason}}})}
  end

  def handle_info(:tick, state) do
    for {index, s} <- state.stores do
      Telemetry.emit(
        [:claim_check, :read_state],
        %{limit: s.limit, inflight: s.inflight, queued: :queue.len(s.queue)},
        %{instance: state.instance, store: index}
      )
    end

    sweep_gate(state.instance)
    Process.send_after(self(), :tick, @tick_ms)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # ── fetches ─────────────────────────────────────────────────────────────────

  defp start(state, store, key, fun, waiters) do
    task = Task.Supervisor.async_nolink(state.task_sup, fun)
    s = store(state, store)

    %{
      state
      | running: Map.put(state.running, task.ref, {key, store}),
        waiting: Map.put(state.waiting, key, waiters)
    }
    |> put_store(store, %{s | inflight: s.inflight + 1})
  end

  defp join(state, key, from, started),
    do: %{state | waiting: Map.update!(state.waiting, key, &[{from, started} | &1])}

  # Every waiter gets the answer. The first one started the fetch: it is the
  # store read, counted here (its caller may be gone). The others count
  # themselves as coalesced, if they are still there to.
  defp finish(state, ref, result) do
    {{key, store}, running} = Map.pop!(state.running, ref)
    {waiters, waiting} = Map.pop!(state.waiting, key)
    now = System.monotonic_time()

    s = store(state, store)
    s = verdict(state, store, %{s | inflight: s.inflight - 1}, result)

    [{head, head_started} | joined] = Enum.reverse(waiters)
    emit_read(state.instance, elem(key, 0), :store, result, store, now - head_started)
    GenServer.reply(head, {:fetched, result})
    for {from, _started} <- joined, do: GenServer.reply(from, {:joined, result})

    %{state | running: running, waiting: waiting}
    |> put_store(store, s)
    |> drain(store)
  end

  # Start queued reads while the store has free slots: a read whose deadline
  # passed is answered without a store request, and one whose key started
  # meanwhile joins that fetch.
  defp drain(state, store) do
    s = store(state, store)

    with true <- s.inflight < s.limit,
         {{:value, {key, fun, from, deadline, started}}, queue} <- :queue.out(s.queue) do
      state = put_store(state, store, %{s | queue: queue})

      cond do
        System.monotonic_time(:millisecond) >= deadline ->
          GenServer.reply(from, {:shed, {:error, {:unavailable, :deadline}}})
          drain(state, store)

        Map.has_key?(state.waiting, key) ->
          state |> join(key, from, started) |> drain(store)

        true ->
          state |> start(store, key, fun, [{from, started}]) |> drain(store)
      end
    else
      _full_or_empty -> state
    end
  end

  # ── the limit ───────────────────────────────────────────────────────────────

  defp verdict(state, store, s, result) do
    case classify(result) do
      :success -> grow(state, store, s)
      :backoff -> back_off(state, store, s)
      :neutral -> s
    end
  end

  defp classify({:ok, _value}), do: :success
  defp classify({:error, :not_found}), do: :success
  defp classify({:error, {:unavailable, _reason}}), do: :backoff
  defp classify(_other), do: :neutral

  defp grow(state, store, %{successes: successes, limit: limit} = s)
       when successes + 1 >= limit do
    new = min(limit + 1, state.max_inflight)
    if new != limit, do: limit_event(state, store, new)
    %{s | limit: new, successes: 0}
  end

  defp grow(_state, _store, s), do: %{s | successes: s.successes + 1}

  defp back_off(state, store, %{halved_at: halved_at, limit: limit} = s) do
    now = System.monotonic_time(:millisecond)

    if halved_at == nil or now - halved_at >= @backoff_window_ms do
      new = max(div(limit, 2), 1)
      if new != limit, do: limit_event(state, store, new)
      %{s | limit: new, successes: 0, halved_at: now}
    else
      s
    end
  end

  defp limit_event(state, store, limit) do
    Telemetry.emit(
      [:claim_check, :read_limit],
      %{limit: limit},
      %{instance: state.instance, store: store}
    )
  end

  # A store's entry is made on its first read.
  defp store(state, store) do
    case state.stores do
      %{^store => s} ->
        s

      _ ->
        %{
          limit: state.max_inflight,
          inflight: 0,
          successes: 0,
          halved_at: nil,
          queue: :queue.new()
        }
    end
  end

  defp put_store(state, store, s), do: %{state | stores: Map.put(state.stores, store, s)}
end
