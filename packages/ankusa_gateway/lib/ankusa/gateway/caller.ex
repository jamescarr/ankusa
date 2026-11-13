defmodule Ankusa.Gateway.Caller do
  @moduledoc """
  One process per remote backend (a policy decision point, an identity
  provider, an enricher, the ACL store): every call to it passes through here,
  so a slow or failing backend costs the gateway a bounded number of
  processes and a bounded wait, never the request path's whole capacity. The
  structure is `Ankusa.ClaimCheck.Reader`'s.

    * **Coalescing** — a `key` has at most one run pending or in flight;
      `call/6` for such a key joins it. `refresh/4` (refresh-ahead: a cached
      decision about to go stale) is dropped for such a key, and while any
      work is queued or the breaker is open, so two results for one key never
      race and a slow run never overwrites a newer one.
    * **A limit** — at most `limit` runs in flight. It starts at
      `max_inflight`, halves (to no less than 1) on an unavailable result at
      most once per 100 ms, and grows by one after `limit` successes in a row,
      up to `max_inflight`.
    * **A queue** — past the limit, at most `queue_max` distinct keys wait, at
      most `partition_queue_max` of them per `partition` (a tenant, a space);
      past either the caller gets `{:error, {:unavailable, :overloaded}}` at
      once. Partitions are served round-robin, so one busy tenant does not
      delay another. A queued key whose callers' deadlines have all passed is
      dropped without running.
    * **A deadline** — `call/6` returns `{:error, {:unavailable, :deadline}}`
      at `deadline` whatever the run is doing; the run still finishes (and
      whatever it caches still lands), so the next caller finds it.
    * **Batching** (`batch: %{max, window_ms}`) — pending keys accumulate
      until `max` of them or until `window_ms` after the first, then one run
      takes them all: `run.(items)` returns a list aligned with `items`. A list
      of another length makes every item `{:error, {:unavailable, :bad_batch}}`.
    * **A breaker** (`breaker: %{failures, open_ms}`, `failures > 0`) — that
      many unavailable results in a row open it for `open_ms`: every call is
      `{:error, {:unavailable, :breaker_open}}` meanwhile. Then one probe run
      is let through; a result that is not unavailable closes it.

  A run that raises or exits is `{:error, {:unavailable, {:exit, reason}}}`
  for its waiters.

  ## Telemetry

    * `[:ankusa, :gateway, :caller_state]` — `%{limit, inflight, queued}`,
      `%{instance, caller}`, every second;
    * `[:ankusa, :gateway, :call]` — `%{duration, batch}`, `%{instance,
      caller, result: :ok | :unavailable}`, one per run;
    * `[:ankusa, :gateway, :caller_breaker]` — `%{}`, `%{instance, caller,
      state: :open | :closed}`, one per transition.
  """

  use GenServer

  alias Ankusa.Telemetry

  @tick_ms 1_000
  @backoff_window_ms 100

  @type result :: {:ok, term()} | {:error, term()} | term()

  # ── API ─────────────────────────────────────────────────────────────────────

  @doc """
  Start a caller. Options: `:instance`, `:name` (any term, unique per
  instance), `:label` (the telemetry tag), `:max_inflight`, `:queue_max`,
  `:partition_queue_max`, `:batch` (`nil` or `%{max, window_ms}`), `:breaker`
  (`%{failures, open_ms}`), `:run` (arity 1; a list in and out when `:batch`
  is set).
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, {:gateway_caller, name}))
  end

  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :instance), Keyword.fetch!(opts, :name)},
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @doc """
  The result of running `item` under `key`: a run already pending or in flight
  for `key` is joined, else one is started or queued. `deadline` is a
  `System.monotonic_time(:millisecond)` value.
  """
  @spec call(atom(), term(), term(), term(), term(), integer()) :: result()
  def call(instance, name, key, partition, item, deadline) do
    timeout = deadline - System.monotonic_time(:millisecond)

    with true <- timeout > 0,
         pid when is_pid(pid) <- Ankusa.whereis(instance, {:gateway_caller, name}) do
      GenServer.call(pid, {:call, key, partition, item, deadline}, timeout)
    else
      false -> {:error, {:unavailable, :deadline}}
      nil -> {:error, {:unavailable, :not_running}}
    end
  catch
    :exit, {:timeout, _} -> {:error, {:unavailable, :deadline}}
    :exit, reason -> {:error, {:unavailable, {:exit, reason}}}
  end

  @doc """
  Run `item` under `key` in the background, unless a run for `key` is pending
  or in flight, any work is queued, or the breaker is open. Never blocks.
  """
  @spec refresh(atom(), term(), term(), term()) :: :ok
  def refresh(instance, name, key, item) do
    case Ankusa.whereis(instance, {:gateway_caller, name}) do
      nil -> :ok
      pid -> GenServer.cast(pid, {:refresh, key, item})
    end
  end

  # ── server ──────────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    # Linked on purpose: its runs must not outlive the caller.
    {:ok, task_sup} = Task.Supervisor.start_link()
    Process.send_after(self(), :tick, @tick_ms)
    max_inflight = Keyword.fetch!(opts, :max_inflight)

    {:ok,
     %{
       instance: Keyword.fetch!(opts, :instance),
       label: Keyword.get(opts, :label, inspect(Keyword.fetch!(opts, :name))),
       run: Keyword.fetch!(opts, :run),
       batch: Keyword.get(opts, :batch),
       breaker: Keyword.get(opts, :breaker, %{failures: 0, open_ms: 1_000}),
       max_inflight: max_inflight,
       queue_max: Keyword.fetch!(opts, :queue_max),
       partition_queue_max:
         Keyword.get(opts, :partition_queue_max, Keyword.fetch!(opts, :queue_max)),
       task_sup: task_sup,
       limit: max_inflight,
       successes: 0,
       halved_at: nil,
       inflight: 0,
       # task ref => {[key], started (native)}
       running: %{},
       # key => %{waiters: [{from, deadline}], item, partition, status: :queued | :running}
       keys: %{},
       # partition => :queue of keys; `rr` is the partitions with a queue, in turn order
       queues: %{},
       rr: :queue.new(),
       queued: 0,
       # the window of the oldest pending batch, a timer ref or nil
       window: nil,
       # :closed | {:open, until_ms} | :probing
       breaker_state: :closed,
       failures: 0
     }}
  end

  # A crash report prints the state: closures and their callers are noise, and
  # a closure's captures are not for a log. Report sizes.
  @impl true
  def format_status(%{state: %{keys: keys} = state} = status) do
    %{
      status
      | state: %{
          state
          | run: :redacted,
            keys: map_size(keys),
            running: map_size(state.running),
            queues: map_size(state.queues)
        }
    }
  end

  def format_status(status), do: status

  @impl true
  def handle_call({:call, key, partition, item, deadline}, from, state) do
    state = check_breaker(state)

    cond do
      Map.has_key?(state.keys, key) ->
        {:noreply, join(state, key, from, deadline)}

      match?({:open, _}, state.breaker_state) ->
        {:reply, {:error, {:unavailable, :breaker_open}}, state}

      state.breaker_state == :probing and state.inflight > 0 ->
        {:reply, {:error, {:unavailable, :breaker_open}}, state}

      state.batch == nil and state.inflight < state.limit ->
        {:noreply,
         start(state, [{key, item}], [
           {key, %{waiters: [{from, deadline}], item: item, partition: partition}}
         ])}

      queue_full?(state, partition) ->
        {:reply, {:error, {:unavailable, :overloaded}}, state}

      true ->
        {:noreply, state |> enqueue(key, partition, item, from, deadline) |> dispatch()}
    end
  end

  @impl true
  def handle_cast({:refresh, key, item}, state) do
    state = check_breaker(state)

    if Map.has_key?(state.keys, key) or state.queued > 0 or state.breaker_state != :closed or
         state.inflight >= state.limit do
      {:noreply, state}
    else
      {:noreply, start(state, [{key, item}], [{key, %{waiters: [], item: item, partition: nil}}])}
    end
  end

  @impl true
  def handle_info({ref, result}, %{running: running} = state) when is_map_key(running, ref) do
    Process.demonitor(ref, [:flush])
    {:noreply, finish(state, ref, result)}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{running: running} = state)
      when is_map_key(running, ref) do
    {:noreply, finish(state, ref, {:exit, reason})}
  end

  def handle_info(:window, state), do: {:noreply, dispatch(%{state | window: :expired})}

  def handle_info(:tick, state) do
    Telemetry.emit(
      [:gateway, :caller_state],
      %{limit: state.limit, inflight: state.inflight, queued: state.queued},
      %{instance: state.instance, caller: state.label}
    )

    Process.send_after(self(), :tick, @tick_ms)
    {:noreply, state |> check_breaker() |> dispatch()}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # ── runs ────────────────────────────────────────────────────────────────────

  # `entries` are `{key, entry}` for keys not yet in `state.keys` (a fresh
  # call or a refresh); queued keys already are.
  defp start(state, items, entries) do
    keys = Enum.map(items, &elem(&1, 0))
    run = state.run

    fun =
      case state.batch do
        nil -> fn -> run.(items |> hd() |> elem(1)) end
        _batch -> fn -> run.(Enum.map(items, &elem(&1, 1))) end
      end

    task = Task.Supervisor.async_nolink(state.task_sup, fun)

    key_map =
      Enum.reduce(entries, state.keys, fn {key, entry}, acc ->
        Map.put(acc, key, Map.put(entry, :status, :running))
      end)

    key_map =
      Enum.reduce(keys, key_map, &Map.update!(&2, &1, fn e -> %{e | status: :running} end))

    breaker_state = if state.breaker_state == :half_open, do: :probing, else: state.breaker_state

    %{
      state
      | running: Map.put(state.running, task.ref, {keys, System.monotonic_time()}),
        keys: key_map,
        inflight: state.inflight + 1,
        breaker_state: breaker_state
    }
  end

  defp join(state, key, from, deadline) do
    %{
      state
      | keys: Map.update!(state.keys, key, &%{&1 | waiters: [{from, deadline} | &1.waiters]})
    }
  end

  defp finish(state, ref, raw) do
    {{keys, started}, running} = Map.pop!(state.running, ref)
    results = results(raw, keys, state.batch)
    duration = System.monotonic_time() - started

    {key_map, unavailable} =
      keys
      |> Enum.zip(results)
      |> Enum.reduce({state.keys, 0}, fn {key, result}, {key_map, n} ->
        {entry, key_map} = Map.pop(key_map, key)
        if entry, do: for({from, _deadline} <- entry.waiters, do: GenServer.reply(from, result))
        {key_map, n + if(unavailable?(result), do: 1, else: 0)}
      end)

    Telemetry.emit(
      [:gateway, :call],
      %{duration: duration, batch: length(keys)},
      %{
        instance: state.instance,
        caller: state.label,
        result: if(unavailable > 0, do: :unavailable, else: :ok)
      }
    )

    %{state | running: running, keys: key_map, inflight: state.inflight - 1}
    |> account(length(keys) - unavailable, unavailable)
    |> dispatch()
  end

  defp results({:exit, reason}, keys, _batch),
    do: List.duplicate({:error, {:unavailable, {:exit, reason}}}, length(keys))

  defp results(result, [_key], nil), do: [result]

  defp results(list, keys, _batch) when is_list(list) and length(list) == length(keys), do: list

  defp results(_bad, keys, _batch),
    do: List.duplicate({:error, {:unavailable, :bad_batch}}, length(keys))

  defp unavailable?({:error, {:unavailable, _}}), do: true
  defp unavailable?(_result), do: false

  # ── the limit and the breaker ───────────────────────────────────────────────

  defp account(state, ok, 0), do: state |> grow(ok) |> breaker_success()
  defp account(state, _ok, unavailable), do: state |> back_off() |> breaker_failure(unavailable)

  defp grow(state, 0), do: state

  defp grow(%{successes: successes, limit: limit} = state, ok) when successes + ok >= limit,
    do: %{state | limit: min(limit + 1, state.max_inflight), successes: 0}

  defp grow(state, ok), do: %{state | successes: state.successes + ok}

  defp back_off(%{halved_at: halved_at, limit: limit} = state) do
    now = System.monotonic_time(:millisecond)

    if halved_at == nil or now - halved_at >= @backoff_window_ms,
      do: %{state | limit: max(div(limit, 2), 1), successes: 0, halved_at: now},
      else: state
  end

  defp breaker_success(%{breaker_state: :probing} = state) do
    breaker_event(state, :closed)
    %{state | breaker_state: :closed, failures: 0}
  end

  defp breaker_success(state), do: %{state | failures: 0}

  defp breaker_failure(%{breaker: %{failures: 0}} = state, _n), do: state

  defp breaker_failure(%{breaker_state: :probing} = state, _n), do: open(state)

  defp breaker_failure(%{breaker: %{failures: limit}} = state, n) do
    failures = state.failures + n
    state = %{state | failures: failures}
    if failures >= limit and state.breaker_state == :closed, do: open(state), else: state
  end

  defp open(state) do
    breaker_event(state, :open)
    until = System.monotonic_time(:millisecond) + state.breaker.open_ms
    state = %{state | breaker_state: {:open, until}, failures: 0}

    # Nothing queued will run before the breaker lets one probe through.
    Enum.reduce(state.queues, state, fn {_partition, queue}, acc ->
      Enum.reduce(:queue.to_list(queue), acc, &fail_queued(&2, &1, :breaker_open))
    end)
    |> Map.merge(%{queues: %{}, rr: :queue.new(), queued: 0})
  end

  defp check_breaker(%{breaker_state: {:open, until}} = state) do
    if System.monotonic_time(:millisecond) >= until,
      do: %{state | breaker_state: :half_open},
      else: state
  end

  defp check_breaker(state), do: state

  defp breaker_event(state, transition) do
    Telemetry.emit(
      [:gateway, :caller_breaker],
      %{},
      %{instance: state.instance, caller: state.label, state: transition}
    )
  end

  # ── the queue ───────────────────────────────────────────────────────────────

  defp queue_full?(state, partition) do
    in_partition =
      case state.queues do
        %{^partition => queue} -> :queue.len(queue)
        _ -> 0
      end

    state.queued >= state.queue_max or in_partition >= state.partition_queue_max
  end

  defp enqueue(state, key, partition, item, from, deadline) do
    {queues, rr} =
      case state.queues do
        %{^partition => queue} ->
          {Map.put(state.queues, partition, :queue.in(key, queue)), state.rr}

        _ ->
          {Map.put(state.queues, partition, :queue.from_list([key])),
           :queue.in(partition, state.rr)}
      end

    entry = %{waiters: [{from, deadline}], item: item, partition: partition, status: :queued}

    %{
      state
      | queues: queues,
        rr: rr,
        queued: state.queued + 1,
        keys: Map.put(state.keys, key, entry)
    }
    |> arm_window()
  end

  defp arm_window(%{batch: %{window_ms: ms}, window: nil} = state) when ms > 0,
    do: %{state | window: Process.send_after(self(), :window, ms)}

  defp arm_window(%{batch: %{window_ms: 0}, window: nil} = state), do: %{state | window: :expired}
  defp arm_window(state), do: state

  # Start queued work while there are free slots (and the breaker allows).
  defp dispatch(%{queued: 0} = state), do: cancel_window(state)

  defp dispatch(state) do
    state = check_breaker(state)

    cond do
      state.inflight >= state.limit ->
        state

      match?({:open, _}, state.breaker_state) ->
        state

      state.breaker_state == :probing ->
        state

      state.batch == nil ->
        state |> take(1) |> start_taken() |> dispatch()

      batch_ready?(state) ->
        state |> take(state.batch.max) |> start_taken() |> cancel_window() |> dispatch()

      true ->
        state
    end
  end

  defp batch_ready?(state), do: state.queued >= state.batch.max or state.window == :expired

  defp cancel_window(%{window: ref} = state) when is_reference(ref) do
    Process.cancel_timer(ref)
    %{state | window: nil}
  end

  defp cancel_window(state), do: %{state | window: nil}

  # Up to `n` live keys, round-robin over the partitions. A key whose callers'
  # deadlines have all passed is dropped on the way.
  defp take(state, n), do: take(state, n, [])

  defp take(state, 0, acc), do: {state, Enum.reverse(acc)}

  defp take(state, n, acc) do
    case :queue.out(state.rr) do
      {:empty, _} ->
        {state, Enum.reverse(acc)}

      {{:value, partition}, rr} ->
        {{:value, key}, queue} = :queue.out(Map.fetch!(state.queues, partition))

        {queues, rr} =
          if :queue.is_empty(queue),
            do: {Map.delete(state.queues, partition), rr},
            else: {Map.put(state.queues, partition, queue), :queue.in(partition, rr)}

        state = %{state | queues: queues, rr: rr, queued: state.queued - 1}

        if expired?(Map.fetch!(state.keys, key)),
          do: take(%{state | keys: Map.delete(state.keys, key)}, n, acc),
          else: take(state, n - 1, [key | acc])
    end
  end

  defp expired?(%{waiters: waiters}) do
    now = System.monotonic_time(:millisecond)
    waiters != [] and Enum.all?(waiters, fn {_from, deadline} -> deadline <= now end)
  end

  defp start_taken({state, []}), do: state

  defp start_taken({state, keys}) do
    items = Enum.map(keys, &{&1, Map.fetch!(state.keys, &1).item})
    start(state, items, [])
  end

  defp fail_queued(state, key, reason) do
    case Map.pop(state.keys, key) do
      {nil, _} ->
        state

      {entry, keys} ->
        for {from, _deadline} <- entry.waiters,
            do: GenServer.reply(from, {:error, {:unavailable, reason}})

        %{state | keys: keys}
    end
  end
end
