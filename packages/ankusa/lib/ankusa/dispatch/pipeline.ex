defmodule Ankusa.Dispatch.Pipeline do
  @moduledoc """
  Async dispatch pipeline: a scheduler over *delivery rows*. Every committed
  hook has one row per sink of its source (`Ankusa.Queue`); this process claims
  the rows that are due, runs each delivery in a `Task.Supervisor` task, and
  records the outcome as a single store batch. At-least-once: a row is only
  deleted after its sink answered `:ok`, and a restart makes every claimed row
  due again.

  ## What wakes it

  A commit sends it `:wake`, so a new hook is claimed at once, not at the next
  poll. Otherwise it sleeps until the earliest due row: a retry is a row due at
  `now + backoff`, so waiting costs no slot and no process — a sink that is
  down for an hour holds up nothing but its own rows.

  ## Concurrency

  Up to `dispatch.concurrency` deliveries run at a time. A window
  (`dispatch.max_inflight` rows, `dispatch.max_inflight_bytes` of stored hook)
  bounds claimed-but-unfinished work, so a dead sink cannot walk the pipeline
  into an OOM. Deliveries are not ordered: two hooks for one sink may run in
  either order, and a retry runs after whatever is due before it.
  An attempt that has not returned after `dispatch.attempt_timeout_ms` (default
  30 s) is killed and counts as a failed attempt, `{:attempt_timeout, ms}`, so a
  hung sink frees its slot; the fallback claim check-in runs inside the same
  deadline.

  ## Per-sink isolation

  Claimed jobs wait in one queue per sink key `{source_id, sink index, sink
  module}`, and slots go round-robin across the keys that have work, so one
  source's backlog cannot queue ahead of every other source's. With
  `dispatch.sink_concurrency` set, one key never runs more than that many
  attempts at once.

  Each key has a circuit breaker. `dispatch.breaker_failures` consecutive
  failures (`0` disables breakers; a `{:permanent, _}` error does not count, see
  `Ankusa.Sink`) open it: the key's queued rows and every row of it claimed
  while it is open are *parked* — written back due when the breaker closes,
  with their attempt count unchanged — for `dispatch.breaker_open_ms`, doubling
  per consecutive open up to `dispatch.breaker_max_open_ms`. After that one
  attempt runs as a probe (rows claimed meanwhile are parked for
  `breaker_open_ms`): success closes the breaker, failure opens it again for
  longer. Breakers live in this process's memory; a restart closes them all.
  Parked rows spend no attempts, so while a breaker stays open a row's retry
  horizon is not bounded by wall-clock time.

  ## Binding

  A row binds to `(sink index, module)` at ack time. Its opts always come from
  the source as it is *now*, so a config fix applies to the backlog and no fun
  or secret is ever persisted. If the source's sinks were reordered the row
  falls back to the one sink with its module; if it cannot be bound it is
  dead-lettered as `{:sink_gone, index, module}`, and a deleted source as
  `{:source_gone, source_id}`.

  ## Claim check

  A body larger than a sink's `c:Ankusa.Sink.inline_max_bytes/1` is checked in
  **once** per hook, before any of its sinks run: the claims of a scan are
  packed per tenant and uploaded together (`Ankusa.ClaimCheck.check_in_batch/2`)
  in a task, and the ref is stored with the hook so every sink and every retry
  reuses it. If a pack fails, its jobs still run: the first attempt checks the
  body in on its own.

  `start_link/1` opts: `:instance` and `:config`.
  """

  use GenServer

  require Logger

  alias Ankusa.{ClaimCheck, Sink, SourceStore, Store, Telemetry}
  alias Ankusa.Queue.{Deliveries, Reclaim}
  alias Ankusa.Sink.Message
  alias Ankusa.Store.Keys

  @housekeeping_ms 1_000
  # The longest pause a sink's `{:retry_after, ms, _}` can impose.
  @max_retry_after_ms 3_600_000
  # Housekeeping ticks between `Reclaim.sweep/1` runs.
  @sweep_every 60
  # Outcome writes are buffered and written as one batch: one store write and one
  # reclaim probe per `@flush_entries` outcomes (or per `@flush_ms`), not one
  # each. A crash inside the window redelivers those hooks, which at-least-once
  # allows and the next synced commit would have flushed anyway.
  @flush_ms 10
  @flush_entries 256
  # The due scan starts at `floor`, so it does not walk the tombstones of every
  # row claimed before it (that made draining a backlog quadratic). The floor
  # follows the claims, trailing the last claimed due time by `@floor_lag_ms`
  # (clock jitter), and is kept under every row that can still become visible
  # beneath it: a commit's wake carries the batch stamp, a retry or a replay
  # lowers it to its own due time. A reset every `@floor_reset_ms` is the
  # backstop for anything unforeseen.
  @floor_lag_ms 1_000
  @floor_reset_ms 300_000
  @holdoff_ms 1_000
  @max_sleep_ms 30_000

  # ── public API ────────────────────────────────────────────────────────────

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :dispatch))
  end

  def child_spec(opts) do
    instance = Keyword.fetch!(opts, :instance)

    %{
      id: {__MODULE__, instance},
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @doc """
  Block until nothing is claimed, running, or due; returns the rows settled
  (delivered or dead-lettered) during the call. A row waiting out a backoff is
  not due, so this does not wait for it.
  """
  @spec tick(atom()) :: {:ok, non_neg_integer()}
  def tick(instance) do
    GenServer.call(Ankusa.via(instance, :dispatch), :tick, 30_000)
  end

  @doc """
  The spare capacity a replay job may use right now: the lag of the oldest due
  row (`lag_ms`, 0 when nothing is due) and whether the in-flight window is
  full. `{:error, :unavailable}` while the pipeline is down or the store
  cannot answer.
  """
  @spec pressure(atom()) ::
          {:ok, %{lag_ms: non_neg_integer(), window_full: boolean()}} | {:error, :unavailable}
  def pressure(instance) do
    GenServer.call(Ankusa.via(instance, :dispatch), :pressure, 1_000)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @doc """
  Check the `config.dispatch` keys this module reads; raises `ArgumentError`
  naming the key.
  """
  @spec validate_config!(Ankusa.Config.t()) :: :ok
  def validate_config!(%Ankusa.Config{dispatch: dispatch}) do
    ms = dispatch.attempt_timeout_ms

    unless is_integer(ms) and ms >= 1 do
      raise ArgumentError,
            "dispatch.attempt_timeout_ms must be a positive integer, got #{inspect(ms)}"
    end

    cap = Map.get(dispatch, :sink_concurrency)
    concurrency = dispatch.concurrency

    unless cap == nil or (is_integer(cap) and is_integer(concurrency) and cap in 1..concurrency) do
      raise ArgumentError,
            "dispatch.sink_concurrency must be nil or an integer from 1 to " <>
              "dispatch.concurrency (#{inspect(concurrency)}), got #{inspect(cap)}"
    end

    failures = Map.get(dispatch, :breaker_failures, 0)

    unless is_integer(failures) and failures >= 0 do
      raise ArgumentError,
            "dispatch.breaker_failures must be a non-negative integer, got #{inspect(failures)}"
    end

    open = Map.get(dispatch, :breaker_open_ms, 1)

    unless is_integer(open) and open >= 1 do
      raise ArgumentError,
            "dispatch.breaker_open_ms must be a positive integer, got #{inspect(open)}"
    end

    max_open = Map.get(dispatch, :breaker_max_open_ms, open)

    unless is_integer(max_open) and max_open >= open do
      raise ArgumentError,
            "dispatch.breaker_max_open_ms must be an integer >= dispatch.breaker_open_ms " <>
              "(#{inspect(open)}), got #{inspect(max_open)}"
    end

    :ok
  end

  # ── GenServer ─────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    config = Keyword.fetch!(opts, :config)

    # Linked to us on purpose: nobody else knows about it, and it must not
    # outlive the pipeline. Tests that start the Pipeline alone still get it.
    {:ok, task_sup} = Task.Supervisor.start_link()

    Process.flag(:trap_exit, true)

    send(self(), :recover)
    Process.send_after(self(), :housekeeping, @housekeeping_ms)

    {:ok,
     %{
       instance: instance,
       config: config,
       task_sup: task_sup,
       recovered?: false,
       # see @floor_lag_ms
       floor: 0,
       floor_reset_at: mono_ms(),
       # rows claimed and not yet finished, and their stored hook bytes
       claimed: 0,
       claimed_bytes: 0,
       window_full?: false,
       # a failed scan backs off instead of spinning
       holdoff?: false,
       # sink key => queue of claimed jobs ready to run; `ring` holds every key
       # whose queue is non-empty, once, in round-robin order
       queues: %{},
       ring: :queue.new(),
       queued: 0,
       running: %{},
       running_by_key: %{},
       # sink key => breaker; a key without an entry is closed with no failures
       breakers: %{},
       # pack task ref => the jobs it will give claims to
       packing: %{},
       packing_seqs: MapSet.new(),
       # seq => jobs of a hook whose claim is still being packed
       waiting: %{},
       # outcome batches the store refused, in order: [{ops, reclaim_pairs}]
       unrecorded: [],
       # outcomes not yet written, newest first: [{ops, reclaim_pairs}]
       buffer: [],
       buffered: 0,
       flush_timer: nil,
       timer: nil,
       settled: 0,
       waiters: [],
       sweep_ticks: 0,
       # replay job id => %{delivered: n, dead: n} since the last housekeeping
       # report to `Ankusa.Dispatch.Replayer`
       replay_outcomes: %{}
     }}
  end

  # A crash report prints the state. `config` carries every sink's options
  # (credentials), and the jobs in `queues`/`running`/`waiting`/`packing`
  # carry the same options plus the hook's body: report sizes, not contents.
  @impl true
  def format_status(%{state: %{config: _} = state} = status) do
    %{
      status
      | state: %{
          state
          | config: :redacted,
            queues: state.queued,
            ring: :queue.len(state.ring),
            running: map_size(state.running),
            breakers: breakers_open(state),
            waiting: map_size(state.waiting),
            packing: map_size(state.packing)
        }
    }
  end

  def format_status(status), do: status

  @impl true
  def handle_call(:tick, from, state) do
    state = state |> fill() |> start_jobs()
    maybe_reply_waiters(%{state | waiters: state.waiters ++ [{from, state.settled}]})
  end

  def handle_call(:pressure, _from, state) do
    reply =
      case Deliveries.next_due_at(state.instance, state.floor) do
        {:ok, nil} -> {:ok, %{lag_ms: 0, window_full: at_capacity?(state)}}
        {:ok, at} -> {:ok, %{lag_ms: max(0, now_ms() - at), window_full: at_capacity?(state)}}
        {:error, _reason} -> {:error, :unavailable}
      end

    {:reply, reply, state}
  end

  # Stopped by its supervisor: write what is buffered, so a graceful stop does
  # not redeliver hooks that were already delivered. The store may already be
  # gone; then they are redelivered, which is the at-least-once contract.
  @impl true
  def terminate(_reason, state) do
    state |> flush() |> flush_unrecorded()
    :ok
  end

  @impl true
  def handle_info(:recover, state) do
    with {:ok, claimed} <- Deliveries.recover_inflight(state.instance, now_ms()),
         :ok <- Reclaim.sweep(state.instance) do
      if claimed > 0 do
        Logger.info(
          "[ankusa] dispatch made #{claimed} claimed delivery row(s) due again after a restart"
        )
      end

      send(self(), :wake)
      {:noreply, %{state | recovered?: true}}
    else
      {:error, reason} ->
        Logger.error(
          "[ankusa] dispatch could not recover from the store, retrying: #{inspect(reason)}"
        )

        Process.send_after(self(), :recover, @holdoff_ms)
        {:noreply, state}
    end
  end

  def handle_info(:wake, state) do
    state = %{state | holdoff?: false} |> fill() |> start_jobs() |> schedule_next()
    maybe_reply_waiters(state)
  end

  # A commit's rows became visible only now, though they were stamped before it
  # (the fsync in between): a scan may already have gone past that stamp.
  def handle_info({:wake, at}, state), do: handle_info(:wake, lower_floor(state, at))

  # A flush makes retries and expansions visible, and the capacity the outcomes
  # freed is claimed here, once, rather than once per outcome.
  def handle_info(:flush, state) do
    state = %{state | flush_timer: nil} |> flush() |> refill() |> start_jobs() |> schedule_next()
    maybe_reply_waiters(state)
  end

  def handle_info(:housekeeping, state) do
    Process.send_after(self(), :housekeeping, @housekeeping_ms)

    # `schedule_next` is here because a flush cancels the flush timer, and with
    # it the refill that would have followed: whatever the outcomes freed is
    # claimed by the wake this arms. It also re-arms a lost wake, once a second.
    state =
      state
      |> flush()
      |> flush_unrecorded()
      |> report_replay_outcomes()
      |> report_state()
      |> schedule_next()

    ticks = state.sweep_ticks + 1

    state =
      if ticks >= @sweep_every do
        sweep(state)
        %{state | sweep_ticks: 0}
      else
        %{state | sweep_ticks: ticks}
      end

    maybe_reply_waiters(state)
  end

  def handle_info({ref, {:packed, results}}, %{packing: packing} = state)
      when is_map_key(packing, ref) do
    Process.demonitor(ref, [:flush])

    state
    |> packed(ref, results)
    |> start_jobs()
    |> maybe_reply_waiters()
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{packing: packing} = state)
      when is_map_key(packing, ref) do
    # The pack task died without a result: its jobs fall back to checking
    # their bodies in one at a time.
    state
    |> packed(ref, %{})
    |> start_jobs()
    |> maybe_reply_waiters()
  end

  def handle_info({ref, {result, fresh_claim}}, %{running: running} = state)
      when is_map_key(running, ref) do
    Process.demonitor(ref, [:flush])
    outcome(state, ref, result, fresh_claim)
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{running: running} = state)
      when is_map_key(running, ref) do
    # No result ever arrived: the task was killed. That is a failed attempt like
    # any other, and the row's own retry policy decides what happens next.
    outcome(state, ref, {:error, {:exit, reason}}, nil)
  end

  # The attempt outlasted `dispatch.attempt_timeout_ms`: kill it and count a
  # failed attempt. A reply that landed in the mailbox meanwhile still counts.
  def handle_info({:attempt_timeout, ref}, %{running: running} = state)
      when is_map_key(running, ref) do
    %{job: job, task: task} = Map.fetch!(running, ref)
    timeout = state.config.dispatch.attempt_timeout_ms

    case Task.shutdown(task, :brutal_kill) do
      {:ok, {result, fresh_claim}} ->
        outcome(state, ref, result, fresh_claim)

      {:exit, reason} ->
        outcome(state, ref, {:error, {:exit, reason}}, nil)

      nil ->
        {mod, _opts} = job.spec

        Logger.warning(
          "[ankusa] sink #{inspect(mod)} did not finish hook #{job.env.id} within " <>
            "#{timeout}ms; attempt #{job.row.attempts + 1} failed (the sink may still " <>
            "complete it; consumers dedupe on the idempotency key)"
        )

        outcome(state, ref, {:error, {:attempt_timeout, timeout}}, nil)
    end
  end

  def handle_info({:EXIT, _pid, reason}, state), do: {:stop, reason, state}

  # Stray messages (e.g. a DOWN from a task already demonitored) must never take
  # down a process other parts of the tree are waiting on.
  def handle_info(_message, state), do: {:noreply, state}

  # Replay jobs learn how many of their deliveries settled, so the Replayer can
  # count and auto-pause. Sent on housekeeping, so at most once a second; a
  # missing Replayer (no :dispatch role tree) just drops them.
  defp report_replay_outcomes(%{replay_outcomes: outcomes} = state)
       when map_size(outcomes) == 0,
       do: state

  defp report_replay_outcomes(state) do
    case Ankusa.whereis(state.instance, :replayer) do
      pid when is_pid(pid) -> send(pid, {:replay_outcomes, state.replay_outcomes})
      nil -> :ok
    end

    %{state | replay_outcomes: %{}}
  end

  # ── claiming ──────────────────────────────────────────────────────────────

  # Nothing is claimed before the restart recovery has made the rows a previous
  # life left claimed due again.
  defp fill(%{recovered?: false} = state), do: state
  defp fill(%{holdoff?: true} = state), do: state

  defp fill(state) do
    if at_capacity?(state) do
      %{state | window_full?: true}
    else
      scan(maybe_reset_floor(state))
    end
  end

  defp at_capacity?(state) do
    dispatch = state.config.dispatch
    state.claimed >= dispatch.max_inflight or state.claimed_bytes >= dispatch.max_inflight_bytes
  end

  defp scan(state) do
    dispatch = state.config.dispatch
    now = now_ms()
    limit = min(dispatch.batch, dispatch.max_inflight - state.claimed)
    budget = dispatch.max_inflight_bytes - state.claimed_bytes

    case Deliveries.due(state.instance, now, state.floor, limit, budget) do
      {:ok, []} ->
        %{state | window_full?: false}

      {:ok, dues} ->
        case claim(state, dues, now) do
          {:ok, state} ->
            # Past the rows just claimed, and only once they are: a failed load or
            # claim leaves them due, and the floor has to stay beneath them.
            state = raise_floor(state, List.last(dues).at)

            # A full page means there is probably more; a short one means
            # nothing else is due right now.
            if length(dues) == limit,
              do: fill(state),
              else: %{state | window_full?: at_capacity?(state)}

          {:error, state} ->
            holdoff(state)
        end

      {:error, reason} ->
        Logger.error("[ankusa] dispatch could not scan the store, retrying: #{inspect(reason)}")
        holdoff(state)
    end
  end

  # Load before claiming: a failed read leaves every row due, with nothing to
  # undo. This process is the only one that moves pending rows, so nothing can
  # change them in between.
  defp claim(state, dues, now) do
    with {:ok, loaded} <- Deliveries.load(state.instance, dues),
         :ok <- Deliveries.claim(state.instance, dues) do
      {:ok, resolve(state, loaded, now)}
    else
      {:error, reason} ->
        Logger.error(
          "[ankusa] dispatch could not claim delivery rows, retrying: #{inspect(reason)}"
        )

        {:error, state}
    end
  end

  defp holdoff(state) do
    state = cancel_timer(state)
    timer = Process.send_after(self(), :wake, @holdoff_ms)
    %{state | holdoff?: true, timer: timer}
  end

  defp maybe_reset_floor(state) do
    if mono_ms() - state.floor_reset_at >= @floor_reset_ms do
      %{state | floor: 0, floor_reset_at: mono_ms()}
    else
      state
    end
  end

  # ── resolving claimed rows ────────────────────────────────────────────────

  # Every claimed row ends up in one of three places: a job to run, a
  # non-delivery outcome written right here (dead-lettered, expanded, dropped),
  # or both ends of a missing row cleaned up.
  defp resolve(state, loaded, now) do
    acc = {state, [], [], [], %{}}

    {state, ops, pairs, jobs, _sources} =
      Enum.reduce(loaded, acc, fn item, acc -> resolve_one(item, acc, now) end)

    state
    |> record(ops, pairs)
    |> stage(Enum.reverse(jobs))
  end

  defp resolve_one(%{row: nil} = item, {state, ops, pairs, jobs, sources}, _now) do
    Logger.warning("[ankusa] delivery row #{item.seq}/#{item.sink} vanished; dropping its claim")
    {state, [{:delete, :index, Keys.inflight(item.seq, item.sink)} | ops], pairs, jobs, sources}
  end

  defp resolve_one(%{env: nil} = item, {state, ops, pairs, jobs, sources}, _now) do
    Logger.error("[ankusa] hook #{item.seq} is missing; dropping its delivery row #{item.sink}")

    drop = [
      {:delete, :deliveries, Keys.delivery(item.seq, item.sink)},
      {:delete, :index, Keys.inflight(item.seq, item.sink)}
    ]

    {state, drop ++ ops, pairs, jobs, sources}
  end

  defp resolve_one(item, {state, ops, pairs, jobs, sources}, now) do
    {source, sources} = fetch_source(state.instance, item.env.source_id, sources)

    cond do
      source == :error ->
        dead(item, {:source_gone, item.env.source_id}, {state, ops, pairs, jobs, sources}, now)

      # The source store could not answer: neither a delivery nor a verdict.
      # The row waits a moment and keeps its attempt count.
      source == :unavailable ->
        later =
          Deliveries.retry_ops(
            item.seq,
            item.sink,
            item.row,
            now + @holdoff_ms,
            "source store unavailable"
          )

        {state, later ++ ops, pairs, jobs, sources}

      Deliveries.unresolved?(item.sink) ->
        expand(item, source, {state, ops, pairs, jobs, sources}, now)

      true ->
        key = {item.env.source_id, item.sink, item.row.module}

        case admit(state, key) do
          {:park, at, state} ->
            {state, park_ops(item, at) ++ ops, pairs, jobs, sources}

          {:pass, state} ->
            case bind(source.sinks, item.sink, item.row.module) do
              {:ok, spec} ->
                job = %{
                  key: key,
                  seq: item.seq,
                  sink: item.sink,
                  size: item.size,
                  row: item.row,
                  env: item.env,
                  spec: spec,
                  claim: item.claim,
                  forward_headers: source.forward_headers
                }

                {state, ops, pairs, [job | jobs], sources}

              :error ->
                reason = {:sink_gone, item.sink, item.row.module}
                dead(item, reason, {state, ops, pairs, jobs, sources}, now)
            end
        end
    end
  end

  # An imported row never recorded its sink: give the hook one row per current
  # sink of its source, and wake up to claim them.
  defp expand(item, source, {state, ops, pairs, jobs, sources}, now) do
    case Deliveries.existing_sinks(state.instance, item.seq) do
      {:ok, existing} ->
        send(self(), :wake)

        expansion =
          Deliveries.expand_ops(item.seq, item.sink, item.size, source.sinks, existing, now)

        pairs =
          if source.sinks == [], do: [cleared(item.seq, item.sink) | pairs], else: pairs

        {state, expansion ++ ops, pairs, jobs, sources}

      {:error, reason} ->
        Logger.warning("[ankusa] could not expand imported row #{item.seq}: #{inspect(reason)}")
        retry_later = Deliveries.retry_ops(item.seq, item.sink, item.row, now + @holdoff_ms, nil)
        {state, retry_later ++ ops, pairs, jobs, sources}
    end
  end

  defp dead(item, reason, {state, ops, pairs, jobs, sources}, now) do
    error = inspect(reason, limit: 50, printable_limit: 4096)
    dead_ops = Deliveries.dead_ops(item.seq, item.sink, item.row, now, error, item.env)

    state =
      state
      |> settle_dead(item.env, item.row.module, item.row.attempts)
      |> bump_replay(item.row, :dead)

    {state, dead_ops ++ ops, pairs, jobs, sources}
  end

  defp settle_dead(state, env, module, attempts) do
    Telemetry.emit([:dispatch, :stop], %{}, %{
      instance: state.instance,
      result: :dlq,
      attempts: attempts,
      sink: module,
      source_id: env.source_id
    })

    Telemetry.emit([:dispatch, :dlq], %{}, %{
      instance: state.instance,
      source_id: env.source_id,
      sink: module
    })

    %{state | settled: state.settled + 1}
  end

  # Sources are looked up once per scan (`sources` is the scan's cache). A store
  # that cannot answer is cached as `:unavailable` for the rest of the scan,
  # and said once.
  defp fetch_source(instance, source_id, sources) do
    case sources do
      %{^source_id => source} ->
        {source, sources}

      _ ->
        source =
          case SourceStore.fetch(instance, source_id) do
            {:ok, source} ->
              source

            :error ->
              :error

            {:error, :unavailable} ->
              Logger.warning(
                "[ankusa] source store unavailable for #{inspect(source_id)}; " <>
                  "its deliveries wait #{@holdoff_ms} ms"
              )

              :unavailable
          end

        {source, Map.put(sources, source_id, source)}
    end
  end

  # The sink at the row's index, if it is still the same module; else the only
  # sink with that module; else the row cannot be bound.
  defp bind(sinks, index, module) do
    case Enum.at(sinks, index) do
      {^module, _opts} = sink ->
        {:ok, sink}

      _ ->
        case Enum.filter(sinks, fn {mod, _opts} -> mod == module end) do
          [sink] -> {:ok, sink}
          _ -> :error
        end
    end
  end

  defp cleared(seq, sink), do: {seq, Keys.cleared(seq, 0, sink)}

  # ── claim check staging ───────────────────────────────────────────────────

  defp stage(state, []), do: state

  defp stage(state, jobs) do
    bytes = Enum.reduce(jobs, 0, &(&1.size + &2))

    state = %{
      state
      | claimed: state.claimed + length(jobs),
        claimed_bytes: state.claimed_bytes + bytes
    }

    {needing, ready} = Enum.split_with(jobs, &needs_claim?/1)
    state = Enum.reduce(ready, state, &enqueue(&2, &1))

    # A hook whose claim is already being packed waits for that pack.
    {waiting, to_pack} = Enum.split_with(needing, &MapSet.member?(state.packing_seqs, &1.seq))

    state =
      Enum.reduce(waiting, state, fn job, state ->
        %{state | waiting: Map.update(state.waiting, job.seq, [job], &[job | &1])}
      end)

    pack(state, to_pack)
  end

  # A body over the sink's inline limit needs a claim, unless the hook already
  # has a stored one. Runs in the pipeline process: a sink whose threshold
  # callback fails needs no claim here — the job runs, and its task reports the
  # failure (`ensure_claim/2`).
  defp needs_claim?(%{claim: claim}) when claim != nil, do: false

  defp needs_claim?(%{spec: {mod, opts}, env: env}) do
    case Sink.inline_max_bytes(mod, opts) do
      {:ok, nil} -> false
      {:ok, max} -> env.size > max
      {:error, _reason} -> false
    end
  end

  defp pack(state, []), do: state

  defp pack(state, jobs) do
    by_seq = Enum.group_by(jobs, & &1.seq)
    items = Enum.map(by_seq, fn {_seq, [job | _]} -> Message.claim_item(job.env) end)
    instance = state.instance

    task =
      Task.Supervisor.async_nolink(state.task_sup, fn ->
        {:packed, ClaimCheck.check_in_batch(instance, items)}
      end)

    %{
      state
      | packing: Map.put(state.packing, task.ref, jobs),
        packing_seqs: MapSet.union(state.packing_seqs, MapSet.new(Map.keys(by_seq)))
    }
  end

  # Persist each hook's ref, give it to the pack's jobs and to the jobs that
  # waited on it (a failed pack leaves a job without one, so it checks its body
  # in itself), and let them all run.
  defp packed(state, ref, results) do
    {jobs, packing} = Map.pop(state.packing, ref)
    seqs = jobs |> Enum.map(& &1.seq) |> Enum.uniq()

    claims =
      jobs
      |> Enum.uniq_by(& &1.seq)
      |> Enum.flat_map(fn job ->
        case Map.get(results, job.env.id) do
          {:ok, claim} -> [{job.seq, claim}]
          _ -> []
        end
      end)
      |> Map.new()

    claim_ops = Enum.flat_map(claims, fn {seq, claim} -> Deliveries.claim_ops(seq, claim) end)
    state = record(state, claim_ops, [])

    {waiting, released} =
      Enum.reduce(seqs, {state.waiting, []}, fn seq, {waiting, released} ->
        {jobs, waiting} = Map.pop(waiting, seq, [])
        {waiting, jobs ++ released}
      end)

    state = %{
      state
      | packing: packing,
        packing_seqs: MapSet.difference(state.packing_seqs, MapSet.new(seqs)),
        waiting: waiting
    }

    Enum.reduce(jobs ++ released, state, fn job, state ->
      job =
        case Map.fetch(claims, job.seq) do
          {:ok, claim} -> %{job | claim: claim}
          :error -> job
        end

      enqueue(state, job)
    end)
  end

  defp enqueue(state, job) do
    case admit(state, job.key) do
      {:park, at, state} -> park(state, job, at)
      {:pass, state} -> push(state, job)
    end
  end

  defp push(state, %{key: key} = job) do
    queue = Map.get(state.queues, key, :queue.new())
    ring = if :queue.is_empty(queue), do: :queue.in(key, state.ring), else: state.ring

    %{
      state
      | queues: Map.put(state.queues, key, :queue.in(job, queue)),
        ring: ring,
        queued: state.queued + 1
    }
  end

  # ── breakers ──────────────────────────────────────────────────────────────

  # Whether a key may run now, or its rows are parked until `at` (wall clock).
  # An open breaker whose period is over turns half-open here: the next job of
  # the key to start is its probe.
  defp admit(state, key) do
    case Map.get(state.breakers, key) do
      %{state: :open, until: until} = breaker ->
        now = mono_ms()

        if now < until do
          {:park, now_ms() + (until - now), state}
        else
          {:pass, put_breaker(state, key, %{breaker | state: :half_open, probe: nil})}
        end

      %{state: :half_open, probe: probe} when probe != nil ->
        {:park, now_ms() + state.config.dispatch.breaker_open_ms, state}

      _closed_or_unprobed ->
        {:pass, state}
    end
  end

  defp put_breaker(state, key, breaker),
    do: %{state | breakers: Map.put(state.breakers, key, breaker)}

  defp breakers_open(state),
    do: Enum.count(state.breakers, fn {_key, b} -> b.state != :closed end)

  # A claimed job goes back to its row, due at `at`, with its attempt count
  # unchanged, and leaves the window.
  defp park(state, job, at) do
    state = %{state | claimed: state.claimed - 1, claimed_bytes: state.claimed_bytes - job.size}
    append(state, park_ops(job, at))
  end

  defp park_ops(%{seq: seq, sink: sink, row: row}, at),
    do: Deliveries.retry_ops(seq, sink, row, at, "breaker open")

  # Buffer outcome ops without the refill a full buffer otherwise triggers:
  # parking runs while a scan is still being resolved.
  defp append(state, ops) do
    state = %{state | buffer: [{ops, []} | state.buffer], buffered: state.buffered + 1}
    if state.buffered >= @flush_entries, do: flush(state), else: arm_flush(state)
  end

  # Every attempt's outcome moves its key's breaker. `:ok` closes it. A failure
  # that is not `{:permanent, _}` counts; the `breaker_failures`-th in a row, or
  # a failed probe, opens it, and whatever of the key is still queued is parked.
  defp trip(state, job, :ok, _probe?) do
    case Map.pop(state.breakers, job.key) do
      {nil, _breakers} ->
        state

      {%{state: :closed}, breakers} ->
        %{state | breakers: breakers}

      {_open_or_half_open, breakers} ->
        breaker_event(state, job, :closed)
        %{state | breakers: breakers}
    end
  end

  defp trip(state, job, {:error, reason}, probe?) do
    dispatch = state.config.dispatch

    cond do
      dispatch.breaker_failures == 0 ->
        state

      match?({:permanent, _}, Sink.classify(reason)) ->
        state

      true ->
        breaker =
          Map.get(state.breakers, job.key, %{
            state: :closed,
            failures: 0,
            opens: 0,
            until: 0,
            probe: nil
          })

        breaker = %{breaker | failures: breaker.failures + 1}

        open? =
          probe? or (breaker.state == :closed and breaker.failures >= dispatch.breaker_failures)

        if open?,
          do: open_breaker(state, job, breaker),
          else: put_breaker(state, job.key, breaker)
    end
  end

  defp open_breaker(state, job, breaker) do
    dispatch = state.config.dispatch
    opens = breaker.opens + 1

    period =
      min(dispatch.breaker_open_ms * Integer.pow(2, opens - 1), dispatch.breaker_max_open_ms)

    until = mono_ms() + period

    breaker_event(state, job, :open)

    {mod, _opts} = job.spec

    Logger.warning(
      "[ankusa] sink #{inspect(mod)} of source #{inspect(job.env.source_id)} failed " <>
        "#{breaker.failures} time(s) in a row; its deliveries pause for #{period} ms"
    )

    state =
      put_breaker(state, job.key, %{
        breaker
        | state: :open,
          opens: opens,
          until: until,
          probe: nil
      })

    park_queued(state, job.key, now_ms() + period)
  end

  defp park_queued(state, key, at) do
    case Map.pop(state.queues, key) do
      {nil, _queues} ->
        state

      {queue, queues} ->
        jobs = :queue.to_list(queue)
        ring = :queue.delete(key, state.ring)
        state = %{state | queues: queues, ring: ring, queued: state.queued - length(jobs)}
        Enum.reduce(jobs, state, &park(&2, &1, at))
    end
  end

  defp breaker_event(state, job, to) do
    {mod, _opts} = job.spec

    Telemetry.emit([:dispatch, :breaker], %{}, %{
      instance: state.instance,
      sink: mod,
      source_id: job.env.source_id,
      state: to
    })
  end

  # ── running ───────────────────────────────────────────────────────────────

  # Round-robin over the keys with queued jobs: one job per key per turn. A key
  # at its `sink_concurrency` cap, or half-open with its probe running, is
  # skipped; a full turn that starts nothing stops the loop.
  defp start_jobs(state), do: start_jobs(state, :queue.len(state.ring))

  defp start_jobs(state, 0), do: state

  defp start_jobs(state, budget) do
    if map_size(state.running) >= state.config.dispatch.concurrency do
      state
    else
      case :queue.out(state.ring) do
        {:empty, _ring} ->
          state

        {{:value, key}, ring} ->
          state = %{state | ring: ring}

          case admit(state, key) do
            {:park, at, state} ->
              state |> park_queued_from_ring(key, at) |> start_jobs(budget - 1)

            {:pass, state} ->
              if at_sink_cap?(state, key) do
                start_jobs(%{state | ring: :queue.in(key, state.ring)}, budget - 1)
              else
                state = start_one(state, key)
                start_jobs(state, :queue.len(state.ring))
              end
          end
      end
    end
  end

  # `key` is already out of the ring.
  defp park_queued_from_ring(state, key, at) do
    {queue, queues} = Map.pop(state.queues, key)
    jobs = :queue.to_list(queue)
    state = %{state | queues: queues, queued: state.queued - length(jobs)}
    Enum.reduce(jobs, state, &park(&2, &1, at))
  end

  defp at_sink_cap?(state, key) do
    case state.config.dispatch.sink_concurrency do
      nil -> false
      cap -> Map.get(state.running_by_key, key, 0) >= cap
    end
  end

  # `key` is out of the ring and has a non-empty queue.
  defp start_one(state, key) do
    {{:value, job}, queue} = :queue.out(Map.fetch!(state.queues, key))

    {queues, ring} =
      if :queue.is_empty(queue),
        do: {Map.delete(state.queues, key), state.ring},
        else: {Map.put(state.queues, key, queue), :queue.in(key, state.ring)}

    # Bind what the task needs *before* building the closure. Reaching into
    # `state.instance`/`state.config` inside it captures the whole state
    # map, so every spawn would copy the queued jobs — thousands of admitted
    # envelopes — into the new process. That copy, not the delivery, was
    # what capped throughput (measured: ~580µs per spawn, 1.5k/s; 46µs and
    # 5.1k/s once hoisted).
    instance = state.instance
    timeout = state.config.dispatch.attempt_timeout_ms

    task =
      Task.Supervisor.async_nolink(state.task_sup, fn ->
        run_job(job, instance)
      end)

    timer = Process.send_after(self(), {:attempt_timeout, task.ref}, timeout)

    # The first job of a half-open key is its probe.
    {probe?, state} =
      case Map.get(state.breakers, key) do
        %{state: :half_open, probe: nil} = breaker ->
          {true, put_breaker(state, key, %{breaker | probe: task.ref})}

        _ ->
          {false, state}
      end

    entry = %{job: job, task: task, timer: timer, probe?: probe?}

    %{
      state
      | queues: queues,
        ring: ring,
        queued: state.queued - 1,
        running: Map.put(state.running, task.ref, entry),
        running_by_key: Map.update(state.running_by_key, key, 1, &(&1 + 1))
    }
  end

  # Runs in the task. Returns `{result, fresh_claim}` and never raises or
  # touches the store — the pipeline process owns every row transition.
  defp run_job(%{env: env, spec: {mod, opts}} = job, instance) do
    case ensure_claim(job, instance) do
      {:ok, claim, fresh} ->
        result = Sink.safe_deliver(mod, env, ctx(job, instance, claim), opts)

        {result, fresh}

      {:error, reason} ->
        {{:error, reason}, nil}
    end
  end

  # A job that has no ref yet but needs one (its pack failed) checks its body in
  # here, once; the new ref is reported back and persisted with the outcome. The
  # sink's threshold callback is user code: evaluated here, in the task, its
  # failure is this attempt's failure (retry policy, DLQ), never the pipeline's.
  defp ensure_claim(%{claim: claim}, _instance) when claim != nil, do: {:ok, claim, nil}

  defp ensure_claim(%{spec: {mod, opts}, env: %{size: size} = env}, instance) do
    case Sink.inline_max_bytes(mod, opts) do
      {:ok, max} when is_integer(max) and size > max ->
        case Message.check_in(instance, env) do
          {:ok, claim} -> {:ok, claim, claim}
          {:error, reason} -> {:error, {:claim_check, reason}}
        end

      {:ok, _max} ->
        {:ok, nil, nil}

      {:error, reason} ->
        {:error, {:inline_max_bytes, reason}}
    end
  end

  defp ctx(%{env: env, row: row} = job, instance, claim) do
    ctx = %{
      instance: instance,
      source_id: env.source_id,
      tenant_id: env.tenant_id,
      attempt: row.attempts + 1,
      forward_headers: job.forward_headers
    }

    ctx =
      case Map.get(row, :replay) do
        r when is_binary(r) -> Map.put(ctx, :replay_id, r)
        _ -> ctx
      end

    if claim, do: Map.put(ctx, :claim, claim), else: ctx
  end

  # ── outcomes ──────────────────────────────────────────────────────────────

  defp outcome(state, ref, result, fresh_claim) do
    {%{job: job, timer: timer, probe?: probe?}, running} = Map.pop!(state.running, ref)
    Process.cancel_timer(timer)

    running_by_key =
      case Map.fetch!(state.running_by_key, job.key) do
        1 -> Map.delete(state.running_by_key, job.key)
        n -> Map.put(state.running_by_key, job.key, n - 1)
      end

    state = %{
      state
      | running: running,
        running_by_key: running_by_key,
        claimed: state.claimed - 1,
        claimed_bytes: state.claimed_bytes - job.size
    }

    attempts = job.row.attempts + 1
    now = now_ms()
    claim_ops = if fresh_claim, do: Deliveries.claim_ops(job.seq, fresh_claim), else: []

    {state, ops, pairs} =
      case result do
        :ok ->
          {mod, _opts} = job.spec

          Telemetry.emit([:dispatch, :stop], %{}, %{
            instance: state.instance,
            result: :ok,
            attempts: attempts,
            sink: mod,
            source_id: job.env.source_id
          })

          {%{state | settled: state.settled + 1} |> bump_replay(job.row, :delivered),
           Deliveries.delivered_ops(job.seq, job.sink), [cleared(job.seq, job.sink)]}

        {:error, reason} ->
          failed(state, job, reason, attempts, now)
      end

    state = trip(state, job, result, probe?)

    state =
      state
      |> record(claim_ops, [])
      |> buffer(ops, pairs)
      |> start_jobs()
      # While outcomes are buffered the store cannot say what is due; the flush
      # asks, once, instead of every outcome asking.
      |> then(fn state -> if state.buffered == 0, do: schedule_next(state), else: state end)

    maybe_reply_waiters(state)
  end

  defp failed(state, job, reason, attempts, now) do
    {rmod, ropts} = state.config.dispatch.retry
    row = %{job.row | attempts: attempts}

    decision =
      case Sink.classify(reason) do
        {:permanent, _term} ->
          :give_up

        {:retry_after, ms, _term} ->
          case rmod.backoff(attempts, ropts) do
            {:retry, delay} -> {:retry, max(delay, min(ms, @max_retry_after_ms))}
            :give_up -> :give_up
          end

        {:transient, _reason} ->
          rmod.backoff(attempts, ropts)
      end

    case decision do
      {:retry, delay} ->
        error = inspect(reason, limit: 50, printable_limit: 4096)
        {state, Deliveries.retry_ops(job.seq, job.sink, row, now + delay, error), []}

      :give_up ->
        {mod, _opts} = job.spec
        error = inspect({:sink, mod, reason}, limit: 50, printable_limit: 4096)
        state = state |> settle_dead(job.env, mod, attempts) |> bump_replay(row, :dead)
        {state, Deliveries.dead_ops(job.seq, job.sink, row, now, error, job.env), []}
    end
  end

  # Dispatch's own gauges (`Ankusa.Metrics`), once per housekeeping tick.
  defp report_state(state) do
    Telemetry.emit(
      [:dispatch, :state],
      %{
        running: map_size(state.running),
        claimed: state.claimed,
        claimed_bytes: state.claimed_bytes,
        runnable: state.queued,
        breakers_open: breakers_open(state)
      },
      %{instance: state.instance}
    )

    state
  end

  # A delivery belonging to a replay job: counted so the Replayer can report
  # and auto-pause. Rows without a `replay` key are live deliveries and skip.
  defp bump_replay(state, row, kind) do
    case Map.get(row, :replay) do
      r when is_binary(r) ->
        counts = Map.get(state.replay_outcomes, r, %{delivered: 0, dead: 0})
        counts = Map.update!(counts, kind, &(&1 + 1))
        %{state | replay_outcomes: Map.put(state.replay_outcomes, r, counts)}

      _ ->
        state
    end
  end

  # Write a batch. The Pipeline's writes are not synced: losing one means a
  # redelivery, and the next synced commit flushes everything before it.
  defp record(state, [], _pairs), do: state

  defp record(state, ops, pairs) do
    case write(state, ops) do
      {:ok, state} ->
        reclaim(state.instance, pairs)
        state

      {:error, reason} ->
        Logger.warning("[ankusa] dispatch outcome not recorded, retrying: #{inspect(reason)}")
        %{state | unrecorded: state.unrecorded ++ [{ops, pairs}]}
    end
  end

  # A failed reclaim only defers: the marker stays, and `Reclaim.sweep/1` finds it.
  defp reclaim(_instance, []), do: :ok

  defp reclaim(instance, pairs) do
    with {:error, reason} <- Reclaim.run(instance, pairs) do
      Logger.warning("[ankusa] hook reclaim deferred to the next sweep: #{inspect(reason)}")
    end

    :ok
  end

  defp lower_floor(state, at), do: %{state | floor: min(state.floor, at)}
  defp raise_floor(state, at), do: %{state | floor: max(state.floor, at - @floor_lag_ms)}

  # Every Pipeline write goes through here. Once it is in, the rows it made due
  # (a retry, an expansion) are visible to scans, so the floor must be at or
  # under the earliest of them: a scan that ran while the write was buffered, or
  # failing and waiting for housekeeping, may have raised it past.
  defp write(state, ops) do
    case Store.write(state.instance, ops, sync: false) do
      :ok -> {:ok, lower_floor_for(state, ops)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp lower_floor_for(state, ops) do
    Enum.reduce(ops, state, fn
      {:put, :index, <<?d, _::binary>> = key, _value}, state ->
        {at, _seq, _sink} = Keys.decode_due(key)
        lower_floor(state, at)

      _op, state ->
        state
    end)
  end

  defp refill(%{window_full?: true} = state), do: fill(state)
  defp refill(state), do: state

  defp buffer(state, ops, pairs) do
    state = %{state | buffer: [{ops, pairs} | state.buffer], buffered: state.buffered + 1}

    if state.buffered >= @flush_entries do
      state |> flush() |> refill()
    else
      arm_flush(state)
    end
  end

  defp arm_flush(%{flush_timer: nil} = state),
    do: %{state | flush_timer: Process.send_after(self(), :flush, @flush_ms)}

  defp arm_flush(state), do: state

  # Everything buffered goes out as one batch, in the order it happened (two
  # outcomes may touch one hook), then one reclaim for the lot.
  defp flush(%{buffer: []} = state), do: state

  defp flush(state) do
    if state.flush_timer, do: Process.cancel_timer(state.flush_timer)

    entries = Enum.reverse(state.buffer)
    ops = Enum.flat_map(entries, &elem(&1, 0))
    pairs = Enum.flat_map(entries, &elem(&1, 1))
    state = %{state | buffer: [], buffered: 0, flush_timer: nil}

    case write(state, ops) do
      {:ok, state} ->
        reclaim(state.instance, pairs)
        state

      {:error, reason} ->
        Logger.warning("[ankusa] dispatch outcomes not recorded, retrying: #{inspect(reason)}")
        %{state | unrecorded: state.unrecorded ++ [{ops, pairs}]}
    end
  end

  # ── housekeeping ──────────────────────────────────────────────────────────

  defp flush_unrecorded(%{unrecorded: []} = state), do: state

  defp flush_unrecorded(%{unrecorded: [{ops, pairs} | rest]} = state) do
    case write(state, ops) do
      {:ok, state} ->
        reclaim(state.instance, pairs)
        flush_unrecorded(%{state | unrecorded: rest})

      {:error, _reason} ->
        state
    end
  end

  defp sweep(state) do
    with {:error, reason} <- Reclaim.sweep(state.instance) do
      Logger.warning("[ankusa] hook reclaim sweep failed: #{inspect(reason)}")
    end
  end

  # ── scheduling ────────────────────────────────────────────────────────────

  defp schedule_next(%{holdoff?: true} = state), do: state
  defp schedule_next(%{recovered?: false} = state), do: state
  # `window_full?` is a hint from the last look: outcomes since may have emptied
  # the window, so what decides is whether it is full now. Believing the flag
  # would cancel the wake of a window that has since drained.
  defp schedule_next(%{window_full?: true} = state) do
    if at_capacity?(state),
      do: cancel_timer(state),
      else: schedule_next(%{state | window_full?: false})
  end

  defp schedule_next(state) do
    case Deliveries.next_due_at(state.instance, state.floor) do
      {:ok, nil} ->
        cancel_timer(state)

      {:ok, at} ->
        now = now_ms()

        if at <= now do
          send(self(), :wake)
          cancel_timer(state)
        else
          set_timer(state, min(at - now, @max_sleep_ms))
        end

      {:error, _reason} ->
        set_timer(state, @holdoff_ms)
    end
  end

  defp set_timer(state, ms) do
    state = cancel_timer(state)
    %{state | timer: Process.send_after(self(), :wake, ms)}
  end

  defp cancel_timer(%{timer: nil} = state), do: state

  defp cancel_timer(%{timer: ref} = state) do
    Process.cancel_timer(ref)
    %{state | timer: nil}
  end

  # ── waiters ───────────────────────────────────────────────────────────────

  defp maybe_reply_waiters(%{waiters: []} = state), do: {:noreply, state}

  defp maybe_reply_waiters(state) do
    if quiet?(state) do
      # Nothing is running: record what is buffered before saying so.
      state = state |> flush() |> schedule_next()

      if idle?(state) do
        Enum.each(state.waiters, fn {from, settled0} ->
          GenServer.reply(from, {:ok, state.settled - settled0})
        end)

        {:noreply, %{state | waiters: []}}
      else
        {:noreply, state}
      end
    else
      {:noreply, state}
    end
  end

  defp quiet?(state) do
    state.recovered? and state.claimed == 0 and map_size(state.running) == 0 and
      map_size(state.packing) == 0 and state.queued == 0
  end

  defp idle?(state), do: quiet?(state) and state.buffered == 0 and not due_now?(state)

  # A store that cannot answer must not hang `tick/1` callers forever.
  defp due_now?(state) do
    case Deliveries.next_due_at(state.instance, state.floor) do
      {:ok, at} when is_integer(at) -> at <= now_ms()
      _ -> false
    end
  end

  defp now_ms, do: System.system_time(:millisecond)
  defp mono_ms, do: System.monotonic_time(:millisecond)
end
