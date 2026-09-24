defmodule Ankusa.Dispatch.Pipeline do
  @moduledoc """
  Async dispatch pipeline: reads committed envelopes from the WAL in ascending
  `seq` order and delivers each to every sink configured on its source, with
  per-source retry/backoff and dead-lettering. At-least-once — the durable
  dispatch cursor only advances once an envelope has been fully handled.

  ## Concurrency and ordering

  Deliveries run in `Task.Supervisor` tasks, up to `dispatch.concurrency` at a
  time, so a slow sink (or a retry backoff) holds up only what it must. What it
  must is decided by `c:Ankusa.Sink.ordering_key/2`: deliveries to the same sink
  with an equal key run one at a time, in `seq` order, while different keys run
  concurrently. `nil` means no constraint.

  The cursor is a **watermark**: `read_seq` when nothing is in flight, else one
  below the lowest admitted-but-unfinished seq. It never moves past an envelope
  that hasn't been fully handled, which is what keeps redelivery at-least-once,
  and it may lag a finished envelope behind it — that is the price of
  concurrency, not a bug (the WAL contract forbids a lower seq appearing later,
  so gaps in the watermark are safe).

  A WAL read window (`dispatch.max_inflight`, `dispatch.max_inflight_bytes`)
  bounds admitted-but-unfinished work, so a dead sink cannot walk the pipeline
  into an OOM.

  `start_link/1` opts: `:instance`, `:config`, and optional `:max_sleep_ms`
  which clamps every backoff sleep (so deterministic tests don't hang).
  """

  use GenServer

  alias Ankusa.{Sink, SourceStore, Telemetry, WAL}
  alias Ankusa.Dispatch.DLQ

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
  Drain until caught up; returns envelopes fully handled during the call.
  """
  @spec tick(atom()) :: {:ok, non_neg_integer()}
  def tick(instance) do
    GenServer.call(Ankusa.via(instance, :dispatch), :tick)
  end

  # ── GenServer ─────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    config = Keyword.fetch!(opts, :config)
    max_sleep = Keyword.get(opts, :max_sleep_ms, nil)

    # Linked to us on purpose: nobody else knows about it, and it must not
    # outlive the pipeline. Tests that start the Pipeline alone still get it.
    {:ok, task_sup} = Task.Supervisor.start_link()

    Process.flag(:trap_exit, true)

    cursor = WAL.get_cursor(instance, :dispatch)

    state = %{
      instance: instance,
      config: config,
      max_sleep: max_sleep,
      task_sup: task_sup,
      # last durably persisted dispatch cursor
      cursor: cursor,
      # last seq read out of the WAL (may be ahead of `cursor`)
      read_seq: cursor,
      # admitted, not yet fully handled
      pending: :gb_sets.empty(),
      # seq => {jobs outstanding, body bytes}
      remaining: %{},
      inflight_bytes: 0,
      # lane => :queue of jobs waiting for that lane to free up
      lanes: %{},
      runnable: :queue.new(),
      running: %{},
      completed: 0,
      waiters: [],
      window_full?: false
    }

    {:ok, schedule(state)}
  end

  @impl true
  def handle_call(:tick, from, state) do
    state = state |> fill() |> start_jobs()
    maybe_reply_waiters(%{state | waiters: state.waiters ++ [{from, state.completed}]})
  end

  @impl true
  def handle_info(:poll, state) do
    state = state |> fill() |> start_jobs() |> persist_cursor()
    {:noreply, schedule(state)}
  end

  def handle_info({ref, result}, %{running: running} = state) when is_map_key(running, ref) do
    Process.demonitor(ref, [:flush])
    job = Map.fetch!(running, ref)
    state = %{state | running: Map.delete(running, ref)}

    state =
      case result do
        {:dead, reason} ->
          # Serialized here on purpose: DLQ appends stay in one process.
          DLQ.write(state.config, job.env, reason)

          Telemetry.emit([:dispatch, :dlq], %{}, %{
            instance: state.instance,
            source_id: job.env.source_id,
            sink: elem(job.sink, 0)
          })

          state

        :ok ->
          state
      end

    state =
      state
      |> release_lane(job)
      |> complete(job.seq)
      |> start_jobs()

    state = if state.window_full?, do: state |> fill() |> start_jobs(), else: state

    maybe_reply_waiters(state)
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{running: running} = state)
      when is_map_key(running, ref) do
    # No result ever arrived: the task was killed. Restarting from the durable
    # cursor (this stops the process) re-reads the envelope, so at-least-once
    # still holds.
    {:stop, {:delivery_task_crashed, reason}, state}
  end

  def handle_info({:EXIT, _pid, reason}, state), do: {:stop, reason, state}

  # Stray messages (e.g. a DOWN from a task already demonitored) must never take
  # down a process other parts of the tree are waiting on.
  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    # Best effort: the WAL may already be gone during a shutdown.
    try do
      persist_cursor(state)
    catch
      :exit, _ -> :ok
    end

    :ok
  end

  # ── reading ───────────────────────────────────────────────────────────────

  defp fill(state), do: fill(state, %{})

  defp fill(state, memo) do
    dispatch = state.config.dispatch

    if map_size(state.remaining) >= dispatch.max_inflight or
         state.inflight_bytes >= dispatch.max_inflight_bytes do
      %{state | window_full?: true}
    else
      requested = min(dispatch.batch, dispatch.max_inflight - map_size(state.remaining))
      envelopes = WAL.read(state.instance, state.read_seq, requested)
      {state, memo} = Enum.reduce(envelopes, {state, memo}, &admit/2)

      # A full read means there is likely more; a short one means the WAL has no
      # more to give right now.
      if length(envelopes) == requested do
        fill(state, memo)
      else
        %{state | window_full?: false}
      end
    end
  end

  defp admit(env, {state, memo}) do
    {sinks, memo} = sinks_for(state.instance, env.source_id, memo)

    if sinks == [] do
      # Nothing to deliver: handled, and it does not enter the window at all.
      {%{state | read_seq: env.seq, completed: state.completed + 1}, memo}
    else
      bytes = byte_size(env.body)

      state = %{
        state
        | read_seq: env.seq,
          pending: :gb_sets.add(env.seq, state.pending),
          remaining: Map.put(state.remaining, env.seq, {length(sinks), bytes}),
          inflight_bytes: state.inflight_bytes + bytes
      }

      state =
        Enum.reduce(sinks, state, fn {mod, opts} = sink, st ->
          enqueue_job(st, %{
            seq: env.seq,
            env: env,
            sink: sink,
            lane: lane(mod, env, opts)
          })
        end)

      {state, memo}
    end
  end

  defp sinks_for(instance, source_id, memo) do
    case Map.fetch(memo, source_id) do
      {:ok, sinks} ->
        {sinks, memo}

      :error ->
        sinks =
          case SourceStore.fetch(instance, source_id) do
            {:ok, source} -> source.sinks
            :error -> []
          end

        {sinks, Map.put(memo, source_id, sinks)}
    end
  end

  defp lane(mod, env, opts) do
    case Sink.ordering_key(mod, env, opts) do
      nil -> nil
      key -> {mod, key}
    end
  end

  defp enqueue_job(state, %{lane: nil} = job) do
    %{state | runnable: :queue.in(job, state.runnable)}
  end

  defp enqueue_job(state, %{lane: lane} = job) do
    case Map.fetch(state.lanes, lane) do
      {:ok, queue} ->
        %{state | lanes: Map.put(state.lanes, lane, :queue.in(job, queue))}

      :error ->
        # Lane was free: this job runs now, and the lane is marked busy until it
        # (and everything queued behind it) is done.
        %{
          state
          | lanes: Map.put(state.lanes, lane, :queue.new()),
            runnable: :queue.in(job, state.runnable)
        }
    end
  end

  # ── running ───────────────────────────────────────────────────────────────

  defp start_jobs(state) do
    cond do
      map_size(state.running) >= state.config.dispatch.concurrency ->
        state

      :queue.is_empty(state.runnable) ->
        state

      true ->
        {{:value, job}, runnable} = :queue.out(state.runnable)

        # Bind what the task needs *before* building the closure. Reaching into
        # `state.instance`/`state.config` inside it captures the whole state
        # map, so every spawn would copy `runnable` — thousands of admitted
        # envelopes — into the new process. That copy, not the delivery, was
        # what capped throughput (measured: ~580µs per spawn, 1.5k/s; 46µs and
        # 5.1k/s once hoisted).
        instance = state.instance
        config = state.config
        max_sleep = state.max_sleep

        task =
          Task.Supervisor.async_nolink(state.task_sup, fn ->
            deliver(job, instance, config, max_sleep)
          end)

        start_jobs(%{state | runnable: runnable, running: Map.put(state.running, task.ref, job)})
    end
  end

  # Runs in the task. Returns `:ok` or `{:dead, reason}`; never raises, never
  # touches the DLQ — the pipeline process owns those two decisions.
  defp deliver(job, instance, config, max_sleep) do
    deliver(job, instance, config, max_sleep, 1)
  end

  defp deliver(%{env: env, sink: {mod, opts}} = job, instance, config, max_sleep, attempt) do
    ctx = %{
      instance: instance,
      source_id: env.source_id,
      tenant_id: env.tenant_id,
      attempt: attempt
    }

    case safe_deliver(mod, env, ctx, opts) do
      :ok ->
        Telemetry.emit([:dispatch, :stop], %{}, %{
          instance: instance,
          result: :ok,
          attempts: attempt
        })

        :ok

      {:error, reason} ->
        {rmod, ropts} = config.dispatch.retry

        case rmod.backoff(attempt, ropts) do
          {:retry, delay} ->
            # Sleeping here blocks this lane only — not the pipeline.
            sleep(delay, max_sleep)
            deliver(job, instance, config, max_sleep, attempt + 1)

          :give_up ->
            Telemetry.emit([:dispatch, :stop], %{}, %{
              instance: instance,
              result: :dlq,
              attempts: attempt
            })

            {:dead, {:sink, mod, reason}}
        end
    end
  end

  # A sink is user code: it may raise, throw, or exit (a `GenServer.call` into a
  # dead process). Any of those is a delivery failure, not a pipeline crash.
  defp safe_deliver(mod, env, ctx, opts) do
    mod.deliver(env, ctx, opts)
  rescue
    error -> {:error, {:raised, error}}
  catch
    :exit, reason -> {:error, {:exit, reason}}
    :throw, value -> {:error, {:throw, value}}
  end

  # ── bookkeeping ───────────────────────────────────────────────────────────

  defp release_lane(state, %{lane: nil}), do: state

  defp release_lane(state, %{lane: lane}) do
    case state.lanes |> Map.fetch!(lane) |> :queue.out() do
      {{:value, next}, queue} ->
        %{
          state
          | lanes: Map.put(state.lanes, lane, queue),
            runnable: :queue.in(next, state.runnable)
        }

      {:empty, _queue} ->
        %{state | lanes: Map.delete(state.lanes, lane)}
    end
  end

  defp complete(%{remaining: remaining} = state, seq) do
    case Map.fetch!(remaining, seq) do
      {1, bytes} ->
        %{
          state
          | remaining: Map.delete(remaining, seq),
            pending: :gb_sets.delete(seq, state.pending),
            inflight_bytes: state.inflight_bytes - bytes,
            completed: state.completed + 1
        }

      {n, bytes} ->
        %{state | remaining: Map.put(remaining, seq, {n - 1, bytes})}
    end
  end

  # ── cursor ────────────────────────────────────────────────────────────────

  defp watermark(%{pending: pending, read_seq: read_seq}) do
    case :gb_sets.is_empty(pending) do
      true -> read_seq
      false -> :gb_sets.smallest(pending) - 1
    end
  end

  defp persist_cursor(state) do
    mark = watermark(state)

    if mark > state.cursor do
      WAL.put_cursor(state.instance, :dispatch, mark)
      %{state | cursor: mark}
    else
      state
    end
  end

  # ── waiters ───────────────────────────────────────────────────────────────

  defp maybe_reply_waiters(%{waiters: []} = state), do: {:noreply, state}

  defp maybe_reply_waiters(state) do
    if idle?(state) do
      state = drain_while_caught_up(state)

      if idle?(state) do
        state = persist_cursor(state)
        completed = state.completed

        Enum.each(state.waiters, fn {from, c0} ->
          GenServer.reply(from, {:ok, completed - c0})
        end)

        {:noreply, %{state | waiters: []}}
      else
        # Something is in flight again; its completion re-checks the waiters.
        {:noreply, state}
      end
    else
      {:noreply, state}
    end
  end

  defp drain_while_caught_up(state) do
    before = state.read_seq
    state = state |> fill() |> start_jobs()

    cond do
      not idle?(state) -> state
      state.read_seq == before -> state
      true -> drain_while_caught_up(state)
    end
  end

  defp idle?(state) do
    map_size(state.running) == 0 and :queue.is_empty(state.runnable) and
      :gb_sets.is_empty(state.pending)
  end

  # ── scheduling ────────────────────────────────────────────────────────────

  defp schedule(state) do
    Process.send_after(self(), :poll, state.config.dispatch.poll_ms)
    state
  end

  defp sleep(delay, nil), do: Process.sleep(delay)
  defp sleep(delay, cap), do: Process.sleep(min(delay, cap))
end
