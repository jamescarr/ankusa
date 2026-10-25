defmodule Ankusa.Lifecycle.Publisher do
  @max_concurrency 8
  @max_replay_pending 1_000
  @batch 200
  @tick_ms 1_000
  @load_retry_ms 1_000
  @replay_attempts 3

  @moduledoc """
  Delivers lifecycle events (`Ankusa.Lifecycle`) to `config.lifecycle.sinks`:
  the reader of the outbox.

  One supervised process per instance. There is no queue to lose: the events
  are the audit rows (`Ankusa.Audit`) flagged `publish: true`, and this process
  keeps one number, the `seq` of the last row every sink has dealt with, in
  `m:lifecycle_cursor` (written `sync: true` on every advance). Booted for the
  first time on a store, it starts at the end of the trail.

  It wakes when `Ankusa.Audit` commits a flagged row (and once a second, as a
  safety net), reads the trail from the cursor with `Ankusa.Audit.page/3`, and
  takes the next flagged row. Each sink gets that row's event as its own job, so
  a sink that is down never keeps another from *receiving* it; but the cursor
  only moves once every sink confirmed the row or gave up on it, so rows are
  delivered in `seq` order and one failing sink holds the events behind that row
  back from all of them. A failed job is retried after the delay
  `config.dispatch.retry` gives (an `Ankusa.RetryPolicy`), until the policy gives
  up or the job is `retry.max_elapsed_ms` (default 24 hours) past its first
  attempt: the instance's `dispatch.retry`, not a source's, so that horizon is
  how long one failing sink can hold the cursor. Then
  the event is dropped *for that sink* (and can be replayed). An attempt that
  has not returned after `config.dispatch.attempt_timeout_ms` is killed and
  counts as a failed attempt.

  A node killed after a sink confirmed a row and before the cursor moved
  delivers the row again; the event `id` is the same, so a consumer dedupes.

  `replay/4` is the other way in: a replay job (`Ankusa.Replay`, `kind:
  :lifecycle`) hands a page of rows to re-emit, tagged with its id. They share
  the concurrency (at most #{@max_concurrency} attempts run at a time) but not the
  cursor, are tried #{@replay_attempts} times, and queue behind the live events. At most
  #{@max_replay_pending} replayed deliveries are pending: past that `replay/4` says
  `{:error, :busy}` and the job asks again.

  Every drop is logged and emitted as `[:ankusa, :lifecycle, :dropped]` with a
  `:reason` of `:gave_up` (the sink's retries ran out) or `:unreadable` (a row
  or its entity could not be read, and the cursor moved past it); a confirmed
  delivery is `[:ankusa, :lifecycle, :delivered]`.

  Sinks are called without a `ctx.claim`, so a sink with an inline threshold
  checks an oversized body in itself (`Ankusa.Sink.Message.encode/3`).
  """

  use GenServer

  require Logger

  alias Ankusa.{Audit, Lifecycle, Sink, Store, Telemetry}
  alias Ankusa.Store.Keys

  @type job :: %{
          env: Ankusa.Envelope.t(),
          type: String.t(),
          subject: String.t(),
          sink: {module(), keyword()},
          attempt: pos_integer(),
          first_attempt_at: integer(),
          row: pos_integer(),
          batch: reference() | nil,
          replay: String.t() | nil
        }

  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :instance)},
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  @doc "Start the publisher for `opts[:instance]`."
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts,
      name: Ankusa.via(Keyword.fetch!(opts, :instance), :lifecycle)
    )
  end

  @doc """
  Tell the publisher the trail has a new row to look at. `:ok` whether or not it
  runs: the row is in the store either way.
  """
  @spec wake(atom()) :: :ok
  def wake(instance) do
    case Ankusa.whereis(instance, :lifecycle) do
      nil ->
        :ok

      pid ->
        send(pid, :wake)
        :ok
    end
  end

  @doc """
  Re-emit `rows` (`{audit_row, entity}` pairs) for the replay job `replay_id`.

  Returns once they are queued. When every delivery has been confirmed or
  given up on, `to` receives `{:lifecycle_replayed, replay_id, %{delivered: n,
  dead: m}}` (counts of sink deliveries, one per row and sink).
  `{:error, :busy}` when the replay backlog is full.
  """
  @spec replay(atom(), String.t(), [{Audit.row(), map()}], pid()) ::
          :ok | {:error, :busy | :not_running | :no_audit_key}
  def replay(instance, replay_id, rows, to) do
    case Ankusa.whereis(instance, :lifecycle) do
      nil -> {:error, :not_running}
      pid -> GenServer.call(pid, {:replay, replay_id, rows, to}, 5_000)
    end
  catch
    :exit, _reason -> {:error, :not_running}
  end

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    config = Ankusa.config(instance)
    {:ok, sup} = Task.Supervisor.start_link()
    send(self(), :load)

    {:ok,
     %{
       instance: instance,
       sinks: config.lifecycle.sinks,
       retry: config.dispatch.retry,
       max_retry_after_ms: config.dispatch.max_retry_after_ms,
       attempt_timeout_ms: config.dispatch.attempt_timeout_ms,
       sup: sup,
       loaded?: false,
       cursor: 0,
       # the flagged row being delivered: `%{seq, pending}` (jobs not settled)
       current: nil,
       ready: :queue.new(),
       replay_ready: :queue.new(),
       running: %{},
       batches: %{},
       replay_pending: 0
     }}
  end

  # Crash reports print the state: the sinks carry credentials and the queued
  # jobs carry the lifecycle envelopes.
  @impl true
  def format_status(%{state: %{sinks: sinks} = state} = status) do
    %{
      status
      | state: %{
          state
          | sinks: Enum.map(sinks, &elem(&1, 0)),
            ready: :queue.len(state.ready),
            replay_ready: :queue.len(state.replay_ready),
            running: map_size(state.running),
            batches: map_size(state.batches)
        }
    }
  end

  def format_status(status), do: status

  # ── replay ────────────────────────────────────────────────────────────────

  @impl true
  def handle_call({:replay, replay_id, rows, to}, _from, state) do
    count = length(rows) * length(state.sinks)

    cond do
      count == 0 ->
        send(to, {:lifecycle_replayed, replay_id, %{delivered: 0, dead: 0}})
        {:reply, :ok, state}

      state.replay_pending + count > @max_replay_pending ->
        {:reply, {:error, :busy}, state}

      true ->
        case replay_jobs(state, replay_id, rows) do
          {:ok, jobs} ->
            ref = make_ref()
            jobs = Enum.map(jobs, &%{&1 | batch: ref})
            batch = %{replay_id: replay_id, to: to, pending: count, delivered: 0, dead: 0}
            ready = Enum.reduce(jobs, state.replay_ready, &:queue.in/2)

            {:reply, :ok,
             start_jobs(%{
               state
               | replay_ready: ready,
                 batches: Map.put(state.batches, ref, batch),
                 replay_pending: state.replay_pending + count
             })}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  defp replay_jobs(state, replay_id, rows) do
    Enum.reduce_while(rows, {:ok, []}, fn {row, entity}, {:ok, jobs} ->
      case Lifecycle.traced_envelope(state.instance, row, entity, replay_id) do
        {:ok, env} ->
          new =
            for sink <- state.sinks do
              %{
                env: env,
                type: Lifecycle.type(row),
                subject: row.resource,
                sink: sink,
                attempt: 1,
                first_attempt_at: System.system_time(:millisecond),
                row: row.seq,
                batch: nil,
                replay: replay_id
              }
            end

          {:cont, {:ok, jobs ++ new}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  # ── the cursor ────────────────────────────────────────────────────────────

  @impl true
  def handle_info(:load, state) do
    case load_cursor(state.instance) do
      {:ok, cursor} ->
        Process.send_after(self(), :tick, @tick_ms)
        {:noreply, drain(%{state | loaded?: true, cursor: cursor})}

      {:error, reason} ->
        Logger.warning("[ankusa] lifecycle cursor load failed, retrying: #{inspect(reason)}")
        Process.send_after(self(), :load, @load_retry_ms)
        {:noreply, state}
    end
  end

  def handle_info(:tick, state) do
    Process.send_after(self(), :tick, @tick_ms)
    {:noreply, state |> advance_if_settled() |> drain()}
  end

  def handle_info(:wake, state), do: {:noreply, drain(state)}

  def handle_info({ref, result}, %{running: running} = state) when is_map_key(running, ref) do
    Process.demonitor(ref, [:flush])
    {%{job: job, timer: timer}, running} = Map.pop!(running, ref)
    Process.cancel_timer(timer)

    {:noreply, %{state | running: running} |> finished(job, result) |> start_jobs()}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{running: running} = state)
      when is_map_key(running, ref) do
    {%{job: job, timer: timer}, running} = Map.pop!(running, ref)
    Process.cancel_timer(timer)

    {:noreply, %{state | running: running} |> retry(job, {:exit, reason}) |> start_jobs()}
  end

  # The attempt outlasted `dispatch.attempt_timeout_ms`: kill it and count a
  # failed attempt. A reply that landed in the mailbox meanwhile still counts.
  def handle_info({:attempt_timeout, ref}, %{running: running} = state)
      when is_map_key(running, ref) do
    {%{job: job, task: task}, running} = Map.pop!(running, ref)
    state = %{state | running: running}

    state =
      case Task.shutdown(task, :brutal_kill) do
        {:ok, result} ->
          finished(state, job, result)

        {:exit, reason} ->
          retry(state, job, {:exit, reason})

        nil ->
          {mod, _opts} = job.sink

          Logger.warning(
            "[ankusa] lifecycle event #{job.type} for #{job.subject}: #{inspect(mod)} did not " <>
              "finish within #{state.attempt_timeout_ms}ms; attempt #{job.attempt} failed"
          )

          retry(state, job, {:attempt_timeout, state.attempt_timeout_ms})
      end

    {:noreply, start_jobs(state)}
  end

  def handle_info({:retry, %{replay: nil} = job}, state) do
    {:noreply, start_jobs(%{state | ready: :queue.in(job, state.ready)})}
  end

  def handle_info({:retry, job}, state) do
    {:noreply, start_jobs(%{state | replay_ready: :queue.in(job, state.replay_ready)})}
  end

  def handle_info(_other, state), do: {:noreply, state}

  defp load_cursor(instance) do
    case Store.get(instance, :default, Keys.lifecycle_cursor()) do
      {:ok, <<seq::64>>} ->
        {:ok, seq}

      {:ok, other} ->
        {:error, {:bad_cursor, other}}

      # First boot of the outbox: what is already in the trail is history.
      :not_found ->
        with {:ok, last} <- Audit.last_seq(instance),
             :ok <- write_cursor(instance, last, sync: true),
             do: {:ok, last}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp write_cursor(instance, seq, opts),
    do: Store.write(instance, [{:put, :default, Keys.lifecycle_cursor(), <<seq::64>>}], opts)

  # Read the trail after the cursor; deliver the first row that carries an event.
  defp drain(%{loaded?: true, current: nil} = state) do
    case Audit.page(state.instance, state.cursor, @batch) do
      {:ok, entries} ->
        walk(state, entries, length(entries) >= @batch)

      {:error, reason} ->
        Logger.warning("[ankusa] lifecycle outbox could not read the trail: #{inspect(reason)}")
        state
    end
  end

  defp drain(state), do: state

  defp walk(state, entries, full?) do
    {passed, rest} = Enum.split_while(entries, fn {_seq, row} -> not publishable?(row) end)
    Enum.each(passed, &report_unreadable(state.instance, &1))
    state = skip(state, passed)

    case rest do
      [{seq, {:ok, row}} | _] ->
        deliver_row(state, seq, row)

      [] ->
        if full?, do: send(self(), :wake)
        state
    end
  end

  defp publishable?({:ok, %{publish: true}}), do: true
  defp publishable?(_decoded), do: false

  defp report_unreadable(instance, {seq, {:error, reason}}) do
    Logger.error(
      "[ankusa] lifecycle outbox skipped the unreadable audit row #{seq}: #{inspect(reason)}"
    )

    dropped(instance, "unknown", "all", :unreadable)
  end

  defp report_unreadable(_instance, {_seq, {:ok, _row}}), do: :ok

  # Rows that carry no event: the cursor passes them without a synced write. A
  # lost write only means they are read (and passed) again after a restart.
  defp skip(state, []), do: state

  defp skip(state, passed) do
    {seq, _row} = List.last(passed)
    _ = write_cursor(state.instance, seq, sync: false)
    %{state | cursor: seq}
  end

  defp deliver_row(state, seq, row) do
    with {:ok, entity} <- Audit.entity(state.instance, seq),
         {:ok, env} <- Lifecycle.traced_envelope(state.instance, row, entity) do
      jobs =
        for sink <- state.sinks do
          %{
            env: env,
            type: Lifecycle.type(row),
            subject: row.resource,
            sink: sink,
            attempt: 1,
            first_attempt_at: System.system_time(:millisecond),
            row: seq,
            batch: nil,
            replay: nil
          }
        end

      ready = Enum.reduce(jobs, state.ready, &:queue.in/2)
      start_jobs(%{state | ready: ready, current: %{seq: seq, pending: length(jobs)}})
    else
      # Pruned out from under the cursor (the retention is shorter than the
      # outage): nothing to say, so pass the row.
      :not_found ->
        Logger.error("[ankusa] lifecycle outbox: the entity of audit row #{seq} is gone")
        dropped(state.instance, Lifecycle.type(row), "all", :unreadable)
        _ = write_cursor(state.instance, seq, sync: false)
        send(self(), :wake)
        %{state | cursor: seq}

      {:error, reason} ->
        Logger.warning("[ankusa] lifecycle outbox cannot build row #{seq}: #{inspect(reason)}")
        state
    end
  end

  # Every sink dealt with the row: the cursor moves, synced, and the next row
  # goes. A write that fails leaves the row current with nothing pending; the
  # tick tries again.
  defp advance_if_settled(%{current: %{pending: 0}} = state), do: advance(state)
  defp advance_if_settled(state), do: state

  defp advance(%{current: %{seq: seq}} = state) do
    case write_cursor(state.instance, seq, sync: true) do
      :ok ->
        send(self(), :wake)
        %{state | cursor: seq, current: nil}

      {:error, reason} ->
        Logger.error("[ankusa] lifecycle cursor write failed, retrying: #{inspect(reason)}")
        state
    end
  end

  # ── jobs ──────────────────────────────────────────────────────────────────

  defp start_jobs(%{running: running} = state) when map_size(running) >= @max_concurrency,
    do: state

  defp start_jobs(state) do
    case next_job(state) do
      nil ->
        state

      {job, state} ->
        {mod, opts} = job.sink

        ctx =
          %{
            instance: state.instance,
            source_id: job.env.source_id,
            tenant_id: job.env.tenant_id,
            attempt: job.attempt
          }
          |> then(&if(job.replay, do: Map.put(&1, :replay_id, job.replay), else: &1))

        task =
          Task.Supervisor.async_nolink(state.sup, fn ->
            Sink.attempt(mod, job.env, job.attempt, job.replay, fn ->
              Sink.safe_deliver(mod, job.env, ctx, opts)
            end)
          end)

        timer = Process.send_after(self(), {:attempt_timeout, task.ref}, state.attempt_timeout_ms)
        entry = %{job: job, task: task, timer: timer}

        start_jobs(%{state | running: Map.put(state.running, task.ref, entry)})
    end
  end

  # Live events first: a replay only takes the attempts they leave free.
  defp next_job(state) do
    case :queue.out(state.ready) do
      {{:value, job}, ready} ->
        {job, %{state | ready: ready}}

      {:empty, _ready} ->
        case :queue.out(state.replay_ready) do
          {{:value, job}, replay_ready} -> {job, %{state | replay_ready: replay_ready}}
          {:empty, _replay_ready} -> nil
        end
    end
  end

  defp finished(state, job, :ok) do
    {mod, _opts} = job.sink

    Telemetry.emit([:lifecycle, :delivered], %{}, %{
      instance: state.instance,
      type: job.type,
      sink: inspect(mod)
    })

    settle(state, job, :delivered)
  end

  defp finished(state, job, {:error, reason}), do: retry(state, job, reason)

  defp retry(state, job, reason) do
    {mod, _opts} = job.sink
    now = System.system_time(:millisecond)

    # The instance's policy, not a source's (lifecycle sinks belong to none):
    # `dispatch.retry`'s horizon bounds the wait behind the cursor.
    schedule =
      if job.replay && job.attempt >= @replay_attempts,
        do: :give_up,
        else:
          Ankusa.RetryPolicy.schedule(
            state.retry,
            Sink.classify(reason),
            %{attempts: job.attempt, first_attempt_at: job.first_attempt_at},
            now,
            state.max_retry_after_ms
          )

    case schedule do
      {:retry, at} ->
        Process.send_after(self(), {:retry, %{job | attempt: job.attempt + 1}}, max(at - now, 0))
        state

      :give_up ->
        Logger.error(
          "[ankusa] lifecycle event #{job.type} for #{job.subject} to #{inspect(mod)} " <>
            "dropped after #{job.attempt} attempts: #{inspect(reason)}"
        )

        dropped(state.instance, job.type, inspect(mod), :gave_up)
        settle(state, job, :dead)
    end
  end

  # A job is over, confirmed or given up on. The live row's cursor moves when
  # its last job is; a replay page is reported when its last one is.
  defp settle(state, %{batch: nil}, _outcome) do
    state = %{state | current: %{state.current | pending: state.current.pending - 1}}
    advance_if_settled(state)
  end

  defp settle(state, %{batch: ref}, outcome) do
    state = %{state | replay_pending: state.replay_pending - 1}

    case Map.fetch(state.batches, ref) do
      {:ok, batch} ->
        batch = %{batch | pending: batch.pending - 1}

        batch =
          case outcome do
            :delivered -> %{batch | delivered: batch.delivered + 1}
            :dead -> %{batch | dead: batch.dead + 1}
          end

        if batch.pending == 0 do
          counts = %{delivered: batch.delivered, dead: batch.dead}
          send(batch.to, {:lifecycle_replayed, batch.replay_id, counts})
          %{state | batches: Map.delete(state.batches, ref)}
        else
          %{state | batches: Map.put(state.batches, ref, batch)}
        end

      :error ->
        state
    end
  end

  defp dropped(instance, type, sink, reason) do
    Telemetry.emit([:lifecycle, :dropped], %{}, %{
      instance: instance,
      type: type,
      sink: sink,
      reason: reason
    })
  end
end
