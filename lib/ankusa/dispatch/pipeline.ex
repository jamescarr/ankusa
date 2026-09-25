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

  ## Claim check

  A body larger than one of its sinks' `c:Ankusa.Sink.inline_max_bytes/1` is
  checked in **once**, before any of its sinks run. Each WAL read batch's
  claims are packed per tenant and uploaded together
  (`Ankusa.ClaimCheck.check_in_batch/2`) in a task, while the batch's jobs wait
  in a FIFO of staged batches. Batches release in read order, so per-lane
  `seq` order holds. The ref rides to every sink and every retry in
  `ctx.claim`. If a pack fails, its jobs still run: the first attempt checks the
  body in on its own, and the ref is reused across that job's retries.

  `start_link/1` opts: `:instance`, `:config`, and optional `:max_sleep_ms`
  which clamps every backoff sleep (so deterministic tests don't hang).
  """

  use GenServer

  alias Ankusa.{ClaimCheck, Sink, SourceStore, Telemetry, WAL}
  alias Ankusa.Dispatch.{DLQ, Receiver}
  alias Ankusa.Sink.Message
  alias Ankusa.WAL.LeaseHelpers

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
  Drain the WAL and deliver everything dispatchable.

  Only a node that holds the dispatch lease drains: on a standby this is a
  no-op returning `{:ok, 0}`, because another node owns the cursor. The return
  is the number of envelopes fully handled during the call.
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
    dispatch = config.dispatch

    # Linked to us on purpose: nobody else knows about it, and it must not
    # outlive the pipeline. Tests that start the Pipeline alone still get it.
    {:ok, task_sup} = Task.Supervisor.start_link()

    Process.flag(:trap_exit, true)

    ttl_ms = dispatch.lease_ttl_ms

    state = %{
      instance: instance,
      config: config,
      max_sleep: max_sleep,
      task_sup: task_sup,
      # the dispatch lease: only its holder may read/advance the cursor
      lease: nil,
      holder: "#{node()}/#{inspect(self())}",
      ttl_ms: ttl_ms,
      renew_ms: div(ttl_ms, 3),
      safety_margin_ms: dispatch.lease_safety_margin_ms,
      # monotonic ms; renew by `lease_renew_at`, step down past `lease_deadline`
      lease_renew_at: 0,
      lease_deadline: 0,
      # last durably persisted dispatch cursor
      cursor: 0,
      # last seq read out of the WAL (may be ahead of `cursor`)
      read_seq: 0,
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
      # the idempotent receiver, one per partition, opened on first use. Each
      # holds its own dedup ledger, so a partition's copies are decided by
      # exactly one consumer.
      receivers: %{},
      # records dropped as duplicates of an event already delivered
      deduplicated: 0,
      # set when the ledger could not be consulted for a record, so this poll
      # stopped at the first of them and will retry from that seq
      stalled?: false,
      waiters: [],
      window_full?: false,
      # read batches admitted but held back until their claims are packed, in
      # read order: %{ref: pack task ref | nil, jobs: [job]}
      staged: :queue.new(),
      # pack task ref => true
      packing: %{}
    }

    case try_acquire(state) do
      {:ok, state} ->
        # This node owns the dispatch cursor: resume where it left off.
        case safe_get_cursor(instance, :dispatch) do
          {:ok, cursor} -> {:ok, schedule(%{state | cursor: cursor, read_seq: cursor})}
          :error -> {:ok, schedule(step_down(state))}
        end

      {:standby, state} ->
        {:ok, schedule(standby(state))}
    end
  end

  @impl true
  def handle_call(:tick, _from, %{lease: nil} = state) do
    # Standby: another node owns the dispatch cursor. Nothing to drain here.
    {:reply, {:ok, 0}, state}
  end

  def handle_call(:tick, from, state) do
    state = state |> fill() |> start_jobs()
    maybe_reply_waiters(%{state | waiters: state.waiters ++ [{from, state.completed}]})
  end

  @impl true
  def handle_info(:poll, %{lease: nil} = state) do
    {:noreply, schedule(state)}
  end

  def handle_info(:poll, state) do
    cond do
      # The lease is about to expire and could not be confirmed: stop now, so
      # no write is attempted on a lease someone else may have taken.
      mono_ms() > state.lease_deadline ->
        {:noreply, schedule(step_down(state))}

      mono_ms() >= state.lease_renew_at ->
        {:noreply, schedule(renew(state))}

      true ->
        state = state |> fill() |> start_jobs() |> persist_cursor()
        {:noreply, schedule(state)}
    end
  end

  def handle_info(:acquire_lease, %{lease: nil} = state) do
    case try_acquire(state) do
      {:ok, state} ->
        # A new holder never resumes from a stale in-memory cursor: re-read the
        # durable one, which is leader-consistent. The poll loop is already
        # running (from `init`), so this must not schedule a second one.
        case safe_get_cursor(state.instance, :dispatch) do
          {:ok, cursor} -> {:noreply, %{state | cursor: cursor, read_seq: cursor}}
          :error -> {:noreply, step_down(state)}
        end

      {:standby, state} ->
        {:noreply, standby(state)}
    end
  end

  def handle_info(:acquire_lease, state), do: {:noreply, state}

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
      if state.lease, do: WAL.release_lease(state.instance, state.lease)
    catch
      :exit, _ -> :ok
    end

    :ok
  end

  # ── reading ───────────────────────────────────────────────────────────────

  defp fill(state), do: fill(%{state | stalled?: false}, %{})

  defp fill(state, memo) do
    dispatch = state.config.dispatch

    if map_size(state.remaining) >= dispatch.max_inflight or
         state.inflight_bytes >= dispatch.max_inflight_bytes do
      %{state | window_full?: true}
    else
      requested = min(dispatch.batch, dispatch.max_inflight - map_size(state.remaining))
      envelopes = WAL.read(state.instance, state.read_seq, requested)
      {state, memo, jobs} = Enum.reduce_while(envelopes, {state, memo, []}, &admit/2)
      state = stage(state, Enum.reverse(jobs))

      # A full read means there is likely more; a short one means the WAL has no
      # more to give right now. A stalled one means the ledger could not answer
      # for a record, so nothing past it may be admitted either: the next copy
      # of that event has to be decided after the copy before it was.
      cond do
        state.stalled? -> %{state | window_full?: false}
        length(envelopes) == requested -> fill(state, memo)
        true -> %{state | window_full?: false}
      end
    end
  end

  defp admit(env, {state, memo, jobs}) do
    {source, memo} = source_for(state.instance, env.source_id, memo)
    sinks = if source, do: source.sinks, else: []

    {decision, state} = receiver_decision(state, source, env)

    case decision do
      {:error, reason} ->
        # The ledger could not be consulted: this record has no decision, and
        # neither has anything after it — admitting one would deliver a copy
        # while an earlier copy of the same event is still undecided, which is
        # the duplicate this stage exists to prevent. `read_seq` stays behind
        # this record and the next poll retries from here.
        Telemetry.emit([:dispatch, :dedup_unavailable], %{}, %{
          instance: state.instance,
          source_id: env.source_id,
          seq: env.seq,
          reason: inspect(reason)
        })

        {:halt, {stalled(state), memo, jobs}}

      {:ok, drop?} ->
        cond do
          sinks == [] ->
            # Nothing to deliver: handled, and it does not enter the window at
            # all.
            {:cont, {handled(state, env), memo, jobs}}

          drop? ->
            # A copy of an event the receiver has already delivered. Handled,
            # and like the sinkless case it never enters the window.
            Telemetry.emit([:dispatch, :dedup], %{}, %{
              instance: state.instance,
              source_id: env.source_id,
              seq: env.seq
            })

            {:cont, {handled(%{state | deduplicated: state.deduplicated + 1}, env), memo, jobs}}

          true ->
            bytes = byte_size(env.body)

            state = %{
              state
              | read_seq: env.seq,
                pending: :gb_sets.add(env.seq, state.pending),
                remaining: Map.put(state.remaining, env.seq, {length(sinks), bytes}),
                inflight_bytes: state.inflight_bytes + bytes
            }

            jobs =
              Enum.reduce(sinks, jobs, fn {mod, opts} = sink, jobs ->
                [
                  %{
                    seq: env.seq,
                    env: env,
                    sink: sink,
                    lane: lane(mod, env, opts),
                    needs_claim: needs_claim?(mod, opts, env),
                    claim: nil
                  }
                  | jobs
                ]
              end)

            {:cont, {state, memo, jobs}}
        end
    end
  end

  # One record could not be decided, so the poll stops where it is.
  defp stalled(state), do: %{state | stalled?: true}

  # Read past, fully handled, and delivered to nobody.
  defp handled(state, env), do: %{state | read_seq: env.seq, completed: state.completed + 1}

  # Ask the idempotent receiver for this record's partition, opening that
  # receiver on first use. Its store is opened here too, so an in-process ledger
  # lives exactly as long as this process consumes the partition.
  defp receiver_decision(state, nil, _env), do: {{:ok, false}, state}

  defp receiver_decision(state, source, env) do
    dispatch = state.config.dispatch
    partition = Receiver.partition(Receiver.scope(env), dispatch.partitions)

    {receiver, receivers} =
      case Map.fetch(state.receivers, partition) do
        {:ok, receiver} ->
          {receiver, state.receivers}

        :error ->
          receiver =
            Receiver.new(partition,
              store: dispatch.dedup_store,
              ttl_ms: dispatch.dedup_ttl_ms
            )

          {receiver, Map.put(state.receivers, partition, receiver)}
      end

    {Receiver.decide(receiver, source, env), %{state | receivers: receivers}}
  end

  defp needs_claim?(mod, opts, env) do
    case Sink.inline_max_bytes(mod, opts) do
      nil -> false
      max -> env.size > max
    end
  end

  # ── claim check staging ───────────────────────────────────────────────────

  # A batch with no claims and nothing staged ahead of it goes straight to the
  # lanes. Anything else joins the FIFO, so no job is ever enqueued ahead of a
  # lower seq still waiting on its pack.
  defp stage(state, []), do: state

  defp stage(state, jobs) do
    items =
      jobs
      |> Enum.filter(& &1.needs_claim)
      |> Enum.uniq_by(& &1.env.id)
      |> Enum.map(&Message.claim_item(&1.env))

    cond do
      items == [] and :queue.is_empty(state.staged) ->
        Enum.reduce(jobs, state, &enqueue_job(&2, &1))

      items == [] ->
        %{state | staged: :queue.in(%{ref: nil, jobs: jobs}, state.staged)}

      true ->
        instance = state.instance

        task =
          Task.Supervisor.async_nolink(state.task_sup, fn ->
            {:packed, ClaimCheck.check_in_batch(instance, items)}
          end)

        %{
          state
          | staged: :queue.in(%{ref: task.ref, jobs: jobs}, state.staged),
            packing: Map.put(state.packing, task.ref, true)
        }
    end
  end

  # Attach each job's ref (a failed pack leaves it nil, so the job checks its
  # body in itself), mark the batch ready, and release every ready batch at the
  # head of the FIFO.
  defp packed(state, ref, results) do
    staged =
      :queue.filter(
        fn
          %{ref: ^ref, jobs: jobs} ->
            [%{ref: nil, jobs: Enum.map(jobs, &attach_claim(&1, results))}]

          batch ->
            [batch]
        end,
        state.staged
      )

    release_staged(%{state | staged: staged, packing: Map.delete(state.packing, ref)})
  end

  defp attach_claim(%{needs_claim: true, env: env} = job, results) do
    case Map.get(results, env.id) do
      {:ok, ref} -> %{job | claim: ref}
      _ -> job
    end
  end

  defp attach_claim(job, _results), do: job

  defp release_staged(state) do
    case :queue.peek(state.staged) do
      {:value, %{ref: nil, jobs: jobs}} ->
        state = Enum.reduce(jobs, state, &enqueue_job(&2, &1))
        release_staged(%{state | staged: :queue.drop(state.staged)})

      _ ->
        state
    end
  end

  defp source_for(instance, source_id, memo) do
    case Map.fetch(memo, source_id) do
      {:ok, source} ->
        {source, memo}

      :error ->
        source =
          case SourceStore.fetch(instance, source_id) do
            {:ok, source} -> source
            :error -> nil
          end

        {source, Map.put(memo, source_id, source)}
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
    # A job whose pack failed checks its body in here, once; the ref then rides
    # along to every retry.
    {job, result} =
      case ensure_claim(job, instance) do
        {:ok, job} -> {job, safe_deliver(mod, env, ctx(job, instance, attempt), opts)}
        {:error, reason} -> {job, {:error, {:claim_check, reason}}}
      end

    case result do
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

  defp ensure_claim(%{needs_claim: true, claim: nil, env: env} = job, instance) do
    with {:ok, ref} <- Message.check_in(instance, env), do: {:ok, %{job | claim: ref}}
  end

  defp ensure_claim(job, _instance), do: {:ok, job}

  defp ctx(%{env: env, claim: claim}, instance, attempt) do
    ctx = %{
      instance: instance,
      source_id: env.source_id,
      tenant_id: env.tenant_id,
      attempt: attempt
    }

    if claim, do: Map.put(ctx, :claim, claim), else: ctx
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

  defp persist_cursor(%{lease: nil} = state), do: state

  defp persist_cursor(state) do
    mark = watermark(state)

    if mark > state.cursor do
      result =
        try do
          WAL.put_cursor(state.instance, :dispatch, mark, state.lease.token)
        catch
          :exit, _ -> {:error, :fenced}
        end

      case result do
        :ok -> %{state | cursor: mark}
        # The lease moved on without us: stop being the dispatcher.
        {:error, :fenced} -> step_down(state)
      end
    else
      state
    end
  end

  # ── lease ─────────────────────────────────────────────────────────────────

  # Try to take the dispatch lease. `{:ok, state}` means this node is active;
  # `{:standby, state}` means another node holds it and this one only waits.
  defp try_acquire(state) do
    sent = mono_ms()

    try do
      case WAL.acquire_lease(state.instance, :dispatch, state.holder, state.ttl_ms) do
        {:ok, lease} ->
          lease = Map.put(lease, :instance, state.instance)
          LeaseHelpers.emit(:acquired, lease)
          {:ok, arm(%{state | lease: lease}, sent)}

        {:error, {:held, _holder}} ->
          {:standby, %{state | lease: nil}}
      end
    catch
      # The WAL process (or the cluster it talks to) is unreachable: treat it
      # the same as "someone else holds the lease" — go standby.
      :exit, _ -> {:standby, %{state | lease: nil}}
    end
  end

  defp renew(state) do
    sent = mono_ms()

    try do
      case WAL.renew_lease(state.instance, state.lease) do
        {:ok, lease} ->
          lease = Map.put(lease, :instance, state.instance)
          LeaseHelpers.emit(:renewed, lease)
          arm(%{state | lease: lease}, sent)

        {:error, :lost} ->
          step_down(state)
      end
    catch
      :exit, _ ->
        step_down(state)
    end
  end

  # Timestamps are monotonic, so a wall-clock jump cannot extend a lease. The
  # deadline is measured from when the acquire/renew was *sent*, not when its
  # reply arrived, so a slow call cannot silently shorten the lease.
  defp arm(state, sent) do
    %{
      state
      | lease_deadline: sent + state.ttl_ms - state.safety_margin_ms,
        lease_renew_at: sent + state.renew_ms
    }
  end

  defp standby(state) do
    Process.send_after(self(), :acquire_lease, state.renew_ms)
    state
  end

  # Lose the lease: drop everything admitted but not finished, keep no cursor,
  # and wait to acquire again. In-flight delivery tasks are NOT killed — they
  # complete or fail on their own and their result messages are ignored, which
  # is at-least-once (a redelivery, never a loss).
  defp step_down(state) do
    # Report the loss once, then answer every waiter: nothing was durably
    # committed by this step-down.
    if state.lease != nil, do: LeaseHelpers.emit(:lost, state.lease)

    Enum.each(state.waiters, fn {from, _c0} -> GenServer.reply(from, {:ok, 0}) end)

    Enum.each(state.running, fn {ref, _job} -> Process.demonitor(ref, [:flush]) end)

    state
    |> Map.merge(%{
      lease: nil,
      pending: :gb_sets.empty(),
      remaining: %{},
      inflight_bytes: 0,
      lanes: %{},
      runnable: :queue.new(),
      running: %{},
      staged: :queue.new(),
      packing: %{},
      window_full?: false,
      waiters: []
    })
    |> standby()
  end

  defp mono_ms, do: System.monotonic_time(:millisecond)

  # A durable cursor read that cannot crash the pipeline: a WAL process that is
  # gone (or a cluster without a leader) is a reason to step down, not to crash.
  defp safe_get_cursor(instance, name) do
    try do
      {:ok, WAL.get_cursor(instance, name)}
    catch
      :exit, _ -> :error
      :error, _ -> :error
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
      :gb_sets.is_empty(state.pending) and :queue.is_empty(state.staged) and
      map_size(state.packing) == 0
  end

  # ── scheduling ────────────────────────────────────────────────────────────

  defp schedule(state) do
    Process.send_after(self(), :poll, state.config.dispatch.poll_ms)
    state
  end

  defp sleep(delay, nil), do: Process.sleep(delay)
  defp sleep(delay, cap), do: Process.sleep(min(delay, cap))
end
