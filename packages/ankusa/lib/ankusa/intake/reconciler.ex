defmodule Ankusa.Intake.Reconciler do
  @moduledoc """
  Carries half-persisted hooks to completion (ADR 0002, sections 3 and 5). Under
  `wal: :dual` a record the writer issued to both places may end with only one of
  them: the local store (the log refused or timed out) or the log (the store
  write failed). The provider got a `503`, and the half that persisted still
  exists, so the record is **completed, never undone**:

    * log half missing: publish it again (up to 256 records at a time)
    * local half missing: `Queue.Writer` re-applies the batch's ops, after one
      point read says the first write did not land after all
    * both missing: local first (the cheaper half, and it makes the hook
      deliverable), then the log

  Retries back off per record from 100 ms to 5 s, jittered; a `throttled` class
  honours its `retry_after`; a `permanent` class (the record can never fit, the
  topic is gone) never completes and is *stuck* straight away. Nothing is ever
  dropped. A record not completed within `intake.reconcile_deadline_ms` is
  stuck: its fleet dedupe claim is released (a retry is then admitted as a new
  hook with the same `idempotency_key`, never an acked loss) and the reconciler
  keeps trying.

  The claim of a carried hook is neither promoted nor released until both halves
  are confirmed (`hold/3`); the reconciler promotes it then.

  This is the live component. Boot-time recovery is `Ankusa.Intake.Recovery`.
  """

  use GenServer

  require Logger

  alias Ankusa.Edge.Fleet
  alias Ankusa.Intake
  alias Ankusa.Intake.{Gate, Pending}

  @tick_ms 100
  @publish_batch 256
  @min_backoff_ms 100
  @max_backoff_ms 5_000
  @permanent_backoff_ms 60_000

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :intake_reconciler))
  end

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc """
  Hand the reconciler the dedupe claim of carried hook `id`. `:held`: it will
  promote it when both halves are confirmed. `:completed`: it already finished,
  promote it yourself. `:stuck`: release it yourself. ETS only; no call.
  """
  @spec hold(atom(), String.t(), term()) :: :held | :completed | :stuck
  def hold(instance, id, claim), do: Pending.hold(Pending.table(instance), id, claim)

  @doc "Run a pass now (the writer calls it after it carried a batch)."
  @spec wake(atom()) :: :ok
  def wake(instance) do
    case Ankusa.whereis(instance, :intake_reconciler) do
      pid when is_pid(pid) -> send(pid, :tick_now)
      nil -> :ok
    end

    :ok
  end

  @doc "Settles the claims of entries a pass or the writer completed."
  @spec finish(atom(), [tuple()], :local | :log) :: :ok
  def finish(instance, entries, half) do
    Enum.each(entries, fn entry ->
      Fleet.promote(instance, elem(entry, 8), elem(entry, 1))

      Ankusa.Telemetry.emit([:intake, :reconciled], %{count: 1}, %{
        instance: instance,
        half: half
      })
    end)
  end

  # ── server ────────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    send(self(), :tick)

    {:ok,
     %{
       instance: instance,
       table: Pending.table(instance),
       timer: nil,
       last_probe: 0
     }}
  end

  @impl true
  def handle_info(:tick_now, state), do: {:noreply, pass(state)}

  def handle_info(:tick, state), do: {:noreply, pass(state)}

  def handle_info(_message, state), do: {:noreply, state}

  # One pass, then the next tick. A failing pass is logged, never fatal.
  defp pass(state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    now = System.monotonic_time(:millisecond)
    config = Ankusa.config(state.instance)

    state =
      try do
        if Pending.recovering?(state.table) do
          state
        else
          state = probe(state, config, now)
          due(state, now)
          sweep_stuck(state, config, now)
          state
        end
      rescue
        exception ->
          Logger.error("[ankusa] intake reconciler pass failed: #{Exception.message(exception)}")
          state
      catch
        kind, reason ->
          Logger.error("[ankusa] intake reconciler pass failed: #{inspect({kind, reason})}")
          state
      end

    %{state | timer: Process.send_after(self(), :tick, @tick_ms)}
  end

  # The adapter's cheap readiness opens the breaker before a publish has to fail.
  defp probe(state, config, now) do
    if now - state.last_probe >= config.intake.probe_ms do
      unless Intake.ready?(state.instance), do: Gate.not_ready(state.table, now)
      %{state | last_probe: now}
    else
      state
    end
  end

  defp due(state, now) do
    t = state.table

    entries =
      t
      |> Pending.due(now, @publish_batch)
      |> Enum.map(&Pending.lookup(t, &1))
      |> Enum.reject(&is_nil/1)

    if entries != [] do
      entries = complete_local(state, entries, now)
      publish(state, entries, now)
    end

    :ok
  end

  # The local half of every entry that lacks it. Returns the entries whose local
  # half is now in the store.
  defp complete_local(state, entries, now) do
    t = state.table
    missing = Enum.filter(entries, &(elem(&1, 3) == :missing))
    batch_ids = missing |> Enum.map(&elem(&1, 7)) |> Enum.uniq()

    done =
      if batch_ids == [] do
        []
      else
        case apply_local(state.instance, batch_ids) do
          {:ok, done_ids, completed} ->
            finish(state.instance, completed, :local)
            done_ids

          :error ->
            []
        end
      end

    Enum.flat_map(entries, fn entry ->
      cond do
        elem(entry, 3) == :ok ->
          [entry]

        elem(entry, 7) in done ->
          case Pending.lookup(t, seq_of(entry)) do
            nil -> []
            live -> [live]
          end

        true ->
          backoff(t, entry, nil, now)
          []
      end
    end)
  end

  defp apply_local(instance, batch_ids) do
    case GenServer.call(Ankusa.via(instance, :queue_writer), {:complete_local, batch_ids}, 30_000) do
      {:ok, done_ids, completed} -> {:ok, done_ids, completed}
      _ -> :error
    end
  catch
    :exit, _ -> :error
  end

  # The log half of the entries whose local half is in: one republish.
  defp publish(_state, [], _now), do: :ok

  defp publish(state, entries, now) do
    t = state.table
    pending = Enum.filter(entries, &(elem(&1, 4) == :missing and elem(&1, 6) != nil))

    if pending != [] do
      records = Enum.map(pending, &{seq_of(&1), elem(&1, 6), elem(&1, 14)})

      case Intake.publish_batch(state.instance, records) do
        {:ok, position} ->
          first = position - length(records) + 1

          completed =
            Enum.flat_map(pending, fn entry ->
              Pending.log_done(t, seq_of(entry), max(first, 0))
            end)

          finish(state.instance, completed, :log)

        {:error, :permanent, _} ->
          Enum.each(pending, &stick(state, &1, now, :permanent))

        {:error, _class, retry_after} ->
          Enum.each(pending, &backoff(t, &1, retry_after, now))
      end
    end

    :ok
  end

  defp seq_of(
         {{:p, seq}, _id, _size, _local, _log, _first_at, _bin, _batch, _claim, _triple,
          _position, _attempts, _stuck, _next_at, _meta}
       ), do: seq

  defp backoff(t, entry, retry_after, now) do
    attempts = elem(entry, 11)
    base = min(@max_backoff_ms, @min_backoff_ms * Integer.pow(2, min(attempts, 10)))
    delay = trunc(base * (0.5 + :rand.uniform() / 2))
    Pending.reschedule(t, seq_of(entry), now + max(delay, retry_after || 0))
  end

  # A `permanent` failure never completes: stuck at once, retried slowly.
  defp stick(state, entry, now, class) do
    seq = seq_of(entry)

    if Pending.mark_stuck(state.table, seq) do
      Logger.error(
        "[ankusa] intake record seq=#{seq} id=#{elem(entry, 1)} cannot be published (#{class}); " <>
          "stuck, retried every #{div(@permanent_backoff_ms, 1000)} s"
      )

      Fleet.release(state.instance, Pending.take_claim(state.table, seq))
      stuck_event(state.instance)
    end

    Pending.reschedule(state.table, seq, now + @permanent_backoff_ms)
  end

  # Past `reconcile_deadline_ms` a record is stuck: its claim goes (the retry is
  # admitted as a new hook with the same idempotency key), the retries go on.
  defp sweep_stuck(state, config, now) do
    deadline = config.intake.reconcile_deadline_ms
    wall = now

    Enum.each(Pending.carried(state.table), fn seq ->
      with entry when entry != nil <- Pending.lookup(state.table, seq),
           false <- elem(entry, 12),
           true <- wall - elem(entry, 5) >= deadline,
           true <- Pending.mark_stuck(state.table, seq) do
        Logger.error(
          "[ankusa] intake record seq=#{seq} id=#{elem(entry, 1)} not completed within " <>
            "#{deadline} ms; its dedupe claim is released, reconciling continues"
        )

        Fleet.release(state.instance, Pending.take_claim(state.table, seq))
        stuck_event(state.instance)
      else
        _ -> :ok
      end
    end)
  end

  defp stuck_event(instance),
    do: Ankusa.Telemetry.emit([:intake, :stuck], %{count: 1}, %{instance: instance})
end
