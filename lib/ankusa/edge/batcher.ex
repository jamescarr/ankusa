defmodule Ankusa.Edge.Batcher do
  @moduledoc """
  Group-commit batcher, one process per partition. Requests hand it an envelope
  and **block on the reply**. The batcher buffers callers, then flushes the whole
  batch to the WAL in one `append` (one fsync). Every blocked caller is replied
  to *after* the commit returns — that is what makes the ack honest.

  The append itself runs in a `Task`, so the batcher keeps accepting requests
  while a commit is in flight: the next batch accumulates behind it and commits
  the instant the previous one finishes. Nothing waits on a commit except the
  callers whose own records are in it — with `max_delay_ms: 0` that is the
  steady state, and the batch is naturally as large as concurrency allows.

  The queue is bounded, and the bound counts buffered *and* in-flight records.
  When it is reached the batcher sheds load: the caller gets `{:error,
  :overload}`, which the edge turns into a `503` with `Retry-After`. Never ack
  what you haven't saved.
  """

  use GenServer

  require Logger

  alias Ankusa.WAL

  # ── api ───────────────────────────────────────────────────────────────────

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    partition = Keyword.fetch!(opts, :partition)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, {:batcher, partition}))
  end

  def child_spec(opts) do
    partition = Keyword.fetch!(opts, :partition)

    %{
      id: {__MODULE__, Keyword.fetch!(opts, :instance), partition},
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @doc """
  Submit a record and block until it is durably committed (or deduped, or shed).

  Returns `{:committed, env}` | `{:duplicate, seq}` | `{:error, :overload}` |
  `{:error, :store_unavailable}`. The WAL itself is never allowed to crash the
  call: a failed append (or a dead WAL process) is reported as
  `:store_unavailable`, which the edge maps to `503`.
  """
  @spec commit(atom(), non_neg_integer(), WAL.entry(), timeout()) ::
          {:committed, Ankusa.Envelope.t()}
          | {:duplicate, non_neg_integer()}
          | {:error, :overload | :store_unavailable}
  def commit(instance, partition, record, timeout \\ 15_000) do
    GenServer.call(Ankusa.via(instance, {:batcher, partition}), {:enqueue, record}, timeout)
  end

  # ── server ────────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    b = config.batcher

    {:ok,
     %{
       instance: config.instance,
       max_batch: b.max_batch,
       max_delay_ms: b.max_delay_ms,
       max_queue: b.max_queue,
       # newest-first
       buffer: [],
       count: 0,
       # nil | %{ref: reference, entries: [{from, record}]}
       inflight: nil,
       timer: nil
     }}
  end

  @impl true
  def handle_call({:enqueue, record}, from, state) do
    if state.count + inflight_size(state) >= state.max_queue do
      Ankusa.Telemetry.emit([:load_shed], %{queue: state.count + inflight_size(state)}, %{
        instance: state.instance
      })

      {:reply, {:error, :overload}, state}
    else
      state = %{state | buffer: [{from, record} | state.buffer], count: state.count + 1}
      {:noreply, maybe_flush(state)}
    end
  end

  @impl true
  def handle_info(:flush, state) do
    state = %{state | timer: nil}

    if state.inflight == nil and state.count > 0 do
      {:noreply, start_commit(state)}
    else
      {:noreply, state}
    end
  end

  def handle_info({ref, result}, %{inflight: %{ref: ref, entries: entries}} = state) do
    Process.demonitor(ref, [:flush])
    state = %{state | inflight: nil}

    case result do
      {:ok, results} ->
        entries
        |> Enum.zip(results)
        |> Enum.each(fn {{from, _record}, replied} -> GenServer.reply(from, replied) end)

      {:error, reason} ->
        # Nothing was acked: every caller in the batch gets a 503-mapped error.
        Logger.warning("[ankusa] WAL append failed: #{inspect(reason)}")

        Enum.each(entries, fn {from, _record} ->
          GenServer.reply(from, {:error, :store_unavailable})
        end)
    end

    {:noreply, if(state.count > 0, do: start_commit(state), else: state)}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{inflight: %{ref: ref}} = state) do
    # Defensive: the commit task died without delivering a result (a kill, not
    # an error it could catch). Fail its callers instead of leaving them
    # blocked until their call times out.
    Logger.warning("[ankusa] WAL append task died: #{inspect(reason)}")

    Enum.each(state.inflight.entries, fn {from, _record} ->
      GenServer.reply(from, {:error, :store_unavailable})
    end)

    state = %{state | inflight: nil}
    {:noreply, if(state.count > 0, do: start_commit(state), else: state)}
  end

  # A stray message (a DOWN from a task we already demonitored, say) must not
  # take down a process with blocked callers on it.
  def handle_info(_message, state), do: {:noreply, state}

  # ── commits ───────────────────────────────────────────────────────────────

  defp inflight_size(%{inflight: nil}), do: 0
  defp inflight_size(%{inflight: %{entries: entries}}), do: length(entries)

  # A commit in flight already owns the batcher's WAL turn; the new record waits
  # in `buffer` and the completion flushes it immediately.
  defp maybe_flush(%{inflight: inflight} = state) when inflight != nil, do: state

  defp maybe_flush(state) do
    cond do
      state.count >= state.max_batch -> start_commit(state)
      state.max_delay_ms == 0 -> start_commit(state)
      state.timer == nil -> arm_timer(state)
      true -> state
    end
  end

  defp start_commit(state) do
    state = cancel_timer(state)
    {entries, remainder} = split_batch(state.buffer, state.max_batch)
    records = Enum.map(entries, fn {_from, record} -> record end)
    instance = state.instance

    task = Task.async(fn -> safe_append(instance, records) end)

    %{
      state
      | buffer: remainder,
        count: length(remainder),
        inflight: %{ref: task.ref, entries: entries}
    }
  end

  # The WAL append runs in a Task, so a failing WAL has to come back as a value:
  # an unhandled exit would take the batcher (linked to the task) and every
  # blocked caller's call down with it.
  defp safe_append(instance, records) do
    WAL.append(instance, records)
  rescue
    error -> {:error, error}
  catch
    :exit, reason -> {:error, reason}
  end

  # `buffer` is newest-first; commit is oldest-first.
  defp split_batch(buffer, max_batch) do
    {entries, remainder} = buffer |> Enum.reverse() |> Enum.split(max_batch)
    {entries, Enum.reverse(remainder)}
  end

  defp arm_timer(state) do
    %{state | timer: Process.send_after(self(), :flush, state.max_delay_ms)}
  end

  defp cancel_timer(%{timer: nil} = state), do: state

  defp cancel_timer(%{timer: ref} = state) do
    Process.cancel_timer(ref)
    %{state | timer: nil}
  end
end
