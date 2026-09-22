defmodule Ankusa.Edge.Batcher do
  @moduledoc """
  Group-commit batcher, one process per partition. Requests hand it an envelope
  and **block on the reply**. The batcher buffers callers, then flushes the whole
  batch to the WAL in one `append` (one fsync) every `max_delay_ms` or once
  `max_batch` items accumulate. Every blocked caller is replied to *after* the
  commit returns — that is what makes the ack honest.

  The queue is bounded. When full it sheds load: the caller gets `{:error,
  :overload}`, which the edge turns into a `503` with `Retry-After`. Never ack
  what you haven't saved.
  """

  use GenServer

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

  Returns `{:committed, env}` | `{:duplicate, seq}` | `{:error, :overload}`.
  A crash of the WAL surfaces as an exit from the call, which the edge maps to
  `503` — again, nothing was acked.
  """
  @spec commit(atom(), non_neg_integer(), WAL.entry(), timeout()) ::
          {:committed, Ankusa.Envelope.t()}
          | {:duplicate, non_neg_integer()}
          | {:error, :overload}
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
       buffer: [],
       count: 0,
       timer: nil
     }}
  end

  @impl true
  def handle_call({:enqueue, record}, from, state) do
    cond do
      state.count >= state.max_queue ->
        Ankusa.Telemetry.emit([:load_shed], %{queue: state.count}, %{instance: state.instance})
        {:reply, {:error, :overload}, state}

      true ->
        state = %{state | buffer: [{from, record} | state.buffer], count: state.count + 1}

        cond do
          state.count >= state.max_batch -> {:noreply, flush(state)}
          state.timer == nil -> {:noreply, arm_timer(state)}
          true -> {:noreply, state}
        end
    end
  end

  @impl true
  def handle_info(:flush, state), do: {:noreply, flush(state)}

  defp arm_timer(state) do
    %{state | timer: Process.send_after(self(), :flush, state.max_delay_ms)}
  end

  defp flush(%{buffer: []} = state), do: cancel_timer(state)

  defp flush(state) do
    state = cancel_timer(state)
    entries = Enum.reverse(state.buffer)
    records = Enum.map(entries, fn {_from, record} -> record end)

    # One append == one fsync for the whole partition batch. If the WAL crashes
    # this call raises; the batcher dies, callers' calls exit, the edge returns
    # 503, and nothing was acked. Crash-before-commit is safe by construction.
    {:ok, results} = WAL.append(state.instance, records)

    entries
    |> Enum.zip(results)
    |> Enum.each(fn {{from, _record}, result} -> GenServer.reply(from, result) end)

    %{state | buffer: [], count: 0}
  end

  defp cancel_timer(%{timer: nil} = state), do: state

  defp cancel_timer(%{timer: ref} = state) do
    Process.cancel_timer(ref)
    %{state | timer: nil}
  end
end
