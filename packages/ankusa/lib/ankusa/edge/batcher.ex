defmodule Ankusa.Edge.Batcher do
  @moduledoc """
  Group-commit batcher, one process per partition. Requests hand it an envelope
  and **block on the reply**. The batcher buffers callers, then flushes the whole
  batch to the queue in one `enqueue` (one fsync). Every blocked caller is replied
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

  ## Deadlines

  Every record carries a deadline: the latest moment its batch may *start*
  committing. A record still buffered at its deadline (a commit ahead of it is
  stalled) is answered `{:error, :store_unavailable}` and dropped, and the
  writer refuses a batch whose earliest deadline passed while it queued behind
  other work (`Ankusa.Queue.enqueue/3`). A batch the writer has started is never
  abandoned: its callers wait for the commit's outcome, however long the disk
  takes. A stall therefore never turns into a `503` for a hook that is then
  committed. The one way a `503` can still cover a durable hook is a process
  dying while the writer is mid-commit (the commit task killed, the batcher
  crashing and taking its tasks down, or the writer crashing after the sync but
  before it replies); the provider's retry then stores the hook a second time.
  The commit task is supervised, not linked: a task that dies takes only its own
  batch's callers with it, never the batcher or the records buffered behind it.
  """

  use GenServer

  require Logger

  alias Ankusa.Queue

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
  Submit a record and block until it is durably committed (or shed).

  Returns `{:committed, env}` | `{:duplicate, env}` | `{:error, :overload}` |
  `{:error, :store_unavailable}`. The queue itself is never allowed to crash the
  call: a failed commit (or a dead writer process) is reported as
  `:store_unavailable`, which the edge maps to `503`.

  `timeout` (milliseconds, or `:infinity`) bounds how long the record may wait
  before its batch *starts* committing. Once it has started the call waits for
  the outcome, so a slow disk is never answered `:store_unavailable` for a hook
  it then commits (see the moduledoc for the crash windows that still can).
  """
  @spec commit(atom(), non_neg_integer(), Queue.entry(), timeout()) ::
          {:committed, Ankusa.Envelope.t()}
          | {:duplicate, Ankusa.Envelope.t()}
          | {:error, :overload | :store_unavailable}
  def commit(instance, partition, record, timeout \\ 15_000) do
    GenServer.call(
      Ankusa.via(instance, {:batcher, partition}),
      {:enqueue, record, deadline(timeout)},
      :infinity
    )
  end

  # `:infinity` stays an atom all the way to the writer. Atoms sort above every
  # integer, so it is never `<= now`, never the minimum of a batch that holds a
  # real deadline, and never earlier than an armed one.
  defp deadline(:infinity), do: :infinity
  defp deadline(timeout_ms), do: System.monotonic_time(:millisecond) + timeout_ms

  # ── server ────────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    b = Ankusa.config(instance).batcher

    # Linked on purpose, like `Dispatch.Pipeline`: the commit tasks must not
    # outlive the batcher.
    {:ok, task_sup} = Task.Supervisor.start_link()

    {:ok,
     %{
       instance: instance,
       max_batch: b.max_batch,
       max_delay_ms: b.max_delay_ms,
       max_queue: b.max_queue,
       max_queue_bytes: b.max_queue_bytes,
       task_sup: task_sup,
       # newest-first: [{from, record, deadline}]
       buffer: [],
       count: 0,
       # the body bytes in `buffer`
       bytes: 0,
       # nil | %{ref: reference, entries: [{from, record, deadline}], bytes: n}
       inflight: nil,
       timer: nil,
       # The timer that answers buffered records at their deadline, and the
       # deadline it was armed for.
       expire_timer: nil,
       expire_at: nil
     }}
  end

  # A crash report prints the state and the last message, and every record —
  # buffered, in flight, or the `{:enqueue, record, deadline}` being handled —
  # carries its source's `{module, opts}` sinks (credentials) and the hook's
  # body: report how many, not what.
  @impl true
  def format_status(status) do
    Map.new(status, fn
      {:state, %{buffer: _, inflight: _} = state} ->
        {:state, %{state | buffer: state.count, inflight: inflight_size(state)}}

      {:message, {:enqueue, _record, deadline}} ->
        {:message, {:enqueue, :redacted, deadline}}

      other ->
        other
    end)
  end

  @impl true
  def handle_call({:enqueue, record, deadline}, from, state) do
    size = record_bytes(record)
    queued = state.count + inflight_size(state)
    queued_bytes = state.bytes + inflight_bytes(state)

    # A record bigger than the byte bound on its own is still admitted into an
    # empty batcher: `max_body_bytes` is the per-record limit, this one bounds
    # how much a partition holds.
    over_bytes? = queued > 0 and queued_bytes + size > state.max_queue_bytes

    if queued >= state.max_queue or over_bytes? do
      Ankusa.Telemetry.emit([:load_shed], %{queue: queued, bytes: queued_bytes}, %{
        instance: state.instance
      })

      {:reply, {:error, :overload}, state}
    else
      state = %{
        state
        | buffer: [{from, record, deadline} | state.buffer],
          count: state.count + 1,
          bytes: state.bytes + size
      }

      {:noreply, state |> maybe_flush() |> ensure_expiry(from, deadline)}
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

  def handle_info(:expire, state) do
    state = cancel_expiry(state)
    {expired, state} = drop_expired(state)

    if expired > 0 do
      Logger.warning(
        "[ankusa] #{expired} hook(s) passed their commit deadline behind a stalled commit; " <>
          "answered 503, nothing committed"
      )
    end

    {:noreply, rearm_expiry(state)}
  end

  def handle_info({ref, result}, %{inflight: %{ref: ref, entries: entries}} = state) do
    Process.demonitor(ref, [:flush])
    state = %{state | inflight: nil}

    case result do
      {:ok, results} ->
        entries
        |> Enum.zip(results)
        |> Enum.each(fn {{from, _record, _deadline}, replied} ->
          GenServer.reply(from, replied)
        end)

      {:error, reason} ->
        # Nothing was acked: every caller in the batch gets a 503-mapped error.
        Logger.warning("[ankusa] store commit failed: #{inspect(reason)}")

        Enum.each(entries, fn {from, _record, _deadline} ->
          GenServer.reply(from, {:error, :store_unavailable})
        end)
    end

    {:noreply, if(state.count > 0, do: start_commit(state), else: state)}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{inflight: %{ref: ref}} = state) do
    # The commit task died without delivering a result (killed from outside;
    # `safe_enqueue/3` turns every error it could catch into a value). The task
    # is not linked, so the batcher and the records buffered behind this batch
    # carry on. Fail the batch's callers instead of leaving them blocked
    # forever. If the task's call is still waiting in the writer's mailbox the
    # writer sees its caller is gone and commits nothing. If the writer is
    # already mid-commit, that batch is durable and its callers were told 503.
    Logger.warning("[ankusa] store commit task died: #{inspect(reason)}")

    Enum.each(state.inflight.entries, fn {from, _record, _deadline} ->
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

  defp inflight_bytes(%{inflight: nil}), do: 0
  defp inflight_bytes(%{inflight: %{bytes: bytes}}), do: bytes

  defp record_bytes(%{envelope: %{body: body}}) when is_binary(body), do: byte_size(body)
  defp record_bytes(_record), do: 0

  defp entries_bytes(entries),
    do:
      Enum.reduce(entries, 0, fn {_from, record, _deadline}, acc -> acc + record_bytes(record) end)

  # A commit in flight already owns the batcher's turn; the new record waits
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
    {_expired, state} = drop_expired(state)

    if state.count == 0 do
      state
    else
      {entries, remainder} = split_batch(state.buffer, state.max_batch)
      records = Enum.map(entries, fn {_from, record, _deadline} -> record end)
      deadline = entries |> Enum.map(fn {_from, _record, deadline} -> deadline end) |> Enum.min()
      instance = state.instance

      task =
        Task.Supervisor.async_nolink(state.task_sup, fn ->
          safe_enqueue(instance, records, deadline)
        end)

      batch_bytes = entries_bytes(entries)

      %{
        state
        | buffer: remainder,
          count: length(remainder),
          bytes: state.bytes - batch_bytes,
          inflight: %{ref: task.ref, entries: entries, bytes: batch_bytes}
      }
    end
  end

  # The commit runs in a Task, so a failing store has to come back as a value.
  # The task is not linked, but a value is still better than a `:DOWN`: it says
  # why, and the batch's callers get their answer in one message.
  defp safe_enqueue(instance, records, deadline) do
    Queue.enqueue(instance, records, deadline)
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

  # ── deadlines ─────────────────────────────────────────────────────────────

  defp now, do: System.monotonic_time(:millisecond)

  # Answers and drops every buffered record past its deadline. Returns how many.
  defp drop_expired(%{count: 0} = state), do: {0, state}

  defp drop_expired(state) do
    now = now()
    {expired, live} = Enum.split_with(state.buffer, fn {_from, _record, d} -> d <= now end)

    Enum.each(expired, fn {from, _record, _deadline} ->
      GenServer.reply(from, {:error, :store_unavailable})
    end)

    {length(expired),
     %{state | buffer: live, count: length(live), bytes: state.bytes - entries_bytes(expired)}}
  end

  # Only a record that is still buffered (it is the newest entry) needs the
  # timer: one that went straight into a commit is the writer's to refuse.
  defp ensure_expiry(%{buffer: [{from, _record, _deadline} | _]} = state, from, deadline) do
    cond do
      state.expire_timer == nil -> arm_expiry(state, deadline)
      deadline < state.expire_at -> state |> cancel_expiry() |> arm_expiry(deadline)
      true -> state
    end
  end

  defp ensure_expiry(state, _from, _deadline), do: state

  defp rearm_expiry(%{buffer: []} = state), do: state

  defp rearm_expiry(state) do
    deadline = state.buffer |> Enum.map(fn {_from, _record, d} -> d end) |> Enum.min()
    arm_expiry(state, deadline)
  end

  # A record with no deadline never expires: no timer to arm.
  defp arm_expiry(state, :infinity), do: state

  defp arm_expiry(state, deadline) do
    ref = Process.send_after(self(), :expire, max(deadline - now(), 0))
    %{state | expire_timer: ref, expire_at: deadline}
  end

  defp cancel_expiry(%{expire_timer: nil} = state), do: state

  defp cancel_expiry(%{expire_timer: ref} = state) do
    Process.cancel_timer(ref)
    %{state | expire_timer: nil, expire_at: nil}
  end
end
