defmodule Ankusa.Queue.Syncer do
  @moduledoc """
  The fsync of the queue's WAL, off the writer's critical path (DUR-4.b).

  `Ankusa.Queue.Writer` applies a group with `sync: false`, which makes it
  visible to every reader at once, and hands what must wait for the disk to this
  process: the replies of the group's callers (`{:reply, calls, reply}`), or, under
  `wal: :dual`, the writer's own notice (`{:notify, pid, ref}`) that the local
  half is durable. The writer then assembles the next group while this process
  syncs the one before it.

  One `Ankusa.Store.sync_wal/1` is in flight at a time. It takes every entry
  queued behind it and answers them all once the fsync returns, so the fsyncs
  are as many as the disk allows, not one per group. `sync_wal` does not enter
  RocksDB's write queue, so the writer's next `sync: false` write is applied
  meanwhile.

  ## The durable watermark

  Three counters (`marks`, an `:atomics` array the writer registers as
  `{instance, :queue_marks}`):

    * `durable_seq`: the highest seq every group up to which has been fsynced;
    * `applied_seq`: the highest seq the writer has applied (set *before* the
      write, so a reader that sees the batch also sees it as not yet durable);
    * `durable_at`: the stamp (`now` of the writer) of the newest durable group.

  A row is externally visible or binding only once it is durable (ground rules 1
  and 5): `visible/2` bounds what the dispatch pipeline may scan while a group is
  applied but not synced, the archive scan stops at `durable_seq`, and a reply is
  never sent before the sync that covers every seq it depends on (a `201
  duplicate` for a key written by a group still in the pipeline waits for that
  group's fsync too). Only the ack latency is pipelined: the visibility of a hook
  and the answer to its retry are not.

  A failed sync **fences** the queue. This process answers every entry it covers,
  and every one that arrives after, with the error, leaves `durable_seq` where it
  was and does not sync again; the store has been told
  (`Store.report_write_failure/2`, which reopens it) and the writer is told, which
  refuses new work, deletes in one synced write the rows of every group that was
  never acked, and only then sends `{:resume, seq, at}`, which raises the
  watermark to where the writer stands. So a hook whose provider was told `503`
  is never visible to dispatch (it is deleted before the watermark moves) and is
  not found by the provider's retry: it is stored once, by the retry.
  """

  use GenServer

  require Logger

  alias Ankusa.Store

  @durable 1
  @applied 2
  @durable_at 3

  @type marks :: :atomics.atomics_ref()

  @type entry :: %{
          upto: non_neg_integer(),
          at: integer(),
          done: {:reply, term(), term()} | {:notify, pid(), reference()}
        }

  # ── marks (read by the writer, dispatch and the archive scan) ─────────────

  @doc false
  @spec new_marks(non_neg_integer(), integer()) :: marks()
  def new_marks(seq, at) do
    marks = :atomics.new(3, signed: true)
    :atomics.put(marks, @durable, seq)
    :atomics.put(marks, @applied, seq)
    :atomics.put(marks, @durable_at, at)
    marks
  end

  @doc false
  def applied(marks), do: :atomics.get(marks, @applied)

  @doc false
  def set_applied(marks, seq), do: :atomics.put(marks, @applied, seq)

  @doc false
  def durable(marks), do: :atomics.get(marks, @durable)

  @doc "Whether a group is applied that no fsync has covered yet."
  @spec unsynced?(marks()) :: boolean()
  def unsynced?(marks), do: applied(marks) > durable(marks)

  @doc """
  Everything up to `seq` (stamp `at`) is durable: raises the watermark, never
  lowers it. The writer calls it after a `sync: true` write, which fsyncs the
  whole WAL.
  """
  @spec mark_durable(marks(), non_neg_integer(), integer()) :: :ok
  def mark_durable(marks, seq, at) do
    raise_to(marks, @durable_at, at)
    raise_to(marks, @durable, seq)
  end

  defp raise_to(marks, index, value) do
    current = :atomics.get(marks, index)

    cond do
      value <= current ->
        :ok

      :atomics.compare_exchange(marks, index, current, value) == :ok ->
        :ok

      true ->
        raise_to(marks, index, value)
    end
  end

  @doc """
  The latest time dispatch may treat as "now": `now` while every applied group is
  durable, else the stamp of the newest durable group less one millisecond, so a
  row of a group still in the pipeline (stamped at or after it) is never due.
  """
  @spec visible(atom(), integer()) :: integer()
  def visible(instance, now) do
    case Registry.lookup(Ankusa.Registry, {instance, :queue_marks}) do
      [{_pid, marks}] ->
        if unsynced?(marks), do: min(now, :atomics.get(marks, @durable_at) - 1), else: now

      [] ->
        now
    end
  end

  @doc "The seq the archive may read up to: every row at or below it is durable."
  @spec durable_seq(atom()) :: non_neg_integer() | :infinity
  def durable_seq(instance) do
    case Registry.lookup(Ankusa.Registry, {instance, :queue_marks}) do
      [{_pid, marks}] -> durable(marks)
      [] -> :infinity
    end
  end

  @doc """
  Whether `now` is later than what `visible/2` lets dispatch see: a row that is
  due but hidden is waiting for an fsync, and dispatch is woken when it returns.
  """
  @spec hidden?(atom(), integer()) :: boolean()
  def hidden?(instance, now), do: visible(instance, now) < now

  # ── the process ───────────────────────────────────────────────────────────

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc "Queue `entry` for the next fsync; answered when one covers it."
  @spec submit(pid(), entry()) :: :ok
  def submit(syncer, entry) do
    send(syncer, {:sync, entry})
    :ok
  end

  @doc """
  The writer has deleted the rows of the groups a failed fsync left: every seq up
  to `seq` is settled (stamp `at`), and the syncer syncs again.
  """
  @spec resume(pid(), non_neg_integer(), integer()) :: :ok
  def resume(syncer, seq, at) do
    send(syncer, {:resume, seq, at})
    :ok
  end

  @impl true
  def init(opts) do
    {:ok,
     %{
       instance: Keyword.fetch!(opts, :instance),
       marks: Keyword.fetch!(opts, :marks),
       writer: Keyword.fetch!(opts, :writer),
       fenced: false
     }}
  end

  @impl true
  def handle_info({:sync, entry}, state), do: {:noreply, run(drain([entry]), state)}

  def handle_info({:resume, seq, at}, state) do
    mark_durable(state.marks, seq, at)
    {:noreply, %{state | fenced: false}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp drain(entries) do
    receive do
      {:sync, entry} -> drain([entry | entries])
    after
      0 -> Enum.reverse(entries)
    end
  end

  defp run(entries, %{fenced: true} = state) do
    finish(entries, {:error, :store_unavailable}, state)
  end

  defp run(entries, state) do
    {upto, at} = covered(entries)

    if upto <= durable(state.marks) do
      # A `sync: true` write has fsynced the WAL since: nothing to wait for.
      finish(entries, :ok, state)
    else
      case sync(state.instance, length(entries)) do
        :ok ->
          mark_durable(state.marks, upto, at)
          finish(entries, :ok, state)

        {:error, reason} = error ->
          Logger.error(
            "[ankusa] WAL sync failed, #{length(entries)} group(s) not acked: #{inspect(reason)}"
          )

          send(state.writer, {:sync_failed, reason})
          finish(entries, error, %{state | fenced: true})
      end
    end
  end

  defp covered(entries) do
    Enum.reduce(entries, {0, 0}, fn entry, {upto, at} ->
      {max(upto, entry.upto), max(at, entry.at)}
    end)
  end

  defp sync(instance, groups) do
    Ankusa.Telemetry.span([:wal_sync], %{instance: instance}, fn ->
      case Store.sync_wal(instance) do
        :ok -> {:ok, %{groups: groups}, %{}}
        {:error, reason} -> throw({:wal_sync_failed, reason})
      end
    end)
  catch
    :throw, {:wal_sync_failed, reason} -> {:error, reason}
  end

  # The marks are raised before anyone is answered or woken, so whoever hears of
  # the sync also sees the watermark it moved.
  defp finish(entries, result, state) do
    Enum.each(entries, &answer(&1, result))

    with :ok <- result, [_ | _] <- entries do
      wake_dispatch(state.instance, entries |> Enum.map(& &1.at) |> Enum.min())
    end

    state
  end

  defp answer(%{done: {:reply, calls, reply}}, :ok), do: Ankusa.Queue.Writer.reply_calls(calls, reply)

  defp answer(%{done: {:reply, calls, _reply}}, {:error, _} = error),
    do: Ankusa.Queue.Writer.reply_calls(calls, error)

  defp answer(%{done: {:notify, pid, ref}}, result), do: send(pid, {:local_synced, ref, result})

  # The rows became visible only now, though they were stamped before the fsync:
  # a scan may already have gone past that stamp.
  defp wake_dispatch(instance, at) do
    case Ankusa.whereis(instance, :dispatch) do
      pid when is_pid(pid) -> send(pid, {:wake, at})
      nil -> :ok
    end
  end
end
