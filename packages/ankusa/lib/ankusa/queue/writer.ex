defmodule Ankusa.Queue.Writer do
  @moduledoc """
  The only seq assigner: commits batches of hooks to the store with
  `sync: true`, so a `2xx` to the provider always means the bytes are on disk.

  One process per instance, under `Ankusa.via(instance, :queue_writer)`. The
  commit is a single atomic store batch: the hook, one pending delivery row and
  due key per sink, the archive obligation when the `:storage` role runs, and
  the `m:next_seq` marker. A failed commit consumes its seqs (gaps are
  allowed) but acks nothing.
  """

  use GenServer

  require Logger

  alias Ankusa.Config
  alias Ankusa.Store
  alias Ankusa.Store.Keys

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :queue_writer))
  end

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    config = Keyword.fetch!(opts, :config)

    # A seq is never reused: if either read fails, refuse to start rather than
    # guess a floor.
    with {:ok, marker} <- marker_seq(instance),
         {:ok, last} <- last_hook_seq(instance) do
      next_seq = max(marker, last + 1)

      Logger.info("[ankusa] store next_seq=#{next_seq}")

      {:ok,
       %{
         instance: instance,
         next_seq: next_seq,
         archive?: Config.role?(config, :storage),
         last_reopen: nil,
         last_at: 0
       }}
    else
      {:error, reason} -> {:stop, {:queue_writer_init_failed, reason}}
    end
  end

  defp marker_seq(instance) do
    case Store.get(instance, :default, Keys.meta("next_seq")) do
      {:ok, <<n::64>>} -> {:ok, n}
      :not_found -> {:ok, 1}
      {:error, reason} -> {:error, reason}
    end
  end

  defp last_hook_seq(instance) do
    %{hi: hi} = Keys.family(:hooks)

    Store.fold(
      instance,
      :hooks,
      {Keys.hook(1), hi},
      0,
      fn key, _v, _acc -> {:halt, :binary.decode_unsigned(key)} end,
      reverse: true
    )
  end

  @impl true
  def handle_call({:enqueue, items}, _from, state) do
    # Never behind a previous batch, even if the wall clock steps back: dispatch
    # keeps its scan floor just under the stamps it has seen, and relies on
    # stamps never going down.
    now = max(System.system_time(:millisecond), state.last_at)

    {ops, batch_size, bytes} = build_ops(items, state.next_seq, now, state.archive?)

    result =
      commit(state.instance, ops, batch_size, bytes)

    case result do
      :ok ->
        wake_dispatch(state.instance, now)

        committed =
          items
          |> Enum.with_index()
          |> Enum.map(fn {{env, _bin, _mods}, i} ->
            {:committed, %{env | seq: state.next_seq + i}}
          end)

        {:reply, {:ok, committed},
         %{state | next_seq: state.next_seq + length(items), last_at: now}}

      {:error, reason} ->
        Logger.error(
          "[ankusa] store commit of #{length(items)} hook(s) failed, nothing acked: #{inspect(reason)}"
        )

        state = maybe_reopen(state)

        {:reply, {:error, reason},
         %{state | next_seq: state.next_seq + length(items), last_at: now}}
    end
  end

  # ── batch construction ───────────────────────────────────────────────────

  defp build_ops(items, first_seq, now, archive?) do
    items
    |> Enum.with_index(first_seq)
    |> Enum.reduce({[], 0, 0}, fn {{_env, bin, mods}, seq}, {ops, count, bytes} ->
      size = byte_size(bin)

      row_ops =
        mods
        |> Enum.with_index()
        |> Enum.flat_map(fn {mod, i} ->
          row =
            :erlang.term_to_binary(%{
              module: mod,
              state: :pending,
              attempts: 0,
              at: now,
              error: nil,
              size: size
            })

          [
            {:put, :deliveries, Keys.delivery(seq, i), row},
            {:put, :index, Keys.due(now, seq, i), <<size::32>>}
          ]
        end)

      archive_ops =
        if archive?, do: [{:put, :index, Keys.archive_pending(seq), <<size::32>>}], else: []

      # A hook with no obligations (no sinks, archive off) consumes a seq but
      # writes nothing.
      ops =
        if row_ops == [] and archive_ops == [] do
          ops
        else
          [{:put, :hooks, Keys.hook(seq), bin} | row_ops] ++ archive_ops ++ ops
        end

      {ops, count + 1, bytes + size}
    end)
    |> then(fn {ops, count, bytes} ->
      {[{:put, :default, Keys.meta("next_seq"), <<first_seq + count::64>>} | ops], count, bytes}
    end)
  end

  # ── commit ───────────────────────────────────────────────────────────────

  # One atomic, synced batch covers every hook. Anything but `:ok` means the
  # batch is not durable — the span then emits `[:ankusa, :commit, :exception]`
  # (never a `:stop`), so the commit duration/batch-size series keep counting
  # only real commits.
  defp commit(instance, ops, batch_size, bytes) do
    Ankusa.Telemetry.span([:commit], %{instance: instance}, fn ->
      case Store.write(instance, ops, sync: true) do
        :ok -> {:ok, %{batch_size: batch_size, bytes: bytes}, %{}}
        {:error, reason} -> throw({:store_commit_failed, reason})
      end
    end)
  catch
    :throw, {:store_commit_failed, reason} -> {:error, reason}
  end

  # After a failed commit the database may be latched in a background error
  # (ENOSPC on the WAL) that survives freeing the space. Reopening clears it.
  # At most once per interval, and a missing store never crashes the Writer.
  @reopen_interval_ms 5_000

  defp maybe_reopen(state) do
    now = System.monotonic_time(:millisecond)

    if state.last_reopen == nil or now - state.last_reopen >= @reopen_interval_ms do
      result =
        try do
          Store.reopen(state.instance)
        catch
          :exit, reason -> {:error, {:store_down, reason}}
        end

      case result do
        :ok -> Logger.warning("[ankusa] store reopened after a failed commit")
        {:error, reason} -> Logger.error("[ankusa] store reopen failed: #{inspect(reason)}")
      end

      %{state | last_reopen: now}
    else
      state
    end
  end

  # The stamp tells dispatch the earliest due time this batch can have: its rows
  # became visible only now, and may be older than anything it has scanned past.
  defp wake_dispatch(instance, at) do
    case Ankusa.whereis(instance, :dispatch) do
      pid when is_pid(pid) -> send(pid, {:wake, at})
      nil -> :ok
    end
  end
end
