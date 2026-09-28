defmodule Ankusa.Storage.Index do
  @moduledoc """
  Durable, append-only index mapping an event id to its byte range inside a
  compacted segment.

  Rows are framed terms (see `Ankusa.DurableLog`) at
  `Config.path(config, "segments/index.log")`. A torn trailing row — a write that
  never completed — is dropped, mirroring the WAL's crash-safety discipline.

  ## The in-memory table

  The file is written once per compaction tick and read on every
  `Ankusa.Storage.fetch/2`, which is the replay/audit path a dashboard drives.
  Replaying the file per lookup would make every fetch O(events), so
  `Ankusa.Storage.Compactor` — the only writer — owns an ETS table of the same
  rows and keeps it current:

    * `open/1` loads the file once, at the compactor's start;
    * `append/2` adds what a tick just wrote, so per-tick work is proportional to
      the new rows rather than to the whole index, and nothing is re-decoded;
    * `lookup/2` is an `:ets.lookup/2` on a `read_concurrency` table, from any
      process — it never queues behind a tick, which a `GenServer.call` into the
      compactor would.

  Rows go in with `:ets.insert_new/2`, so the first row wins: an event that was
  compacted twice still resolves to its earliest frame, matching the linear scan
  this replaces.

  The table is `:protected` and dies with the compactor, and it is found through
  `Ankusa.Registry` rather than a name of its own, so a stale reference is not
  possible. A lookup that finds no table — a node without the `:storage` role, or
  the moment between a crash and the restart that reloads it — reads the file
  instead, so a lookup is never wrong, only occasionally the old speed.
  """

  alias Ankusa.{Config, DurableLog}

  @type row :: %{
          event_id: String.t(),
          source_id: String.t(),
          tenant_id: String.t(),
          received_at: integer(),
          seq: non_neg_integer(),
          segment_key: String.t(),
          offset: non_neg_integer(),
          length: pos_integer()
        }

  @doc """
  Create the live table from the on-disk index. Called by the compactor, which
  owns it; the file stays the durable record and the only thing read at startup.
  """
  @spec open(Config.t()) :: :ets.table()
  def open(%Config{} = config) do
    table = :ets.new(:ankusa_storage_index, [:set, :protected, read_concurrency: true])

    for row <- DurableLog.read(path(config)) do
      :ets.insert_new(table, {row.event_id, row})
    end

    {:ok, _pid} = Registry.register(Ankusa.Registry, key(config.instance), table)
    table
  end

  @doc """
  Append rows, keeping the live table in step.

  The table is updated *before* the file: a lookup must never miss a row the file
  is about to have, and the reverse mistake — a row in the table that a failed
  write never made durable — cannot survive a crash, because the table dies with
  the compactor and `open/1` rebuilds it from the file. The segment those rows
  point into is `PUT` before this is called, so the table never references bytes
  that aren't in the blob store.
  """
  @spec append(Config.t(), [row()]) :: :ok
  def append(config, rows) do
    case table(config.instance) do
      {:ok, table} ->
        for row <- rows, do: :ets.insert_new(table, {row.event_id, row})

      :error ->
        :ok
    end

    # Synced: the compactor advances its cursor and truncates the WAL right
    # after this, so an index row still in the page cache when power is lost
    # would leave compacted records unreachable.
    DurableLog.append(path(config), rows, sync: true)
  end

  @doc "Every row in the index file, in append order."
  @spec all(Config.t()) :: [row()]
  def all(config), do: DurableLog.read(path(config))

  @spec lookup(Config.t(), String.t()) :: {:ok, row()} | :error
  def lookup(config, event_id) do
    case table(config.instance) do
      {:ok, table} -> lookup_in(table, config, event_id)
      :error -> from_file(config, event_id)
    end
  end

  defp lookup_in(table, config, event_id) do
    case :ets.lookup(table, event_id) do
      [{^event_id, row}] -> {:ok, row}
      [] -> :error
    end
  rescue
    # The owner died between the registry read and here; the file still has it.
    ArgumentError -> from_file(config, event_id)
  end

  # No table: a node that doesn't run the compactor, or one between a crash and
  # the restart that reloads it. This is what lookups did before the table
  # existed — O(rows), and only on that path.
  defp from_file(config, event_id) do
    config
    |> path()
    |> DurableLog.read()
    |> Enum.find_value(:error, fn row ->
      if row.event_id == event_id, do: {:ok, row}
    end)
  end

  defp table(instance) do
    case Registry.lookup(Ankusa.Registry, key(instance)) do
      [{_pid, table}] -> {:ok, table}
      [] -> :error
    end
  end

  defp key(instance), do: {instance, :storage_index}

  defp path(config), do: Config.path(config, "segments/index.log")
end
