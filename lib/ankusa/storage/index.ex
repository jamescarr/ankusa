defmodule Ankusa.Storage.Index do
  @moduledoc """
  Durable, append-only index mapping an event id to its byte range inside a
  compacted segment.

  Rows are framed terms (see `Ankusa.DurableLog`) at
  `Config.path(config, "segments/index.log")`. A torn trailing row — a write that
  never completed — is dropped, mirroring the WAL's crash-safety discipline.

  ## Why the decoded map is cached

  The index is written once per compaction tick and read on every
  `Ankusa.Storage.fetch/2`, which is the replay/audit path a dashboard drives.
  Decoding the whole file per lookup makes every fetch O(events); instead the
  decoded map lives in `:persistent_term`, keyed by the file's `{size, mtime}`.
  An append always changes the size, so a stale map is never served, and the
  entry is simply rebuilt on the next lookup.

  `:persistent_term.get/2` hands back the term without copying it, which is what
  suits a read-mostly map here. Writing to it costs a global GC scan, which is
  affordable precisely because it happens once per append.
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

  @spec append(Config.t(), [row()]) :: :ok
  def append(config, rows), do: DurableLog.append(path(config), rows)

  @doc "Every row in the index, in append order."
  @spec all(Config.t()) :: [row()]
  def all(config), do: DurableLog.read(path(config))

  @spec lookup(Config.t(), String.t()) :: {:ok, row()} | :error
  def lookup(config, event_id), do: Map.fetch(by_id(config), event_id)

  defp by_id(config) do
    path = path(config)

    with {:ok, %File.Stat{size: size, mtime: mtime}} <- File.stat(path, time: :posix) do
      key = {__MODULE__, path}
      stamp = {size, mtime}

      case :persistent_term.get(key, nil) do
        {^stamp, by_id} -> by_id
        _stale_or_absent -> load(path, key, stamp)
      end
    else
      {:error, _} -> %{}
    end
  end

  defp load(path, key, stamp) do
    # First row wins, matching the linear scan this replaced: an event that was
    # somehow compacted twice still resolves to its earliest frame.
    by_id =
      path
      |> DurableLog.read()
      |> Enum.reduce(%{}, fn row, acc -> Map.put_new(acc, row.event_id, row) end)

    :persistent_term.put(key, {stamp, by_id})
    by_id
  end

  defp path(config), do: Config.path(config, "segments/index.log")
end
