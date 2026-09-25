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

  ## Sharing the index across storage replicas

  Only one storage node holds the `:storage` lease and compacts, but *every*
  storage node serves `Ankusa.Storage.fetch/2`. A standby therefore has no local
  index to read unless it can build one from the blob store, which is what
  `repair/1` does: each segment gets a sidecar object at
  `seg/<padded-first>-<padded-last>.idx` holding that segment's rows in this same
  framed format, and a node folds every sidecar above its local high-water mark
  (`hwm/1`) into its file and table. If a sidecar is missing or undecodable the
  segment itself is walked — its records are self-delimiting — so a missing
  sidecar costs a repair, never a wrong (or missing) lookup.

  The high-water mark is the *last segment key fully folded*, and its absence
  simply means "nothing folded yet".
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

  @doc """
  The last segment key fully folded into the local file, or `nil` when nothing
  has been folded.
  """
  @spec hwm(Config.t()) :: String.t() | nil
  def hwm(%Config{} = config) do
    case File.read(hwm_path(config)) do
      {:ok, key} ->
        case String.trim(key) do
          "" -> nil
          trimmed -> trimmed
        end

      {:error, _} ->
        nil
    end
  end

  @doc "Record that `key` — and everything before it — is folded locally."
  @spec put_hwm(Config.t(), String.t()) :: :ok
  def put_hwm(%Config{} = config, key) do
    path = hwm_path(config)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, key)
    :ok
  end

  @doc """
  Fold every segment in the blob store above the local high-water mark into the
  local file and table.

  This is how a standby storage replica (and a fresh holder of the `:storage`
  lease) gets a usable index without ever having run a compaction: segment keys
  are zero-padded, so lexicographic order is numeric order, and rows go in with
  `:ets.insert_new` as usual — the first row for an event wins, so replaying a
  segment that is already indexed is harmless.

  Idempotent and cheap when nothing changed: an empty delta touches nothing.
  """
  @spec repair(Config.t()) :: :ok | {:error, {String.t(), term()}}
  def repair(%Config{} = config) do
    # A node that was compacting before its hwm file existed already has a local
    # index; seed the hwm from it so it does not re-fold every segment.
    watermark = hwm(config) || seeded_hwm(config)

    pending =
      config.instance
      |> Ankusa.BlobStore.list("seg/")
      |> Enum.filter(&String.ends_with?(&1, ".seg"))
      |> Enum.sort()
      |> Enum.reject(&(watermark != nil and &1 <= watermark))

    failed =
      Enum.reduce_while(pending, nil, fn key, _failed ->
        case fold(config, key) do
          :ok -> {:cont, nil}
          {:error, reason} -> {:halt, {key, reason}}
        end
      end)

    # Stop before the segment that failed: its hwm must not advance past it, so
    # the next repair retries it.
    case failed do
      nil ->
        case List.last(pending) do
          nil -> :ok
          key -> put_hwm(config, key)
        end

      {key, reason} ->
        {:error, {key, reason}}
    end
  end

  # The local index's highest segment key, when the file exists and is non-empty;
  # `nil` means "nothing folded locally".
  defp seeded_hwm(config) do
    case DurableLog.read(path(config)) do
      [] -> nil
      rows -> rows |> Enum.map(& &1.segment_key) |> Enum.max()
    end
  end

  defp fold(config, key) do
    rows =
      case sidecar_rows(config, key) do
        {:ok, rows} ->
          rows

        :error ->
          Ankusa.Telemetry.emit([:storage, :index_repaired], %{}, %{
            instance: config.instance,
            segment: key,
            reason: :missing_sidecar
          })

          segment_rows(config, key)
      end

    append(config, rows)
    :ok
  rescue
    e ->
      Ankusa.Telemetry.emit([:index, :repair_failed], %{}, %{instance: config.instance, key: key})
      {:error, e}
  end

  defp sidecar_rows(config, key) do
    with {:ok, bin} <- Ankusa.BlobStore.get(config.instance, sidecar_key(key)),
         rows when is_list(rows) <- DurableLog.decode(bin),
         expected <- segment_count(key),
         true <- expected == nil or length(rows) == expected do
      {:ok, rows}
    else
      _ -> :error
    end
  rescue
    # A corrupt sidecar must not fail the repair: fall back to the segment.
    _ -> :error
  end

  # The segment key is `seg/<padded-first>-<padded-last>.seg`; records are
  # contiguous, so the record count is `last - first + 1`. A key that does not
  # match has no count, so the sidecar is trusted.
  defp segment_count(key) do
    base = Path.basename(key, ".seg")

    case String.split(base, "-", parts: 2) do
      [first, last] ->
        with {f, ""} <- Integer.parse(first),
             {l, ""} <- Integer.parse(last),
             true <- l >= f do
          l - f + 1
        else
          _ -> nil
        end

      _ ->
        nil
    end
  end

  # Walk the segment's self-delimiting records. This is `Ankusa.Codec.Raw`'s
  # framing — `<<len::32, crc::32, payload::binary-size(len)>>` per record — which
  # is the default codec; a codec that is not self-delimiting has no sidecar-free
  # fallback and relies on its sidecar existing.
  defp segment_rows(config, key) do
    case Ankusa.BlobStore.get(config.instance, key) do
      {:ok, segment} -> walk(segment, key, 0, [])
      {:error, reason} -> raise "segment #{key} GET failed: #{inspect(reason)}"
    end
  end

  defp walk(<<len::32, crc::32, payload::binary-size(len), tail::binary>>, key, offset, acc) do
    if :erlang.crc32(payload) != crc do
      raise "segment #{key}: CRC mismatch at offset #{offset}"
    end

    env = Ankusa.Envelope.from_binary(payload)

    row = %{
      event_id: env.id,
      source_id: env.source_id,
      tenant_id: env.tenant_id,
      received_at: env.received_at,
      seq: env.seq,
      segment_key: key,
      offset: offset,
      length: 8 + len
    }

    walk(tail, key, offset + 8 + len, [row | acc])
  end

  defp walk(_torn, _key, _offset, acc), do: Enum.reverse(acc)

  defp sidecar_key(key), do: String.replace_suffix(key, ".seg", ".idx")

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

  defp hwm_path(config), do: Config.path(config, "segments/index.hwm")
end
