defmodule Ankusa.Storage do
  @moduledoc """
  Read side of long-term segment storage. `fetch/2` resolves an event id to its
  segment, range-reads exactly its frame from the blob store, and unframes it
  back into the original `Ankusa.Envelope`.

  A segment's catalogue row lives in the node's `Ankusa.Store`, with the range of
  event ids it holds; its index object (`….idx` beside the segment in the blob
  store, `Ankusa.Storage.Index`) maps each id to `{offset, length, seq}`. Event
  ids are time-ordered, so the id range narrows a lookup to a segment or two.
  Events archived by a 0.3 node resolve through the locations imported from its
  index log.

  When the catalogue has no segment for an id (a fresh node, or a segment another
  node wrote) and the id carries a time (a UUIDv7), the dated layout
  (`Ankusa.Storage.Layout`) says where else to look: the id's day, and the day
  before, are listed in the object store, every node's `_manifest.json` that can
  hold the id is kept, and each such node's segments are searched by a bisection
  over their index objects. That is a handful of object reads per fetch, not a
  catalogue lookup: `Ankusa.Queue.Archive.reconcile/2` puts the segments in the
  catalogue.

  Nothing read from the object store is trusted: an index that does not decode
  (`Ankusa.Storage.Index`) or a frame whose CRC fails is `:error`.

  A hook of a tenant that was erased (`Ankusa.Tenant`) after the hook was
  received is `{:error, :gone}` (`410` where it is served): the segment still
  holds its frame until the bucket's lifecycle expires it, and the tombstone is
  what keeps it from being read in the meantime.
  """

  alias Ankusa.{BlobStore, Config, Envelope, Tenant}
  alias Ankusa.Queue.Archive
  alias Ankusa.Storage.{Index, Layout, Segment}

  # What a fallback may spend on one fetch: the keys of one day's listing are
  # the object store's to return, but only this many manifests are read.
  @fallback_nodes 64

  @typep location :: %{
           key: String.t(),
           offset: non_neg_integer(),
           length: pos_integer(),
           seq: pos_integer(),
           codec: module()
         }

  @spec fetch(atom(), String.t()) :: {:ok, Envelope.t()} | :error | {:error, :gone}
  def fetch(instance, event_id) do
    %Config{storage: %{codec: {configured, _}}} = config = Ankusa.config(instance)

    with {:ok, loc} <- locate(instance, config, configured, event_id),
         {:ok, frame} <-
           BlobStore.get_range(instance, :segments, loc.key, loc.offset, loc.length),
         {:ok, payload} <- loc.codec.decode_record(frame),
         {:ok, env} <- Envelope.from_binary(payload) do
      served(instance, %{env | seq: loc.seq})
    else
      _ -> :error
    end
  end

  # A tombstone that cannot be read is not "not erased": the answer is `:error`.
  defp served(instance, env) do
    case Tenant.erased_at(instance, env.tenant_id || "default") do
      {:ok, at} when env.received_at <= at -> {:error, :gone}
      {:error, _reason} -> :error
      _not_erased_or_later -> {:ok, env}
    end
  end

  @spec locate(atom(), Config.t(), module(), String.t()) :: {:ok, location()} | :error
  defp locate(instance, config, configured, event_id) do
    case Archive.legacy_location(instance, event_id) do
      {:ok, {key, offset, length, seq}} ->
        {:ok, %{key: key, offset: offset, length: length, seq: seq, codec: configured}}

      :error ->
        with {:ok, rows} <- Archive.segments_containing(instance, event_id),
             nil <- Enum.find_value(rows, &locate_in(instance, &1, configured, event_id)) do
          fallback(instance, config, configured, event_id)
        else
          {:ok, _loc} = found -> found
          _ -> :error
        end
    end
  end

  defp locate_in(instance, %{key: key} = row, configured, event_id) do
    with {:ok, index} <- segment_index(instance, row),
         {:ok, {offset, length, seq}} <- Map.fetch(index, event_id) do
      {:ok,
       %{
         key: key,
         offset: offset,
         length: length,
         seq: seq,
         codec: Segment.row_codec(row, configured)
       }}
    else
      _ -> nil
    end
  end

  @doc """
  The index object of a segment (`%{event_id => {offset, length, seq}}`), as the
  catalogue row `row` names it: one object read, no frame. `:error` when the
  object is missing, unreadable, or does not decode (`Ankusa.Storage.Index`).
  """
  @spec segment_index(atom(), map()) :: {:ok, Index.t()} | :error
  def segment_index(instance, %{idx_key: idx_key}), do: read_index(instance, idx_key)

  defp read_index(instance, idx_key) do
    with {:ok, bin} <- BlobStore.get(instance, :segments, idx_key),
         {:ok, index} <- Index.decode(bin) do
      {:ok, index}
    else
      _ -> :error
    end
  end

  # ── the day-prefix fallback ───────────────────────────────────────────────

  # Past the catalogue's retention the catalogue stops finding a hook, and so
  # does this (the objects may outlive the rows, e.g. `archive.retention_days`
  # unset): `fetch/2` of an expired hook is `:error` either way.
  defp fallback(instance, config, configured, event_id) do
    with {:ok, ms} <- Ankusa.UUIDv7.timestamp_ms(event_id),
         true <- ms >= System.system_time(:millisecond) - retention_ms(config),
         [_ | _] = days <- Layout.days_for(event_id) do
      Enum.find_value(days, :error, fn dt ->
        case BlobStore.list(instance, :segments, Layout.day_prefix(dt)) do
          {:ok, keys} -> scan_day(instance, configured, keys, event_id)
          {:error, _reason} -> nil
        end
      end)
    else
      _ -> :error
    end
  end

  defp retention_ms(%Config{archive: archive, storage: storage}),
    do: (archive.retention_days || storage.catalogue_retention_days) * 86_400_000

  # One day's keys, grouped by node: a node whose manifest says its ids cannot
  # include `event_id` is skipped, the rest are searched.
  defp scan_day(instance, configured, keys, event_id) do
    keys
    |> Enum.map(&Layout.parse/1)
    |> Enum.filter(&(&1 != nil and &1.kind == :dated))
    |> Enum.group_by(& &1.node)
    |> Enum.take(@fallback_nodes)
    |> Enum.find_value(fn {_node, segs} ->
      if node_may_hold?(instance, segs, event_id) do
        search_node(instance, configured, Enum.sort_by(segs, & &1.first), event_id)
      end
    end)
  end

  defp node_may_hold?(instance, [%{key: key} | _], event_id) do
    manifest_key = Layout.manifest_key(key)

    with {:ok, bin} <- BlobStore.get(instance, :segments, manifest_key),
         {:ok, %{"min_id" => min, "max_id" => max}} when is_binary(min) and is_binary(max) <-
           Layout.decode_manifest(bin) do
      min <= event_id and event_id <= max
    else
      # No manifest (its writer died before it) or one that does not decode: the
      # node's segments are searched anyway.
      _ -> true
    end
  end

  # Segments of one node are in seq order, which is time order to a few seconds
  # of skew: bisect for the first whose highest id reaches `event_id`, then look
  # at it and its neighbours.
  defp search_node(instance, configured, segs, event_id) do
    segments = List.to_tuple(segs)
    n = tuple_size(segments)
    {first, indexes} = bisect(instance, segments, event_id, 0, n, %{})

    Enum.find_value(max(first - 1, 0)..min(first + 1, n - 1)//1, fn i ->
      seg = elem(segments, i)

      with {:ok, index} <- index_at(instance, indexes, seg, i),
           {:ok, {offset, length, seq}} <- Map.fetch(index, event_id),
           {:ok, codec} <- codec_of(instance, seg, configured) do
        {:ok, %{key: seg.key, offset: offset, length: length, seq: seq, codec: codec}}
      else
        _ -> nil
      end
    end)
  end

  # The index of the first segment whose highest id is at least `event_id` (`n`
  # when none is), with the indexes read on the way.
  defp bisect(_instance, _segments, _event_id, lo, lo, indexes), do: {lo, indexes}

  defp bisect(instance, segments, event_id, lo, hi, indexes) do
    mid = div(lo + hi, 2)
    seg = elem(segments, mid)

    {index, indexes} =
      case index_at(instance, indexes, seg, mid) do
        {:ok, index} -> {index, Map.put(indexes, mid, {:ok, index})}
        :error -> {nil, Map.put(indexes, mid, :error)}
      end

    if index != nil and elem(Index.id_range(index), 1) >= event_id,
      do: bisect(instance, segments, event_id, lo, mid, indexes),
      else: bisect(instance, segments, event_id, mid + 1, hi, indexes)
  end

  defp index_at(_instance, indexes, _seg, i) when is_map_key(indexes, i),
    do: Map.fetch!(indexes, i)

  defp index_at(instance, _indexes, seg, _i), do: read_index(instance, seg.idx_key)

  # A segment another node wrote may use another codec: its header says which.
  defp codec_of(instance, seg, configured) do
    case BlobStore.get_range(instance, :segments, seg.key, 0, Segment.header_bytes()) do
      {:ok, head} ->
        case Segment.parse_header(head) do
          {:ok, :legacy} -> {:ok, configured}
          {:ok, %{codec_id: id}} -> {:ok, Segment.codec_for_id(id) || configured}
          {:error, _reason} -> :error
        end

      _ ->
        :error
    end
  end
end
