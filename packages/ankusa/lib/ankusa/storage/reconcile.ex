defmodule Ankusa.Storage.Reconcile do
  @moduledoc false

  # Squares a store's archive catalogue with the segments in the object store
  # (`Ankusa.Queue.Archive.reconcile/2`). The restore path calls it at boot with
  # `node: :own, advance_seq: true`; an operator, or a node that joined a bucket
  # other nodes archive into, calls it with every node's segments.

  require Logger

  alias Ankusa.{BlobStore, Config, Envelope, Store}
  alias Ankusa.Queue.{Archive, Reclaim}
  alias Ankusa.Storage.{Index, Layout, Segment}
  alias Ankusa.Store.Keys

  @type summary :: %{
          catalogued: non_neg_integer(),
          present: non_neg_integer(),
          conflicts: non_neg_integer(),
          skipped: non_neg_integer()
        }

  @spec run(Config.t() | atom(), keyword()) :: {:ok, summary()} | {:error, term()}
  def run(target, opts \\ [])

  def run(instance, opts) when is_atom(instance), do: run(Ankusa.config(instance), opts)

  def run(%Config{instance: instance} = config, opts) do
    {mod, bopts, prefix} = BlobStore.resolve_config(config, :segments)
    store = {mod, bopts}

    with {:ok, segments} <- list_segments(instance, store, prefix, config, opts),
         {:ok, summary} <- recatalogue(instance, config, store, segments),
         :ok <- advance_seq(instance, segments, Keyword.get(opts, :advance_seq, false)) do
      if summary.catalogued > 0,
        do: Logger.warning("[ankusa] #{summary.catalogued} archived segment(s) re-catalogued")

      if summary.conflicts > 0 do
        Logger.warning(
          "[ankusa] #{summary.conflicts} archived segment(s) share a first seq with a catalogued " <>
            "one and were left out of the catalogue; Storage.fetch/2 still finds them by day"
        )
      end

      {:ok, summary}
    end
  end

  # Every segment under the dated root and the legacy `seg/` prefix, sorted by
  # key, narrowed to one node's when `node:` says so (legacy segments belong to
  # whoever owns the prefix, so they always stay).
  defp list_segments(instance, {mod, bopts}, prefix, config, opts) do
    node =
      case Keyword.get(opts, :node, :all) do
        :all -> :all
        :own -> Layout.node_name(config)
        name when is_binary(name) -> name
      end

    with {:ok, dated} <- guarded(fn -> mod.list(instance, prefix <> Layout.root(), bopts) end),
         {:ok, legacy} <- guarded(fn -> mod.list(instance, prefix <> "seg/", bopts) end) do
      segments =
        for key <- dated ++ legacy,
            seg when not is_nil(seg) <- [Layout.parse(key)],
            node == :all or seg.kind == :legacy or seg.node == node,
            do: seg

      {:ok, segments}
    end
  end

  defp recatalogue(instance, config, store, segments) do
    Enum.reduce_while(
      segments,
      {:ok, %{catalogued: 0, present: 0, conflicts: 0, skipped: 0}},
      fn seg, {:ok, acc} ->
        case recatalogue_one(instance, config, store, seg) do
          {:ok, outcome} -> {:cont, {:ok, Map.update!(acc, outcome, &(&1 + 1))}}
          {:error, reason} -> {:halt, {:error, {seg.key, reason}}}
        end
      end
    )
  end

  defp recatalogue_one(instance, config, store, seg) do
    case Store.get(instance, :archive, Keys.segment(seg.first)) do
      {:ok, bin} -> {:ok, if(row_key(bin) == seg.key, do: :present, else: :conflicts)}
      :not_found -> catalogue(instance, config, store, seg)
      {:error, reason} -> {:error, reason}
    end
  end

  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp row_key(bin) do
    case :erlang.binary_to_term(bin) do
      %{key: key} -> key
      _ -> nil
    end
  end

  # The segment's index names its hooks; the first bytes of the object name its
  # format and codec. A segment missing either object (its writer died between
  # the PUTs) or whose index is damaged is logged and left out: the catalogue
  # only ever describes segments that can be read. Any other failure is an error.
  defp catalogue(instance, config, {mod, bopts}, seg) do
    with {:ok, idx} <- read_index(instance, mod, bopts, seg),
         {:ok, head} <- read_header(instance, mod, bopts, seg) do
      row = segment_row(config, seg, idx, head)

      case settle(instance, row, idx) do
        :ok -> {:ok, :catalogued}
        {:error, reason} -> {:error, reason}
      end
    else
      {:skip, why} ->
        Logger.warning("[ankusa] segment #{seg.key} #{why}; not catalogued")
        {:ok, :skipped}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_index(instance, mod, bopts, seg) do
    case guarded(fn -> mod.get(instance, seg.idx_key, bopts) end) do
      {:ok, bin} ->
        case Index.decode(bin) do
          {:ok, idx} -> {:ok, idx}
          {:error, reason} -> {:skip, "has an index that does not decode (#{reason})"}
        end

      {:error, :not_found} ->
        {:skip, "has no index object"}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_header(instance, mod, bopts, seg) do
    case guarded(fn ->
           mod.get_range(instance, seg.key, 0, Segment.header_bytes(), bopts)
         end) do
      {:ok, bin} ->
        case Segment.parse_header(bin) do
          {:ok, head} -> {:ok, head}
          {:error, reason} -> {:skip, "has a header that does not parse (#{reason})"}
        end

      {:error, :not_found} ->
        {:skip, "has no segment object"}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp segment_row(config, seg, idx, head) do
    {min_id, max_id} = Index.id_range(idx)
    {configured, _opts} = config.storage.codec

    {format, codec, trailer} =
      case head do
        :legacy -> {0, configured, 0}
        %{codec_id: id} -> {1, Segment.codec_for_id(id) || configured, Segment.trailer_bytes()}
      end

    %{
      key: seg.key,
      idx_key: seg.idx_key,
      first_seq: seg.first,
      last_seq: seg.last,
      min_id: min_id,
      max_id: max_id,
      count: map_size(idx),
      bytes: Index.frames_end(idx) + trailer,
      format: format,
      codec: codec,
      node: seg.node,
      dt: seg.dt,
      sha256: nil
    }
  end

  # The row, and every obligation of this store the segment already settles, in
  # one batch, exactly as the compactor records a segment it wrote. An obligation
  # is settled only when the hook it is for is the one the segment holds for that
  # seq: another node's segment shares seqs with this store's, not hooks.
  defp settle(instance, row, idx) do
    ids = Map.new(idx, fn {id, {_offset, _length, seq}} -> {seq, id} end)
    seqs = ids |> Map.keys() |> Enum.sort()

    with {:ok, held} <-
           Store.multi_get(instance, :index, Enum.map(seqs, &Keys.archive_pending/1)),
         pending = for({seq, {:ok, _}} <- Enum.zip(seqs, held), do: seq),
         {:ok, hooks} <- Archive.hooks(instance, pending),
         owed = for({seq, bin} <- hooks, bin != nil, same_hook?(bin, ids[seq]), do: seq),
         :ok <- Archive.archived(instance, row, owed) do
      markers = Enum.map(owed, &{&1, Keys.cleared(&1, 1, 0)})

      # A failed reclaim only defers: the markers stay and the sweep finds them.
      with {:error, reason} <- Reclaim.run(instance, markers) do
        Logger.warning("[ankusa] hook reclaim deferred to the next sweep: #{inspect(reason)}")
      end

      :ok
    end
  end

  defp same_hook?(bin, id) do
    match?({:ok, %{id: ^id}}, Envelope.from_binary(bin))
  end

  # Synced, after every catalogue row: the batches before it are durable once it
  # is, and the queue writer starts only after the store's init returns. Only a
  # boot may do this: a running writer keeps its own counter.
  defp advance_seq(_instance, _segments, false), do: :ok
  defp advance_seq(_instance, [], true), do: :ok

  defp advance_seq(instance, segments, true) do
    next = (segments |> Enum.map(& &1.last) |> Enum.max()) + 1

    marker =
      case Store.get(instance, :default, Keys.meta("next_seq")) do
        {:ok, <<n::64>>} -> {:ok, n}
        :not_found -> {:ok, 1}
        {:error, reason} -> {:error, reason}
      end

    with {:ok, current} <- marker,
         :ok <-
           Store.write(
             instance,
             [{:put, :default, Keys.meta("next_seq"), <<max(current, next)::64>>}],
             sync: true
           ) do
      if next > current,
        do: Logger.warning("[ankusa] next_seq advanced to #{next} past archived segments")

      :ok
    end
  end

  # A blob store is user code: a raise or an exit is this reconcile's failure.
  defp guarded(fun) do
    case fun.() do
      :ok -> :ok
      {:ok, _} = ok -> ok
      {:error, _} = error -> error
      other -> {:error, {:bad_return, other}}
    end
  rescue
    error -> {:error, {:raised, error}}
  catch
    :exit, reason -> {:error, {:exit, reason}}
    :throw, value -> {:error, {:throw, value}}
  end
end
