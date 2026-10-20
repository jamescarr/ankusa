defmodule Ankusa.Queue.Archive do
  @moduledoc false

  # The archive's side of the store.
  #
  #   ?a(seq)       archive obligation: `<<size::32, ord::32>>`, `ord` the hook's tenant
  #                 ordinal. The hook must be written to a segment before it can be deleted.
  #                 Written in the commit
  #                 batch, only while the `:storage` role runs.
  #   ?S(first_seq) one row per segment: key, index key, seq/id ranges, count, bytes,
  #                 and (format 7) `format` (0 = bare frames, 1 = `Storage.Segment`
  #                 header + sha256), `codec` (module), `node`, `dt` (day dir), `sha256`.
  #                 A row without `format` is a format 0 segment.
  #   ?L(event_id)  where a hook archived by a 0.3 node lives: `{segment, offset,
  #                 length, seq}`, imported once from `segments/index.log`.
  #
  # Reads are sized in bytes from the keys' values, so nothing is read and
  # discarded to find out how much of a backlog fits a segment.

  alias Ankusa.Queue.Deliveries
  alias Ankusa.Queue.Deliveries.Counters
  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @doc """
  Archive obligations after `after_seq`, ascending, until their sizes reach
  `roll_bytes` — always at least one, so a hook bigger than a segment still
  gets one. `more?` says whether obligations remain past the returned ones.
  Only a hook the queue writer's fsync has covered is archived
  (`Ankusa.Queue.Syncer.durable_seq/1`): the obligations above it are read once
  the sync returns.
  """
  @spec pending(atom(), non_neg_integer(), non_neg_integer()) ::
          {:ok, [{pos_integer(), non_neg_integer()}], boolean()} | {:error, term()}
  def pending(instance, after_seq, roll_bytes) do
    %{hi: hi} = Keys.family(:archive_pending)
    range = {Keys.archive_pending(after_seq + 1), hi}
    durable = Ankusa.Queue.Syncer.durable_seq(instance)

    result =
      Store.fold(instance, :archive_pending, range, {[], 0, false}, fn
        <<?a, seq::64>>, _size, {taken, bytes, _more?} when seq > durable ->
          {:halt, {taken, bytes, false}}

        <<?a, seq::64>>, <<size::32, _ord::32>>, {taken, bytes, _more?} ->
          if taken != [] and bytes >= roll_bytes,
            do: {:halt, {taken, bytes, true}},
            else: {:cont, {[{seq, size} | taken], bytes + size, false}}
      end)

    case result do
      {:ok, {taken, _bytes, more?}} -> {:ok, Enum.reverse(taken), more?}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "The stored hooks for `seqs`: `{seq, binary}`, or `{seq, nil}` for one that is gone."
  @spec hooks(atom(), [pos_integer()]) ::
          {:ok, [{pos_integer(), binary() | nil}]} | {:error, term()}
  def hooks(instance, seqs) do
    with {:ok, results} <- Store.multi_get(instance, :hooks, Enum.map(seqs, &Keys.hook/1)) do
      {:ok,
       seqs
       |> Enum.zip(results)
       |> Enum.map(fn
         {seq, {:ok, bin}} -> {seq, bin}
         {seq, :not_found} -> {seq, nil}
       end)}
    end
  end

  @doc """
  Record a written segment: its catalogue row, every obligation it settles
  gone, and a marker per obligation so `Ankusa.Queue.Reclaim` can look at each
  hook. `row` is `nil` when every hook was already gone and nothing was written.
  Each obligation that was still there also takes one off its tenant's
  `archive_pending` counter, in the same batch.
  """
  @spec archived(atom(), map() | nil, [pos_integer()], [pos_integer()]) :: :ok | {:error, term()}
  def archived(instance, row, archived_seqs, missing_seqs \\ []) do
    catalogue =
      case row do
        nil -> []
        row -> [{:put, :archive, Keys.segment(row.first_seq), :erlang.term_to_binary(row)}]
      end

    settled =
      Enum.flat_map(archived_seqs, fn seq ->
        [
          {:delete, :index, Keys.archive_pending(seq)},
          {:put, :index, Keys.cleared(seq, 1, 0), <<>>}
        ]
      end)

    # An obligation whose hook is gone can never be archived; drop it.
    dropped = Enum.map(missing_seqs, &{:delete, :index, Keys.archive_pending(&1)})

    # Only this process settles obligations, so the rows read here are the ones
    # the deletes remove.
    seqs = archived_seqs ++ missing_seqs

    with {:ok, held} <- Store.multi_get(instance, :index, Enum.map(seqs, &Keys.archive_pending/1)) do
      deltas =
        for {_seq, {:ok, <<_size::32, ord::32>>}} <- Enum.zip(seqs, held),
            do: {ord, :archive_pending, -1}

      Store.write(
        instance,
        Deliveries.coalesce(catalogue ++ settled ++ dropped ++ Counters.ops(deltas)),
        sync: false
      )
    end
  end

  @doc "Where an event archived by a 0.3 node lives, if it did."
  @spec legacy_location(atom(), String.t()) ::
          {:ok, {String.t(), non_neg_integer(), pos_integer(), pos_integer()}} | :error
  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  def legacy_location(instance, id) do
    case Store.get(instance, :archive, Keys.legacy_location(id)) do
      {:ok, bin} -> {:ok, :erlang.binary_to_term(bin)}
      _ -> :error
    end
  end

  @doc """
  The first catalogue row with `first_seq > after_first_seq` whose id range
  overlaps `[min_id, max_id]`, or `nil` when no such segment exists. The
  replay engine walks a time window one segment at a time with this.
  """
  @spec next_segment(atom(), non_neg_integer(), String.t(), String.t()) ::
          {:ok, map() | nil} | {:error, term()}
  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  def next_segment(instance, after_first_seq, min_id, max_id) do
    %{hi: hi} = Keys.family(:segments)
    range = {Keys.segment(after_first_seq + 1), hi}

    Store.fold(instance, :segments, range, nil, fn _key, value, acc ->
      row = :erlang.binary_to_term(value)

      if row.max_id >= min_id and row.min_id <= max_id,
        do: {:halt, row},
        else: {:cont, acc}
    end)
  end

  @doc "Catalogue rows whose id range could hold `id`, oldest first."
  @spec segments_containing(atom(), String.t()) :: {:ok, [map()]} | {:error, term()}
  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  def segments_containing(instance, id) do
    %{lo: lo, hi: hi} = Keys.family(:segments)

    result =
      Store.fold(instance, :segments, {lo, hi}, [], fn _key, value, acc ->
        row = :erlang.binary_to_term(value)

        if row.min_id <= id and id <= row.max_id,
          do: {:cont, [row | acc]},
          else: {:cont, acc}
      end)

    with {:ok, rows} <- result, do: {:ok, Enum.reverse(rows)}
  end

  @doc """
  The catalogue row of the segment with the highest `first_seq` at or below `seq`
  (the only segment that can hold the hook of `seq`), or `nil`: one reverse seek.
  """
  @spec segment_at(atom(), pos_integer()) :: {:ok, map() | nil} | {:error, term()}
  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  def segment_at(instance, seq) do
    %{lo: lo} = Keys.family(:segments)

    with {:ok, found} <-
           Store.first(instance, :segments, {lo, Keys.segment(seq + 1)}, reverse: true) do
      case found do
        nil -> {:ok, nil}
        {_key, value} -> {:ok, :erlang.binary_to_term(value)}
      end
    end
  end

  @doc """
  Catalogue rows, oldest first, at most `limit`, whose newest hook is older than
  `cutoff_ms` (`{key, row}`). Segments are in `seq` order, which is time order up
  to a few seconds of skew, so the scan stops at the first segment that is not
  expired. A segment whose ids are not UUIDv7 has no age and is never listed.
  `after_first_seq` resumes a walk after the segment that starts at it.
  """
  @spec expired_segments(atom(), integer(), pos_integer(), non_neg_integer()) ::
          {:ok, [{binary(), map()}]} | {:error, term()}
  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  def expired_segments(instance, cutoff_ms, limit, after_first_seq \\ 0) do
    %{hi: hi} = Keys.family(:segments)

    result =
      Store.fold(instance, :segments, {Keys.segment(after_first_seq + 1), hi}, [], fn key,
                                                                                      value,
                                                                                      acc ->
        row = :erlang.binary_to_term(value)

        case Ankusa.UUIDv7.timestamp_ms(row.max_id) do
          {:ok, ms} when ms < cutoff_ms ->
            acc = [{key, row} | acc]
            if length(acc) >= limit, do: {:halt, acc}, else: {:cont, acc}

          {:ok, _newer} ->
            {:halt, acc}

          :error ->
            {:cont, acc}
        end
      end)

    with {:ok, rows} <- result, do: {:ok, Enum.reverse(rows)}
  end

  @doc """
  The keys of the 0.3 import's location rows (`?L`), oldest first, at most
  `limit`, whose hook id is older than `cutoff_ms`. Ids sort by time, so the scan
  stops at the first one that is not expired; an id that is not a UUIDv7 has no
  age and is never listed.
  """
  @spec expired_legacy(atom(), integer(), pos_integer()) :: {:ok, [binary()]} | {:error, term()}
  def expired_legacy(instance, cutoff_ms, limit) do
    %{lo: lo, hi: hi} = Keys.family(:legacy_locations)

    result =
      Store.fold(instance, :legacy_locations, {lo, hi}, [], fn <<?L, id::binary>> = key,
                                                               _value,
                                                               acc ->
        case Ankusa.UUIDv7.timestamp_ms(id) do
          {:ok, ms} when ms < cutoff_ms ->
            acc = [key | acc]
            if length(acc) >= limit, do: {:halt, acc}, else: {:cont, acc}

          {:ok, _newer} ->
            {:halt, acc}

          :error ->
            {:cont, acc}
        end
      end)

    with {:ok, keys} <- result, do: {:ok, Enum.reverse(keys)}
  end

  @doc """
  Whether any delivery row (pending or dead) is still owed for a hook of the
  segment `row`. Such a hook is read from the store, not the archive, but it is
  evicted from the DLQ only while the catalogue can find it, so its segment's
  row stays.
  """
  @spec owed?(atom(), map()) :: {:ok, boolean()} | {:error, term()}
  def owed?(instance, %{first_seq: first, last_seq: last}) do
    with {:ok, found} <-
           Store.first(
             instance,
             :deliveries,
             {Keys.delivery(first, 0), Keys.delivery(last + 1, 0)}
           ),
         do: {:ok, found != nil}
  end

  @doc "Delete catalogue rows (`?S` and `?L` keys) in one batch."
  @spec delete_rows(atom(), [binary()]) :: :ok | {:error, term()}
  def delete_rows(_instance, []), do: :ok

  def delete_rows(instance, keys),
    do: Store.write(instance, Enum.map(keys, &{:delete, :archive, &1}), sync: false)

  @doc """
  Rebuild catalogue rows from the segments in the object store: every segment of
  the dated layout (`archive/v1/dt=…/<node>/`) and of the legacy `seg/` prefix
  that has no row at its first seq gets one, built from its index object and the
  first bytes of the segment, and the archive obligations it settles are cleared
  (only those whose hook is the one the segment holds for that seq, so another
  node's segment never clears this node's obligations).

  Options: `node: :all | :own | name` (default `:all`: every node that archives
  under this `storage.key_prefix`), `advance_seq: boolean()` (move `m:next_seq`
  past the segments found; only safe before the queue writer starts, i.e. at
  boot). A segment whose first seq already has a row for a different segment (two
  nodes' seqs overlap) is counted in `conflicts` and left out; `Storage.fetch/2`
  finds it by day instead.

  `{:ok, %{catalogued:, present:, conflicts:, skipped:}}`.
  """
  @spec reconcile(Ankusa.Config.t() | atom(), keyword()) ::
          {:ok, Ankusa.Storage.Reconcile.summary()} | {:error, term()}
  defdelegate reconcile(target, opts \\ []), to: Ankusa.Storage.Reconcile, as: :run
end
