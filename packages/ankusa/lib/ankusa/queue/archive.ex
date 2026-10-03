defmodule Ankusa.Queue.Archive do
  @moduledoc false

  # The archive's side of the store.
  #
  #   ?a(seq)       archive obligation: `<<size::32>>`. The hook must be written
  #                 to a segment before it can be deleted. Written in the commit
  #                 batch, only while the `:storage` role runs.
  #   ?S(first_seq) one row per segment: key, index key, seq/id ranges, count.
  #   ?L(event_id)  where a hook archived by a 0.3 node lives: `{segment, offset,
  #                 length, seq}`, imported once from `segments/index.log`.
  #
  # Reads are sized in bytes from the keys' values, so nothing is read and
  # discarded to find out how much of a backlog fits a segment.

  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @doc """
  Archive obligations after `after_seq`, ascending, until their sizes reach
  `roll_bytes` — always at least one, so a hook bigger than a segment still
  gets one. `more?` says whether obligations remain past the returned ones.
  """
  @spec pending(atom(), non_neg_integer(), non_neg_integer()) ::
          {:ok, [{pos_integer(), non_neg_integer()}], boolean()} | {:error, term()}
  def pending(instance, after_seq, roll_bytes) do
    %{hi: hi} = Keys.family(:archive_pending)
    range = {Keys.archive_pending(after_seq + 1), hi}

    result =
      Store.fold(instance, :archive_pending, range, {[], 0, false}, fn
        <<?a, seq::64>>, <<size::32>>, {taken, bytes, _more?} ->
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

    Store.write(instance, catalogue ++ settled ++ dropped, sync: false)
  end

  @doc "Where an event archived by a 0.3 node lives, if it did."
  @spec legacy_location(atom(), String.t()) ::
          {:ok, {String.t(), non_neg_integer(), pos_integer(), pos_integer()}} | :error
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
end
