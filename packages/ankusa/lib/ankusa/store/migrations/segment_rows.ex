defmodule Ankusa.Store.Migrations.SegmentRows do
  @moduledoc false

  # Store format 6 -> 7 (DUR-8): the archive catalogue's segment rows (`?S`) record
  # how the segment is stored. A row gains `format` (`0` = bare frames, `1` = the
  # `Ankusa.Storage.Segment` header and sha256 trailer), `codec` (the module), `node`
  # and `dt` (the dated layout's directory parts) and `sha256`.
  #
  # The keys keep their layout. Every row a format 6 store holds describes a
  # segment the compactor wrote before the header existed, under `seg/` with no
  # node or day, so it becomes `format: 0` with the rest `nil`: `Storage` reads a
  # `nil` codec as the configured one. One step reads up to `@batch` rows after its
  # cursor (the last `?S` key it handled; `nil` on the first step) and rewrites
  # the ones that lack `format`, so a resumed step is idempotent. A row that is not
  # a map is left alone.

  @behaviour Ankusa.Store.Migration

  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @batch 500

  @impl true
  def describe,
    do: "record format 0, codec, node and day on the archive's segment rows (rows `?S`)"

  @impl true
  def step(instance, cursor) do
    %{lo: lo, hi: hi} = Keys.family(:segments)
    from = if is_nil(cursor), do: lo, else: cursor <> <<0>>

    with {:ok, rows} <- segment_rows(instance, {from, hi}) do
      ops =
        for {key, %{} = row} <- rows, not Map.has_key?(row, :format) do
          row = Map.merge(row, %{format: 0, codec: nil, node: nil, dt: nil, sha256: nil})
          {:put, :archive, key, :erlang.term_to_binary(row)}
        end

      if length(rows) < @batch,
        do: {:ok, ops, :done},
        else: {:ok, ops, {:more, rows |> List.last() |> elem(0)}}
    end
  end

  # `[{key, row}]`, oldest first, at most `@batch`.
  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp segment_rows(instance, range) do
    result =
      Store.fold(instance, :segments, range, [], fn key, value, acc ->
        acc = [{key, :erlang.binary_to_term(value)} | acc]
        if length(acc) >= @batch, do: {:halt, acc}, else: {:cont, acc}
      end)

    with {:ok, rows} <- result, do: {:ok, Enum.reverse(rows)}
  end
end
