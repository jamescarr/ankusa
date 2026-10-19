defmodule Ankusa.Store.Sweep do
  @moduledoc false

  # One ordered pass over a scan family (`Ankusa.Store.Keys.family/1`) that hands
  # the rows a caller wants to change to it in chunks. For the jobs that must
  # find rows by something the key does not lead with (a tenant ordinal in a
  # value, a tenant id in the middle of a key): the cost is the family, once.
  #
  # The fold resumes after the last key it saw, so a family of millions of rows
  # is read once, not once per chunk, and a chunk is written before the next is
  # collected, so memory is a chunk. The scan is a `Store.fold/6`: a scan cut
  # short by corruption is an error, never a shorter family.

  alias Ankusa.Store
  alias Ankusa.Store.Keys

  # Keys looked at per fold before it hands back, so one fold is bounded even
  # when nothing matches.
  @budget 50_000

  @doc """
  Walks `family`. `match` takes `key, value` and returns `{:ok, hit}` or
  `:skip`; every `chunk` hits (and the rest at the end) go to `apply_chunk`,
  in key order, which returns `{:ok, n}` (how many it changed) or `{:error,
  reason}`. `{:ok, total}` is the sum of the `n`s.
  """
  @spec run(
          atom(),
          atom(),
          (binary(), binary() -> {:ok, term()} | :skip),
          ([term()] -> {:ok, non_neg_integer()} | {:error, term()}),
          pos_integer()
        ) :: {:ok, non_neg_integer()} | {:error, term()}
  def run(instance, family, match, apply_chunk, chunk \\ 500) do
    %{lo: lo} = Keys.family(family)
    walk(instance, family, lo, match, apply_chunk, chunk, 0)
  end

  defp walk(instance, family, from, match, apply_chunk, chunk, total) do
    %{hi: hi} = Keys.family(family)

    scan =
      Store.fold(instance, family, {from, hi}, {0, 0, [], nil}, fn key,
                                                                   value,
                                                                   {scanned, n, hits, _last} ->
        {n, hits} =
          case match.(key, value) do
            {:ok, hit} -> {n + 1, [hit | hits]}
            :skip -> {n, hits}
          end

        acc = {scanned + 1, n, hits, key}
        if scanned + 1 >= @budget or n >= chunk, do: {:halt, acc}, else: {:cont, acc}
      end)

    with {:ok, {scanned, n, hits, last}} <- scan,
         {:ok, changed} <- apply_hits(hits, apply_chunk) do
      if scanned >= @budget or n >= chunk,
        do: walk(instance, family, last <> <<0>>, match, apply_chunk, chunk, total + changed),
        else: {:ok, total + changed}
    end
  end

  defp apply_hits([], _apply_chunk), do: {:ok, 0}
  defp apply_hits(hits, apply_chunk), do: apply_chunk.(Enum.reverse(hits))
end
