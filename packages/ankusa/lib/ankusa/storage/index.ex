defmodule Ankusa.Storage.Index do
  @moduledoc """
  The index object beside a segment: `%{event_id => {offset, length, seq}}`.

  ## Format 1

      "ANKIDX" <> <<version::8, count::32>>   (11 bytes)
      :erlang.term_to_binary(index)
      sha256(everything above)                (32 bytes)

  The term is decoded with `binary_to_term(bin, [:safe])`, so an object that
  names an atom this node has never seen, or carries a fun or a pid, is refused
  rather than executed or interned, and the decoded shape is checked (binary ids
  to three non-negative integers, as many as `count` says) before anything else
  sees it.

  ## Format 0

  The bare `term_to_binary` map the compactor wrote before the header existed.
  It is read the same way (`[:safe]` and the shape check), so legacy segments
  stay readable and a hostile or damaged one is `{:error, :bad_index}`.
  """

  @magic "ANKIDX"
  @version 1
  @header_bytes 11
  @trailer_bytes 32

  @type t :: %{optional(String.t()) => {non_neg_integer(), pos_integer(), pos_integer()}}

  @doc "Encode `index` (`%{id => {offset, length, seq}}`) as a format 1 object."
  @spec encode(t()) :: binary()
  def encode(index) when is_map(index) do
    head = [@magic, <<@version::8, map_size(index)::32>>, :erlang.term_to_binary(index)]
    IO.iodata_to_binary([head, :crypto.hash(:sha256, head)])
  end

  @doc """
  Decode an index object of either format: `{:ok, index}`, or
  `{:error, :bad_index | :unsupported_format | :sha_mismatch}`.
  """
  @spec decode(binary()) :: {:ok, t()} | {:error, atom()}
  def decode(<<@magic, @version::8, count::32, rest::binary>> = object)
      when byte_size(rest) > @trailer_bytes do
    body = byte_size(object) - @trailer_bytes
    <<covered::binary-size(body), digest::binary-size(@trailer_bytes)>> = object

    if :crypto.hash_equals(:crypto.hash(:sha256, covered), digest) do
      <<_head::binary-size(@header_bytes), term::binary>> = covered
      with {:ok, index} <- term(term), do: counted(index, count)
    else
      {:error, :sha_mismatch}
    end
  end

  def decode(<<@magic, version::8, _rest::binary>>) when version != @version,
    do: {:error, :unsupported_format}

  def decode(<<@magic, _rest::binary>>), do: {:error, :bad_index}

  def decode(bin) when is_binary(bin), do: term(bin)
  def decode(_other), do: {:error, :bad_index}

  defp counted(index, count) when map_size(index) == count, do: {:ok, index}
  defp counted(_index, _count), do: {:error, :bad_index}

  # `:safe` refuses unknown atoms and funs; the shape is checked right after.
  # sobelow_skip ["Misc.BinToTerm"]
  defp term(bin) do
    case :erlang.binary_to_term(bin, [:safe]) do
      index when is_map(index) and map_size(index) > 0 -> shaped(index)
      _ -> {:error, :bad_index}
    end
  rescue
    ArgumentError -> {:error, :bad_index}
  end

  defp shaped(index) do
    if Enum.all?(index, &entry?/1), do: {:ok, index}, else: {:error, :bad_index}
  end

  defp entry?({id, {offset, length, seq}}),
    do:
      is_binary(id) and is_integer(offset) and offset >= 0 and is_integer(length) and
        length > 0 and is_integer(seq) and seq > 0

  defp entry?(_other), do: false

  @doc "The lowest and highest event id of an index: `{min_id, max_id}`."
  @spec id_range(t()) :: {String.t(), String.t()}
  def id_range(index), do: index |> Map.keys() |> Enum.min_max()

  @doc "Where an index ends: the end of the last frame, the position the trailer starts at."
  @spec frames_end(t()) :: non_neg_integer()
  def frames_end(index) do
    index
    |> Map.values()
    |> Enum.reduce(0, fn {offset, length, _seq}, acc -> max(acc, offset + length) end)
  end
end
