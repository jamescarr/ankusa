defmodule Ankusa.Codec do
  @moduledoc """
  Segment record framing. `encode/1` packs many records into one immutable
  segment binary and returns a per-record index (byte offset + length within the
  segment). A single range `GET` on the blob store then reads exactly one record,
  which `decode_record/1` validates and unframes.

  `Ankusa.Storage.Segment` wraps what `encode/1` returns in the segment header
  and sha256 trailer, and shifts the offsets past the header; a codec only frames
  records. It may export `id/0`, the one-byte id the header records for it (`1` is
  `Ankusa.Codec.Raw`); without one the header says `0` and the catalogue row names
  the module.
  """

  @type segment_record :: %{key: String.t(), payload: binary()}
  @type index_entry :: %{key: String.t(), offset: non_neg_integer(), length: pos_integer()}

  @callback encode([segment_record()]) :: {segment :: binary(), index :: [index_entry()]}
  @callback decode_record(binary()) :: {:ok, binary()} | {:error, term()}
  @callback id() :: 1..255

  @optional_callbacks id: 0
end
