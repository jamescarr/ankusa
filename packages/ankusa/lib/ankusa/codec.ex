defmodule Ankusa.Codec do
  @moduledoc """
  Segment record framing. `encode/1` packs many records into one immutable
  segment binary and returns a per-record index (byte offset + length within the
  segment). A single range `GET` on the blob store then reads exactly one record,
  which `decode_record/1` validates and unframes.
  """

  @type segment_record :: %{key: String.t(), payload: binary()}
  @type index_entry :: %{key: String.t(), offset: non_neg_integer(), length: pos_integer()}

  @callback encode([segment_record()]) :: {segment :: binary(), index :: [index_entry()]}
  @callback decode_record(binary()) :: {:ok, binary()} | {:error, term()}
end
