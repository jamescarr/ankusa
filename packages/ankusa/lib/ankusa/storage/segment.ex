defmodule Ankusa.Storage.Segment do
  @moduledoc """
  The segment object: what the compactor `PUT`s and a replay reads whole.

  ## Format 1

      "ANKSEG" <> <<version::8, codec_id::8, count::32>>   (12 bytes)
      <codec frames, back to back>
      sha256(everything above)                              (32 bytes)

  `codec_id` names the `Ankusa.Codec` that framed the records (`1` is
  `Ankusa.Codec.Raw`; `0` is a codec with no id, which the catalogue row names by
  module). `count` is the number of frames. The index (`Ankusa.Storage.Index`)
  holds each record's absolute offset and length *within the object*, header
  included, so a range read of `[offset, offset + length)` is exactly one frame
  and the codec's `decode_record/1` is unchanged.

  A reader that fetches one frame (`Storage.fetch/2`) is guarded by the frame's
  own CRC; a reader that has the whole object (`verify/1`, used by replay and
  reconcile) also checks the sha256, so a damaged or truncated object is
  `{:error, _}`, never a silently short read.

  ## Format 0

  The segments written before the header existed: bare frames, no trailer. They
  stay readable (`verify/2` passes them through); the catalogue row's `format: 0`
  says so. A reader tells the two apart by the `"ANKSEG"` magic, and a format
  newer than this release is `{:error, :unsupported_format}`.
  """

  @magic "ANKSEG"
  @version 1
  @header_bytes 12
  @trailer_bytes 32

  @type format :: 0 | 1
  @type header :: %{format: 1, codec_id: 0..255, count: non_neg_integer()}

  @doc "The current segment format."
  @spec version() :: pos_integer()
  def version, do: @version

  @doc "Bytes before the first frame (format 1)."
  @spec header_bytes() :: pos_integer()
  def header_bytes, do: @header_bytes

  @doc "Bytes after the last frame (format 1)."
  @spec trailer_bytes() :: pos_integer()
  def trailer_bytes, do: @trailer_bytes

  @doc """
  Pack `records` with `codec`: `{object, index, sha256_hex}`. `index` is the
  codec's, its offsets moved past the header so they are positions in `object`.
  """
  @spec encode(module(), [Ankusa.Codec.segment_record()]) ::
          {binary(), [Ankusa.Codec.index_entry()], String.t()}
  def encode(codec, records) do
    {frames, index} = codec.encode(records)
    header = <<@magic, @version::8, codec_id(codec)::8, length(records)::32>>
    digest = :crypto.hash(:sha256, [header, frames])
    object = IO.iodata_to_binary([header, frames, digest])

    {object, Enum.map(index, &%{&1 | offset: &1.offset + @header_bytes}),
     Base.encode16(digest, case: :lower)}
  end

  @doc """
  The codec id a segment records for `codec`: its `id/0` when it has one, else
  `0`.
  """
  @spec codec_id(module()) :: 0..255
  def codec_id(codec) do
    if Code.ensure_loaded?(codec) and function_exported?(codec, :id, 0), do: codec.id(), else: 0
  end

  @doc "The codec module a recorded `codec_id` stands for, `nil` when it names none."
  @spec codec_for_id(0..255) :: module() | nil
  def codec_for_id(1), do: Ankusa.Codec.Raw
  def codec_for_id(_id), do: nil

  @doc """
  The codec to read a catalogue row's segment with: the module the row recorded
  when it is loaded here, else `default` (the configured codec).
  """
  @spec row_codec(map(), module()) :: module()
  def row_codec(%{codec: codec}, default) when is_atom(codec) and not is_nil(codec),
    do: if(Code.ensure_loaded?(codec), do: codec, else: default)

  def row_codec(_row, default), do: default

  @doc """
  Read a segment's header from the first bytes of the object (at least
  `header_bytes/0` of them): `{:ok, header}` for format 1, `{:ok, :legacy}` for
  an object that has no magic.
  """
  @spec parse_header(binary()) ::
          {:ok, header() | :legacy} | {:error, :unsupported_format | :truncated}
  def parse_header(<<@magic, @version::8, codec_id::8, count::32, _rest::binary>>),
    do: {:ok, %{format: 1, codec_id: codec_id, count: count}}

  def parse_header(<<@magic, other::8, _rest::binary>>) when other != @version,
    do: {:error, :unsupported_format}

  def parse_header(<<@magic, _short::binary>>), do: {:error, :truncated}
  def parse_header(<<_::binary-size(@header_bytes), _::binary>>), do: {:ok, :legacy}
  def parse_header(_short), do: {:error, :truncated}

  @doc """
  Check a whole object against the format the catalogue row recorded (`0` for a
  legacy segment, which has nothing to check, `1`): `{:ok, header | :legacy}` or
  `{:error, :bad_magic | :unsupported_format | :truncated | :sha_mismatch}`.
  """
  @spec verify(binary(), format()) :: {:ok, header() | :legacy} | {:error, atom()}
  def verify(object, 0) when is_binary(object), do: {:ok, :legacy}

  def verify(object, 1) when is_binary(object) do
    with {:ok, %{format: 1} = header} <- header_of(object),
         :ok <- check_digest(object) do
      {:ok, header}
    end
  end

  def verify(_object, _format), do: {:error, :unsupported_format}

  defp header_of(<<@magic, _::binary>> = object) do
    case parse_header(object) do
      {:ok, :legacy} -> {:error, :bad_magic}
      other -> other
    end
  end

  defp header_of(object) when byte_size(object) < @header_bytes, do: {:error, :truncated}
  defp header_of(_object), do: {:error, :bad_magic}

  defp check_digest(object) when byte_size(object) < @header_bytes + @trailer_bytes,
    do: {:error, :truncated}

  defp check_digest(object) do
    body = byte_size(object) - @trailer_bytes
    <<covered::binary-size(^body), digest::binary-size(@trailer_bytes)>> = object

    if :crypto.hash_equals(:crypto.hash(:sha256, covered), digest),
      do: :ok,
      else: {:error, :sha_mismatch}
  end
end
