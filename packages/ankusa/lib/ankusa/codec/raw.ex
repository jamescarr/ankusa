defmodule Ankusa.Codec.Raw do
  @moduledoc """
  Default `Ankusa.Codec`: length-prefixed, CRC-checked framing with no
  compression.

  Each record is framed as:

      <<len::32, crc32::32, payload::binary>>

  where `crc32 = :erlang.crc32(payload)` covers the payload only. `encode/1`
  concatenates the frames into one segment binary and returns a per-record index
  of `{key, offset, length}` — `offset` is the frame start byte within the
  segment and `length` is the full frame length (payload plus the 8-byte header),
  exactly the byte range `Ankusa.BlobStore.get_range/4` must fetch for
  `decode_record/1`.
  """

  @behaviour Ankusa.Codec

  @header_bytes 8

  @impl true
  def encode(records) do
    {frames, index, _off} =
      Enum.reduce(records, {[], [], 0}, fn %{key: key, payload: payload}, {frames, index, off} ->
        len = byte_size(payload)
        crc = :erlang.crc32(payload)
        frame = <<len::32, crc::32, payload::binary>>
        flen = @header_bytes + len
        entry = %{key: key, offset: off, length: flen}
        {[frame | frames], [entry | index], off + flen}
      end)

    segment = frames |> Enum.reverse() |> IO.iodata_to_binary()
    {segment, Enum.reverse(index)}
  end

  @impl true
  def decode_record(<<len::32, crc::32, payload::binary-size(len)>>) do
    if :erlang.crc32(payload) == crc do
      {:ok, payload}
    else
      {:error, :crc_mismatch}
    end
  end

  def decode_record(_bin), do: {:error, :malformed}
end
