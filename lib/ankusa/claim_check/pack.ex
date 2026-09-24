defmodule Ankusa.ClaimCheck.Pack do
  @moduledoc """
  Builds a claim-check pack: a standard ZIP archive with every entry stored
  uncompressed, one entry per claim (named by the claim's id), followed by a
  `manifest.json` entry that lists each claim's offset, length, digest, content
  type, and receive time.

  Stored entries are what make a claim byte-addressable: the offset returned
  for a claim points at its entry's raw data, so the gateway serves a claim
  with one ranged read and never parses the archive. The central directory at
  the end is the index any ZIP reader (`unzip`, Python `zipfile`,
  `java.util.zip`, Erlang `:zip`) uses to list the pack with no Ankusa code.

  No ZIP64: a pack must stay under 65,535 entries and 4 GiB. Callers split
  well below that (`claim_check.pack_max_bytes`); `build/1` raises if asked to
  exceed it rather than write an archive no reader could open.

  Output is deterministic for the same claims: fixed timestamps (1980-01-01,
  the DOS epoch) and no extra fields.
  """

  @type claim :: %{
          required(:id) => String.t(),
          required(:body) => binary(),
          required(:sha256) => String.t(),
          optional(:content_type) => String.t() | nil,
          optional(:received_at) => integer() | nil
        }

  @type placement :: %{id: String.t(), offset: non_neg_integer(), length: non_neg_integer()}

  @manifest "manifest.json"
  @local_header 30
  @central_header 46
  @end_record 22
  @max_entries 65_535
  @max_bytes 0xFFFFFFFF
  # DOS date for 1980-01-01 (year 0, month 1, day 1): the earliest valid value.
  @dos_date 0x21

  @doc "The fixed size of a local file header before its name, in bytes."
  @spec local_header_bytes() :: pos_integer()
  def local_header_bytes, do: @local_header

  @doc """
  Bytes a pack adds for one claim named `id`, excluding the manifest: its local
  header and its central-directory record. Used to size packs up front.
  """
  @spec entry_overhead(String.t()) :: pos_integer()
  def entry_overhead(id), do: @local_header + @central_header + 2 * byte_size(id)

  @doc """
  Build a pack. Returns the archive as iodata (no copy of the bodies) and where
  each claim's bytes landed, in input order.
  """
  @spec build([claim()]) :: {iodata(), [placement()]}
  def build([_ | _] = claims) do
    if length(claims) + 1 > @max_entries do
      raise ArgumentError, "a claim pack holds at most #{@max_entries - 1} claims"
    end

    {entries, placements, offset} =
      Enum.reduce(claims, {[], [], 0}, fn claim, {entries, placements, offset} ->
        {entry, data_offset, next} = local_entry(claim.id, claim.body, offset)

        placement = %{id: claim.id, offset: data_offset, length: byte_size(claim.body)}
        {[entry | entries], [placement | placements], next}
      end)

    placements = Enum.reverse(placements)
    manifest = manifest(claims, placements)
    {manifest_entry, _data_offset, cd_offset} = local_entry(@manifest, manifest, offset)
    entries = Enum.reverse([manifest_entry | entries])

    central = Enum.map(entries, & &1.central)
    cd_size = IO.iodata_length(central)

    if cd_offset + cd_size + @end_record > @max_bytes do
      raise ArgumentError, "a claim pack must stay under 4 GiB"
    end

    count = length(entries)

    end_record =
      <<0x06054B50::little-32, 0::little-16, 0::little-16, count::little-16, count::little-16,
        cd_size::little-32, cd_offset::little-32, 0::little-16>>

    {[Enum.map(entries, & &1.local), central, end_record], placements}
  end

  defp local_entry(name, body, offset) do
    crc = :erlang.crc32(body)
    size = byte_size(body)
    name_len = byte_size(name)

    local =
      [
        <<0x04034B50::little-32, 20::little-16, 0::little-16, 0::little-16, 0::little-16,
          @dos_date::little-16, crc::little-32, size::little-32, size::little-32,
          name_len::little-16, 0::little-16>>,
        name,
        body
      ]

    central =
      [
        <<0x02014B50::little-32, 20::little-16, 20::little-16, 0::little-16, 0::little-16,
          0::little-16, @dos_date::little-16, crc::little-32, size::little-32, size::little-32,
          name_len::little-16, 0::little-16, 0::little-16, 0::little-16, 0::little-16,
          0::little-32, offset::little-32>>,
        name
      ]

    data_offset = offset + @local_header + name_len
    {%{local: local, central: central}, data_offset, data_offset + size}
  end

  defp manifest(claims, placements) do
    rows =
      Enum.zip_with(claims, placements, fn claim, placement ->
        %{
          "id" => claim.id,
          "offset" => placement.offset,
          "length" => placement.length,
          "sha256" => claim.sha256,
          "content_type" => Map.get(claim, :content_type),
          "received_at" => Map.get(claim, :received_at)
        }
      end)

    JSON.encode!(%{"v" => 1, "claims" => rows})
  end
end
