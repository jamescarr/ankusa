defmodule Ankusa.ClaimCheck.Pack do
  @moduledoc """
  Builds and indexes a claim-check pack: a standard ZIP archive with every
  entry stored uncompressed, laid out as

    1. `index.bin` — one 8-byte row per claim, in claim order: the offset of
       the claim's bytes in the pack and their length, each a big-endian
       unsigned 32-bit integer
    2. one entry per claim, named by its claim id
    3. `manifest.json` — each claim's claim id, hook id, offset, length,
       digest, content type, and receive time

  The index sits first so a reader holding only a claim id finds its bytes
  without knowing the pack's size: read the index entry's header and rows
  up to the claim's position (`index_prefix_bytes/1`, `locate/2`), then read
  the claim's range. Stored entries are what make that range the claim's raw
  bytes. The central directory at the end is what any ZIP reader (`unzip`,
  Python `zipfile`, `java.util.zip`, Erlang `:zip`) uses to list the pack with
  no Ankusa code.

  No ZIP64: a pack must stay under 65,535 entries and 4 GiB. Callers split
  well below that (`claim_check.pack_max_bytes`); `build/1` raises if asked to
  exceed it rather than write an archive no reader could open.

  Output is deterministic for the same claims: fixed timestamps (1980-01-01,
  the DOS epoch) and no extra fields.
  """

  @type claim :: %{
          required(:claim_id) => String.t(),
          required(:id) => String.t(),
          required(:body) => binary(),
          required(:sha256) => String.t(),
          optional(:content_type) => String.t() | nil,
          optional(:received_at) => integer() | nil
        }

  @type placement :: %{
          claim_id: String.t(),
          offset: non_neg_integer(),
          length: non_neg_integer()
        }

  @index "index.bin"
  @index_name byte_size(@index)
  @index_row 8
  @manifest "manifest.json"
  @local_header 30
  @central_header 46
  @end_record 22
  @max_entries 65_535
  @max_bytes 0xFFFFFFFF
  # A claim id is a ULID: always 26 bytes.
  @claim_name 26
  # DOS date for 1980-01-01 (year 0, month 1, day 1): the earliest valid value.
  @dos_date 0x21

  @doc """
  Bytes a pack adds for one claim, excluding the manifest: its index row,
  local header, and central-directory record. Used to size packs up front.
  """
  @spec entry_overhead() :: pos_integer()
  def entry_overhead, do: @index_row + @local_header + @central_header + 2 * @claim_name

  @doc """
  Build a pack. Returns the archive as iodata (no copy of the bodies) and where
  each claim's bytes landed, in input order.
  """
  @spec build([claim()]) :: {iodata(), [placement()]}
  def build([_ | _] = claims) do
    count = length(claims)

    if count + 2 > @max_entries do
      raise ArgumentError, "a claim pack holds at most #{@max_entries - 2} claims"
    end

    first = index_data_offset() + count * @index_row

    {entries, placements, offset} =
      Enum.reduce(claims, {[], [], first}, fn claim, {entries, placements, offset} ->
        {entry, data_offset, next} = local_entry(claim.claim_id, claim.body, offset)

        placement = %{
          claim_id: claim.claim_id,
          offset: data_offset,
          length: byte_size(claim.body)
        }

        {[entry | entries], [placement | placements], next}
      end)

    placements = Enum.reverse(placements)
    manifest = manifest(claims, placements)
    {manifest_entry, _data_offset, cd_offset} = local_entry(@manifest, manifest, offset)

    if cd_offset > @max_bytes do
      raise ArgumentError, "a claim pack must stay under 4 GiB"
    end

    index = for p <- placements, into: <<>>, do: <<p.offset::32, p.length::32>>
    {index_entry, _data_offset, ^first} = local_entry(@index, index, 0)
    entries = [index_entry | Enum.reverse([manifest_entry | entries])]

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

  @doc """
  How many bytes, from the start of a pack, `locate/2` needs to find the
  claim at `index`: the index entry's header and its rows up to that claim.
  """
  @spec index_prefix_bytes(non_neg_integer()) :: pos_integer()
  def index_prefix_bytes(index), do: index_data_offset() + (index + 1) * @index_row

  @doc """
  Where the claim at `index` sits, from a pack's first
  `index_prefix_bytes(index)` bytes (a shorter read, when the object is
  smaller, is fine). `:error` when the bytes don't start with a pack's index
  or the pack holds fewer claims.
  """
  @spec locate(binary(), non_neg_integer()) ::
          {:ok, non_neg_integer(), non_neg_integer()} | :error
  def locate(prefix, index) do
    skip = index * @index_row

    case prefix do
      <<0x04034B50::little-32, _::binary-14, size::little-32, _::binary-4, @index_name::little-16,
        0::little-16, @index, _::binary-size(^skip), offset::32, length::32, _::binary>>
      when skip + @index_row <= size ->
        {:ok, offset, length}

      _ ->
        :error
    end
  end

  defp index_data_offset, do: @local_header + @index_name

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
          "claim_id" => claim.claim_id,
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
