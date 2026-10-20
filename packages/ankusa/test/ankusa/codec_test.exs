defmodule Ankusa.Codec.RawTest do
  use ExUnit.Case, async: true

  alias Ankusa.Codec.Raw

  test "encode/decode_record round-trips every record with correct byte ranges" do
    records = [
      %{key: "a", payload: "hello"},
      %{key: "b", payload: <<0, 1, 2, 3, 255>>},
      %{key: "c", payload: :erlang.term_to_binary(%{deep: [1, 2, 3]})}
    ]

    {segment, index} = Raw.encode(records)

    assert length(index) == length(records)

    for {rec, entry} <- Enum.zip(records, index) do
      assert entry.key == rec.key
      # length is the full frame: 8-byte header + payload
      assert entry.length == 8 + byte_size(rec.payload)
      frame = binary_part(segment, entry.offset, entry.length)
      assert {:ok, rec.payload} == Raw.decode_record(frame)
    end

    # offsets are contiguous and cover the whole segment
    total = Enum.reduce(index, 0, fn e, acc -> acc + e.length end)
    assert total == byte_size(segment)
    assert hd(index).offset == 0
  end

  test "decode_record detects a corrupted payload as :crc_mismatch" do
    {segment, [_entry]} = Raw.encode([%{key: "x", payload: "correct-bytes"}])
    # keep the original len/crc header but flip the first payload byte
    <<len::32, crc::32, first, rest::binary>> = segment
    corrupted = <<len::32, crc::32, Bitwise.bxor(first, 1)::8, rest::binary>>

    assert {:error, :crc_mismatch} == Raw.decode_record(corrupted)
  end

  test "decode_record rejects a truncated/garbage frame as :malformed" do
    assert {:error, :malformed} == Raw.decode_record(<<1, 2, 3>>)
    # header promises 100 payload bytes that are not present
    assert {:error, :malformed} == Raw.decode_record(<<100::32, 0::32, "short">>)
  end

  test "Raw is codec id 1" do
    assert Raw.id() == 1
    assert Ankusa.Storage.Segment.codec_id(Raw) == 1
    assert Ankusa.Storage.Segment.codec_for_id(1) == Raw
  end
end

defmodule Ankusa.Storage.SegmentTest do
  use ExUnit.Case, async: true

  alias Ankusa.Codec.Raw
  alias Ankusa.Storage.Segment

  # One record, "hello", framed by Raw; the same bytes a format 0 segment holds.
  @frame "000000053610a68668656c6c6f"
  # The same record as a format 1 segment: header "ANKSEG" 1 1 count=1, the frame, sha256.
  @v1 "414e4b534547010100000001000000053610a68668656c6c6fbde8caef71b35e9a596806543425a369db116831495e843ac5f53c461a6a1478"

  defp hex(h), do: Base.decode16!(h, case: :lower)

  describe "versioned header" do
    test "versioned header: the golden format 1 vector verifies and decodes" do
      object = hex(@v1)

      assert {:ok, %{format: 1, codec_id: 1, count: 1}} = Segment.verify(object, 1)
      assert {:ok, %{format: 1, codec_id: 1, count: 1}} = Segment.parse_header(object)

      frame = binary_part(object, Segment.header_bytes(), 13)
      assert {:ok, "hello"} = Raw.decode_record(frame)
    end

    test "versioned header: encode/2 produces the golden bytes, offsets past the header" do
      {object, [entry], sha} = Segment.encode(Raw, [%{key: "a", payload: "hello"}])

      assert object == hex(@v1)
      assert entry == %{key: "a", offset: 12, length: 13}

      assert sha ==
               object |> binary_part(byte_size(object) - 32, 32) |> Base.encode16(case: :lower)

      assert {:ok, "hello"} =
               object |> binary_part(entry.offset, entry.length) |> Raw.decode_record()
    end

    test "versioned header: the golden format 0 vector (bare frames) stays readable" do
      frame = hex(@frame)

      assert {:ok, :legacy} = Segment.verify(frame, 0)
      assert {:ok, :legacy} = Segment.parse_header(frame)
      assert {:ok, "hello"} = Raw.decode_record(frame)
    end

    test "versioned header: a format newer than this release is refused" do
      <<_magic::binary-6, _version, rest::binary>> = hex(@v1)
      newer = <<"ANKSEG", 2, rest::binary>>

      assert {:error, :unsupported_format} = Segment.parse_header(newer)
      assert {:error, :unsupported_format} = Segment.verify(newer, 1)
      assert {:error, :unsupported_format} = Segment.verify(hex(@v1), 9)
    end

    test "versioned header: a tampered or cut segment is {:error, _}" do
      object = hex(@v1)

      # a payload byte flipped
      <<head::binary-14, byte, rest::binary>> = object
      flipped = <<head::binary, Bitwise.bxor(byte, 1), rest::binary>>
      assert {:error, :sha_mismatch} = Segment.verify(flipped, 1)

      # the trailer flipped
      size = byte_size(object) - 1
      <<body::binary-size(^size), last>> = object
      assert {:error, :sha_mismatch} = Segment.verify(<<body::binary, Bitwise.bxor(last, 1)>>, 1)

      # cut anywhere
      assert {:error, :sha_mismatch} = Segment.verify(binary_part(object, 0, size), 1)
      assert {:error, :truncated} = Segment.verify(binary_part(object, 0, 20), 1)
      assert {:error, :truncated} = Segment.verify(<<"ANKSEG", 1>>, 1)
      assert {:error, :truncated} = Segment.verify("", 1)

      # a format 0 body claimed to be format 1
      assert {:error, :bad_magic} = Segment.verify(hex(@frame) <> :binary.copy(<<0>>, 40), 1)
    end

    test "versioned header: a codec without an id is recorded as 0" do
      defmodule NoIdCodec do
        @behaviour Ankusa.Codec
        defdelegate encode(records), to: Ankusa.Codec.Raw
        defdelegate decode_record(bin), to: Ankusa.Codec.Raw
      end

      {object, _index, _sha} = Segment.encode(NoIdCodec, [%{key: "a", payload: "x"}])
      assert {:ok, %{codec_id: 0}} = Segment.parse_header(object)
      assert Segment.codec_for_id(0) == nil
      assert Segment.row_codec(%{codec: NoIdCodec}, Raw) == NoIdCodec
      assert Segment.row_codec(%{codec: nil}, Raw) == Raw
      assert Segment.row_codec(%{codec: Missing.Codec.Dur8}, Raw) == Raw
      assert Segment.row_codec(%{}, Raw) == Raw
    end
  end
end

defmodule Ankusa.Storage.IndexTest do
  use ExUnit.Case, async: true

  alias Ankusa.Storage.Index

  # `%{"evt-a" => {12, 13, 1}}`: the bare term (format 0), and as a format 1 object.
  @v0 "8374000000016d000000056576742d616803610c610d6101"
  @v1 "414e4b49445801000000018374000000016d000000056576742d616803610c610d6101f8fb731487fe04e91c7096dcaad6d02eca9018e0ae754d14ec3912cfba748bff"

  defp hex(h), do: Base.decode16!(h, case: :lower)

  describe "safe idx" do
    test "safe idx: the golden vectors of both formats decode to the same index" do
      expected = %{"evt-a" => {12, 13, 1}}

      assert {:ok, ^expected} = Index.decode(hex(@v0))
      assert {:ok, ^expected} = Index.decode(hex(@v1))
      assert Index.encode(expected) == hex(@v1)
    end

    test "safe idx: ranges over the index" do
      index = %{"b" => {0, 10, 2}, "a" => {10, 5, 1}, "c" => {15, 7, 3}}
      assert Index.id_range(index) == {"a", "c"}
      assert Index.frames_end(index) == 22
    end

    test "safe idx: a tampered format 1 index is {:error, :sha_mismatch}" do
      object = hex(@v1)
      <<head::binary-20, byte, rest::binary>> = object

      assert {:error, :sha_mismatch} =
               Index.decode(<<head::binary, Bitwise.bxor(byte, 1), rest::binary>>)

      assert {:error, :sha_mismatch} =
               Index.decode(binary_part(object, 0, byte_size(object) - 1) <> <<0>>)
    end

    test "safe idx: a cut or newer index is refused" do
      object = hex(@v1)
      assert {:error, :bad_index} = Index.decode(binary_part(object, 0, 30))
      assert {:error, :bad_index} = Index.decode("ANKIDX")
      <<_magic::binary-6, _version, rest::binary>> = object
      assert {:error, :unsupported_format} = Index.decode(<<"ANKIDX", 2, rest::binary>>)
    end

    test "safe idx: a count that disagrees with the map is refused" do
      term = :erlang.term_to_binary(%{"a" => {0, 1, 1}})
      head = <<"ANKIDX", 1, 5::32, term::binary>>
      object = head <> :crypto.hash(:sha256, head)
      assert {:error, :bad_index} = Index.decode(object)
    end

    test "safe idx: an atom this node has never seen is refused, not created" do
      name = "never_interned_index_dur8_" <> Integer.to_string(System.unique_integer([:positive]))
      hostile = <<131, 116, 1::32, 109, 1::32, "a", 119, byte_size(name), name::binary>>

      assert {:error, :bad_index} = Index.decode(hostile)
      assert_raise ArgumentError, fn -> String.to_existing_atom(name) end

      # the same inside a well-formed, correctly signed format 1 object
      head = <<"ANKIDX", 1, 1::32, hostile::binary>>
      assert {:error, :bad_index} = Index.decode(head <> :crypto.hash(:sha256, head))
    end

    test "safe idx: anything that is not an id to {offset, length, seq} map is refused" do
      for bad <- [
            %{},
            [],
            "text",
            %{"a" => {1, 2}},
            %{"a" => {-1, 2, 3}},
            %{"a" => {1, 0, 3}},
            %{"a" => {1, 2, 0}},
            %{1 => {1, 2, 3}},
            %{"a" => fn -> :boom end}
          ] do
        assert {:error, :bad_index} = Index.decode(:erlang.term_to_binary(bad))
      end

      assert {:error, :bad_index} = Index.decode("not a term at all")
      assert {:error, :bad_index} = Index.decode(nil)
    end
  end
end
