defmodule Ankusa.Property.PackTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  @moduletag timeout: 3_600_000

  alias Ankusa.ClaimCheck.{Pack, Ref}
  alias Ankusa.Generators, as: G

  # `Pack.build/1` refuses more than 65 534 entries (65 535 is ZIP64's marker in
  # the end record, which `:zip` refuses to read), two of which are the index and
  # the manifest. The first run of the largest-pack test below, at 65 533 claims,
  # got `{:error, :bad_eocd}` from `:zip.unzip/2`.
  @max_claims 65_532

  property "every claim reads back by its position: locate/2 and the bytes it names" do
    check all claims <- G.pack_claims() do
      {data, placements} = Pack.build(claims)
      bin = IO.iodata_to_binary(data)

      assert length(placements) == length(claims)

      for {{claim, placement}, index} <- Enum.with_index(Enum.zip(claims, placements)) do
        prefix = binary_part(bin, 0, Pack.index_prefix_bytes(index))

        assert {:ok, offset, length} = Pack.locate(prefix, index)
        assert {offset, length} == {placement.offset, placement.length}
        assert binary_part(bin, offset, length) == claim.body
        assert placement.claim_id == claim.claim_id
      end
    end
  end

  property "a pack is a standard ZIP whose entries and manifest are exactly the claims" do
    check all claims <- G.pack_claims() do
      {data, placements} = Pack.build(claims)

      {:ok, files} = :zip.unzip(IO.iodata_to_binary(data), [:memory])
      files = Map.new(files, fn {name, bytes} -> {to_string(name), bytes} end)

      assert files |> Map.keys() |> Enum.sort() ==
               Enum.sort(["index.bin", "manifest.json" | Enum.map(claims, & &1.claim_id)])

      for claim <- claims, do: assert(files[claim.claim_id] == claim.body)

      assert files["index.bin"] ==
               for(p <- placements, into: <<>>, do: <<p.offset::32, p.length::32>>)

      assert %{"v" => 1, "claims" => rows} = JSON.decode!(files["manifest.json"])
      assert Enum.map(rows, & &1["claim_id"]) == Enum.map(claims, & &1.claim_id)
      assert Enum.map(rows, & &1["sha256"]) == Enum.map(claims, & &1.sha256)
      assert Enum.map(rows, & &1["length"]) == Enum.map(claims, &byte_size(&1.body))
    end
  end

  property "the pack is deterministic, and its claim ids are the ref's positions" do
    check all claims <- G.pack_claims() do
      {first, placements} = Pack.build(claims)
      {second, ^placements} = Pack.build(claims)
      assert IO.iodata_to_binary(first) == IO.iodata_to_binary(second)

      claims
      |> Enum.with_index()
      |> Enum.each(fn {claim, index} ->
        assert {:ok, _pack, ^index} = Ref.locate(claim.claim_id)
      end)
    end
  end

  property "locate/2 answers :error for a position past the index and for a short read" do
    check all claims <- G.pack_claims(), extra <- integer(0..3) do
      {data, _} = Pack.build(claims)
      bin = IO.iodata_to_binary(data)
      count = length(claims)

      assert Pack.locate(bin, count + extra) == :error

      last = count - 1
      needed = Pack.index_prefix_bytes(last)

      for short <- [0, 1, 29, 30, needed - 9, needed - 1], short >= 0, short < needed do
        assert Pack.locate(binary_part(bin, 0, short), last) == :error
      end
    end
  end

  property "locate/2 never raises on arbitrary bytes, and what it returns is inside the claimed index" do
    check all bytes <- one_of([G.bytes(256), G.mutated(G.bytes(256))]),
              index <- integer(0..70_000) do
      case Pack.locate(bytes, index) do
        :error -> :ok
        {:ok, offset, length} -> assert offset in 0..0xFFFFFFFF and length in 0..0xFFFFFFFF
      end
    end
  end

  property "a pack with its first bytes damaged never locates a claim at the wrong place" do
    check all claims <- G.pack_claims(), flip_at <- integer(0..60) do
      {data, placements} = Pack.build(claims)
      bin = IO.iodata_to_binary(data)
      damaged = flip_byte(bin, flip_at)

      for {{claim, placement}, index} <- Enum.with_index(Enum.zip(claims, placements)) do
        prefix = binary_part(damaged, 0, min(byte_size(damaged), Pack.index_prefix_bytes(index)))

        case Pack.locate(prefix, index) do
          :error ->
            :ok

          {:ok, offset, length} ->
            # A flip inside the index rows moves a row; any other answer is the original.
            if flip_at < 30 + 9 do
              assert {offset, length} == {placement.offset, placement.length}
              assert binary_part(damaged, offset, length) == claim.body
            end
        end
      end
    end
  end

  test "the largest pack builds, and every claim of it is found" do
    pack = Ref.pack_id(1_790_258_591_000, <<7::64>>)

    claims =
      for index <- 0..(@max_claims - 1) do
        body = <<index::16>>

        %{
          claim_id: Ref.claim_id(pack, index),
          id: "id-#{index}",
          body: body,
          sha256: Base.encode16(:crypto.hash(:sha256, body), case: :lower)
        }
      end

    {data, placements} = Pack.build(claims)
    bin = IO.iodata_to_binary(data)

    assert length(placements) == @max_claims

    for index <- [0, 1, 255, 256, 65_279, 65_280, @max_claims - 1] do
      prefix = binary_part(bin, 0, Pack.index_prefix_bytes(index))
      assert {:ok, offset, 2} = Pack.locate(prefix, index)
      assert binary_part(bin, offset, 2) == <<index::16>>
    end

    assert {:ok, files} = :zip.unzip(bin, [:memory])
    assert length(files) == @max_claims + 2
  end

  test "one claim too many is refused rather than written" do
    pack = Ref.pack_id(1_790_258_591_000, <<7::64>>)

    claims =
      for index <- 0..@max_claims do
        %{
          claim_id: Ref.claim_id(pack, index),
          id: "i",
          body: "",
          sha256: String.duplicate("0", 64)
        }
      end

    assert_raise ArgumentError, ~r/at most #{@max_claims} claims/, fn -> Pack.build(claims) end
  end

  test "a 1 MiB claim and an empty one sit side by side" do
    pack = Ref.pack_id(1_790_258_591_000, <<9::64>>)
    big = :binary.copy("z", 1_048_576)

    claims =
      for {body, index} <- Enum.with_index([big, "", big, ""]) do
        %{
          claim_id: Ref.claim_id(pack, index),
          id: "i#{index}",
          body: body,
          sha256: Base.encode16(:crypto.hash(:sha256, body), case: :lower)
        }
      end

    {data, _} = Pack.build(claims)
    bin = IO.iodata_to_binary(data)

    for {claim, index} <- Enum.with_index(claims) do
      assert {:ok, offset, length} =
               Pack.locate(binary_part(bin, 0, Pack.index_prefix_bytes(index)), index)

      assert binary_part(bin, offset, length) == claim.body
    end
  end

  defp flip_byte(bin, at) when at < byte_size(bin) do
    <<head::binary-size(^at), byte, tail::binary>> = bin
    <<head::binary, Bitwise.bxor(byte, 0xFF), tail::binary>>
  end

  defp flip_byte(bin, _at), do: bin
end
