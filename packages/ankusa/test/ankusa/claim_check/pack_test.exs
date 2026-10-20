defmodule Ankusa.ClaimCheck.PackTest do
  use ExUnit.Case, async: true

  alias Ankusa.ClaimCheck.{Pack, Ref}

  defp claims(bodies) do
    pack_id = Ref.new_pack_id()

    bodies
    |> Enum.with_index()
    |> Enum.map(fn {{body, content_type}, index} ->
      %{
        claim_id: Ref.claim_id(pack_id, index),
        id: "hook-#{index}",
        body: body,
        sha256: Base.encode16(:crypto.hash(:sha256, body), case: :lower),
        content_type: content_type,
        received_at: 1_760_000_000_000
      }
    end)
  end

  test "the index locates every claim from a prefix read, and nothing past the last" do
    claims =
      claims([
        {:crypto.strong_rand_bytes(70_000), "application/json"},
        {"second body", "application/json"},
        {"", nil},
        {:crypto.strong_rand_bytes(1), "application/json"}
      ])

    {data, placements} = Pack.build(claims)
    bin = IO.iodata_to_binary(data)

    for {{claim, placement}, index} <- claims |> Enum.zip(placements) |> Enum.with_index() do
      prefix = binary_part(bin, 0, Pack.index_prefix_bytes(index))

      assert {:ok, offset, length} = Pack.locate(prefix, index)
      assert {offset, length} == {placement.offset, placement.length}
      assert binary_part(bin, offset, length) == claim.body
    end

    # The whole pack is a longer prefix than any row needs: the index's own
    # size, not the read length, bounds it.
    assert Pack.locate(bin, length(claims)) == :error
    assert Pack.locate(binary_part(bin, 0, 10), 0) == :error
    assert Pack.locate("not a pack at all, but long enough to hold a row", 0) == :error
  end

  test "a pack is a standard ZIP: entries named by claim id, plus a manifest that matches the placements" do
    claims = claims([{"first", "application/json"}, {"second", nil}])
    {data, placements} = Pack.build(claims)

    {:ok, files} = :zip.unzip(IO.iodata_to_binary(data), [:memory])
    files = Map.new(files, fn {name, bytes} -> {to_string(name), bytes} end)

    assert Map.keys(files) |> Enum.sort() ==
             Enum.sort(["index.bin", "manifest.json" | Enum.map(claims, & &1.claim_id)])

    for claim <- claims, do: assert(files[claim.claim_id] == claim.body)

    assert files["index.bin"] ==
             for(p <- placements, into: <<>>, do: <<p.offset::32, p.length::32>>)

    %{"v" => 1, "claims" => rows} = JSON.decode!(files["manifest.json"])

    assert rows ==
             Enum.zip_with(claims, placements, fn claim, placement ->
               %{
                 "claim_id" => claim.claim_id,
                 "id" => claim.id,
                 "offset" => placement.offset,
                 "length" => placement.length,
                 "sha256" => claim.sha256,
                 "content_type" => claim.content_type,
                 "received_at" => claim.received_at
               }
             end)
  end

  describe "the manifest's digests" do
    setup do
      claims =
        claims([{"first", "application/json"}, {"", nil}, {:crypto.strong_rand_bytes(300), nil}])

      {data, _placements} = Pack.build(claims)
      {:ok, pack_id, 0} = Ref.locate(hd(claims).claim_id)
      %{claims: claims, bin: IO.iodata_to_binary(data), pack_id: pack_id}
    end

    # Where the manifest's entry starts: right after the last claim.
    defp manifest_json(bin, claims) do
      last = length(claims) - 1
      prefix = binary_part(bin, 0, Pack.index_prefix_bytes(last))
      {:ok, offset, length} = Pack.locate(prefix, last)
      header = binary_part(bin, offset + length, Pack.manifest_header_bytes())
      {:ok, size} = Pack.manifest_size(header)
      binary_part(bin, offset + length + Pack.manifest_header_bytes(), size)
    end

    test "count, then the manifest's place and size, then every claim's digest", %{
      claims: claims,
      bin: bin,
      pack_id: pack_id
    } do
      assert Pack.count(binary_part(bin, 0, 64)) == {:ok, 3}

      json = manifest_json(bin, claims)
      assert {:ok, digests} = Pack.digests(json, pack_id, 3)
      assert byte_size(digests) == 3 * 32

      for {claim, index} <- Enum.with_index(claims) do
        assert {:ok, digest} = Pack.digest(digests, index)
        assert Base.encode16(digest, case: :lower) == claim.sha256
      end

      assert Pack.digest(digests, 3) == :error
    end

    test "bytes that are not a pack's index or manifest header are refused", %{bin: bin} do
      assert Pack.count("not a pack at all, but long enough to hold a header or two") == :error
      assert Pack.count(binary_part(bin, 0, 10)) == :error
      assert Pack.manifest_size(binary_part(bin, 0, Pack.manifest_header_bytes())) == :error
      assert Pack.manifest_size("short") == :error
    end

    test "a manifest that is not exactly this pack's claims is refused", %{
      claims: claims,
      bin: bin,
      pack_id: pack_id
    } do
      json = manifest_json(bin, claims)

      assert Pack.digests(json, pack_id, 2) == :error
      assert Pack.digests(json, Ref.new_pack_id(), 3) == :error
      assert Pack.digests("{", pack_id, 3) == :error
      assert Pack.digests(~s({"claims": 5}), pack_id, 3) == :error
      assert Pack.digests(:binary.replace(json, ~s("sha256"), ~s("sha257")), pack_id, 3) == :error

      assert Pack.digests(
               :binary.replace(json, claims |> hd() |> Map.fetch!(:sha256), "XYZ"),
               pack_id,
               3
             ) == :error
    end
  end
end
