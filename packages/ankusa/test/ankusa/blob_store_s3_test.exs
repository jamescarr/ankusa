defmodule Ankusa.BlobStore.S3Test do
  @moduledoc """
  Integration test against the floci S3 emulator. Requires
  `docker compose -f docker-compose.integration.yml up -d floci s3-bootstrap`;
  run with `mix test --include integration`.
  """

  use ExUnit.Case, async: false
  @moduletag :integration

  alias Ankusa.BlobStore.S3

  @opts [
    bucket: "ankusa-segments-dev",
    region: "us-east-1",
    endpoint: "http://localhost:4566",
    access_key_id: "test",
    secret_access_key: "test"
  ]

  defp key(tag), do: "seg/it-#{tag}-#{System.unique_integer([:positive])}.seg"

  test "put/get round-trips a segment verbatim" do
    key = key("roundtrip")
    body = :crypto.strong_rand_bytes(4096)

    assert :ok = S3.put(:i, key, body, @opts)
    assert {:ok, ^body} = S3.get(:i, key, @opts)
  end

  test "put of iodata stores its bytes, signed over the same bytes" do
    key = key("iodata")
    chunk = :crypto.strong_rand_bytes(2048)

    assert :ok = S3.put(:i, key, [chunk, ["tail", ?!]], @opts)
    assert {:ok, body} = S3.get(:i, key, @opts)
    assert body == chunk <> "tail!"
  end

  test "get_range returns exactly the requested byte slice" do
    key = key("range")
    body = for i <- 0..255, into: <<>>, do: <<i>>

    assert :ok = S3.put(:i, key, body, @opts)
    assert {:ok, <<10, 11, 12, 13>>} = S3.get_range(:i, key, 10, 4, @opts)
  end

  test "get on a missing key is :not_found" do
    assert {:error, :not_found} = S3.get(:i, key("missing"), @opts)
  end

  test "list returns keys under a prefix, and delete removes them" do
    key = key("listed")
    assert :ok = S3.put(:i, key, "x", @opts)

    assert {:ok, keys} = S3.list(:i, "seg/it-listed", @opts)
    assert key in keys

    assert :ok = S3.delete(:i, key, @opts)
    assert {:error, :not_found} = S3.get(:i, key, @opts)
    assert {:ok, keys} = S3.list(:i, "seg/it-listed", @opts)
    refute key in keys
  end

  test "put_if_absent writes a free key, and refuses an existing one leaving it intact" do
    key = key("conditional")

    assert :ok = S3.put_if_absent(:i, key, "first", @opts)
    assert {:error, :already_exists} = S3.put_if_absent(:i, key, "second", @opts)
    assert {:ok, "first"} = S3.get(:i, key, @opts)

    # the same bytes again are the same object, not a collision
    assert :ok = S3.put_if_absent(:i, key, "first", @opts)
  end

  test "a 16 MiB object goes up in parts and reads back byte for byte" do
    key = key("multipart")
    body = :crypto.strong_rand_bytes(16 * 1_048_576)

    assert :ok = S3.put(:i, key, body, @opts)
    assert {:ok, ^body} = S3.get(:i, key, @opts)
    assert {:ok, slice} = S3.get_range(:i, key, 8 * 1_048_576 - 2, 4, @opts)
    assert slice == binary_part(body, 8 * 1_048_576 - 2, 4)
  end

  describe "objects" do
    test "put_object, head_object and get_object round-trip metadata and the version" do
      key = key("object-roundtrip")

      assert {:ok, version} =
               S3.put_object(
                 :i,
                 key,
                 "hello",
                 @opts ++ [content_type: "text/plain", metadata: %{"color" => "blue"}]
               )

      assert is_binary(version) and version != ""

      assert {:ok, meta} = S3.head_object(:i, key, @opts)
      assert meta.size == 5
      assert meta.content_type == "text/plain"
      assert meta.metadata["color"] == "blue"
      assert meta.version == version

      assert {:ok, got, "hello"} = S3.get_object(:i, key, @opts)
      assert got.size == 5
      assert got.version == version
      assert got.metadata["color"] == "blue"
    end

    test "get_object reads a range and still reports the whole size" do
      key = key("object-range")
      body = for i <- 0..255, into: <<>>, do: <<i>>

      assert {:ok, _version} = S3.put_object(:i, key, body, @opts)
      assert {:ok, meta, <<10, 11, 12, 13>>} = S3.get_object(:i, key, @opts ++ [range: {10, 13}])
      assert meta.size == 256
    end

    test "put_object's conditions: :absent creates once, and a stale version loses" do
      key = key("object-conditional")

      assert {:ok, version} = S3.put_object(:i, key, "first", @opts ++ [if: :absent])
      assert {:error, :already_exists} = S3.put_object(:i, key, "second", @opts ++ [if: :absent])

      assert {:ok, next} = S3.put_object(:i, key, "second", @opts ++ [if: {:version, version}])
      assert next != version

      assert {:error, :version_mismatch} =
               S3.put_object(:i, key, "third", @opts ++ [if: {:version, version}])

      assert {:ok, "second"} = S3.get(:i, key, @opts)
    end

    test "list_page lists sizes, a delimited prefix and the next cursor" do
      prefix = key("object-list") <> "/"
      assert {:ok, _} = S3.put_object(:i, prefix <> "a.txt", "aaa", @opts)
      assert {:ok, _} = S3.put_object(:i, prefix <> "b.txt", "bb", @opts)
      assert {:ok, _} = S3.put_object(:i, prefix <> "nested/c.txt", "c", @opts)

      assert {:ok, page} = S3.list_page(:i, prefix, @opts ++ [delimiter: "/"])
      assert %{key: prefix <> "a.txt", size: 3} in page.objects
      assert %{key: prefix <> "b.txt", size: 2} in page.objects
      assert (prefix <> "nested/") in page.prefixes

      assert {:ok, first} = S3.list_page(:i, prefix, @opts ++ [limit: 1])
      assert [%{key: _}] = first.objects
      assert is_binary(first.next)

      assert {:ok, second} = S3.list_page(:i, prefix, @opts ++ [limit: 1, cursor: first.next])
      assert [%{key: second_key}] = second.objects
      assert second_key > hd(first.objects).key
    end

    test "delete_object removes the object, and deleting it again is :ok" do
      key = key("object-delete")

      assert {:ok, _version} = S3.put_object(:i, key, "x", @opts)
      assert :ok = S3.delete_object(:i, key, @opts)
      assert {:error, :not_found} = S3.head_object(:i, key, @opts)
      assert :ok = S3.delete_object(:i, key, @opts)
    end
  end
end
