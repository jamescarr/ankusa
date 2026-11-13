defmodule Ankusa.BlobStore.AzureIntegrationTest do
  @moduledoc """
  Integration test against the floci-az Azure Blob emulator. Requires
  `docker compose -f docker-compose.integration.yml up -d floci-az azure-bootstrap`;
  run with `mix test --include integration`.

  floci-az runs in `dev` auth mode (credentials are not validated), so the
  adapter is exercised unauthenticated — the same way the floci S3/GCS suites
  prove request *plumbing* against a real Azure-shaped endpoint.
  """

  use ExUnit.Case, async: false
  @moduletag :integration

  alias Ankusa.BlobStore.Azure

  @opts [
    account_name: "devstoreaccount1",
    container: "ankusa-segments-dev",
    endpoint: "http://localhost:4577/devstoreaccount1"
  ]

  defp key(tag), do: "seg/it-#{tag}-#{System.unique_integer([:positive])}.seg"

  test "put/get round-trips a segment verbatim" do
    key = key("roundtrip")
    body = :crypto.strong_rand_bytes(4096)

    assert :ok = Azure.put(:i, key, body, @opts)
    assert {:ok, ^body} = Azure.get(:i, key, @opts)
  end

  test "get_range returns exactly the requested byte slice" do
    key = key("range")
    body = for i <- 0..255, into: <<>>, do: <<i>>

    assert :ok = Azure.put(:i, key, body, @opts)
    assert {:ok, <<10, 11, 12, 13>>} = Azure.get_range(:i, key, 10, 4, @opts)
  end

  test "get on a missing key is :not_found" do
    assert {:error, :not_found} = Azure.get(:i, key("missing"), @opts)
  end

  test "list returns keys under a prefix, and delete removes them" do
    key = key("listed")
    assert :ok = Azure.put(:i, key, "x", @opts)

    assert {:ok, keys} = Azure.list(:i, "seg/it-listed", @opts)
    assert key in keys

    assert :ok = Azure.delete(:i, key, @opts)
    assert {:error, :not_found} = Azure.get(:i, key, @opts)
    assert {:ok, keys} = Azure.list(:i, "seg/it-listed", @opts)
    refute key in keys
  end

  test "put_if_absent writes a free key, and refuses an existing one leaving it intact" do
    key = key("conditional")

    assert :ok = Azure.put_if_absent(:i, key, "first", @opts)
    assert {:error, :already_exists} = Azure.put_if_absent(:i, key, "second", @opts)
    assert {:ok, "first"} = Azure.get(:i, key, @opts)

    # the same bytes again are the same object, not a collision
    assert :ok = Azure.put_if_absent(:i, key, "first", @opts)
  end

  test "a 16 MiB object goes up in parts and reads back byte for byte" do
    key = key("multipart")
    body = :crypto.strong_rand_bytes(16 * 1_048_576)

    assert :ok = Azure.put(:i, key, body, @opts)
    assert {:ok, ^body} = Azure.get(:i, key, @opts)
    assert {:ok, slice} = Azure.get_range(:i, key, 8 * 1_048_576 - 2, 4, @opts)
    assert slice == binary_part(body, 8 * 1_048_576 - 2, 4)
  end

  describe "objects" do
    test "put_object, head_object and get_object round-trip a blob with its type" do
      key = key("roundtrip")
      body = "hello objects"

      assert {:ok, version} =
               Azure.put_object(:i, key, body, @opts ++ [content_type: "text/plain"])

      assert is_binary(version)

      assert {:ok, meta} = Azure.head_object(:i, key, @opts)
      assert meta.size == byte_size(body)
      assert meta.version == version
      assert meta.content_type == "text/plain"

      assert {:ok, %{version: ^version}, ^body} = Azure.get_object(:i, key, @opts)
    end

    test "get_object returns the requested range and reports the whole size" do
      key = key("range")
      body = for i <- 0..255, into: <<>>, do: <<i>>

      assert {:ok, _version} = Azure.put_object(:i, key, body, @opts)

      assert {:ok, meta, <<10, 11, 12, 13>>} =
               Azure.get_object(:i, key, @opts ++ [range: {10, 13}])

      assert meta.size == 256
    end

    test "a version condition: a stale :if is :version_mismatch, the current one writes" do
      key = key("cas")
      assert {:ok, v1} = Azure.put_object(:i, key, "first", @opts)

      assert {:error, :version_mismatch} =
               Azure.put_object(:i, key, "second", @opts ++ [if: {:version, "stale"}])

      assert {:ok, v2} = Azure.put_object(:i, key, "second", @opts ++ [if: {:version, v1}])
      refute v2 == v1

      assert {:ok, %{version: ^v2}, "second"} = Azure.get_object(:i, key, @opts)

      assert {:error, :already_exists} =
               Azure.put_object(:i, key, "third", @opts ++ [if: :absent])
    end

    test "list_page pages under a prefix, and delete_object removes the blob" do
      prefix = key("list") <> "/"
      first = prefix <> "a.txt"
      second = prefix <> "b.txt"

      assert {:ok, _version} = Azure.put_object(:i, first, "a", @opts)
      assert {:ok, _version} = Azure.put_object(:i, second, "b", @opts)

      assert {:ok, page} = Azure.list_page(:i, prefix, @opts ++ [limit: 1])
      assert [%{key: ^first, size: 1}] = page.objects
      assert is_binary(page.next)

      assert {:ok, rest} = Azure.list_page(:i, prefix, @opts ++ [cursor: page.next])
      assert [%{key: ^second, size: 1}] = rest.objects

      assert :ok = Azure.delete_object(:i, first, @opts)
      assert {:error, :not_found} = Azure.head_object(:i, first, @opts)
      assert :ok = Azure.delete_object(:i, first, @opts)
    end

    test "put_object stores the content type and the metadata" do
      key = key("metadata")

      assert {:ok, _version} =
               Azure.put_object(
                 :i,
                 key,
                 "meta",
                 @opts ++ [content_type: "text/plain", metadata: %{"owner" => "alice"}]
               )

      assert {:ok, meta} = Azure.head_object(:i, key, @opts)
      assert meta.content_type == "text/plain"
      assert meta.metadata == %{"owner" => "alice"}
    end
  end
end
