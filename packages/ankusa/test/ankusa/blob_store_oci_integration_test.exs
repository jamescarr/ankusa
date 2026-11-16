defmodule Ankusa.BlobStore.OCIIntegrationTest do
  @moduledoc """
  Integration test against the floci-oci Object Storage emulator. Requires
  `docker compose -f docker-compose.integration.yml up -d floci-oci oci-bootstrap`;
  run with `mix test --include integration`.

  floci-oci parses the request signature for tenancy context but never verifies
  it, so any locally generated RSA key works — the adapter's real signing path
  is exercised (the `Authorization` header is built and sent), and the
  signature itself is pinned separately in
  `test/ankusa/blob_store_oci_signing_test.exs` against OCI's reference vectors.
  """

  use ExUnit.Case, async: false
  @moduletag :integration

  alias Ankusa.BlobStore.OCI
  alias Ankusa.Test.OCIKey, as: Key

  defp opts do
    [
      region: "us-ashburn-1",
      namespace: "floci-local",
      bucket: "ankusa-segments-dev",
      endpoint: "http://localhost:4599",
      tenancy_ocid:
        "ocid1.tenancy.oc1..flocilocaltenancy0000000000000000000000000000000000000000",
      user_ocid: "ocid1.user.oc1..anyuser",
      key_fingerprint: "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99",
      private_key: Key.private_key_pem()
    ]
  end

  defp key(tag), do: "seg/it-#{tag}-#{System.unique_integer([:positive])}.seg"

  test "put/get round-trips a segment verbatim" do
    key = key("roundtrip")
    body = :crypto.strong_rand_bytes(4096)

    assert :ok = OCI.put(:i, key, body, opts())
    assert {:ok, ^body} = OCI.get(:i, key, opts())
  end

  test "get_range returns exactly the requested byte slice" do
    key = key("range")
    body = for i <- 0..255, into: <<>>, do: <<i>>

    assert :ok = OCI.put(:i, key, body, opts())
    assert {:ok, <<10, 11, 12, 13>>} = OCI.get_range(:i, key, 10, 4, opts())
  end

  test "get on a missing key is :not_found" do
    assert {:error, :not_found} = OCI.get(:i, key("missing"), opts())
  end

  test "list returns keys under a prefix, and delete removes them" do
    key = key("listed")
    assert :ok = OCI.put(:i, key, "x", opts())

    assert {:ok, keys} = OCI.list(:i, "seg/it-listed", opts())
    assert key in keys

    assert :ok = OCI.delete(:i, key, opts())
    assert {:error, :not_found} = OCI.get(:i, key, opts())
    assert {:ok, keys} = OCI.list(:i, "seg/it-listed", opts())
    refute key in keys
  end

  test "put_if_absent writes a free key, and refuses an existing one leaving it intact" do
    key = key("conditional")

    assert :ok = OCI.put_if_absent(:i, key, "first", opts())
    assert {:error, :already_exists} = OCI.put_if_absent(:i, key, "second", opts())
    assert {:ok, "first"} = OCI.get(:i, key, opts())

    # the same bytes again are the same object, not a collision
    assert :ok = OCI.put_if_absent(:i, key, "first", opts())
  end

  test "a 16 MiB object goes up in parts and reads back byte for byte" do
    key = key("multipart")
    body = :crypto.strong_rand_bytes(16 * 1_048_576)

    assert :ok = OCI.put(:i, key, body, opts())
    assert {:ok, ^body} = OCI.get(:i, key, opts())
    assert {:ok, slice} = OCI.get_range(:i, key, 8 * 1_048_576 - 2, 4, opts())
    assert slice == binary_part(body, 8 * 1_048_576 - 2, 4)
  end

  describe "objects" do
    test "put_object, head_object and get_object round-trip an object with its type" do
      key = key("roundtrip")
      body = "hello objects"

      assert {:ok, version} =
               OCI.put_object(:i, key, body, opts() ++ [content_type: "text/plain"])

      assert is_binary(version)

      assert {:ok, meta} = OCI.head_object(:i, key, opts())
      assert meta.size == byte_size(body)
      assert meta.version == version
      assert meta.content_type == "text/plain"

      assert {:ok, %{version: ^version}, ^body} = OCI.get_object(:i, key, opts())
    end

    test "get_object returns the requested range and reports the whole size" do
      key = key("range")
      body = for i <- 0..255, into: <<>>, do: <<i>>

      assert {:ok, _version} = OCI.put_object(:i, key, body, opts())

      assert {:ok, meta, <<10, 11, 12, 13>>} =
               OCI.get_object(:i, key, opts() ++ [range: {10, 13}])

      assert meta.size == 256
    end

    test "a version condition: a stale :if is :version_mismatch, the current one writes" do
      key = key("cas")
      assert {:ok, v1} = OCI.put_object(:i, key, "first", opts())

      assert {:error, :version_mismatch} =
               OCI.put_object(:i, key, "second", opts() ++ [if: {:version, "stale"}])

      assert {:ok, v2} = OCI.put_object(:i, key, "second", opts() ++ [if: {:version, v1}])
      refute v2 == v1

      assert {:ok, %{version: ^v2}, "second"} = OCI.get_object(:i, key, opts())

      assert {:error, :already_exists} = OCI.put_object(:i, key, "third", opts() ++ [if: :absent])
    end

    test "list_page pages under a prefix, and delete_object removes the object" do
      prefix = key("list") <> "/"
      first = prefix <> "a.txt"
      second = prefix <> "b.txt"

      assert {:ok, _version} = OCI.put_object(:i, first, "a", opts())
      assert {:ok, _version} = OCI.put_object(:i, second, "b", opts())

      assert {:ok, page} = OCI.list_page(:i, prefix, opts() ++ [limit: 1])
      assert [%{key: ^first, size: 1}] = page.objects
      assert is_binary(page.next)

      assert {:ok, rest} = OCI.list_page(:i, prefix, opts() ++ [cursor: page.next])
      assert [%{key: ^second, size: 1}] = rest.objects

      assert :ok = OCI.delete_object(:i, first, opts())
      assert {:error, :not_found} = OCI.head_object(:i, first, opts())
      assert :ok = OCI.delete_object(:i, first, opts())
    end

    test "put_object stores the content type and the metadata" do
      key = key("metadata")

      assert {:ok, _version} =
               OCI.put_object(
                 :i,
                 key,
                 "meta",
                 opts() ++ [content_type: "text/plain", metadata: %{"owner" => "alice"}]
               )

      assert {:ok, meta} = OCI.head_object(:i, key, opts())
      assert meta.content_type == "text/plain"
      assert meta.metadata == %{"owner" => "alice"}
    end
  end
end
