defmodule Ankusa.BlobStore.OCIIntegrationTest do
  @moduledoc """
  Integration test against the floci-oci Object Storage emulator. Requires
  `docker compose up -d floci-oci oci-bootstrap`; run with
  `mix test --include integration`.

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

    assert key in OCI.list(:i, "seg/it-listed", opts())

    assert :ok = OCI.delete(:i, key, opts())
    assert {:error, :not_found} = OCI.get(:i, key, opts())
    refute key in OCI.list(:i, "seg/it-listed", opts())
  end
end
