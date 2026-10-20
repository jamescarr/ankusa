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
end
