defmodule Ankusa.BlobStore.AzureIntegrationTest do
  @moduledoc """
  Integration test against the floci-az Azure Blob emulator. Requires
  `docker compose up -d floci-az azure-bootstrap`; run with
  `mix test --include integration`.

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

    assert key in Azure.list(:i, "seg/it-listed", @opts)

    assert :ok = Azure.delete(:i, key, @opts)
    assert {:error, :not_found} = Azure.get(:i, key, @opts)
    refute key in Azure.list(:i, "seg/it-listed", @opts)
  end
end
