defmodule Ankusa.BlobStore.GCSTest do
  @moduledoc """
  Integration test against the floci-gcp GCS emulator. Requires
  `docker compose up -d floci-gcp gcs-bootstrap`; run with
  `mix test --include integration`.
  """

  use ExUnit.Case, async: false
  @moduletag :integration

  alias Ankusa.BlobStore.GCS

  @opts [bucket: "ankusa-segments-dev", endpoint: "http://localhost:4588"]

  defp key(tag), do: "seg/it-#{tag}-#{System.unique_integer([:positive])}.seg"

  test "put/get round-trips a segment verbatim" do
    key = key("roundtrip")
    body = :crypto.strong_rand_bytes(4096)

    assert :ok = GCS.put(:i, key, body, @opts)
    assert {:ok, ^body} = GCS.get(:i, key, @opts)
  end

  test "get_range returns exactly the requested byte slice" do
    key = key("range")
    body = for i <- 0..255, into: <<>>, do: <<i>>

    assert :ok = GCS.put(:i, key, body, @opts)
    assert {:ok, <<10, 11, 12, 13>>} = GCS.get_range(:i, key, 10, 4, @opts)
  end

  test "get on a missing key is :not_found" do
    assert {:error, :not_found} = GCS.get(:i, key("missing"), @opts)
  end

  test "list returns keys under a prefix, and delete removes them" do
    key = key("listed")
    assert :ok = GCS.put(:i, key, "x", @opts)

    assert key in GCS.list(:i, "seg/it-listed", @opts)

    assert :ok = GCS.delete(:i, key, @opts)
    assert {:error, :not_found} = GCS.get(:i, key, @opts)
    refute key in GCS.list(:i, "seg/it-listed", @opts)
  end
end
