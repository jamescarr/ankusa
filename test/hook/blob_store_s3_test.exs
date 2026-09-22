defmodule Hook.BlobStore.S3Test do
  @moduledoc """
  Integration test against the floci S3 emulator. Requires
  `docker compose up -d floci s3-bootstrap`; run with
  `mix test --include integration`.
  """

  use ExUnit.Case, async: false
  @moduletag :integration

  alias Hook.BlobStore.S3

  @opts [
    bucket: "hook-segments-dev",
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

    assert key in S3.list(:i, "seg/it-listed", @opts)

    assert :ok = S3.delete(:i, key, @opts)
    assert {:error, :not_found} = S3.get(:i, key, @opts)
    refute key in S3.list(:i, "seg/it-listed", @opts)
  end
end
