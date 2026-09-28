defmodule AnkusaServer.GcsTokenTest do
  @moduledoc """
  The deterministic half of `AnkusaServer.GcsToken`: the cache the metadata path
  reads.

  The metadata *fetch* itself needs the GCE metadata server and cannot be made
  deterministic from here (the URL is fixed in the module, so there is no seam to
  stub) — it is exercised by running on GCE, which is where it matters.
  """

  use ExUnit.Case, async: false

  @cache_key {AnkusaServer.GcsToken, :token}

  test "metadata/0 uses a cached token that is not near expiry" do
    on_exit(fn -> :persistent_term.erase(@cache_key) end)

    :persistent_term.put(@cache_key, {"cached", System.monotonic_time(:second) + 3600})

    assert {:ok, "cached"} = AnkusaServer.GcsToken.metadata()
  end
end
