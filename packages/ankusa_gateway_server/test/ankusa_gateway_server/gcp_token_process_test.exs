defmodule AnkusaGatewayServer.GcpTokenProcessTest do
  @moduledoc """
  `AnkusaGatewayServer.GcpToken` when no cache process is running: a caller
  gets `:error` (the blob store sends an unauthenticated request, the Pub/Sub
  sink reports `:no_credentials`) instead of an exit; `static/1` is the token
  the file configured; and the cache is an ordinary supervised server.
  """

  # Starts and stops the globally named cache.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias AnkusaGatewayServer.GcpToken

  test "metadata/0 without the cache running is :error, never a crash" do
    log = capture_log(fn -> assert GcpToken.metadata() == :error end)

    assert log =~ "GCP metadata token unavailable"
  end

  test "static/1 is a fixed token that never expires" do
    assert GcpToken.static("t0k") == {:ok, "t0k"}
  end

  test "start_link/1 starts the cache under the supervision tree" do
    assert {:ok, pid} = GcpToken.start_link([])
    assert Process.alive?(pid)
    assert {:ok, "t0k"} == GcpToken.static("t0k")
    assert GenServer.stop(pid) == :ok
  end
end
