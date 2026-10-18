defmodule AnkusaServer.GcpTokenTest do
  @moduledoc """
  `AnkusaServer.GcpToken.metadata/0` against a `Req.Test` stand-in for the GCE
  metadata server: the cache, single-flight refresh, and no retries.
  """

  use ExUnit.Case, async: false

  alias AnkusaServer.GcpToken

  @moduletag capture_log: true

  setup do
    test = self()

    pid =
      start_supervised!(
        {GcpToken, req_options: [plug: {Req.Test, GcpToken}], failure_backoff_ms: 300}
      )

    Req.Test.allow(GcpToken, test, pid)
    %{server: pid}
  end

  defp stub(fun) do
    test = self()

    Req.Test.stub(GcpToken, fn conn ->
      send(test, :metadata_request)
      fun.(conn)
    end)
  end

  defp requests(acc \\ 0) do
    receive do
      :metadata_request -> requests(acc + 1)
    after
      100 -> acc
    end
  end

  test "concurrent callers share one fetch, and the token is cached" do
    stub(fn conn ->
      # Slow enough that every caller arrives while the first fetch runs.
      Process.sleep(200)
      Req.Test.json(conn, %{"access_token" => "tok", "expires_in" => 3600})
    end)

    results =
      1..10
      |> Enum.map(fn _ -> Task.async(&GcpToken.metadata/0) end)
      |> Enum.map(&Task.await(&1, 5_000))

    assert results == List.duplicate({:ok, "tok"}, 10)
    assert {:ok, "tok"} = GcpToken.metadata()
    assert requests() == 1
  end

  test "a failed fetch is :error after exactly one request, and a later call tries again" do
    stub(fn conn -> Plug.Conn.send_resp(conn, 500, "boom") end)

    assert GcpToken.metadata() == :error
    assert requests() == 1

    stub(fn conn -> Req.Test.json(conn, %{"access_token" => "late", "expires_in" => "120"}) end)
    Process.sleep(300)
    assert {:ok, "late"} = GcpToken.metadata()
  end

  test "callers queued behind a failing fetch share its failure" do
    stub(fn conn ->
      Process.sleep(200)
      Plug.Conn.send_resp(conn, 500, "boom")
    end)

    results =
      1..10
      |> Enum.map(fn _ -> Task.async(&GcpToken.metadata/0) end)
      |> Enum.map(&Task.await(&1, 5_000))

    assert results == List.duplicate(:error, 10)
    assert requests() == 1
  end

  test "a token close to expiry is refreshed" do
    stub(fn conn -> Req.Test.json(conn, %{"access_token" => "short", "expires_in" => 30}) end)

    assert {:ok, "short"} = GcpToken.metadata()
    assert {:ok, "short"} = GcpToken.metadata()
    # 30 s of life is inside the 60 s margin: every call refetches.
    assert requests() == 2
  end
end
