defmodule AnkusaServer.GcsTokenTest do
  @moduledoc """
  `AnkusaServer.GcsToken.metadata/0` against a `Req.Test` stand-in for the GCE
  metadata server: the cache, single-flight refresh, and no retries.
  """

  use ExUnit.Case, async: false

  alias AnkusaServer.GcsToken

  @moduletag capture_log: true

  setup do
    test = self()
    pid = start_supervised!({GcsToken, req_options: [plug: {Req.Test, GcsToken}]})
    Req.Test.allow(GcsToken, test, pid)
    %{server: pid}
  end

  defp stub(fun) do
    test = self()

    Req.Test.stub(GcsToken, fn conn ->
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
      |> Enum.map(fn _ -> Task.async(&GcsToken.metadata/0) end)
      |> Enum.map(&Task.await(&1, 5_000))

    assert results == List.duplicate({:ok, "tok"}, 10)
    assert {:ok, "tok"} = GcsToken.metadata()
    assert requests() == 1
  end

  test "a failed fetch is :error after exactly one request, and the next call tries again" do
    stub(fn conn -> Plug.Conn.send_resp(conn, 500, "boom") end)

    assert GcsToken.metadata() == :error
    assert requests() == 1

    stub(fn conn -> Req.Test.json(conn, %{"access_token" => "late", "expires_in" => "120"}) end)
    assert {:ok, "late"} = GcsToken.metadata()
  end

  test "a token close to expiry is refreshed" do
    stub(fn conn -> Req.Test.json(conn, %{"access_token" => "short", "expires_in" => 30}) end)

    assert {:ok, "short"} = GcsToken.metadata()
    assert {:ok, "short"} = GcsToken.metadata()
    # 30 s of life is inside the 60 s margin: every call refetches.
    assert requests() == 2
  end
end
