defmodule Ankusa.HttpClientTest do
  @moduledoc """
  `Ankusa.HttpClient` against a real socket: what Finch's connection pool
  does when it is exhausted, which no `Req.Test` plug reaches.
  """

  # `capture_io(:stderr, ...)` is VM-wide.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Ankusa.HttpClient

  defmodule SlowPlug do
    @behaviour Plug
    import Plug.Conn

    @impl true
    def init(opts), do: opts

    @impl true
    def call(conn, _opts) do
      Process.sleep(300)
      send_resp(conn, 200, "ok")
    end
  end

  test "an exhausted connection pool is a transport error, never a raise" do
    port = Ankusa.TestHelpers.free_port()
    start_supervised!({Bandit, plug: SlowPlug, scheme: :http, ip: {127, 0, 0, 1}, port: port})
    url = "http://127.0.0.1:#{port}/"
    test = self()

    # A connect timeout nobody else uses gets this test a pool of its own (50
    # connections); 70 requests that each hold one for 300 ms with a 20 ms
    # checkout timeout must leave some without one. Req warns on the
    # top-level `pool_timeout` key, which is why stderr is captured.
    capture_io(:stderr, fn ->
      results =
        1..70
        |> Task.async_stream(
          fn _ ->
            HttpClient.request(:get, url, [], nil, 5_000,
              connect_options: [timeout: 7_777],
              pool_timeout: 20
            )
          end,
          max_concurrency: 70,
          timeout: 10_000
        )
        |> Enum.map(fn {:ok, result} -> result end)

      send(test, {:results, results})
    end)

    assert_received {:results, results}
    assert Enum.count(results, &match?({:ok, 200, "ok"}, &1)) >= 50

    errors = Enum.reject(results, &match?({:ok, 200, _}, &1))
    assert errors != []
    assert Enum.all?(errors, &match?({:error, %RuntimeError{}}, &1))
  end
end
