defmodule Ankusa.Gateway.SupervisorTest do
  @moduledoc """
  The gateway as an embedder starts it: a core instance with no role, then
  `{Ankusa.Gateway, config}`, over real sockets.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.ClaimCheck.Ref

  defp get(port, path), do: Req.get!("http://127.0.0.1:#{port}#{path}", retry: false)

  defp start(core_opts \\ []) do
    metrics_port = free_port()
    port = free_port()

    config =
      test_config(
        [
          roles: [],
          metrics: %{enabled: true, port: metrics_port, extra: [Ankusa.Gateway.Metrics]}
        ] ++ core_opts
      )

    start_supervised!({Ankusa.Instance, config})

    start_supervised!(
      {Ankusa.Gateway, Ankusa.Gateway.Config.new(instance: config.instance, port: port)}
    )

    %{inst: config.instance, port: port, metrics_port: metrics_port}
  end

  @tag :capture_log
  test "health, readiness and the metrics of a claim request over real sockets" do
    %{inst: inst, port: port, metrics_port: metrics_port} = start()

    assert %{status: 200, body: %{"status" => "ok"}} = get(port, "/health")
    assert %{status: 200, body: %{"gateway" => "ok", "status" => "ready"}} = get(port, "/ready")

    {:ok, claims} = Ankusa.ClaimCheck.check_in(inst, "acme", [%{id: "a", body: "hello"}])
    assert %{status: 200, body: "hello"} = get(port, Ref.path(claims["a"].ref))

    scrape = get(metrics_port, "/metrics").body

    assert scrape =~
             ~r/ankusa_gateway_requests_total\{api="claims",instance="#{inst}",status="200"\} 1/

    assert scrape =~ "ankusa_gateway_request_duration_seconds_bucket{"
  end

  @tag :capture_log
  test "boots beside a wal: :none core" do
    %{port: port} = start(wal: :none)
    assert %{status: 200} = get(port, "/ready")
  end

  @tag :capture_log
  test "the gateway's own failure domain is :gateway, under its own supervisor" do
    %{inst: inst} = start()
    assert is_pid(Ankusa.whereis(inst, :gateway))
    assert is_pid(Ankusa.Instance.Isolated.subtree(inst, :gateway))
  end

  test "starting the gateway without the instance raises" do
    inst = :"nogw#{System.unique_integer([:positive])}"
    config = Ankusa.Gateway.Config.new(instance: inst, port: 0)

    assert {:error, {{%ArgumentError{message: message}, _stack}, _spec}} =
             start_supervised({Ankusa.Gateway, config})

    assert message == "Ankusa.Gateway: start Ankusa.Instance #{inst} first"
  end
end
