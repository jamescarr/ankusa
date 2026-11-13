defmodule AnkusaGatewayServer.ApplicationTest do
  @moduledoc """
  The image's boot, in the test VM: a config file, `autostart: true`, the
  application started; the gateway answers on its port, and `prep_stop/1`
  (SIGTERM) flips its `/ready` to `503` before the tree stops.
  """

  use ExUnit.Case, async: false

  @moduletag :capture_log

  setup do
    port = Ankusa.TestHelpers.free_port()
    dir = Path.join(System.tmp_dir!(), "gateway_app_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    path = Path.join(dir, "gateway.yml")

    File.write!(path, """
    config_version: 2
    node: {data_dir: #{dir}/data}
    # No pause before the listener closes: the drain is what is tested, not its timing.
    listener: {drain_delay_ms: 0}
    gateway: {port: #{port}}
    """)

    previous = System.get_env("ANKUSA_CONFIG")
    System.put_env("ANKUSA_CONFIG", path)
    Application.put_env(:ankusa_gateway_server, :autostart, true)

    on_exit(fn ->
      Application.stop(:ankusa_gateway_server)
      Application.put_env(:ankusa_gateway_server, :autostart, false)

      if previous,
        do: System.put_env("ANKUSA_CONFIG", previous),
        else: System.delete_env("ANKUSA_CONFIG")

      Ankusa.Health.undrain(:default)
      File.rm_rf(dir)
    end)

    # The test run started the application with autostart off: start it again.
    _ = Application.stop(:ankusa_gateway_server)
    {:ok, _apps} = Application.ensure_all_started(:ankusa_gateway_server)
    %{url: "http://127.0.0.1:#{port}"}
  end

  test "the gateway answers, and a drain flips /ready to 503", %{url: url} do
    assert %{status: 200, body: %{"status" => "ok"}} = Req.get!(url <> "/health", retry: false)
    assert %{status: 200, body: %{"gateway" => "ok"}} = Req.get!(url <> "/ready", retry: false)

    assert AnkusaGatewayServer.Application.prep_stop(:default) == :default

    assert %{status: 503, body: %{"status" => "draining"}} =
             Req.get!(url <> "/ready", retry: false)
  end

  test "without autostart the application starts nothing" do
    _ = Application.stop(:ankusa_gateway_server)
    Application.put_env(:ankusa_gateway_server, :autostart, false)
    {:ok, _apps} = Application.ensure_all_started(:ankusa_gateway_server)

    assert Supervisor.which_children(AnkusaGatewayServer.Supervisor) == []
    assert AnkusaGatewayServer.Application.prep_stop(nil) == nil
  end
end
