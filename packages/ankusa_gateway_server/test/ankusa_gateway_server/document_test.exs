defmodule AnkusaGatewayServer.DocumentTest do
  @moduledoc """
  Where the gateway image looks for its config file, from the operator's side:
  `ANKUSA_CONFIG`, else `/etc/ankusa/gateway.yml`, else `./gateway.yml` (the
  working directory of a `docker run`), and with none of them on disk the
  defaults with a warning — never a refusal to boot.
  """

  # Changes the working directory, which is global to the VM.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias AnkusaGatewayServer.Config

  @moduletag capture_log: true

  setup do
    dir = Path.join(System.tmp_dir!(), "gateway_locate_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    %{dir: dir}
  end

  test "./gateway.yml is read when nothing names one", %{dir: dir} do
    File.write!(Path.join(dir, "gateway.yml"), "config_version: 2\ngateway: {port: 4321}\n")

    File.cd!(dir, fn ->
      assert Config.load!(env: %{}).gateway.port == 4321
      assert Config.load!(env: %{}).doc["config_version"] == 2
    end)
  end

  test "with no file at all the defaults load, and a warning says so", %{dir: dir} do
    log =
      capture_log(fn ->
        File.cd!(dir, fn -> assert Config.load!(env: %{}).gateway.port == 4001 end)
      end)

    assert log =~ "no config file found; starting the gateway on defaults"
  end

  test "a file that is empty is an empty document" do
    path = Path.join(System.tmp_dir!(), "gateway_empty_#{System.unique_integer([:positive])}.yml")
    File.write!(path, "")
    on_exit(fn -> File.rm(path) end)

    loaded = Config.load!(path: path, env: %{})
    assert loaded.gateway.port == 4001
    assert loaded.config.claim_check.read_cache_bytes > 0
  end
end
