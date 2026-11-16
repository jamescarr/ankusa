defmodule AnkusaGatewayServer.CLITest do
  @moduledoc """
  The two lines the image's `check-config` and `print-config` commands print
  (`AnkusaGatewayServer.CLI`): the smoke test's one-line summary, and the
  redacted effective config the operator reads — core's view with the gateway's
  own under `"gateway"`, and no token in either.
  """

  use ExUnit.Case, async: true

  alias Ankusa.Admin.Auth.Token
  alias AnkusaGatewayServer.{CLI, Config}

  defp load!(yaml) do
    path = Path.join(System.tmp_dir!(), "gateway_cli_#{System.unique_integer([:positive])}.yml")
    File.write!(path, yaml)
    on_exit(fn -> File.rm(path) end)
    Config.load!(path: path, env: %{})
  end

  @yaml """
  config_version: 2
  storage: {type: local}
  gateway:
    port: 4321
    claims: {enabled: false}
    auth:
      authenticators:
        - type: token
          tokens: [{id: ops, token: plaintext-canary, scopes: [claims:read]}]
  """

  test "check-config prints the gateway's port, whether claims are served and the store" do
    assert CLI.check_config_line(load!(@yaml)) ==
             "config OK: gateway=4321 claims=off storage=Ankusa.BlobStore.LocalFS"
  end

  test "print-config prints core's redacted view with the gateway's under \"gateway\"" do
    view = CLI.config_view(load!(@yaml))

    assert %{"port" => 4321, "claims" => %{"enabled" => false}} = view["gateway"]
    assert %{"blob_store" => %{"module" => "Ankusa.BlobStore.LocalFS"}} = view["storage"]

    printed = JSON.encode!(view)
    refute printed =~ "plaintext-canary"
    refute printed =~ Token.hash("plaintext-canary")

    # The authenticator is named and typed; its token list keeps the id and the
    # scopes, and the hash is hidden.
    assert [%{"type" => "token", "name" => "token-0"} = authenticator] =
             view["gateway"]["auth"]["authenticators"]

    assert [token] = authenticator["config"]["tokens"]
    assert %{"id" => "ops", "sha256" => "[REDACTED]", "scopes" => ["claims:read"]} = token
  end

  test "a token given as its hash is still hidden, and a live config shows claims on" do
    loaded =
      load!("""
      config_version: 2
      gateway:
        port: 4002
        auth:
          authenticators:
            - type: token
              tokens: [{id: w, sha256: "#{Token.hash("t")}", scopes: [claims:read]}]
      """)

    assert CLI.check_config_line(loaded) =~ "claims=on"
    assert CLI.config_view(loaded)["gateway"]["port"] == 4002

    refute JSON.encode!(CLI.config_view(loaded)) =~ Token.hash("t")
  end
end
