defmodule AnkusaGatewayServer.ConfigTest do
  @moduledoc """
  The gateway image's config file, from the operator's side: a file written
  for the full image's gateway loads here, the full image's own sections are
  refused by name, the env overrides win, and every error names its key.
  """

  use ExUnit.Case, async: true

  alias Ankusa.Admin.Auth.Token
  alias AnkusaGatewayServer.{Config, ConfigError}

  @tls_dir Path.expand("../../../ankusa/test/fixtures/tls", __DIR__)

  defp tmp_config(yaml) do
    path = Path.join(System.tmp_dir!(), "gateway_#{System.unique_integer([:positive])}.yml")
    File.write!(path, yaml)
    on_exit(fn -> File.rm(path) end)
    path
  end

  defp load!(yaml, env \\ %{}), do: Config.load!(path: tmp_config(yaml), env: env)

  defp error!(yaml, env \\ %{}) do
    assert_raise(ConfigError, fn -> load!(yaml, env) end).message
  end

  test "a version-1 file written for the full image's gateway loads, migrated" do
    hash = Token.hash("worker-token")

    loaded =
      load!("""
      node:
        roles: [claim_check]
        data_dir: /tmp/gw-data
      storage:
        type: local
      claim_check:
        port: 4100
        ip: 0.0.0.0
        auth:
          mode: token
          tokens: [{id: worker, sha256: #{hash}, scopes: [claims:read, "tenant:*"]}]
        read_cache_bytes: 0
      """)

    assert %Ankusa.Config{roles: [], data_dir: "/tmp/gw-data"} = loaded.config
    assert loaded.config.claim_check.read_cache_bytes == 0
    assert loaded.config.metrics.extra == [Ankusa.Gateway.Metrics]

    assert %Ankusa.Gateway.Config{
             port: 4100,
             ip: "0.0.0.0",
             auth: %{authenticators: [%{type: :token, config: %{tokens: [%{id: "worker"}]}}]}
           } = loaded.gateway

    assert loaded.doc["config_version"] == 2
  end

  test "the image's own gateway.yml loads" do
    loaded = Config.load!(path: Path.expand("../../rel/gateway.yml", __DIR__), env: %{})
    assert loaded.gateway.port == 4001
    assert loaded.config.metrics.enabled
    assert loaded.config.roles == []
  end

  test "every section of the full image is refused by name; anything else is an unknown key" do
    for key <-
          ~w(http admin routes rate_limits dedupe fleet quarantine batcher dispatch wal intake backup sources source_store lifecycle audit tenants queue archive health) do
      assert error!("config_version: 2\n#{key}: {}\n") ==
               "#{key}: not available in the gateway image (jamescarr/ankusa-gateway); use jamescarr/ankusa"
    end

    assert error!("config_version: 2\nnope: {}\n") == ~s(config: unknown key "nope")
  end

  test "node.roles is [claim_check] or absent" do
    assert load!("node: {roles: [claim_check]}\n").config.roles == []

    for roles <- ["[edge]", "[claim_check, dispatch]"] do
      assert error!("node: {roles: #{roles}}\n") ==
               "node.roles: this image runs only the gateway; remove it or set [claim_check]"
    end

    assert error!("node: {roles: [claim_check]}\n", %{"ANKUSA_ROLES" => "edge"}) =~
             "node.roles: this image runs only the gateway"
  end

  test "each env override reaches its key" do
    loaded =
      load!("config_version: 2\n", %{
        "ANKUSA_ROLES" => "claim_check",
        "ANKUSA_DATA_DIR" => "/tmp/gw-env",
        "ANKUSA_REGION" => "eu-west-1",
        "ANKUSA_LOG_LEVEL" => "debug",
        "ANKUSA_LOG_FORMAT" => "json",
        "ANKUSA_CLAIM_CHECK_PORT" => "4111",
        "ANKUSA_CLAIM_CHECK_IP" => "0.0.0.0",
        "ANKUSA_CLAIM_CHECK_TOKEN" => "env-token",
        "ANKUSA_METRICS_ENABLED" => "true",
        "ANKUSA_METRICS_PORT" => "9400",
        "ANKUSA_METRICS_IP" => "0.0.0.0"
      })

    assert loaded.config.data_dir == "/tmp/gw-env"
    assert loaded.config.dispatch.region == "eu-west-1"
    assert loaded.log == %{level: :debug, format: :json}
    assert %{enabled: true, port: 9400, ip: "0.0.0.0"} = loaded.config.metrics
    assert %{port: 4111, ip: "0.0.0.0"} = loaded.gateway

    assert [%{config: %{tokens: [%{id: "ANKUSA_CLAIM_CHECK_TOKEN", sha256: sha}]}}] =
             loaded.gateway.auth.authenticators

    assert sha == Token.hash("env-token")
    refute inspect(loaded) =~ "env-token"

    s3 =
      load!("config_version: 2\n", %{
        "ANKUSA_STORAGE_TYPE" => "s3",
        "ANKUSA_S3_BUCKET" => "claims",
        "ANKUSA_S3_REGION" => "us-west-2"
      })

    assert {Ankusa.BlobStore.S3, opts} = s3.config.storage.blob_store
    assert {opts[:bucket], opts[:region]} == {"claims", "us-west-2"}

    gcs =
      load!("config_version: 2\nstorage: {type: gcs, gcs: {auth: none}}\n", %{
        "ANKUSA_GCS_BUCKET" => "gb"
      })

    assert {Ankusa.BlobStore.GCS, gcs_opts} = gcs.config.storage.blob_store
    assert gcs_opts[:bucket] == "gb"
  end

  test "an unset ${VAR} names the dotted key; a ${file:} secret resolves" do
    assert error!("config_version: 2\ngateway: {ip: \"${GW_IP}\"}\n") ==
             "gateway.ip: ${GW_IP} is not set"

    dir = Path.join(System.tmp_dir!(), "gw_secret_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    file = Path.join(dir, "token")
    File.write!(file, "file-token\n")

    loaded =
      load!("""
      config_version: 2
      gateway:
        auth:
          authenticators:
            - type: token
              tokens: [{id: w, token: "${file:#{file}}", scopes: [claims:read]}]
      """)

    assert [%{config: %{tokens: [%{sha256: sha}]}}] = loaded.gateway.auth.authenticators
    assert sha == Token.hash("file-token")
  end

  test "TLS and authentication mistakes carry gateway. paths" do
    tls = "certfile: #{@tls_dir}/server.pem, keyfile: #{@tls_dir}/server-key.pem"

    for {yaml, message} <- [
          {"gateway: {tls: {certfile: /nope.pem, keyfile: /nope-key.pem}}",
           "gateway.tls.certfile must be a readable file"},
          {"gateway: {tls: {#{tls}, verify: maybe}}",
           ~s(gateway.tls.verify: unknown value "maybe")},
          {"gateway: {ip: 0.0.0.0}",
           "gateway.auth.authenticators is empty but gateway.ip (0.0.0.0)"},
          {"gateway: {auth: {authenticators: [{type: jwt, issuer: i, audience: a}]}}",
           "gateway.auth.authenticators[0] needs jwks_url or keys"},
          {"gateway: {auth: {authenticators: [{type: mtls, subject_map: {\"cn:a\": [claims:read]}}]}}",
           "gateway.auth.authenticators[0]: mtls needs gateway.tls with verify: :verify_peer"},
          {"gateway: {auth: {authenticators: [{type: token, tokens: [{id: a, sha256: zz}]}]}}",
           "gateway.auth.authenticators[0].tokens[0].sha256 must be the 64-character hex"},
          {"claim_check: {port: 1}", "claim_check.port: moved to gateway.port (config_version 2)"}
        ] do
      assert error!("config_version: 2\n" <> yaml <> "\n") =~ message, yaml
    end
  end
end
