defmodule AnkusaGatewayServer.SecretsTest do
  @moduledoc """
  `${sm:}` references through the gateway image's loader: a token held in
  Vault and one held in AWS Secrets Manager reach the gateway's
  authenticators, hashed, and the plaintext reaches nothing. The managers are
  a loopback stub speaking just enough of Vault KV v2 and AWS `GetSecretValue`.
  """

  use ExUnit.Case, async: true

  alias Ankusa.Admin.Auth.Token
  alias AnkusaGatewayServer.Config

  defmodule Stub do
    @moduledoc false
    @behaviour Plug

    @impl true
    def init(test), do: test

    @impl true
    def call(conn, test) do
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      send(test, {:hit, conn.method, conn.request_path, conn.req_headers})

      case {conn.method, conn.request_path} do
        {"GET", "/v1/secret/data/gateway/workers"} ->
          json(conn, 200, %{"data" => %{"data" => %{"token" => "vault-token"}, "metadata" => %{}}})

        {"POST", "/"} ->
          %{"SecretId" => "prod/ops-token"} = JSON.decode!(body)
          json(conn, 200, %{"SecretString" => "aws-token"})

        _ ->
          json(conn, 404, %{"errors" => []})
      end
    end

    defp json(conn, status, term) do
      conn
      |> Plug.Conn.put_resp_content_type("application/json")
      |> Plug.Conn.send_resp(status, JSON.encode!(term))
    end
  end

  test "Vault and AWS references resolve into the gateway's token authenticator" do
    port = Ankusa.TestHelpers.free_port()

    start_supervised!(
      {Bandit, plug: {Stub, self()}, scheme: :http, ip: {127, 0, 0, 1}, port: port}
    )

    address = "http://127.0.0.1:#{port}"

    path = Path.join(System.tmp_dir!(), "gateway_sm_#{System.unique_integer([:positive])}.yml")
    on_exit(fn -> File.rm(path) end)

    File.write!(path, """
    config_version: 2
    secrets:
      providers:
        vault: {type: vault, address: "#{address}", token: vault-auth-canary}
        aws:
          type: aws
          region: us-east-1
          endpoint: "#{address}"
          access_key_id: AKIDEXAMPLE
          secret_access_key: aws-secret-canary
    gateway:
      auth:
        authenticators:
          - type: token
            tokens:
              - {id: workers, token: "${sm:vault:gateway/workers#token}", scopes: [claims:read]}
              - {id: ops, token: "${sm:aws:prod/ops-token}", scopes: [claims:read, "tenant:*"]}
    """)

    loaded = Config.load!(path: path, env: %{})

    assert [%{config: %{tokens: [workers, ops]}}] = loaded.gateway.auth.authenticators
    assert workers.sha256 == Token.hash("vault-token")
    assert ops.sha256 == Token.hash("aws-token")

    assert_received {:hit, "GET", "/v1/secret/data/gateway/workers", vault_headers}
    assert {"x-vault-token", "vault-auth-canary"} in vault_headers
    assert_received {:hit, "POST", "/", aws_headers}
    assert {"x-amz-target", "secretsmanager.GetSecretValue"} in aws_headers

    refute inspect(loaded.gateway) =~ "vault-token"
    refute inspect(loaded.config) =~ "aws-token"
  end
end
