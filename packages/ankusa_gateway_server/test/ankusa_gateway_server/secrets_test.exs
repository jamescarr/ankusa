defmodule AnkusaGatewayServer.SecretsTest do
  @moduledoc """
  `${file:}` and `${sm:}` references through the gateway image's loader, from
  the operator's side: the file a Kubernetes secret volume holds, a Vault KV v2
  and an AWS Secrets Manager secret reaching the gateway's authenticators
  (hashed) or `secrets.kek`, and every mistake named by dotted key and
  reference, never by value.

  The managers are a loopback stub speaking just enough of Vault KV v2 and AWS
  `GetSecretValue`.
  """

  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Ankusa.Admin.Auth.Token
  alias AnkusaGatewayServer.{Config, ConfigError, Secrets}

  @moduletag capture_log: true

  @kek Base.encode64(:binary.copy(<<7>>, 32))

  defmodule Stub do
    @moduledoc false
    # State lives in an Agent: `vault` (path => fields | :denied | :malformed),
    # `aws` (id => string | {:binary, bytes} | {:status, code, body}),
    # `down?` (every request answers 503), `hits` (newest first).
    @behaviour Plug

    @impl true
    def init(agent), do: agent

    @impl true
    def call(conn, agent) do
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      Agent.update(
        agent,
        &%{&1 | hits: [{conn.method, conn.request_path, conn.req_headers} | &1.hits]}
      )

      state = Agent.get(agent, & &1)

      cond do
        state.down? -> Plug.Conn.send_resp(conn, 503, "down")
        conn.method == "GET" -> vault(conn, state)
        true -> aws(conn, state, body)
      end
    end

    defp vault(%{request_path: "/v1/" <> rest} = conn, state) do
      case String.split(rest, "/data/", parts: 2) do
        [_mount, path] -> vault_secret(conn, state, path)
        _not_a_data_path -> json(conn, 404, %{"errors" => []})
      end
    end

    defp vault(conn, _state), do: json(conn, 404, %{"errors" => []})

    defp vault_secret(conn, state, path) do
      case state.vault do
        %{^path => :denied} ->
          json(conn, 403, %{"errors" => ["permission denied"]})

        %{^path => :malformed} ->
          Plug.Conn.send_resp(conn, 200, "not json")

        %{^path => fields} ->
          json(conn, 200, %{"data" => %{"data" => fields, "metadata" => %{}}})

        _missing ->
          json(conn, 404, %{"errors" => []})
      end
    end

    defp aws(conn, state, body) do
      %{"SecretId" => id} = JSON.decode!(body)

      case state.aws do
        %{^id => {:status, status, reply}} ->
          json(conn, status, reply)

        %{^id => {:binary, bytes}} ->
          json(conn, 200, %{"SecretBinary" => Base.encode64(bytes)})

        %{^id => :malformed} ->
          Plug.Conn.send_resp(conn, 200, "not json")

        %{^id => value} ->
          json(conn, 200, %{"SecretString" => value})

        _missing ->
          json(conn, 400, %{"__type" => "ResourceNotFoundException", "message" => "nope"})
      end
    end

    defp json(conn, status, term) do
      conn
      |> Plug.Conn.put_resp_content_type("application/json")
      |> Plug.Conn.send_resp(status, JSON.encode!(term))
    end
  end

  # ── fixtures ────────────────────────────────────────────────────────────────

  setup do
    dir = Path.join(System.tmp_dir!(), "gateway_secrets_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    %{dir: dir}
  end

  defp write_secret(dir, name, contents) do
    path = Path.join(dir, name)
    File.write!(path, contents)
    path
  end

  defp config_file(yaml) do
    path =
      Path.join(System.tmp_dir!(), "gateway_secrets_#{System.unique_integer([:positive])}.yml")

    File.write!(path, yaml)
    on_exit(fn -> File.rm(path) end)
    path
  end

  defp load!(yaml, opts \\ []),
    do: Config.load!(Keyword.merge([path: config_file(yaml), env: %{}], opts))

  defp error!(yaml, opts \\ []) do
    assert_raise(ConfigError, fn -> load!(yaml, opts) end).message
  end

  # A `token` authenticator whose one token's plaintext is `secret_line`.
  defp token_config(secret_line, yaml \\ "") do
    """
    config_version: 2
    #{yaml}
    gateway:
      auth:
        authenticators:
          - type: token
            tokens:
              - id: w
                #{secret_line}
                scopes: [claims:read]
    """
  end

  defp token_sha(loaded) do
    assert [%{config: %{tokens: [%{sha256: sha}]}}] = loaded.gateway.auth.authenticators
    sha
  end

  defp kek_config(kek_line) do
    "config_version: 2\nsecrets:\n  #{kek_line}\n"
  end

  defp start_stub(state \\ %{}) do
    agent =
      start_supervised!(
        {Agent, fn -> Map.merge(%{vault: %{}, aws: %{}, down?: false, hits: []}, state) end},
        id: make_ref()
      )

    port = Ankusa.TestHelpers.free_port()

    start_supervised!(
      {Bandit, plug: {Stub, agent}, scheme: :http, ip: {127, 0, 0, 1}, port: port},
      id: make_ref()
    )

    %{agent: agent, address: "http://127.0.0.1:#{port}"}
  end

  defp stub_set(stub, fun), do: Agent.update(stub.agent, fun)
  defp stub_hits(stub), do: stub.agent |> Agent.get(& &1.hits) |> Enum.reverse()

  defp providers(stub, extra \\ "") do
    """
    secrets:
      #{extra}
      providers:
        vault: {type: vault, address: "#{stub.address}", token: vault-auth-canary}
        aws:
          type: aws
          region: us-east-1
          endpoint: "#{stub.address}"
          access_key_id: AKIDEXAMPLE
          secret_access_key: aws-secret-canary
    """
  end

  # ── file secrets ────────────────────────────────────────────────────────────

  describe "a ${file:} reference" do
    test "reads the file, minus one trailing newline", %{dir: dir} do
      file = write_secret(dir, "token", "file-token\n")

      assert token_sha(load!(token_config(~s(token: "${file:#{file}}")))) ==
               Token.hash("file-token")

      crlf = write_secret(dir, "crlf", "windows\r\n")

      assert token_sha(load!(token_config(~s(token: "${file:#{crlf}}")))) ==
               Token.hash("windows")

      bare = write_secret(dir, "bare", "no-newline")

      assert token_sha(load!(token_config(~s(token: "${file:#{bare}}")))) ==
               Token.hash("no-newline")

      two = write_secret(dir, "two", "keeps-one\n\n")

      assert token_sha(load!(token_config(~s(token: "${file:#{two}}")))) ==
               Token.hash("keeps-one\n")
    end

    test "sits among text and other variables, and reaches a raw value", %{dir: dir} do
      middle = write_secret(dir, "mid", "MIDDLE")

      loaded =
        load!(
          """
          config_version: 2
          secrets:
            kek: ${SECRETS_KEK}
          gateway:
            auth:
              authenticators:
                - type: token
                  tokens:
                    - id: w
                      token: "pre-${file:#{middle}}-${SUFFIX}"
                      scopes: [claims:read]
          """,
          env: %{"SECRETS_KEK" => @kek, "SUFFIX" => "post"}
        )

      assert loaded.config.secrets.kek == @kek
      assert token_sha(loaded) == Token.hash("pre-MIDDLE-post")
    end

    test "a file that cannot be used fails the load by dotted key and reference", %{dir: dir} do
      missing = Path.join(dir, "nope")

      assert error!(token_config(~s(token: "${file:#{missing}}"))) ==
               "gateway.auth.authenticators[0].tokens[0].token: ${file:#{missing}}: file not found"

      assert error!(kek_config(~s(kek: "${file:#{dir}}"))) ==
               "secrets.kek: ${file:#{dir}}: is a directory, not a file"

      big = write_secret(dir, "big", String.duplicate("x", 65_537))

      message = error!(kek_config(~s(kek: "${file:#{big}}")))
      assert message == "secrets.kek: ${file:#{big}}: file is larger than 65536 bytes"
      refute message =~ "xxxxxxxx"

      empty = write_secret(dir, "empty", "")

      assert error!(token_config(~s(token: "${file:#{empty}}"))) ==
               "gateway.auth.authenticators[0].tokens[0].token: must not be empty"
    end

    test "a relative path is refused when the reference is parsed" do
      assert error!(kek_config(~s(kek: "${file:relative/path}"))) ==
               "secrets.kek: a file secret needs an absolute path, got relative/path"
    end

    test "an empty string value is not a reference" do
      assert error!(token_config(~s(token: ""))) ==
               "gateway.auth.authenticators[0].tokens[0].token: must not be empty"
    end
  end

  # ── secret managers ─────────────────────────────────────────────────────────

  describe "a ${sm:} reference" do
    test "resolves from Vault KV v2 and AWS Secrets Manager, each kind of value" do
      stub =
        start_stub(%{
          vault: %{
            "ankusa/stripe" => %{"whsec" => "vault-whsec", "other" => "x"},
            "ankusa/single" => %{"only" => "vault-single"}
          },
          aws: %{
            "prod/stripe" => ~s({"whsec":"aws-whsec"}),
            "prod/plain" => "aws-plain",
            "prod/binary" => {:binary, "aws-binary"}
          }
        })

      loaded =
        load!(
          providers(stub) <>
            """
            gateway:
              auth:
                authenticators:
                  - type: token
                    tokens:
                      - {id: v1, token: "${sm:vault:ankusa/stripe#whsec}", scopes: [claims:read]}
                      - {id: v2, token: "${sm:vault:ankusa/single}", scopes: [claims:read]}
                      - {id: a1, token: "${sm:aws:prod/stripe#whsec}", scopes: [claims:read]}
                      - {id: a2, token: "${sm:aws:prod/plain}", scopes: [claims:read]}
                      - {id: a3, token: "${sm:aws:prod/binary}", scopes: [claims:read]}
            """
        )

      assert [%{config: %{tokens: tokens}}] = loaded.gateway.auth.authenticators

      assert Enum.map(tokens, & &1.sha256) ==
               Enum.map(
                 ["vault-whsec", "vault-single", "aws-whsec", "aws-plain", "aws-binary"],
                 &Token.hash/1
               )

      hits = stub_hits(stub)

      assert {"GET", "/v1/secret/data/ankusa/stripe", vault_headers} =
               Enum.find(hits, &(elem(&1, 0) == "GET"))

      assert {"x-vault-token", "vault-auth-canary"} in vault_headers

      assert {"POST", "/", aws_headers} = Enum.find(hits, &(elem(&1, 0) == "POST"))
      assert {"x-amz-target", "secretsmanager.GetSecretValue"} in aws_headers

      {_name, authorization} = List.keyfind(aws_headers, "authorization", 0)

      assert authorization =~
               ~r"^AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/\d{8}/us-east-1/secretsmanager/aws4_request"

      refute inspect(loaded.config) =~ "vault-whsec"
      refute inspect(loaded.gateway) =~ "aws-plain"
    end

    test "a namespace goes in its own header, and a reference used twice is fetched once" do
      stub = start_stub(%{vault: %{"ankusa/p" => %{"v" => "one"}}})

      yaml =
        """
        secrets:
          providers:
            vault:
              type: vault
              address: "#{stub.address}"
              token: vault-auth-canary
              namespace: team-a
              mount: app
        """

      loaded =
        load!(
          yaml <>
            """
            gateway:
              auth:
                authenticators:
                  - type: token
                    tokens:
                      - {id: a, token: "${sm:vault:ankusa/p#v}", scopes: [claims:read]}
                      - {id: b, token: "${sm:vault:ankusa/p#v}", scopes: [claims:read]}
            """
        )

      assert [%{config: %{tokens: [x, y]}}] = loaded.gateway.auth.authenticators
      assert {x.sha256, y.sha256} == {Token.hash("one"), Token.hash("one")}

      assert [{"GET", "/v1/app/data/ankusa/p", headers}] = stub_hits(stub)
      assert {"x-vault-namespace", "team-a"} in headers
    end

    test "a missing, unreadable or unavailable secret fails the boot, naming key and reference" do
      stub =
        start_stub(%{
          vault: %{
            "ankusa/multi" => %{"a" => "1", "b" => "2"},
            "ankusa/denied" => :denied,
            "ankusa/malformed" => :malformed
          }
        })

      ref = fn r -> token_config(~s(token: "#{r}")) end

      assert error!(providers(stub) <> ref.("${sm:vault:ankusa/missing#whsec}")) ==
               "gateway.auth.authenticators[0].tokens[0].token: " <>
                 "${sm:vault:ankusa/missing#whsec}: secret not found " <>
                 "(or not readable with the provider's credentials)"

      for r <- [
            "${sm:vault:ankusa/multi}",
            "${sm:vault:ankusa/multi#c}",
            "${sm:vault:ankusa/denied#a}",
            "${sm:aws:prod/missing}",
            "${sm:vault:a/../b}"
          ] do
        assert error!(providers(stub) <> ref.(r)) =~ "secret not found", r
      end

      # A reply that is not the manager's JSON is unavailable, like an outage.
      assert error!(providers(stub) <> ref.("${sm:vault:ankusa/malformed#a}")) =~
               "secret provider unavailable"

      # A provider that is down is unavailable, and its credentials are not printed.
      stub_set(stub, &%{&1 | down?: true})

      message = error!(providers(stub) <> ref.("${sm:vault:ankusa/multi#a}"))

      assert message ==
               "gateway.auth.authenticators[0].tokens[0].token: " <>
                 "${sm:vault:ankusa/multi#a}: secret provider unavailable"

      for credential <- ["vault-auth-canary", "aws-secret-canary"],
          do: refute(message =~ credential)

      assert error!(providers(stub) <> ref.("${sm:aws:prod/anything}")) =~
               "secret provider unavailable"
    end

    test "an AWS reply that is not a secret is unavailable, or not found by its type" do
      stub =
        start_stub(%{
          aws: %{
            "prod/throttled" => {:status, 400, %{"__type" => "ThrottlingException"}},
            "prod/forbidden" => {:status, 400, %{"__type" => "AccessDeniedException"}},
            "prod/odd" => :malformed,
            "prod/empty" => ~s({"Nothing" => 1})
          }
        })

      for {id, expectation} <- [
            {"${sm:aws:prod/throttled}", "secret provider unavailable"},
            {"${sm:aws:prod/forbidden}", "secret not found"},
            {"${sm:aws:prod/odd}", "secret provider unavailable"},
            {"${sm:aws:prod/empty#nothing}", "secret not found"},
            {"${sm:aws:prod/stripe#missing}", "secret not found"}
          ] do
        assert error!(providers(stub) <> token_config(~s(token: "#{id}"))) =~ expectation, id
      end
    end

    test "a provider that is not configured, or is configured wrong, is a load error" do
      stub = start_stub()

      assert error!(token_config(~s(token: "${sm:vault:x}"))) ==
               "gateway.auth.authenticators[0].tokens[0].token: " <>
                 "${sm:vault:…} names no provider (secrets.providers has: none)"

      assert error!(providers(stub) <> token_config(~s(token: "${sm:nope:x}"))) ==
               "gateway.auth.authenticators[0].tokens[0].token: ${sm:nope:…} names no provider " <>
                 "(secrets.providers has: aws, vault)"

      for {provider, message} <- [
            {"{type: gcp}", "secrets.providers.p.type must be one of aws, vault, got \"gcp\""},
            {"{type: vault, token: t}", ~s(secrets.providers.p: missing required key "address")},
            {"{type: vault, address: \"http://vault.example.com\", token: t}",
             "secrets.providers.p.address must be an https:// URL (http:// is accepted only for a loopback address)"},
            {"{type: aws, region: us-east-1, extra: 1}",
             ~s(secrets.providers.p: unknown key "extra")},
            {"{type: aws}", ~s(secrets.providers.p: missing required key "region")},
            {"{type: aws, region: us-east-1, access_key_id: \"\"}",
             "secrets.providers.p.access_key_id must not be empty"},
            {"{type: aws, region: [1]}",
             "secrets.providers.p.region: expected a string, got [1]"},
            {"{type: vault, address: \"https://v\", token: t, timeout_ms: 0}",
             "secrets.providers.p.timeout_ms must be an integer in 1..60000, got 0"},
            {"[x]", "secrets.providers.p: expected a mapping, got [\"x\"]"}
          ] do
        yaml =
          "config_version: 2\nsecrets:\n  providers:\n    p: #{provider}\n" <> "gateway: {}\n"

        assert error!(yaml) == message, provider
      end

      yaml =
        "config_version: 2\nsecrets:\n  providers:\n    Bad_Name: {type: aws, region: r}\n"

      assert error!(yaml) =~ "a provider name is [a-z0-9_-]+"

      assert error!("config_version: 2\nsecrets: {cache_ttl_ms: -1}\n") ==
               "secrets.cache_ttl_ms must be an integer in 0..86400000, got -1"

      assert error!("config_version: 2\nsecrets: {cache_ttl_ms: soon}\n") ==
               ~s(secrets.cache_ttl_ms must be an integer in 0..86400000, got "soon")
    end

    test "a provider's own credentials come from a variable or a file, never from a manager", %{
      dir: dir
    } do
      stub = start_stub(%{vault: %{"p" => %{"v" => "found"}}})
      token = write_secret(dir, "token", "vault-token-from-file\n")

      yaml =
        """
        config_version: 2
        secrets:
          providers:
            vault: {type: vault, address: "#{stub.address}", token: "${file:#{token}}"}
        """

      loaded =
        load!(
          yaml <>
            "gateway: {auth: {authenticators: [{type: token, tokens: [{id: w, token: \"${sm:vault:p#v}\", scopes: [claims:read]}]}]}}\n"
        )

      assert token_sha(loaded) == Token.hash("found")
      assert {"x-vault-token", "vault-token-from-file"} in elem(hd(stub_hits(stub)), 2)

      nested =
        """
        config_version: 2
        secrets:
          providers:
            vault: {type: vault, address: "#{stub.address}", token: "${sm:vault:p#v}"}
        """

      assert error!(nested) ==
               "secrets.providers.vault.token: ${sm:vault:…} cannot configure a provider " <>
                 "(use ${VAR} or ${file:/path} for a provider's own credentials)"
    end

    test "cache_ttl_ms reuses a fetched value across loads; refresh: true and ttl 0 do not" do
      stub = start_stub(%{vault: %{"p" => %{"v" => "one"}}})

      yaml =
        providers(stub, "cache_ttl_ms: 60000") <>
          """
          gateway: {auth: {authenticators: [{type: token, tokens: [{id: w, token: "${sm:vault:p#v}", scopes: [claims:read]}]}]}}
          """

      path = config_file(yaml)

      first = Config.load!(path: path, env: %{})
      assert token_sha(first) == Token.hash("one")
      assert length(stub_hits(stub)) == 1

      stub_set(stub, &%{&1 | vault: %{"p" => %{"v" => "two"}}})

      cached = Config.load!(path: path, env: %{}, secrets: first.secrets)
      assert token_sha(cached) == Token.hash("one")
      assert length(stub_hits(stub)) == 1

      forced = Config.load!(path: path, env: %{}, secrets: first.secrets, refresh: true)
      assert token_sha(forced) == Token.hash("two")
      assert length(stub_hits(stub)) == 2

      # ttl 0 (the default): every load asks
      path = config_file(providers(stub) <> config_tail())

      fresh = Config.load!(path: path, env: %{})
      Config.load!(path: path, env: %{}, secrets: fresh.secrets)
      assert length(stub_hits(stub)) == 4
    end

    test "a reload that cannot resolve a reference keeps the value it had and warns" do
      stub = start_stub(%{vault: %{"p" => %{"v" => "kept-value"}}})
      path = config_file(providers(stub) <> config_tail())

      first = Config.load!(path: path, env: %{})
      assert token_sha(first) == Token.hash("kept-value")

      stub_set(stub, &%{&1 | down?: true})

      # The value the last good load resolved stands in; `log_kept/1` is what
      # the reload prints about it (the gateway image itself does not reload).
      again = Config.load!(path: path, env: %{}, secrets: first.secrets)
      assert token_sha(again) == Token.hash("kept-value")

      assert [%{ref: "${sm:vault:p#v}", path: path, reason: :unavailable}] = again.secrets.kept

      assert path == "gateway.auth.authenticators[0].tokens[0].token"
      refute inspect(again.secrets) =~ "kept-value"
    end

    test "a new reference on a reload is still an error" do
      stub = start_stub(%{vault: %{"p" => %{"v" => "one"}}})

      first =
        load!(
          providers(stub) <>
            "gateway: {auth: {authenticators: [{type: token, tokens: [{id: w, token: \"${sm:vault:p#v}\", scopes: [claims:read]}]}]}}\n"
        )

      path =
        config_file(
          providers(stub) <>
            """
            gateway:
              auth:
                authenticators:
                  - type: token
                    tokens:
                      - {id: w, token: "${sm:vault:p#v}", scopes: [claims:read]}
                      - {id: n, token: "${sm:vault:new#v}", scopes: [claims:read]}
            """
        )

      message =
        assert_raise(ConfigError, fn ->
          Config.load!(path: path, env: %{}, secrets: first.secrets)
        end).message

      assert message =~ "${sm:vault:new#v}: secret not found"
    end

    test "inspecting what a load resolved shows no secret" do
      stub = start_stub(%{vault: %{"p" => %{"v" => "inspect-canary"}}})

      loaded =
        load!(
          providers(stub) <>
            "gateway: {auth: {authenticators: [{type: token, tokens: [{id: w, token: \"${sm:vault:p#v}\", scopes: [claims:read]}]}]}}\n"
        )

      shown = inspect(loaded.secrets)

      refute shown =~ "inspect-canary"
      refute shown =~ "vault-auth-canary"
      refute inspect(loaded.config) =~ "inspect-canary"
    end
  end

  # The one reference every cache/reload case points at.
  defp config_tail do
    """
    gateway:
      auth:
        authenticators:
          - type: token
            tokens:
              - {id: w, token: "${sm:vault:p#v}", scopes: [claims:read]}
    """
  end

  test "log_kept names each kept reference, its place and the reason, never a value" do
    kept = [
      %{ref: "${sm:vault:a#v}", path: "secrets.kek", reason: :unavailable},
      %{ref: "${file:/run/secrets/t}", path: "gateway.auth.authenticators[0]", reason: :enoent}
    ]

    log = capture_log(fn -> assert Secrets.log_kept(kept) == :ok end)

    assert log =~ "${sm:vault:a#v} (secrets.kek) secret provider unavailable"
    assert log =~ "${file:/run/secrets/t} (gateway.auth.authenticators[0]) file not found"
  end
end
