defmodule AnkusaServer.ConfigTest do
  @moduledoc """
  The config file is the operator interface, so this suite is written from the
  operator's side: does my file load, does the variable I left unset fail by
  name, does the env var I set actually win, and does printing the config leak
  a secret.

  Every case goes through `load!/1` with an explicit `env:` map, so nothing here
  depends on the developer's real environment.
  """

  use ExUnit.Case, async: true

  alias AnkusaServer.{Config, ConfigError}

  @fixture_env %{
    "ANKUSA_WAL_POSTGRES_URL" => "postgres://ankusa:fixture-pg-password@postgres:5432/ankusa",
    "S3_BUCKET" => "fixture-bucket",
    "S3_REGION" => "us-east-1",
    "S3_ENDPOINT" => "https://s3.fixture.invalid",
    "AWS_ACCESS_KEY_ID" => "fixture-key-id",
    "AWS_SECRET_ACCESS_KEY" => "fixture-s3-secret",
    "GCS_BUCKET" => "fixture-gcs-bucket",
    "STRIPE_WHSEC" => "whsec_fixture-stripe-secret",
    "SINK_URL" => "http://sink.fixture.invalid/hooks",
    "RABBITMQ_URL" => "amqp://ankusa:fixture-rabbit-password@rabbitmq:5672",
    "RABBITMQ_EXCHANGE" => "ankusa.hooks",
    "KAFKA_BROKERS" => "redpanda:9092",
    "KAFKA_TOPIC" => "ankusa.hooks",
    "KAFKA_USERNAME" => "ankusa",
    "KAFKA_SASL_PASSWORD" => "fixture-kafka-password",
    "NATS_SERVERS" => "nats:4222",
    "NATS_SUBJECT" => "ankusa.hooks",
    "NATS_USERNAME" => "ankusa",
    "NATS_PASSWORD" => "fixture-nats-password",
    "STANDARD_WEBHOOKS_SECRET" => "whsec_Zml4dHVyZQ==",
    "GITHUB_WEBHOOK_SECRET" => "fixture-github-secret"
  }

  @fixture_secrets ~w(fixture-pg-password fixture-s3-secret
                      whsec_fixture-stripe-secret fixture-rabbit-password
                      fixture-kafka-password fixture-nats-password fixture-github-secret
                      whsec_Zml4dHVyZQ==)

  # ── the shipped configs ─────────────────────────────────────────────────────

  test "every shipped config loads" do
    for path <- shipped_configs() do
      assert %Ankusa.Config{} = Config.load!(path: path, env: @fixture_env).config,
             "#{path} did not load"
    end
  end

  test "the reference config spells out the whole schema" do
    config = Config.load!(path: "config-examples/reference.yml", env: @fixture_env).config

    assert config.admin == %{enabled: true, port: 4002}
    assert config.route_resolver == {Ankusa.RouteResolver.Path, [prefix: ["webhooks"]]}
    assert {Ankusa.SourceStore.Static, opts} = config.source_store
    assert Map.keys(opts[:sources]) |> Enum.sort() == ["github", "open", "standard", "stripe"]

    %{verifier: {Ankusa.Verifier.Hmac, stripe_opts}, sinks: sinks} =
      source_from(config, "stripe")

    assert stripe_opts[:scheme] == :stripe
    assert stripe_opts[:secret] == @fixture_env["STRIPE_WHSEC"]
    assert stripe_opts[:tolerance] == 300

    assert Enum.map(sinks, &elem(&1, 0)) == [
             Ankusa.Sink.Log,
             Ankusa.Sink.Http,
             Ankusa.Sink.RabbitMQ,
             Ankusa.Sink.Kafka,
             Ankusa.Sink.NATS
           ]
  end

  test "the baked image config is the demo: admin on, one open source" do
    config = Config.load!(path: "rel/ankusa.yml", env: %{}).config

    assert config.admin == %{enabled: true, port: 4002}
    assert {Ankusa.SourceStore.Static, opts} = config.source_store
    assert Map.keys(opts[:sources]) == ["demo"]
  end

  test "the fleet config needs no S3 variables and takes its WAL from env" do
    env = %{"ANKUSA_WAL_POSTGRES_URL" => "postgres://ankusa:pw@postgres:5432/ankusa"}
    config = Config.load!(path: "config-examples/fleet-postgres-s3.yml", env: env).config

    assert {Ankusa.WAL.Postgres, opts} = config.wal
    assert opts[:hostname] == "postgres"
    assert opts[:database] == "ankusa"
    assert {Ankusa.BlobStore.S3, s3} = config.storage.blob_store
    assert s3[:bucket] == "ankusa-segments"
  end

  # ── interpolation ───────────────────────────────────────────────────────────

  test "an unset variable fails by its dotted key, unless the file gave a default" do
    path =
      tmp_config("""
      sources:
        stripe:
          verify: {type: stripe, secret: "${STRIPE_WHSEC}"}
          sinks: [{type: log}]
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message == "sources.stripe.verify.secret: ${STRIPE_WHSEC} is not set"

    fallback =
      tmp_config("""
      http:
        port: "${PORT_THAT_IS_NOT_SET:-4711}"
      log:
        level: "${LOG_LEVEL:-debug}"
      sources:
        demo:
          verify: {type: none}
          sinks: [{type: log}]
      """)

    loaded = Config.load!(path: fallback, env: %{})
    assert loaded.config.port == 4711
    assert loaded.log_level == :debug

    loaded = Config.load!(path: fallback, env: %{"PORT_THAT_IS_NOT_SET" => "5000"})
    assert loaded.config.port == 5000
  end

  test "a variable set to the empty string is still set" do
    path = tmp_config("node: {data_dir: \"${DATA_DIR}\"}\n")

    assert Config.load!(path: path, env: %{"DATA_DIR" => ""}).config.data_dir == ""
  end

  test "an empty default makes a variable optional" do
    path = tmp_config("node: {data_dir: \"${DATA_DIR:-}\"}\n")

    assert Config.load!(path: path, env: %{}).config.data_dir == ""
  end

  # ── env overrides ───────────────────────────────────────────────────────────

  test "ANKUSA_HTTP_PORT beats PORT beats the file" do
    path =
      tmp_config(
        "http: {port: 5000}\nsources: {demo: {verify: {type: none}, sinks: [{type: log}]}}\n"
      )

    assert Config.load!(path: path, env: %{}).config.port == 5000
    assert Config.load!(path: path, env: %{"PORT" => "6000"}).config.port == 6000

    assert Config.load!(path: path, env: %{"PORT" => "6000", "ANKUSA_HTTP_PORT" => "7000"}).config.port ==
             7000
  end

  test "an env override fills a section the file left empty" do
    path = tmp_config("http:\n  # port: 4000\n")

    assert Config.load!(path: path, env: %{"PORT" => "6000"}).config.port == 6000
  end

  test "the env override table wins, and is coerced from strings" do
    path = tmp_config("sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}\n")

    config =
      Config.load!(
        path: path,
        env: %{
          "ANKUSA_ROLES" => "edge,dispatch",
          "ANKUSA_DATA_DIR" => "/data",
          "ANKUSA_ADMIN_PORT" => "9000",
          "ANKUSA_WAL_TYPE" => "disk"
        }
      ).config

    assert config.roles == [:edge, :dispatch]
    assert config.data_dir == "/data"
    assert config.admin.port == 9000
    assert config.wal == {Ankusa.WAL.DiskLog, []}
  end

  # ── the dispatch dedup ledger ───────────────────────────────────────────────

  test "dispatch.dedup_store names its implementation" do
    path =
      tmp_config("""
      dispatch:
        partitions: 2
        dedup_ttl_ms: 60000
        dedup_store: ra
      wal:
        type: ra
        ra:
          members:
            - ankusa_wal_default@wal-0
            - ankusa_wal_default@wal-1
      sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
      """)

    config = Config.load!(path: path, env: %{}).config

    assert config.dispatch.partitions == 2
    assert config.dispatch.dedup_ttl_ms == 60_000

    # The ledger is the WAL cluster's replicated state, so `ra` takes that
    # cluster's members rather than a list of its own.
    assert {Ankusa.DedupStore.Ra, [members: members]} = config.dispatch.dedup_store

    assert members == [
             {:ankusa_wal_default, :"ankusa_wal_default@wal-0"},
             {:ankusa_wal_default, :"ankusa_wal_default@wal-1"}
           ]
  end

  test "ANKUSA_DISPATCH_DEDUP_STORE overrides the file, like every other key" do
    path =
      tmp_config("""
      dispatch: {dedup_store: ets}
      wal:
        type: ra
        ra:
          members: [ankusa_wal_default@wal-0]
      sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
      """)

    config = Config.load!(path: path, env: %{"ANKUSA_DISPATCH_DEDUP_STORE" => "ra"}).config

    assert {Ankusa.DedupStore.Ra, [members: [{:ankusa_wal_default, :"ankusa_wal_default@wal-0"}]]} =
             config.dispatch.dedup_store
  end

  test "dispatch.dedup_store ets is the in-process ledger" do
    path = tmp_config("dispatch: {dedup_store: ets}\n")

    assert {Ankusa.DedupStore.ETS, []} =
             Config.load!(path: path, env: %{}).config.dispatch.dedup_store
  end

  test "dispatch.dedup_store \"ra\" without a Ra WAL is rejected by name" do
    path = tmp_config("dispatch: {dedup_store: ra}\n")

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ "dispatch.dedup_store"
    assert error.message =~ "wal.type"
  end

  test "dispatch.dedup_store with an unknown value lists the valid ones" do
    path = tmp_config("dispatch: {dedup_store: redis}\n")

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ ~s(expected ets or ra, got "redis")
  end

  # ── validation errors ───────────────────────────────────────────────────────

  test "an unknown key names its path, including list indexes" do
    path =
      tmp_config("""
      sources:
        a:
          sinks:
            - {type: http, url: "http://sink.invalid", urll: "http://typo.invalid"}
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ "sources.a.sinks[0]"
    assert error.message =~ ~s(unknown key "urll")
  end

  test "the removed claim-check keys tokens/remote/max_bytes are rejected by name" do
    for key <- ["tokens", "remote", "max_bytes"] do
      path =
        tmp_config("""
        claim_check: {#{key}: null}
        sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
        """)

      error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
      assert error.message == ~s(claim_check: unknown key "#{key}")
    end
  end

  test "an unknown verifier type lists the valid ones" do
    path =
      tmp_config("""
      sources:
        a:
          verify: {type: strip}
          sinks: [{type: log}]
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ ~s(sources.a.verify.type: unknown value "strip")
    assert error.message =~ "none, stripe, github, standard_webhooks"
  end

  test "a wrong type says what it wanted" do
    path = tmp_config("http: {port: fast}\n")

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message == ~s(http.port: expected an integer, got "fast")
  end

  test "a signature verifier without a secret is rejected" do
    path =
      tmp_config("""
      sources:
        a:
          verify: {type: stripe}
          sinks: [{type: log}]
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ ~s(sources.a.verify: missing required key "secret")
  end

  test "a source with no sinks is rejected" do
    path = tmp_config("sources: {a: {verify: {type: none}, sinks: []}}\n")

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ "sources.a.sinks: must not be empty"
  end

  test "a missing file named by ANKUSA_CONFIG is a startup error" do
    error =
      assert_raise ConfigError, fn ->
        Config.load!(env: %{"ANKUSA_CONFIG" => "/nope/ankusa.yml"})
      end

    assert error.message == "config file /nope/ankusa.yml not found (ANKUSA_CONFIG)"
  end

  test "a YAML syntax error names the file and the position" do
    path = tmp_config("http:\n  port: 4000\n   bad-indent: 1\n")

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ path
    assert error.message =~ "line"
  end

  # ── translation: Postgres ───────────────────────────────────────────────────

  test "a Postgres URL becomes Postgrex opts" do
    config =
      Config.load!(
        path: tmp_config(wal(%{url: "postgres://user:pw@db.internal:5433/ankusa"})),
        env: %{}
      ).config

    assert {Ankusa.WAL.Postgres, opts} = config.wal
    assert opts[:hostname] == "db.internal"
    assert opts[:port] == 5433
    assert opts[:username] == "user"
    assert opts[:password] == "pw"
    assert opts[:database] == "ankusa"
  end

  test "discrete Postgres keys, and defaults, become Postgrex opts" do
    config =
      Config.load!(
        path: tmp_config(wal(%{host: "db", username: "u", password: "p", database: "d"})),
        env: %{}
      ).config

    assert {Ankusa.WAL.Postgres, opts} = config.wal
    assert opts[:hostname] == "db"
    refute Keyword.has_key?(opts, :port)
    assert opts[:database] == "d"
  end

  test "a Postgres URL and discrete keys together are rejected" do
    path =
      tmp_config(wal(%{url: "postgres://user:pw@db:5432/ankusa", host: "db", database: "ankusa"}))

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ "mutually exclusive"
  end

  test "wal.type postgres without a postgres block is rejected" do
    path = tmp_config("wal: {type: postgres}\n")

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message == ~s(wal.postgres: required when wal.type is "postgres")
  end

  # ── translation: storage ────────────────────────────────────────────────────

  test "storage.gcs.auth selects the token provider" do
    assert {Ankusa.BlobStore.GCS, opts} = gcs_opts(%{auth: "metadata"})
    assert opts[:token_provider] == {AnkusaServer.GcsToken, :metadata, []}

    assert {Ankusa.BlobStore.GCS, opts} = gcs_opts(%{auth: "token", token: "ya29.static"})
    assert opts[:token_provider] == {AnkusaServer.GcsToken, :static, ["ya29.static"]}

    assert {Ankusa.BlobStore.GCS, opts} = gcs_opts(%{auth: "none"})
    refute Keyword.has_key?(opts, :token_provider)
  end

  test "auth: token without a token is rejected" do
    path = tmp_config(storage_gcs(%{auth: "token"}))

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message == ~s(storage.gcs: missing required key "token")
  end

  # ── translation: sources ────────────────────────────────────────────────────

  test "sink and verifier options are translated, not passed through" do
    path =
      tmp_config("""
      sources:
        full:
          tenant: acme
          on_verify_failure: accept_flag
          verify: {type: standard_webhooks, secret: "whsec_abc", tolerance_seconds: 60}
          dedup_key: {type: rules, header: "x-event-id", json_path: "data.meta.id"}
          sinks:
            - {type: http, url: "http://sink.invalid/h", method: put, timeout_ms: 250,
               headers: {x-one: "1"}}
            - {type: kafka, brokers: ["b:9092"], topic: t, ssl: true,
               sasl: {mechanism: scram_sha_512, username: u, password: p}}
            - {type: nats, servers: "${NATS_SERVERS}", subject: "ankusa.full",
               inline_max_bytes: 4096, publish_timeout_ms: 250,
               auth: {username: nu, password: np}}
      """)

    env = %{"NATS_SERVERS" => "n1:4222,n2:4222"}

    config = Config.load!(path: path, env: env).config
    {Ankusa.SourceStore.Static, store} = config.source_store
    source = Ankusa.Source.new("full", store[:sources]["full"])

    assert source.tenant_id == "acme"
    assert source.on_verify_failure == :accept_flag

    assert source.verifier ==
             {Ankusa.Verifier.Hmac,
              [scheme: :standard_webhooks, secret: "whsec_abc", tolerance: 60]}

    assert source.dedup == :auto

    assert source.dedup_key ==
             {Ankusa.DedupKey.Rules, [header: "x-event-id", json: ["data", "meta", "id"]]}

    assert [
             {Ankusa.Sink.Http,
              [
                url: "http://sink.invalid/h",
                method: :put,
                headers: [{"x-one", "1"}],
                timeout_ms: 250
              ]},
             {Ankusa.Sink.Kafka, kafka},
             {Ankusa.Sink.NATS, nats}
           ] = source.sinks

    assert kafka[:brokers] == ["b:9092"]
    assert kafka[:ssl] == true
    assert kafka[:sasl] == {:scram_sha_512, "u", "p"}

    # The comma-separated env var form of `servers` too.
    assert nats[:servers] == ["n1:4222", "n2:4222"]
    assert nats[:subject] == "ankusa.full"
    assert nats[:inline_max_bytes] == 4096
    assert nats[:publish_timeout_ms] == 250
    assert nats[:username] == "nu"
    assert nats[:password] == "np"
  end

  test "a NATS auth block takes exactly one scheme, and only whole ones" do
    assert {Ankusa.Sink.NATS, opts} = nats_opts("")
    refute Keyword.has_key?(opts, :username)

    assert {Ankusa.Sink.NATS, opts} = nats_opts("auth: {token: t0ken}")
    assert opts[:token] == "t0ken"

    # Operator mode: a seed signs the server's nonce, the jwt vouches for the
    # account. gnat needs both.
    assert {Ankusa.Sink.NATS, opts} = nats_opts("auth: {nkey_seed: SUAseed, jwt: eyJacc}")
    assert opts[:nkey_seed] == "SUAseed"
    assert opts[:jwt] == "eyJacc"

    error = nats_error("auth: {token: t0ken, username: u, password: p}")
    assert error.message =~ "sources.a.sinks[0].auth"
    assert error.message =~ "username/password, token are mutually exclusive"

    assert nats_error("auth: {username: u}").message ==
             "sources.a.sinks[0].auth: username and password go together"

    assert nats_error("auth: {jwt: eyJacc}").message ==
             "sources.a.sinks[0].auth: jwt requires nkey_seed"
  end

  # The seed is the private key half of the pair; it must not print. The jwt
  # beside it is a signed, public statement about the account, so it stays.
  test "a NATS nkey_seed does not survive printing the config" do
    printed = print_config(tmp_config(nats_sink("auth: {nkey_seed: SUAseed, jwt: eyJacc}")))

    refute printed =~ "SUAseed"
    assert printed =~ "eyJacc"
    assert printed =~ "[REDACTED]"
  end

  test "named hmac schemes shopify and slack resolve to the engine preset" do
    path =
      tmp_config("""
      sources:
        shop:
          verify: {type: shopify, secret: "shpss_abc"}
          sinks: [{type: log}]
        slack:
          verify: {type: slack, secret: "slack-secret", tolerance_seconds: 30}
          sinks: [{type: log}]
      """)

    config = Config.load!(path: path, env: %{}).config

    assert source_from(config, "shop").verifier ==
             {Ankusa.Verifier.Hmac, [scheme: :shopify, secret: "shpss_abc"]}

    assert source_from(config, "slack").verifier ==
             {Ankusa.Verifier.Hmac, [scheme: :slack, secret: "slack-secret", tolerance: 30]}
  end

  test "type hmac builds an inline scheme from the descriptor keys" do
    path =
      tmp_config("""
      sources:
        custom:
          verify:
            type: hmac
            secret: "custom-secret"
            signature_header: "X-Custom-Sig"
            sig_prefix: "sha256="
            signed: "{body}"
            hash: sha256
            encoding: hex
            tolerance_seconds: 120
          sinks: [{type: log}]
      """)

    config = Config.load!(path: path, env: %{}).config

    assert source_from(config, "custom").verifier ==
             {Ankusa.Verifier.Hmac,
              [
                scheme: %Ankusa.Verifier.Hmac.Scheme{
                  signature_header: "X-Custom-Sig",
                  parse: :whole,
                  sig_prefix: "sha256=",
                  sig_key: nil,
                  version: nil,
                  signed: "{body}",
                  hash: :sha256,
                  encoding: :hex,
                  secret_decode: :raw,
                  timestamp: nil
                },
                secret: "custom-secret",
                tolerance: 120
              ]}
  end

  test "type hmac maps timestamp_header and rejects a bad enum" do
    path =
      tmp_config("""
      sources:
        custom:
          verify:
            type: hmac
            secret: "custom-secret"
            signature_header: "X-Custom-Sig"
            timestamp_header: "X-Timestamp"
            signed: "{body}.{ts}"
          sinks: [{type: log}]
      """)

    config = Config.load!(path: path, env: %{}).config

    assert {Ankusa.Verifier.Hmac, opts} = source_from(config, "custom").verifier
    assert opts[:scheme].timestamp == {:header, "X-Timestamp"}
    assert opts[:scheme].signed == "{body}.{ts}"

    bad =
      tmp_config("""
      sources:
        custom:
          verify:
            type: hmac
            secret: "custom-secret"
            signature_header: "X-Custom-Sig"
            parse: bogus
          sinks: [{type: log}]
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: bad, env: %{}) end
    assert error.message =~ ~s(sources.custom.verify.parse: unknown value "bogus")
  end

  test "routing tenant_path uses the tenant resolver" do
    path =
      tmp_config("""
      http: {routing: tenant_path, prefix: /hooks}
      sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
      """)

    assert Config.load!(path: path, env: %{}).config.route_resolver ==
             {Ankusa.RouteResolver.TenantPath, [prefix: ["hooks"]]}
  end

  # ── the whole point of redaction ────────────────────────────────────────────

  test "no fixture secret survives printing the loaded config" do
    for path <- shipped_configs() do
      printed = print_config(path, @fixture_env)

      for secret <- @fixture_secrets do
        refute printed =~ secret, "#{path} leaked #{inspect(secret)}"
      end

      # Every shipped example config redacts at least one secret, so the refute
      # above is not vacuous. The baked demo config (`rel/ankusa.yml`) is
      # deliberately secret-free.
      if path != "rel/ankusa.yml" do
        assert printed =~ "[REDACTED]", "#{path} should redact at least one secret"
      end
    end
  end

  test "a static GCS token does not survive printing" do
    printed = print_config(tmp_config(storage_gcs(%{auth: "token", token: "ya29.leak"})))

    refute printed =~ "ya29.leak"
  end

  test "http sink header values do not survive printing" do
    path =
      tmp_config("""
      sources:
        a:
          verify: {type: none}
          sinks:
            - {type: http, url: "http://sink.invalid", headers: {authorization: "Bearer leak"}}
      """)

    refute print_config(path) =~ "Bearer leak"
  end

  # ── helpers ─────────────────────────────────────────────────────────────────

  defp shipped_configs, do: ["rel/ankusa.yml" | Path.wildcard("config-examples/*.yml")]

  # The store is a plain map of source_id => opts; `Source.new/2` is what the
  # running store would do with them.
  defp source_from(config, id) do
    {Ankusa.SourceStore.Static, store} = config.source_store
    Ankusa.Source.new(id, store[:sources][id])
  end

  defp print_config(path, env \\ %{}) do
    Config.load!(path: path, env: env).config
    |> Ankusa.Admin.Redact.config()
    |> JSON.encode!()
  end

  defp tmp_config(yaml) do
    path = Path.join(System.tmp_dir!(), "ankusa_config_#{System.unique_integer([:positive])}.yml")
    File.write!(path, yaml)
    on_exit(fn -> File.rm(path) end)
    path
  end

  defp wal(postgres) do
    """
    wal:
      type: postgres
      postgres:
    #{indent(postgres)}
    sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
    """
  end

  defp gcs_opts(gcs) do
    config = Config.load!(path: tmp_config(storage_gcs(gcs)), env: %{}).config
    config.storage.blob_store
  end

  defp storage_gcs(gcs) do
    """
    storage:
      type: gcs
      gcs:
    #{indent(Map.put(gcs, :bucket, "fixture-bucket"))}
    sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
    """
  end

  defp nats_opts(auth) do
    config = Config.load!(path: tmp_config(nats_sink(auth)), env: %{}).config
    {Ankusa.SourceStore.Static, store} = config.source_store
    source = Ankusa.Source.new("a", store[:sources]["a"])
    [sink] = source.sinks
    sink
  end

  defp nats_error(auth) do
    assert_raise ConfigError, fn -> Config.load!(path: tmp_config(nats_sink(auth)), env: %{}) end
  end

  # One source whose only sink is NATS, with the auth block spliced in beside
  # the sink's own keys (an empty string for "no auth block at all"). The
  # interpolation sits at the same static indentation as `servers:` so the
  # heredoc's dedent leaves it under `sinks`, not at the root.
  defp nats_sink(auth) do
    """
    sources:
      a:
        verify: {type: none}
        sinks:
          - type: nats
            servers: ["n1:4222"]
            subject: ankusa.a
            #{auth}
    """
  end

  # Renders a map as YAML at one more level of indentation than the key above.
  defp indent(map) do
    map
    |> Enum.map_join("\n", fn {key, value} -> "    #{key}: #{render(value)}" end)
  end

  defp render(value) when is_binary(value), do: inspect(value)
  defp render(value), do: to_string(value)
end
