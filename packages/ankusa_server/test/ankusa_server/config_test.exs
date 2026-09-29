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

  @fixture_secrets ~w(fixture-s3-secret
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

  # ── wal.type: none ──────────────────────────────────────────────────────────

  test "wal: {type: none} keeps only the roles that do not read the log" do
    path =
      tmp_config("""
      node: {roles: [edge, dispatch, storage]}
      wal: {type: none}
      sources:
        demo:
          verify: {type: none}
          sinks: [{type: rabbitmq, url: "amqp://guest:guest@rabbitmq:5672", exchange: ankusa.hooks}]
      """)

    config = Config.load!(path: path, env: %{}).config
    assert config.wal == :none
    assert config.roles == [:edge]
  end

  test "ANKUSA_WAL_TYPE=none reaches the same config key" do
    path =
      tmp_config("""
      sources:
        demo:
          verify: {type: none}
          sinks: [{type: http, url: "http://sink.invalid/hooks"}]
      """)

    config = Config.load!(path: path, env: %{"ANKUSA_WAL_TYPE" => "none"}).config
    assert config.wal == :none
    assert config.roles == [:edge]
  end

  test "wal: {type: none} rejects a source whose only sink keeps nothing" do
    path =
      tmp_config("""
      wal: {type: none}
      sources:
        demo:
          verify: {type: none}
          sinks: [{type: log}]
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ ~s(source "demo")
    assert error.message =~ "wal.type: disk"
  end

  test "an unknown wal.type lists disk and none" do
    error =
      assert_raise ConfigError, fn ->
        Config.load!(path: tmp_config("wal: {type: memory}\n"), env: %{})
      end

    assert error.message =~ ~s(wal.type: unknown value "memory")
    assert error.message =~ "disk, none"
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

  test "a removed dedup key on a source is rejected by name" do
    path =
      tmp_config("""
      sources:
        demo:
          verify: {type: none}
          dedup: {type: stripe}
          sinks: [{type: log}]
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ ~s(unknown key "dedup")
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

  # ── translation: routes ─────────────────────────────────────────────────────

  test "routes are off unless the file says otherwise" do
    path = tmp_config("http: {port: 4000}\n")

    assert Config.load!(path: path, env: %{}).config.routes == %Ankusa.Config{}.routes
  end

  test "a routes section maps to core's routes config" do
    path =
      tmp_config("""
      routes:
        enabled: true
        max_routes: 50
        log_sample: 0
        ip_denied_status: 404
        trusted_proxies: ["10.0.0.0/8", "192.168.0.0/16"]
        store: {type: ets}
        cache: {max_size: 100, ttl_ms: 1000, negative_ttl_ms: 500, gc_interval_ms: 10000}
        ip_rules:
          default: deny
          rules:
            - {action: allow, cidr: "203.0.113.0/24"}
        admin: {port: 4100}
        seed:
          - {id: seed, path: "/hooks/seed", methods: [POST], metadata: {owner: acme}}
      """)

    routes = Config.load!(path: path, env: %{}).config.routes

    assert routes.enabled
    assert routes.max_routes == 50
    assert routes.log_sample == 0
    assert routes.ip_denied_status == 404
    assert routes.trusted_proxies == ["10.0.0.0/8", "192.168.0.0/16"]
    assert routes.store == {Ankusa.Routes.Store.ETS, []}

    assert routes.cache == %{
             max_size: 100,
             ttl_ms: 1000,
             negative_ttl_ms: 500,
             gc_interval_ms: 10000
           }

    assert routes.ip_rules == %{
             default: :deny,
             rules: [%{action: :allow, cidr: "203.0.113.0/24"}]
           }

    assert routes.admin == %{port: 4100}

    assert [%{"id" => "seed", "path" => "/hooks/seed", "metadata" => %{"owner" => "acme"}}] =
             routes.seed
  end

  test "a redis store is selected by type, or by a url with no type" do
    for store <- [
          "{type: redis, url: redis://cache:6379, namespace: ankusa:routes}",
          "{url: redis://cache:6379}"
        ] do
      path = tmp_config("routes: {enabled: true, store: #{store}}\n")

      routes = Config.load!(path: path, env: %{}).config.routes

      assert {Ankusa.Routes.Store.Redis, opts} = routes.store
      assert opts[:url] == "redis://cache:6379"
    end

    path =
      tmp_config("""
      routes:
        enabled: true
        store: {type: redis, url: redis://cache:6379, namespace: ankusa:routes, tick_ms: 5000}
      """)

    assert {Ankusa.Routes.Store.Redis, opts} =
             Config.load!(path: path, env: %{}).config.routes.store

    assert opts[:namespace] == "ankusa:routes"
    assert opts[:tick_ms] == 5000
  end

  test "a url with type: ets is rejected instead of silently dropped" do
    path = tmp_config("routes: {enabled: true, store: {type: ets, url: redis://cache:6379}}\n")

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ ~s(routes.store: "url" is only valid with type: redis)
    assert error.message =~ "set type: redis or remove it"

    # The env override is a url too, and the shipped reference.yml has
    # `type: ets` live: setting only ANKUSA_ROUTES_STORE_URL must not look like
    # it shares definitions with the rest of the fleet.
    path = tmp_config("routes: {enabled: true, store: {type: ets}}\n")

    error =
      assert_raise ConfigError, fn ->
        Config.load!(path: path, env: %{"ANKUSA_ROUTES_STORE_URL" => "redis://from-env:6379"})
      end

    assert error.message =~ ~s(routes.store: "url" is only valid with type: redis)

    # Absent and explicitly null are the same thing: not set.
    path = tmp_config("routes: {enabled: true, store: {url: null}}\n")

    assert Config.load!(path: path, env: %{}).config.routes.store ==
             {Ankusa.Routes.Store.ETS, []}
  end

  test "namespace and tick_ms with an ets store are rejected, and all keys are named at once" do
    path = tmp_config("routes: {enabled: true, store: {namespace: ankusa:routes}}\n")

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ ~s(routes.store: "namespace" is only valid with type: redis)

    path = tmp_config("routes: {enabled: true, store: {tick_ms: 5000}}\n")

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ ~s(routes.store: "tick_ms" is only valid with type: redis)

    path =
      tmp_config(
        "routes: {enabled: true, store: {type: ets, url: redis://cache:6379, tick_ms: 5000}}\n"
      )

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ ~s("url", "tick_ms" are only valid with type: redis)
    assert error.message =~ "set type: redis or remove them"
  end

  test "type: redis without a url is rejected" do
    path = tmp_config("routes: {enabled: true, store: {type: redis}}\n")

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message == ~s(routes.store: missing required key "url")
  end

  test "an unknown store type lists the valid ones" do
    path = tmp_config("routes: {enabled: true, store: {type: cluster}}\n")

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ ~s(routes.store.type: unknown value "cluster")
    assert error.message =~ "ets, redis"
  end

  test "ANKUSA_ROUTES_ENABLED takes true/false in any case and nothing else" do
    path = tmp_config("routes: {enabled: false}\n")

    for value <- ["true", "TRUE", "True"] do
      loaded = Config.load!(path: path, env: %{"ANKUSA_ROUTES_ENABLED" => value})
      assert loaded.config.routes.enabled
    end

    for value <- ["false", "FALSE", "False"] do
      loaded = Config.load!(path: path, env: %{"ANKUSA_ROUTES_ENABLED" => value})
      refute loaded.config.routes.enabled
    end

    for value <- ["1", ""] do
      error =
        assert_raise ConfigError, fn ->
          Config.load!(path: path, env: %{"ANKUSA_ROUTES_ENABLED" => value})
        end

      assert error.message == ~s(routes.enabled: expected a boolean, got #{inspect(value)})
    end
  end

  test "an unknown ip_rules default or rule action is rejected by name" do
    path = tmp_config("routes: {enabled: true, ip_rules: {default: block}}\n")

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ ~s(routes.ip_rules.default: unknown value "block")
    assert error.message =~ "expected one of allow, deny"

    path =
      tmp_config("""
      routes:
        enabled: true
        ip_rules:
          rules:
            - {action: drop, cidr: "10.0.0.0/8"}
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ ~s(routes.ip_rules.rules[0].action: unknown value "drop")
    assert error.message =~ "expected one of allow, deny"
  end

  test "routes: {enabled: true} keeps every core default" do
    path = tmp_config("routes: {enabled: true}\n")

    routes = Config.load!(path: path, env: %{}).config.routes

    assert routes.enabled
    assert routes.max_routes == 10_000
    assert routes.store == {Ankusa.Routes.Store.ETS, []}
    assert routes.admin == %{port: 4003}
    assert routes.ip_denied_status == 403
    assert routes.log_sample == 100
    assert routes.trusted_proxies == []
    assert routes.ip_rules == %{default: :allow, rules: []}

    assert routes.cache == %{
             max_size: 50_000,
             ttl_ms: 30_000,
             negative_ttl_ms: 5_000,
             gc_interval_ms: 60_000
           }
  end

  test "trusted_proxies takes a list or one comma-separated string" do
    for value <- [~s(["10.0.0.0/8", "192.168.0.0/16"]), ~s("10.0.0.0/8, 192.168.0.0/16")] do
      path = tmp_config("routes: {enabled: true, trusted_proxies: #{value}}\n")

      assert Config.load!(path: path, env: %{}).config.routes.trusted_proxies ==
               ["10.0.0.0/8", "192.168.0.0/16"]
    end

    # An empty list is "no proxies", the default — not the empty-list error.
    path = tmp_config("routes: {enabled: true, trusted_proxies: []}\n")
    assert Config.load!(path: path, env: %{}).config.routes.trusted_proxies == []
  end

  test "the routes env overrides win over the file" do
    path =
      tmp_config("""
      routes:
        enabled: false
        store: {type: redis, url: redis://from-file:6379}
      """)

    env = %{
      "ANKUSA_ROUTES_ENABLED" => "true",
      "ANKUSA_ROUTES_STORE_URL" => "redis://from-env:6379"
    }

    routes = Config.load!(path: path, env: env).config.routes

    assert routes.enabled
    assert {Ankusa.Routes.Store.Redis, opts} = routes.store
    assert opts[:url] == "redis://from-env:6379"
  end

  test "an unknown routes key is rejected by name, at every level" do
    path = tmp_config("routes: {enabled: true, foo: 1}\n")
    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message == ~s(routes: unknown key "foo")

    path = tmp_config("routes: {enabled: true, cache: {ttl: 1}}\n")
    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message == ~s(routes.cache: unknown key "ttl")

    path = tmp_config("routes: {enabled: true, seed: [{id: a, pth: /x}]}\n")
    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message == ~s(routes.seed[0]: unknown key "pth")

    path = tmp_config("routes: {enabled: true, admin: {token: t}}\n")
    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message == ~s(routes.admin: unknown key "token")
  end

  test "core's route validation runs at load, so check-config catches a bad rule" do
    path =
      tmp_config("""
      routes:
        enabled: true
        ip_rules: {rules: [{action: allow, cidr: "10.0.0.0/33"}]}
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end

    assert error.message =~
             ~s(routes.ip_rules.rules are invalid: rule 0: invalid cidr "10.0.0.0/33")
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

  # ── source_store ────────────────────────────────────────────────────────────

  test "source_store defaults to the static store" do
    path = tmp_config("sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}")
    config = Config.load!(path: path, env: %{}).config

    assert {Ankusa.SourceStore.Static, opts} = config.source_store
    assert Map.keys(opts[:sources]) == ["demo"]
  end

  test "source_store: persistent keeps the seeds and attaches the decoder" do
    path =
      tmp_config("""
      source_store: {type: persistent}
      sources:
        "acme.billing":
          verify: {type: hmac, secret: "s3cr3t", signature_header: X-Sig}
          sinks: [{type: log}]
      """)

    config = Config.load!(path: path, env: %{}).config

    assert {Ankusa.SourceStore.Persistent, opts} = config.source_store
    assert Map.has_key?(opts[:sources], "acme.billing")
    assert is_function(opts[:decoder], 2)
  end

  test "source_from_map!/2 is the YAML validation, wrapped" do
    opts =
      Config.source_from_map!("acme.new", %{"sinks" => [%{"type" => "log"}]})

    assert opts == [sinks: [{Ankusa.Sink.Log, []}]]

    error =
      assert_raise ConfigError, fn ->
        Config.source_from_map!("acme.new", %{})
      end

    assert error.message ==
             ~s(sources.acme.new.sinks: missing required key "sinks")

    error =
      assert_raise ConfigError, fn ->
        Config.source_from_map!("acme.new", %{"sinks" => [%{"type" => "bogus"}]})
      end

    assert error.message =~ "sources.acme.new.sinks[0].type"
  end

  test "an unknown source_store type is rejected by name" do
    path =
      tmp_config(
        "source_store: {type: redis}\nsources: {demo: {verify: {type: none}, sinks: [{type: log}]}}"
      )

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ "source_store.type"
    assert error.message =~ ~s(unknown value "redis")
  end

  test "an unknown source_store key is rejected" do
    path = tmp_config("source_store: {type: static, path: /tmp/x}\n")

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ ~s(source_store: unknown key "path")
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
