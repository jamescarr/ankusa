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
    "SINK_URL" => "https://sink.fixture.invalid/hooks",
    "SINK_WHSEC" => "whsec_c2luay1maXh0dXJl",
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
    # A password in the URL userinfo: the printed config must redact it.
    "REDIS_SINK_URL" => "redis://:fixture-redis-password@redis:6379",
    "SQS_QUEUE_URL" => "https://sqs.us-east-1.amazonaws.com/000000000000/fixture.fifo",
    "STANDARD_WEBHOOKS_SECRET" => "whsec_Zml4dHVyZQ==",
    "GITHUB_WEBHOOK_SECRET" => "fixture-github-secret",
    "GCP_TOKEN" => "fixture-gcp-token",
    "CLAIM_CHECK_TOKEN" => "fixture-claim-check-token"
  }

  @fixture_secrets ~w(fixture-s3-secret
                      whsec_fixture-stripe-secret fixture-rabbit-password
                      fixture-kafka-password fixture-nats-password fixture-github-secret
                      fixture-redis-password fixture-gcp-token fixture-claim-check-token
                      whsec_Zml4dHVyZQ== whsec_c2luay1maXh0dXJl)

  # ── the shipped configs ─────────────────────────────────────────────────────

  test "every shipped config loads" do
    for path <- shipped_configs() do
      assert %Ankusa.Config{} = Config.load!(path: path, env: @fixture_env).config,
             "#{path} did not load"
    end
  end

  test "the reference config spells out the whole schema" do
    config = Config.load!(path: "config-examples/reference.yml", env: @fixture_env).config

    assert Map.delete(config.admin, :auth) == %{
             enabled: true,
             port: 4002,
             ip: "127.0.0.1",
             gauge_interval_ms: 15_000,
             tls: nil
           }

    # The reference spells the auth blocks out at their defaults.
    for auth <- [config.admin.auth, config.routes.admin.auth, config.claim_check.auth] do
      assert %{mode: :none, allow_insecure: false, tokens: [], mtls: %{subject_map: %{}}} = auth
    end

    assert config.quarantine == %{burst: 100, rate: 20, max_bytes: 1_073_741_824}

    assert config.store_headers == Ankusa.Edge.StoredHeaders.default()
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
             Ankusa.Sink.NATS,
             Ankusa.Sink.Redis,
             Ankusa.Sink.SQS,
             Ankusa.Sink.GooglePubSub
           ]

    {Ankusa.Sink.Http, http_opts} = Enum.at(sinks, 1)
    assert http_opts[:secret] == @fixture_env["SINK_WHSEC"]
    assert http_opts[:max_response_bytes] == 65_536

    assert %{
             sink_concurrency: 8,
             breaker_failures: 5,
             breaker_open_ms: 30_000,
             breaker_max_open_ms: 300_000,
             tenants: %{
               default: %{max_inflight: 1024, max_inflight_bytes: 33_554_432, concurrency: 16}
             }
           } = config.dispatch

    assert config.batcher.max_queue_bytes == 268_435_456
    assert config.storage.key_prefix == ""

    assert %{
             blob_stores: nil,
             placement: :ordered,
             key_layout: :flat,
             store_cooldown_ms: 30_000,
             read_deadline_ms: 5_000,
             read_timeout_ms: 3_000,
             read_retries: 1,
             read_max_inflight: 48,
             read_queue_max: 256,
             read_cache_bytes: 134_217_728,
             read_cache_max_entry_bytes: 1_048_576,
             max_concurrent_reads: 1024,
             ref_signing_keys: [],
             ref_ttl_ms: nil,
             accept_unsigned_refs: nil,
             authorize: nil
           } = config.claim_check
  end

  test "the baked image config is the demo: admin on, one open source" do
    config = Config.load!(path: "rel/ankusa.yml", env: %{}).config

    assert Map.delete(config.admin, :auth) == %{
             enabled: true,
             port: 4002,
             ip: "127.0.0.1",
             gauge_interval_ms: 15_000,
             tls: nil
           }

    assert config.admin.auth.mode == :none

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

  test "a ${…} that is not a variable reference fails instead of staying literal" do
    path =
      tmp_config("""
      sources:
        stripe:
          verify: {type: stripe, secret: "${stripe_secret}"}
          sinks: [{type: log}]
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end

    assert error.message ==
             "sources.stripe.verify.secret: unresolved ${…} (use ${NAME}, ${NAME:-default}, " <>
               "${file:/abs/path} or ${sm:<provider>:<ref>}; variable names are [A-Z0-9_]+; " <>
               "use ${NAME:-} for an empty default)"
  end

  test "a substituted value may itself contain ${" do
    path =
      tmp_config("""
      sources:
        stripe:
          verify: {type: stripe, secret: "${STRIPE_WHSEC}"}
          sinks: [{type: log}]
      """)

    loaded = Config.load!(path: path, env: %{"STRIPE_WHSEC" => "whsec_a${b}c"})
    %{verifier: {_hmac, opts}} = source_from(loaded.config, "stripe")
    assert opts[:secret] == "whsec_a${b}c"
  end

  test "a verify secret that does not decode fails the load, naming the source" do
    path =
      tmp_config("""
      sources:
        hooks:
          verify: {type: standard_webhooks, secret: "whsec_!!!"}
          sinks: [{type: log}]
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message == "source hooks: verifier secret is missing or undecodable"
  end

  test "an out-of-range number fails the load with core's message" do
    path =
      tmp_config(
        "batcher: {partitions: 0}\nsources: {demo: {verify: {type: none}, sinks: [{type: log}]}}\n"
      )

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message == "batcher.partitions must be a positive integer, got 0"
  end

  test "claim_check.store and storage.key_prefix land in core's config" do
    path =
      tmp_config("""
      storage: {key_prefix: "node-a/"}
      claim_check:
        store:
          type: s3
          s3: {bucket: claims, region: us-east-1, session_token: tok}
      sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
      """)

    config = Config.load!(path: path, env: %{}).config
    assert config.storage.key_prefix == "node-a/"

    assert [{Ankusa.BlobStore.S3, opts}] = config.claim_check.blob_stores
    assert opts[:bucket] == "claims"
    assert opts[:session_token] == "tok"
    assert config.storage.blob_store == {Ankusa.BlobStore.LocalFS, []}
  end

  test "claim_check.stores lands as blob_stores in list order, with placement, layout and cooldown" do
    path =
      tmp_config("""
      claim_check:
        stores:
          - type: s3
            s3: {bucket: claims-east, region: us-east-1}
          - type: gcs
            gcs: {bucket: claims-gcs, auth: none}
        placement: hash
        key_layout: sharded
        store_cooldown_ms: 5000
      sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
      """)

    cc = Config.load!(path: path, env: %{}).config.claim_check

    assert [{Ankusa.BlobStore.S3, s3}, {Ankusa.BlobStore.GCS, gcs}] = cc.blob_stores
    assert {s3[:bucket], s3[:region]} == {"claims-east", "us-east-1"}
    assert gcs[:bucket] == "claims-gcs"
    assert {cc.placement, cc.key_layout, cc.store_cooldown_ms} == {:hash, :sharded, 5000}
  end

  test "unset or null, claim_check keeps core's store, placement, layout and cooldown defaults" do
    for section <- ["", "claim_check: {store: null, stores: null}\n"] do
      path =
        tmp_config(section <> "sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}\n")

      cc = Config.load!(path: path, env: %{}).config.claim_check

      assert {cc.blob_stores, cc.placement, cc.key_layout, cc.store_cooldown_ms} ==
               {nil, :ordered, :flat, 30_000}
    end
  end

  test "claim_check read keys land in core's config, and core's ranges name a bad one" do
    path =
      tmp_config("""
      claim_check: {read_deadline_ms: 2500, read_cache_bytes: 0, read_queue_max: 0}
      sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
      """)

    cc = Config.load!(path: path, env: %{}).config.claim_check
    assert {cc.read_deadline_ms, cc.read_cache_bytes, cc.read_queue_max} == {2500, 0, 0}

    path =
      tmp_config("""
      claim_check: {read_timeout_ms: 0}
      sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message == "claim_check.read_timeout_ms must be a positive integer, got 0"
  end

  test "claim_check signing keys, ttl, unsigned policy and request cap land in core's config" do
    path =
      tmp_config("""
      claim_check:
        ref_signing_keys: ["#{String.duplicate("a", 16)}", "#{String.duplicate("b", 16)}"]
        ref_ttl_ms: 3600000
        accept_unsigned_refs: true
        max_concurrent_reads: 0
      sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
      """)

    cc = Config.load!(path: path, env: %{}).config.claim_check

    assert cc.ref_signing_keys == [String.duplicate("a", 16), String.duplicate("b", 16)]

    assert {cc.ref_ttl_ms, cc.accept_unsigned_refs, cc.max_concurrent_reads} ==
             {3_600_000, true, 0}

    # one comma-separated string, the form `${VAR}` carries
    path =
      tmp_config("""
      claim_check: {ref_signing_keys: "#{String.duplicate("a", 16)}, #{String.duplicate("b", 16)}"}
      sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
      """)

    cc = Config.load!(path: path, env: %{}).config.claim_check
    assert cc.ref_signing_keys == [String.duplicate("a", 16), String.duplicate("b", 16)]

    # empty or null is "no signing"
    for empty <- ["[]", "null", ~s("")] do
      path =
        tmp_config("""
        claim_check: {ref_signing_keys: #{empty}, accept_unsigned_refs: null, ref_ttl_ms: null}
        sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
        """)

      cc = Config.load!(path: path, env: %{}).config.claim_check
      assert {cc.ref_signing_keys, cc.accept_unsigned_refs, cc.ref_ttl_ms} == {[], nil, nil}
    end
  end

  test "claim_check ref keys: core's ranges name a bad one" do
    for {yaml, message} <- [
          {"{ref_ttl_ms: 0}", "claim_check.ref_ttl_ms must be nil or a positive integer, got 0"},
          {"{max_concurrent_reads: -1}",
           "claim_check.max_concurrent_reads must be a non-negative integer, got -1"},
          {"{accept_unsigned_refs: maybe}",
           ~s(claim_check.accept_unsigned_refs: expected a boolean, got "maybe")}
        ] do
      path =
        tmp_config("""
        claim_check: #{yaml}
        sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
        """)

      error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
      assert error.message == message, yaml
    end
  end

  test "claim_check store/stores mistakes fail the load, naming the key" do
    for {yaml, message} <- [
          {"claim_check: {store: {type: local}, stores: [{type: local}]}",
           "claim_check: store and stores are exclusive; list every store under stores"},
          {"claim_check: {stores: []}", "claim_check.stores: must not be empty"},
          {"claim_check: {stores: {type: local}}",
           ~s(claim_check.stores: expected a list of stores, got %{"type" => "local"})},
          {"claim_check: {stores: [local]}",
           ~s(claim_check.stores[0]: expected a mapping, got "local")},
          {"claim_check: {stores: [{type: local, bucket: x}]}",
           ~s(claim_check.stores[0]: unknown key "bucket")},
          {"claim_check: {placement: nope}",
           ~s(claim_check.placement: unknown value "nope"; expected one of ordered, hash)},
          {"claim_check: {key_layout: hashed}",
           ~s(claim_check.key_layout: unknown value "hashed"; expected one of flat, sharded)}
        ] do
      path =
        tmp_config(yaml <> "\nsources: {demo: {verify: {type: none}, sinks: [{type: log}]}}\n")

      error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
      assert error.message == message
    end
  end

  test "the backup section lands in core's config, the env flag overrides it, unknown keys are named" do
    path =
      tmp_config("""
      backup:
        enabled: true
        interval_ms: 30000
        keep: 5
        store: {type: s3, s3: {bucket: b, region: r}}
      sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
      """)

    backup = Config.load!(path: path, env: %{}).config.backup
    assert %{enabled: true, interval_ms: 30_000, keep: 5} = backup
    assert {Ankusa.BlobStore.S3, opts} = backup.blob_store
    assert opts[:bucket] == "b"
    assert opts[:region] == "r"

    # Off by default, and the storage bucket unless a store is named.
    path = tmp_config("sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}\n")
    assert %{enabled: false, blob_store: nil} = Config.load!(path: path, env: %{}).config.backup

    assert Config.load!(path: path, env: %{"ANKUSA_BACKUP_ENABLED" => "true"}).config.backup.enabled

    path = tmp_config("backup: {enabled: true, foo: 1}\n")
    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message == ~s(backup: unknown key "foo")

    path = tmp_config("backup: {keep: 0}\n")
    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message == "backup.keep must be a positive integer, got 0"
  end

  @kek Base.encode64(:binary.copy(<<7>>, 32))
  @kek_old Base.encode64(:binary.copy(<<8>>, 32))

  test "secrets.kek and kek_previous reach core's config through ${VAR}, and are never printed" do
    path =
      tmp_config("""
      secrets:
        kek: ${SECRETS_KEK}
        kek_previous: ${SECRETS_PREVIOUS:-}
      sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
      """)

    env = %{"SECRETS_KEK" => "prod-1:" <> @kek}
    config = Config.load!(path: path, env: env).config
    assert config.secrets == %{kek: "prod-1:" <> @kek, kek_previous: [], kek_provider: nil}

    env = Map.put(env, "SECRETS_PREVIOUS", "old-1:#{@kek_old}, #{@kek_old}x")
    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: env) end
    assert error.message =~ "secrets.kek_previous must be a base64-encoded 32-byte key"
    refute error.message =~ @kek_old

    env = Map.put(env, "SECRETS_PREVIOUS", "old-1:" <> @kek_old)
    config = Config.load!(path: path, env: env).config
    assert config.secrets.kek_previous == ["old-1:" <> @kek_old]

    printed = print_config(path, env)
    refute printed =~ @kek
    refute printed =~ @kek_old
    assert printed =~ ~s("secrets")
  end

  test "without a secrets section nothing is encrypted, as before" do
    path = tmp_config("sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}\n")

    assert Config.load!(path: path, env: %{}).config.secrets == %{
             kek: nil,
             kek_previous: [],
             kek_provider: nil
           }
  end

  test "secrets mistakes fail the load, naming the key and never the value" do
    demo = "\nsources: {demo: {verify: {type: none}, sinks: [{type: log}]}}\n"

    for {yaml, message} <- [
          {"secrets: {kek: not-base64!}", "secrets.kek must be a base64-encoded 32-byte key"},
          {~s(secrets: {kek: ""}), "secrets.kek must be a base64-encoded 32-byte key"},
          {"secrets: {kek: #{@kek}, extra: 1}", ~s(secrets: unknown key "extra")},
          {"secrets: {kek: [a]}", "secrets.kek: expected a string"},
          {"secrets: {kek_previous: [#{@kek_old}]}",
           "secrets.kek_previous needs secrets.kek or secrets.kek_provider"}
        ] do
      path = tmp_config(yaml <> demo)
      error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
      assert error.message =~ message
      refute error.message =~ @kek
    end
  end

  test "sse on s3 and gcs stores lands in core's config for storage, backup and claim stores" do
    path =
      tmp_config("""
      storage:
        type: s3
        s3: {bucket: segs, region: us-east-1, sse: {type: kms, key_id: "arn:aws:kms:us-east-1:1:key/a"}}
      backup:
        enabled: true
        store: {type: gcs, gcs: {bucket: bk, auth: none, sse: {type: kms, key_id: "projects/p/k"}}}
      claim_check:
        stores:
          - {type: s3, s3: {bucket: c1, region: us-east-1, sse: {type: aes256}}}
          - {type: local}
      sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
      """)

    config = Config.load!(path: path, env: %{}).config

    assert {Ankusa.BlobStore.S3, s3} = config.storage.blob_store
    assert s3[:sse] == %{type: :kms, key_id: "arn:aws:kms:us-east-1:1:key/a"}

    assert {Ankusa.BlobStore.GCS, gcs} = config.backup.blob_store
    assert gcs[:sse] == %{type: :kms, key_id: "projects/p/k"}

    assert [{Ankusa.BlobStore.S3, claims}, {Ankusa.BlobStore.LocalFS, []}] =
             config.claim_check.blob_stores

    assert claims[:sse] == %{type: :aes256}
  end

  test "sse mistakes fail the load, naming the key" do
    demo = "\nsources: {demo: {verify: {type: none}, sinks: [{type: log}]}}\n"

    for {yaml, message} <- [
          {"storage: {type: s3, s3: {bucket: b, region: r, sse: {type: rot13}}}",
           ~s(storage.s3.sse.type: unknown value "rot13"; expected one of)},
          {"storage: {type: s3, s3: {bucket: b, region: r, sse: {key_id: k}}}",
           ~s(storage.s3.sse: missing required key "type")},
          {"storage: {type: s3, s3: {bucket: b, region: r, sse: {type: kms, kms: k}}}",
           ~s(storage.s3.sse: unknown key "kms")},
          {"storage: {type: gcs, gcs: {bucket: b, auth: none, sse: {type: kms}}}",
           "storage.blob_store: sse type :kms needs a key_id on gcs"},
          {"storage: {type: s3, s3: {bucket: b, region: r, sse: {type: aes256, key_id: k}}}",
           "storage.blob_store: sse key_id is only for type :kms"},
          {"backup: {enabled: true, store: {type: gcs, gcs: {bucket: b, auth: none, sse: {type: kms}}}}",
           "backup.blob_store: sse type :kms needs a key_id on gcs"}
        ] do
      path = tmp_config(yaml <> demo)
      error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
      assert error.message =~ message
    end
  end

  test "log.banner is on by default, the file or ANKUSA_BANNER turns it off, a non-boolean is named" do
    demo = "sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}\n"

    banner = fn yaml, env -> Config.load!(path: tmp_config(yaml), env: env).banner end

    assert banner.(demo, %{}) == true
    assert banner.(demo, %{"ANKUSA_BANNER" => "false"}) == false
    assert banner.("log: {banner: false}\n" <> demo, %{}) == false

    error =
      assert_raise ConfigError, fn ->
        Config.load!(path: tmp_config("log: {banner: maybe}\n"), env: %{})
      end

    assert error.message == ~s(log.banner: expected a boolean, got "maybe")
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
    assert config.wal == :disk
  end

  test "ANKUSA_ADMIN_IP and ANKUSA_CLAIM_CHECK_IP set the listener addresses" do
    path = tmp_config("sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}\n")

    assert Config.load!(path: path, env: %{}).config.admin.ip == "127.0.0.1"

    # Off loopback the admin API must authenticate, so the address comes with a token.
    config =
      Config.load!(
        path: path,
        env: %{
          "ANKUSA_ADMIN_IP" => "0.0.0.0",
          "ANKUSA_ADMIN_TOKEN" => "operator-token",
          "ANKUSA_CLAIM_CHECK_IP" => "::"
        }
      ).config

    assert config.admin.ip == "0.0.0.0"
    assert config.claim_check.ip == "::"
  end

  test "ANKUSA_ADMIN_IP off loopback without authentication fails the load, naming admin.auth" do
    path = tmp_config("sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}\n")

    error =
      assert_raise ConfigError, fn ->
        Config.load!(path: path, env: %{"ANKUSA_ADMIN_IP" => "0.0.0.0"})
      end

    assert error.message =~
             "admin.auth.mode is :none but admin.ip (0.0.0.0) is not a loopback address"

    # The explicit opt-out for a deployment authenticated in front of the port.
    open =
      tmp_config("""
      admin: {auth: {mode: none, allow_insecure: true}}
      sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
      """)

    assert Config.load!(path: open, env: %{"ANKUSA_ADMIN_IP" => "0.0.0.0"}).config.admin.auth.allow_insecure
  end

  test "a listener ip that is not an address fails load! naming the key" do
    path =
      tmp_config("""
      admin: {ip: nope}
      sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ "admin.ip"
  end

  describe "listener hardening and TLS" do
    @tls_dir Path.expand("../../../ankusa/test/fixtures/tls", __DIR__)
    @sources "sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}\n"

    test "the listener, metrics and TLS sections load into core's config" do
      path =
        tmp_config("""
        listener:
          max_connections: 5000
          num_acceptors: 16
          read_timeout_ms: 5000
          body_read_timeout_ms: 8000
          request_deadline_ms: 20000
          max_requests_per_connection: 500
          max_header_count: 32
          max_header_length: 8192
          http2: true
          tls:
            certfile: #{@tls_dir}/server.pem
            keyfile: #{@tls_dir}/server-key.pem
            cacertfile: #{@tls_dir}/ca.pem
            verify: verify_peer
            versions: [tlsv1.3]
            ciphers: [TLS_AES_256_GCM_SHA384]
        admin:
          tls: {certfile: #{@tls_dir}/server.pem, keyfile: #{@tls_dir}/server-key.pem}
        claim_check:
          tls: {certfile: #{@tls_dir}/server.pem, keyfile: #{@tls_dir}/server-key.pem}
        routes:
          admin:
            tls: {certfile: #{@tls_dir}/server.pem, keyfile: #{@tls_dir}/server-key.pem}
        metrics:
          enabled: true
          port: 9100
          ip: 0.0.0.0
          tls: {certfile: #{@tls_dir}/server.pem, keyfile: #{@tls_dir}/server-key.pem}
          detail: source
          max_series_per_metric: 100
        #{@sources}
        """)

      config = Config.load!(path: path, env: %{}).config

      assert %{
               max_connections: 5000,
               num_acceptors: 16,
               read_timeout_ms: 5000,
               body_read_timeout_ms: 8000,
               request_deadline_ms: 20_000,
               max_requests_per_connection: 500,
               max_header_count: 32,
               max_header_length: 8192,
               http2: true,
               tls: %{
                 verify: :verify_peer,
                 versions: [:"tlsv1.3"],
                 ciphers: ["TLS_AES_256_GCM_SHA384"],
                 cacertfile: cacertfile
               }
             } = config.listener

      assert cacertfile == Path.join(@tls_dir, "ca.pem")

      for tls <- [
            config.admin.tls,
            config.claim_check.tls,
            config.routes.admin.tls,
            config.metrics.tls
          ] do
        assert %{verify: :verify_none, versions: [:"tlsv1.2", :"tlsv1.3"]} = tls
      end

      assert %{
               enabled: true,
               port: 9100,
               ip: "0.0.0.0",
               detail: :source,
               max_series_per_metric: 100
             } = config.metrics
    end

    test "without the sections the hardened defaults apply and nothing is TLS" do
      config = Config.load!(path: tmp_config(@sources), env: %{}).config

      assert config.listener.read_timeout_ms == 10_000
      assert config.listener.http2 == false
      assert config.listener.tls == nil

      assert config.metrics == %{
               enabled: false,
               port: 4004,
               ip: "127.0.0.1",
               tls: nil,
               detail: :tenant,
               max_series_per_metric: 5_000
             }
    end

    test "ANKUSA_METRICS_* turn the metrics listener on and move it" do
      path = tmp_config(@sources)

      config =
        Config.load!(
          path: path,
          env: %{
            "ANKUSA_METRICS_ENABLED" => "true",
            "ANKUSA_METRICS_PORT" => "9200",
            "ANKUSA_METRICS_IP" => "0.0.0.0"
          }
        ).config

      assert %{enabled: true, port: 9200, ip: "0.0.0.0"} = config.metrics
    end

    test "a bad listener or TLS key fails load! naming it" do
      tls = "certfile: #{@tls_dir}/server.pem, keyfile: #{@tls_dir}/server-key.pem"

      for {yaml, message} <- [
            {"listener: {timeout: 1}", ~s(listener: unknown key "timeout")},
            {"listener: {tls: {#{tls}, password: x}}", ~s(listener.tls: unknown key "password")},
            {"listener: {tls: {#{tls}, verify: maybe}}",
             ~s(listener.tls.verify: unknown value "maybe")},
            {"listener: {tls: {#{tls}, versions: [ssl3]}}",
             ~s(listener.tls.versions: unknown value "ssl3")},
            {"listener: {read_timeout_ms: fast}",
             "listener.read_timeout_ms: expected an integer"},
            {"listener: {http2: sure}", "listener.http2: expected a boolean"},
            {"metrics: {port: nope}", "metrics.port: expected an integer"},
            {"metrics: {detail: everything}", ~s(metrics.detail: unknown value "everything")},
            {"metrics: {max_series_per_metric: lots}",
             "metrics.max_series_per_metric: expected an integer"},
            {"admin: {tls: {#{tls}, verify: nope}}", ~s(admin.tls.verify: unknown value "nope")}
          ] do
        path = tmp_config("#{yaml}\n#{@sources}")
        error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
        assert error.message =~ message, "#{yaml}: #{error.message}"
      end
    end

    test "core's TLS checks run at load, so check-config catches them" do
      tls = "certfile: #{@tls_dir}/server.pem, keyfile: #{@tls_dir}/server-key.pem"

      for {yaml, message} <- [
            {"listener: {tls: {#{tls}, verify: verify_peer}}",
             "listener.tls.cacertfile is required with listener.tls.verify: :verify_peer"},
            {"admin: {tls: {certfile: #{@tls_dir}/server.pem}}",
             "admin.tls.keyfile is required when admin.tls is set"},
            {"claim_check: {tls: {certfile: /nope.pem, keyfile: /nope-key.pem}}",
             "claim_check.tls.certfile must be a readable file"},
            {"listener: {max_connections: 0}",
             "listener.max_connections must be a positive integer, got 0"},
            {"metrics: {max_series_per_metric: 0}",
             "metrics.max_series_per_metric must be a positive integer, got 0"}
          ] do
        path = tmp_config("#{yaml}\n#{@sources}")
        error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
        assert error.message =~ message, "#{yaml}: #{error.message}"
      end
    end
  end

  describe "control-plane auth" do
    @tls_dir Path.expand("../../../ankusa/test/fixtures/tls", __DIR__)
    @sources "sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}\n"
    @every_scope Ankusa.Principal.scopes()

    defp load_auth(yaml, env \\ %{}),
      do: Config.load!(path: tmp_config(yaml <> "\n" <> @sources), env: env).config

    defp load_error(yaml, env \\ %{}) do
      error = assert_raise ConfigError, fn -> load_auth(yaml, env) end
      error.message
    end

    test "tokens are hashed at load: the plaintext never reaches core's config" do
      config =
        load_auth("""
        admin:
          auth:
            mode: token
            tokens:
              - {id: ci, token: "ci-secret-token", scopes: [dlq:read, "tenant:acme"]}
              - id: pre-hashed
                sha256: #{Ankusa.Admin.Auth.Token.hash("other-secret")}
                scopes: "config:read, metrics:read"
        """)

      assert [
               %{id: "ci", sha256: hash, scopes: ["dlq:read", "tenant:acme"]},
               %{id: "pre-hashed", scopes: ["config:read", "metrics:read"]}
             ] = config.admin.auth.tokens

      assert hash == Ankusa.Admin.Auth.Token.hash("ci-secret-token")
      refute inspect(config) =~ "ci-secret-token"
    end

    test "token mistakes are named" do
      for {entry, message} <- [
            {"{id: ci, scopes: [dlq:read]}", "admin.auth.tokens[0]: needs token or sha256"},
            {"{id: ci, token: x, sha256: abc, scopes: [dlq:read]}",
             "admin.auth.tokens[0]: token and sha256 are alternatives"},
            {"{id: ci, token: \"\", scopes: [dlq:read]}",
             "admin.auth.tokens[0].token: must not be empty"},
            {"{token: x, scopes: [dlq:read]}",
             ~s(admin.auth.tokens[0]: missing required key "id")},
            {"{id: ci, token: x}", ~s(admin.auth.tokens[0]: missing required key "scopes")},
            {"{id: ci, token: x, scopes: [dlq:read], extra: 1}",
             ~s(admin.auth.tokens[0]: unknown key "extra")},
            {"{id: ci, token: x, scopes: [dlq:read, admin:all]}",
             ~s(has unknown scopes ["admin:all"])}
          ] do
        assert load_error("admin: {auth: {mode: token, tokens: [#{entry}]}}") =~ message
      end
    end

    test "jwt and mtls blocks land in core's config" do
      config =
        load_auth("""
        admin:
          tls: {certfile: #{@tls_dir}/server.pem, keyfile: #{@tls_dir}/server-key.pem, cacertfile: #{@tls_dir}/ca.pem, verify: verify_peer}
          auth:
            mode: mtls
            mtls:
              subject_map:
                "cn:ops-laptop": [dlq:read, "tenant:*"]
                "uri:spiffe://prod/ops": metrics:read
        claim_check:
          auth:
            mode: jwt
            jwt:
              issuer: https://login.example.com/
              audience: [ankusa-claims, ankusa]
              jwks_url: https://login.example.com/jwks.json
              refresh_ms: 60000
              leeway_s: 5
              scope_claim: scp
        """)

      assert config.admin.auth.mtls.subject_map == %{
               "cn:ops-laptop" => ["dlq:read", "tenant:*"],
               "uri:spiffe://prod/ops" => ["metrics:read"]
             }

      assert %{
               mode: :jwt,
               jwt: %{
                 issuer: "https://login.example.com/",
                 audience: ["ankusa-claims", "ankusa"],
                 jwks_url: "https://login.example.com/jwks.json",
                 refresh_ms: 60_000,
                 min_refresh_ms: 10_000,
                 leeway_s: 5,
                 scope_claim: "scp"
               }
             } = config.claim_check.auth
    end

    test "core's rules run at load: mtls needs verify_peer, jwt needs a key source" do
      assert load_error(
               "admin: {auth: {mode: mtls, mtls: {subject_map: {\"cn:a\": [dlq:read]}}}}"
             ) =~
               "admin.auth.mode: :mtls needs admin.tls with verify: :verify_peer"

      assert load_error("admin: {auth: {mode: jwt, jwt: {issuer: i, audience: a}}}") =~
               "needs admin.auth.jwt.jwks_url or admin.auth.jwt.keys"

      assert load_error("admin: {auth: {mode: basic}}") =~
               ~s(admin.auth.mode: unknown value "basic")

      assert load_error("admin: {auth: {tokns: []}}") =~ ~s(admin.auth: unknown key "tokns")

      assert load_error("claim_check: {auth: {jwt: {secret: x}}}") =~
               ~s(claim_check.auth.jwt: unknown key "secret")
    end

    test "ANKUSA_ADMIN_TOKEN gives the admin API one token holding every scope and every tenant" do
      config = load_auth("", %{"ANKUSA_ADMIN_TOKEN" => "env-operator-token"})

      assert %{mode: :token, tokens: [%{id: "ANKUSA_ADMIN_TOKEN", sha256: hash, scopes: scopes}]} =
               config.admin.auth

      assert hash == Ankusa.Admin.Auth.Token.hash("env-operator-token")
      assert scopes == @every_scope ++ ["tenant:*"]
      refute inspect(config) =~ "env-operator-token"
    end

    test "the env tokens replace the file's auth block, one per control plane" do
      config =
        load_auth(
          """
          admin:
            auth: {mode: none}
          """,
          %{
            "ANKUSA_ADMIN_TOKEN" => "a",
            "ANKUSA_ROUTES_ADMIN_TOKEN" => "r",
            "ANKUSA_CLAIM_CHECK_TOKEN" => "c"
          }
        )

      assert [%{id: "ANKUSA_ADMIN_TOKEN"}] = config.admin.auth.tokens

      assert [
               %{
                 id: "ANKUSA_ROUTES_ADMIN_TOKEN",
                 scopes: ["routes:read", "routes:write", "tenant:*"]
               }
             ] =
               config.routes.admin.auth.tokens

      assert [%{id: "ANKUSA_CLAIM_CHECK_TOKEN", scopes: ["claims:read", "tenant:*"]}] =
               config.claim_check.auth.tokens
    end

    test "an empty token variable is an error, not a way to switch authentication off" do
      assert load_error("", %{"ANKUSA_ADMIN_TOKEN" => ""}) =~
               "ANKUSA_ADMIN_TOKEN is set but empty"
    end
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
          sinks: [{type: http, url: "https://sink.invalid/hooks", allow_unsigned: true}]
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

  test "wal: {type: none} rejects a source whose only sink is Redis pub/sub" do
    path =
      tmp_config("""
      wal: {type: none}
      sources:
        demo:
          verify: {type: none}
          sinks: [{type: redis, url: "redis://redis:6379", channel: "ankusa.hooks"}]
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ ~s(source "demo")
    assert error.message =~ "wal.type: disk"
  end

  # The check is "at least one durable sink", not "all of them" — but every sink
  # still has to confirm for a request to be acked. See delivery.md#direct-mode.
  test "wal: {type: none} accepts a Redis sink next to a durable one" do
    path =
      tmp_config("""
      wal: {type: none}
      sources:
        demo:
          verify: {type: none}
          sinks:
            - {type: rabbitmq, url: "amqp://guest:guest@rabbitmq:5672", exchange: ankusa.hooks}
            - {type: redis, url: "redis://redis:6379", channel: "ankusa.hooks"}
      """)

    config = Config.load!(path: path, env: %{}).config
    assert config.wal == :none
  end

  test "an unknown wal.type lists disk and none" do
    error =
      assert_raise ConfigError, fn ->
        Config.load!(path: tmp_config("wal: {type: memory}\n"), env: %{})
      end

    assert error.message =~ ~s(wal.type: unknown value "memory")
    assert error.message =~ "disk, none"
  end

  # ── lifecycle ───────────────────────────────────────────────────────────────

  test "lifecycle.sinks takes the same sink types a source does" do
    path =
      tmp_config("""
      sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
      lifecycle:
        sinks:
          - {type: kafka, brokers: ["redpanda:9092"], topic: ankusa.lifecycle}
      """)

    config = Config.load!(path: path, env: %{}).config

    assert [{Ankusa.Sink.Kafka, opts}] = config.lifecycle.sinks
    assert opts[:topic] == "ankusa.lifecycle"
  end

  test "without a lifecycle section lifecycle events are off" do
    path = tmp_config("sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}")

    assert Config.load!(path: path, env: %{}).config.lifecycle == %{sinks: []}
  end

  test "a lifecycle section without sinks, or with an unknown key, is rejected by name" do
    path = tmp_config("lifecycle: {}")
    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ "lifecycle.sinks"
    assert error.message =~ ~s(missing required key "sinks")

    path = tmp_config("lifecycle: {sinks: [{type: log}], topic: x}")
    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ ~s(lifecycle: unknown key "topic")
  end

  test "a source named like the reserved lifecycle source is rejected" do
    path =
      tmp_config("""
      sources:
        "ankusa:lifecycle": {verify: {type: none}, sinks: [{type: log}]}
      lifecycle:
        sinks: [{type: log}]
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ ~s("ankusa:lifecycle" is reserved for lifecycle events)
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

  test "the removed dispatch.poll_ms and http-sink ordered keys are rejected by name" do
    path = tmp_config("dispatch: {poll_ms: 200}\n")
    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ "dispatch"
    assert error.message =~ ~s(unknown key "poll_ms")

    path =
      tmp_config("""
      sources:
        a:
          sinks:
            - {type: http, url: "http://sink.invalid", ordered: true}
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ "sources.a.sinks[0]"
    assert error.message =~ ~s(unknown key "ordered")
  end

  test "dispatch.attempt_timeout_ms loads, and a non-positive value fails with the key's name" do
    path = tmp_config("dispatch: {attempt_timeout_ms: 2500}\n")
    assert Config.load!(path: path, env: %{}).config.dispatch.attempt_timeout_ms == 2500

    path = tmp_config("dispatch: {attempt_timeout_ms: 0}\n")
    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ "dispatch.attempt_timeout_ms must be a positive integer, got 0"
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

  test "the removed dedup `type:` form on a source is rejected by name" do
    path =
      tmp_config("""
      sources:
        demo:
          verify: {type: none}
          dedup: {type: stripe}
          sinks: [{type: log}]
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ "sources.demo"
    assert error.message =~ ~s(unknown key "dedup")
  end

  test "dedupe accepts a preset, a header map, a json map, and a ttl" do
    config =
      Config.load!(
        path:
          tmp_config("""
          sources:
            a: {verify: {type: none}, dedupe: github, sinks: [{type: log}]}
            b: {verify: {type: none}, dedupe: {header: X-Custom-Id}, sinks: [{type: log}]}
            c: {verify: {type: none}, dedupe: {json: data.id, ttl_seconds: 60}, sinks: [{type: log}]}
          """),
        env: %{}
      ).config

    sources = config.source_store |> elem(1) |> Keyword.fetch!(:sources)

    assert sources["a"][:dedupe] == :github
    assert sources["b"][:dedupe] == %{header: "X-Custom-Id"}
    assert sources["c"][:dedupe] == %{json: "data.id", ttl_ms: 60_000}

    # `Source.new/2` (what every fetch builds) folds the header name.
    assert Ankusa.Source.new("b", sources["b"]).dedupe.from == {:header, "x-custom-id"}
  end

  test "a dedupe map with none or several sources, a blank header, or a bad ttl is rejected" do
    for {yaml, fragment} <- [
          {"dedupe: {ttl_seconds: 60}", "set exactly one of preset, header, json"},
          {"dedupe: {header: x-id, json: data.id}", "set exactly one of preset, header, json"},
          {"dedupe: {header: ''}", "must not be empty"},
          {"dedupe: {header: x-id, ttl_seconds: 0}", "must be > 0"},
          {"dedupe: {preset: nope}", ~s(unknown value "nope")}
        ] do
      path =
        tmp_config("""
        sources:
          demo:
            verify: {type: none}
            #{yaml}
            sinks: [{type: log}]
        """)

      error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
      assert error.message =~ "sources.demo.dedupe"
      assert error.message =~ fragment
    end
  end

  test "forward_headers accepts a list (lowercased) or nothing, and rejects other shapes" do
    config =
      Config.load!(
        path:
          tmp_config("""
          sources:
            a: {verify: {type: none}, forward_headers: [X-GitHub-Event], sinks: [{type: log}]}
            b: {verify: {type: none}, forward_headers: [], sinks: [{type: log}]}
            c: {verify: {type: none}, sinks: [{type: log}]}
          """),
        env: %{}
      ).config

    sources = config.source_store |> elem(1) |> Keyword.fetch!(:sources)

    assert sources["a"][:forward_headers] == ["x-github-event"]
    assert sources["b"][:forward_headers] == []
    assert sources["c"][:forward_headers] == nil

    # The list survives `Source.new/2` — `[]` means "forward nothing", not an
    # error, and every fetch builds the struct through this path.
    assert Ankusa.Source.new("b", sources["b"]).forward_headers == []
    assert Ankusa.Source.new("a", sources["a"]).forward_headers == ["x-github-event"]
    assert Ankusa.Source.new("c", sources["c"]).forward_headers == :default

    path =
      tmp_config("""
      sources:
        demo:
          verify: {type: none}
          forward_headers: X-GitHub-Event
          sinks: [{type: log}]
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ "sources.demo.forward_headers"
    assert error.message =~ "a list of header names"
  end

  test "store_headers: the http lists and a source override parse, lowercase, and reject other shapes" do
    default = Ankusa.Edge.StoredHeaders.default()
    assert Config.load!(path: tmp_config("{}\n"), env: %{}).config.store_headers == default

    config =
      Config.load!(
        path:
          tmp_config("""
          http:
            store_headers: {drop: [Authorization, X-Foo], hash: [X-Tenant-Token]}
          sources:
            a: {verify: {type: none}, store_headers: {hash: [Cookie]}, sinks: [{type: log}]}
            b: {verify: {type: none}, store_headers: {drop: []}, sinks: [{type: log}]}
            c: {verify: {type: none}, sinks: [{type: log}]}
          """),
        env: %{}
      ).config

    assert config.store_headers == %{drop: ["authorization", "x-foo"], hash: ["x-tenant-token"]}

    {Ankusa.SourceStore.Static, opts} = config.source_store
    sources = opts[:sources]
    assert sources["a"][:store_headers] == %{hash: ["cookie"]}
    assert sources["b"][:store_headers] == %{drop: []}
    assert sources["c"][:store_headers] == nil
    assert Ankusa.Source.new("a", sources["a"]).store_headers == %{drop: nil, hash: ["cookie"]}

    # only `hash` given: `drop` keeps the default
    only_hash = tmp_config("http: {store_headers: {hash: [x-a]}}\n")

    assert Config.load!(path: only_hash, env: %{}).config.store_headers ==
             %{drop: default.drop, hash: ["x-a"]}

    path =
      tmp_config("""
      sources:
        demo:
          verify: {type: none}
          store_headers: {drop: authorization}
          sinks: [{type: log}]
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ "sources.demo.store_headers.drop"
    assert error.message =~ "a list of header names"

    error =
      assert_raise ConfigError, fn ->
        Config.load!(path: tmp_config("http: {store_headers: [authorization]}\n"), env: %{})
      end

    assert error.message =~ "http.store_headers"
    assert error.message =~ "a mapping"

    error =
      assert_raise ConfigError, fn ->
        Config.load!(path: tmp_config("http: {store_headers: {keep: []}}\n"), env: %{})
      end

    assert error.message =~ "http.store_headers"
    assert error.message =~ "keep"
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

  test "a verifier secret may be a list, for a rotation window" do
    path =
      tmp_config("""
      sources:
        a:
          verify: {type: stripe, secret: ["${A}", "${B}"]}
          sinks: [{type: log}]
      """)

    config = Config.load!(path: path, env: %{"A" => "a", "B" => "b"}).config
    assert %{verifier: {Ankusa.Verifier.Hmac, opts}} = source_from(config, "a")
    assert opts[:secret] == ["a", "b"]
  end

  test "a verifier secret must be a non-empty string or a list of them" do
    for {secret, message} <- [
          {"[]", "sources.a.verify.secret: expected a string or a list of strings, got []"},
          {"[a, 1]",
           ~s(sources.a.verify.secret: expected a string or a list of strings, got ["a", 1])},
          {"[a, b, c, d, e, f, g, h, i]", "sources.a.verify.secret: at most 8 secrets, got 9"},
          # An empty HMAC key verifies whatever anyone signs with it, so an
          # unset `${VAR:-}` is a load error, not a skipped rotation slot.
          {~s("${UNSET:-}"), "sources.a.verify.secret: must not be empty"},
          {~s(["${A}", "${UNSET:-}"]), "sources.a.verify.secret[1]: must not be empty"}
        ] do
      path =
        tmp_config(
          "sources: {a: {verify: {type: stripe, secret: #{secret}}, sinks: [{type: log}]}}\n"
        )

      error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{"A" => "a"}) end
      assert error.message == message
    end
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

    assert Config.load!(path: path, env: %{}).config.routes == Ankusa.Config.new().routes
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
          - {id: bound, path: "/hooks/bound", tenant_id: acme, source_id: "acme.billing"}
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
             gc_interval_ms: 10_000
           }

    assert routes.ip_rules == %{
             default: :deny,
             rules: [%{action: :allow, cidr: "203.0.113.0/24"}]
           }

    assert Map.delete(routes.admin, :auth) == %{port: 4100, ip: "127.0.0.1", tls: nil}

    assert [
             %{"id" => "seed", "path" => "/hooks/seed", "metadata" => %{"owner" => "acme"}},
             %{
               "id" => "bound",
               "path" => "/hooks/bound",
               "tenant_id" => "acme",
               "source_id" => "acme.billing"
             }
           ] = routes.seed
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
    assert Map.delete(routes.admin, :auth) == %{port: 4003, ip: "127.0.0.1", tls: nil}
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

  # ── translation: rate limits ────────────────────────────────────────────────

  test "the fleet scopes and the fleet store map to core's config" do
    path =
      tmp_config("""
      rate_limits:
        scope: fleet
        default: {rate: 10, burst: 20}
      dedupe: {scope: fleet}
      fleet:
        store: {url: "${FLEET_REDIS}", namespace: "ankusa:fleet:prod", pool_size: 8}
        timeout_ms: 15
        pending_ttl_ms: 20000
        breaker_window_ms: 2000
        breaker_min_calls: 50
        breaker_failure_rate: 0.25
        breaker_open_ms: 10000
      """)

    config = Config.load!(path: path, env: %{"FLEET_REDIS" => "redis://redis:6379"}).config

    assert config.rate_limits.scope == :fleet
    assert config.dedupe == %{scope: :fleet}

    assert config.fleet == %{
             store:
               {Ankusa.RateLimiter.Store.Redis,
                [url: "redis://redis:6379", namespace: "ankusa:fleet:prod", pool_size: 8]},
             timeout_ms: 15,
             pending_ttl_ms: 20_000,
             breaker_window_ms: 2_000,
             breaker_min_calls: 50,
             breaker_failure_rate: 0.25,
             breaker_open_ms: 10_000
           }
  end

  test "no fleet keys means node scope, as core defaults" do
    config = Config.load!(path: tmp_config("node: {}\n"), env: %{}).config

    assert config.rate_limits.scope == :node
    assert config.dedupe == %Ankusa.Config{}.dedupe
    assert config.fleet == %Ankusa.Config{}.fleet
  end

  test "a bad fleet key names the key, and core's ranges and scope rules come back as core's message" do
    for {yaml, message} <- [
          {"fleet: {timeouts: 1}", ~s(fleet: unknown key "timeouts")},
          {"fleet: {store: {type: memcached, url: x}}",
           ~s(fleet.store.type: unknown value "memcached")},
          {"fleet: {store: {namespace: x}}", ~s(fleet.store: missing required key "url")},
          {"fleet: {timeout_ms: soon}", ~s(fleet.timeout_ms: expected an integer, got "soon")},
          {"fleet: {timeout_ms: 0}", "fleet.timeout_ms must be an integer from 1 to 1000, got 0"},
          {"rate_limits: {scope: cluster}", ~s(rate_limits.scope: unknown value "cluster")},
          {"dedupe: {scope: fleet}", "dedupe.scope: :fleet needs fleet.store"},
          {"rate_limits: {scope: fleet}", "rate_limits.scope: :fleet needs fleet.store"}
        ] do
      error =
        assert_raise ConfigError, fn ->
          Config.load!(path: tmp_config(yaml <> "\n"), env: %{})
        end

      assert error.message =~ message
    end
  end

  test "a rate_limits section maps to core's rate_limits config" do
    path =
      tmp_config("""
      rate_limits:
        default: {rate: 0.5, burst: 5}
        tenants:
          acme: {rate: 100, burst: 200}
          globex: {rate: "${GLOBEX_RATE}", burst: "10"}
      """)

    rate_limits = Config.load!(path: path, env: %{"GLOBEX_RATE" => "12.5"}).config.rate_limits

    assert rate_limits ==
             Map.merge(%Ankusa.Config{}.rate_limits, %{
               default: %{rate: 0.5, burst: 5},
               tenants: %{
                 "acme" => %{rate: 100, burst: 200},
                 "globex" => %{rate: 12.5, burst: 10}
               }
             })
  end

  test "the admission keys map to core's: per ip, node, sources and a tenant's bytes, body and queue" do
    path =
      tmp_config("""
      rate_limits:
        per_ip: {rate: 20, burst: 40}
        node: {rate: 5000.5, burst: "10000"}
        sources:
          default: {bytes_per_second: 50000000, burst_bytes: 8000000}
        default: {rate: 5, burst: 5, max_queue: 100}
        tenants:
          acme:
            rate: 100
            burst: 200
            bytes_per_second: 1000000.5
            burst_bytes: 2000000
            max_body_bytes: 1000000
            max_queue: 50
            max_queue_bytes: 4000000
      """)

    rate_limits = Config.load!(path: path, env: %{}).config.rate_limits

    assert rate_limits ==
             Map.merge(%Ankusa.Config{}.rate_limits, %{
               per_ip: %{rate: 20, burst: 40},
               node: %{rate: 5000.5, burst: 10_000},
               sources: %{default: %{bytes_per_second: 50_000_000, burst_bytes: 8_000_000}},
               default: %{rate: 5, burst: 5, max_queue: 100},
               tenants: %{
                 "acme" => %{
                   rate: 100,
                   burst: 200,
                   bytes_per_second: 1_000_000.5,
                   burst_bytes: 2_000_000,
                   max_body_bytes: 1_000_000,
                   max_queue: 50,
                   max_queue_bytes: 4_000_000
                 }
               }
             })
  end

  test "sources.default: absent keeps core's 100 MB/s, null switches the ceiling off" do
    absent = tmp_config("rate_limits: {per_ip: {rate: 1, burst: 1}}\n")

    assert Config.load!(path: absent, env: %{}).config.rate_limits.sources ==
             %{default: %{bytes_per_second: 100_000_000}}

    off = tmp_config("rate_limits: {sources: {default: null}}\n")
    assert Config.load!(path: off, env: %{}).config.rate_limits.sources == %{default: nil}
  end

  test "no rate_limits section means what core defaults to" do
    path = tmp_config("http: {port: 4000}\n")

    assert Config.load!(path: path, env: %{}).config.rate_limits ==
             %Ankusa.Config{}.rate_limits
  end

  test "a bad rate limit names the key, and core's ranges come back as core's message" do
    for {yaml, message} <- [
          {"rate_limits: {defaults: {rate: 1, burst: 1}}",
           ~s(rate_limits: unknown key "defaults")},
          {"rate_limits: {tenants: {acme: {rate: 1}}}",
           ~s(rate_limits.tenants.acme: missing required key "burst")},
          {"rate_limits: {tenants: {acme: {rate: fast, burst: 1}}}",
           ~s(rate_limits.tenants.acme.rate: expected a number, got "fast")},
          {"rate_limits: {tenants: {acme: {rate: 1, burst: 1, per: s}}}",
           ~s(rate_limits.tenants.acme: unknown key "per")},
          {"rate_limits: {tenants: [acme]}",
           ~s(rate_limits.tenants: expected a mapping of tenant id to limit, got ["acme"])},
          {"rate_limits: {default: {rate: 0, burst: 1}}",
           "rate_limits.default.rate must be a number greater than 0, got 0"},
          {"rate_limits: {per_ip: {rate: 1}}",
           ~s(rate_limits.per_ip: missing required key "burst")},
          {"rate_limits: {per_ip: {rate: 1, burst: 1, max_queue: 1}}",
           ~s(rate_limits.per_ip: unknown key "max_queue")},
          {"rate_limits: {node: {rate: 0, burst: 1}}",
           "rate_limits.node.rate must be a number greater than 0, got 0"},
          {"rate_limits: {sources: {other: {bytes_per_second: 1}}}",
           ~s(rate_limits.sources: unknown key "other")},
          {"rate_limits: {sources: {default: {burst_bytes: 5}}}",
           ~s(rate_limits.sources.default: missing required key "bytes_per_second")},
          {"rate_limits: {sources: {default: {bytes_per_second: fast}}}",
           ~s(rate_limits.sources.default.bytes_per_second: expected a number, got "fast")},
          {"rate_limits: {tenants: {acme: {rate: 1, burst: 1, max_queue: lots}}}",
           ~s(rate_limits.tenants.acme.max_queue: expected an integer, got "lots")},
          {"rate_limits: {tenants: {acme: {rate: 1, burst: 1, max_queue: 0}}}",
           "rate_limits.tenants.acme.max_queue must be an integer of at least 1, got 0"},
          {"rate_limits: {tenants: {acme: {rate: 1, burst: 1, max_body_bytes: 99999999}}}",
           "rate_limits.tenants.acme.max_body_bytes must be an integer of at least 1 and at " <>
             "most the instance's max_body_bytes (8000000), got 99999999"}
        ] do
      path = tmp_config(yaml <> "\n")
      error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
      assert error.message == message
    end
  end

  test "dispatch.tenants maps to core's: default merges over the built-in, a tenant id overrides" do
    path =
      tmp_config("""
      dispatch:
        sink_concurrency: 4
        tenants:
          default: {max_inflight: 512}
          acme: {concurrency: "24", max_inflight_bytes: 1000000}
          globex:
      """)

    dispatch = Config.load!(path: path, env: %{}).config.dispatch

    assert dispatch.sink_concurrency == 4

    assert dispatch.tenants == %{
             :default => %{max_inflight: 512, max_inflight_bytes: 33_554_432, concurrency: 16},
             "acme" => %{concurrency: 24, max_inflight_bytes: 1_000_000},
             "globex" => %{}
           }
  end

  test "no dispatch.tenants means what core defaults to" do
    path = tmp_config("dispatch: {batch: 64}\n")

    assert Config.load!(path: path, env: %{}).config.dispatch.tenants ==
             %Ankusa.Config{}.dispatch.tenants
  end

  test "dispatch.sink_concurrency: absent keeps core's 8, null switches the per-sink cap off" do
    absent = tmp_config("dispatch: {batch: 64}\n")
    assert Config.load!(path: absent, env: %{}).config.dispatch.sink_concurrency == 8

    off = tmp_config("dispatch: {sink_concurrency: null}\n")
    assert Config.load!(path: off, env: %{}).config.dispatch.sink_concurrency == nil
  end

  test "a bad dispatch.tenants names the key, and core's ranges come back as core's message" do
    for {yaml, message} <- [
          {"dispatch: {tenants: [acme]}",
           ~s(dispatch.tenants: expected a mapping of tenant id to limits, got ["acme"])},
          {"dispatch: {tenants: {acme: {max_inflight: lots}}}",
           ~s(dispatch.tenants.acme.max_inflight: expected an integer, got "lots")},
          {"dispatch: {tenants: {acme: {inflight: 1}}}",
           ~s(dispatch.tenants.acme: unknown key "inflight")},
          {"dispatch: {tenants: {acme: {concurrency: 0}}}",
           ~s(dispatch.tenants["acme"].concurrency must be a positive integer, got 0)},
          {"dispatch: {tenants: {default: {max_inflight_bytes: -5}}}",
           "dispatch.tenants.default.max_inflight_bytes must be a positive integer, got -5"}
        ] do
      path = tmp_config(yaml <> "\n")
      error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
      assert error.message == message
    end
  end

  test "a quarantine section maps to core's quarantine config" do
    path = tmp_config("quarantine: {burst: 5, rate: 0.5, max_bytes: 1024}\n")

    assert Config.load!(path: path, env: %{}).config.quarantine ==
             %{burst: 5, rate: 0.5, max_bytes: 1024}
  end

  test "a bad quarantine key names it, and core's ranges come back as core's message" do
    for {yaml, message} <- [
          {"quarantine: {max_byte: 1}", ~s(quarantine: unknown key "max_byte")},
          {"quarantine: {rate: slow}", ~s(quarantine.rate: expected a number, got "slow")},
          {"quarantine: {burst: 0}", "quarantine.burst must be a positive integer, got 0"},
          {"quarantine: {max_bytes: 0}", "quarantine.max_bytes must be a positive integer, got 0"}
        ] do
      path = tmp_config(yaml <> "\n")
      error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
      assert error.message == message
    end
  end

  # ── translation: storage ────────────────────────────────────────────────────

  test "storage.gcs.auth selects the token provider" do
    assert {Ankusa.BlobStore.GCS, opts} = gcs_opts(%{auth: "metadata"})
    assert opts[:token_provider] == {AnkusaServer.GcpToken, :metadata, []}

    assert {Ankusa.BlobStore.GCS, opts} = gcs_opts(%{auth: "token", token: "ya29.static"})
    assert opts[:token_provider] == {AnkusaServer.GcpToken, :static, ["ya29.static"]}

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
          verify: {type: standard_webhooks, secret: "whsec_YWJj", tolerance_seconds: 60}
          sinks:
            - {type: http, url: "https://sink.invalid/h", method: put, timeout_ms: 250,
               allow_unsigned: true, headers: {x-one: "1"}}
            - {type: rabbitmq, exchange: ankusa.full, idle_timeout_ms: 0}
            - {type: kafka, brokers: ["b:9092"], topic: t, ssl: true,
               sasl: {mechanism: scram_sha_512, username: u, password: p},
               idle_timeout_ms: 120000}
            - {type: nats, servers: "${NATS_SERVERS}", subject: "ankusa.full",
               inline_max_bytes: 4096, publish_timeout_ms: 250,
               auth: {username: nu, password: np}, idle_timeout_ms: 30000}
            - {type: redis, url: "redis://redis:6379", channel: ankusa.full,
               inline_max_bytes: 2048, publish_timeout_ms: 300, idle_timeout_ms: "60000"}
            - {type: sqs, queue_url: "https://sqs.us-east-1.amazonaws.com/1/h.fifo",
               region: us-east-1, message_group_id: g, max_message_bytes: 262144}
            - {type: google_pubsub, project: p, topic: t, ordering_key: k,
               max_message_bytes: 1000}
      """)

    env = %{"NATS_SERVERS" => "n1:4222,n2:4222"}

    config = Config.load!(path: path, env: env).config
    {Ankusa.SourceStore.Static, store} = config.source_store
    source = Ankusa.Source.new("full", store[:sources]["full"])

    assert source.tenant_id == "acme"
    assert source.on_verify_failure == :accept_flag

    assert source.verifier ==
             {Ankusa.Verifier.Hmac,
              [scheme: :standard_webhooks, secret: "whsec_YWJj", tolerance: 60]}

    assert [
             {Ankusa.Sink.Http,
              [
                url: "https://sink.invalid/h",
                method: :put,
                headers: [{"x-one", "1"}],
                timeout_ms: 250,
                allow_unsigned: true
              ]},
             {Ankusa.Sink.RabbitMQ, [exchange: "ankusa.full", idle_timeout_ms: 0]},
             {Ankusa.Sink.Kafka, kafka},
             {Ankusa.Sink.NATS, nats},
             {Ankusa.Sink.Redis, redis},
             {Ankusa.Sink.SQS, sqs},
             {Ankusa.Sink.GooglePubSub, pubsub}
           ] = source.sinks

    assert kafka[:brokers] == ["b:9092"]
    assert kafka[:ssl] == true
    assert kafka[:sasl] == {:scram_sha_512, "u", "p"}
    assert kafka[:idle_timeout_ms] == 120_000

    # The comma-separated env var form of `servers` too.
    assert nats[:servers] == ["n1:4222", "n2:4222"]
    assert nats[:subject] == "ankusa.full"
    assert nats[:inline_max_bytes] == 4096
    assert nats[:publish_timeout_ms] == 250
    assert nats[:username] == "nu"
    assert nats[:password] == "np"
    assert nats[:idle_timeout_ms] == 30_000

    assert redis[:url] == "redis://redis:6379"
    assert redis[:channel] == "ankusa.full"
    assert redis[:inline_max_bytes] == 2048
    assert redis[:publish_timeout_ms] == 300
    assert redis[:idle_timeout_ms] == 60_000

    assert sqs[:queue_url] == "https://sqs.us-east-1.amazonaws.com/1/h.fifo"
    assert sqs[:region] == "us-east-1"
    assert sqs[:message_group_id] == "g"
    assert sqs[:max_message_bytes] == 262_144

    assert pubsub[:project] == "p"
    assert pubsub[:topic] == "t"
    assert pubsub[:ordering_key] == "k"
    assert pubsub[:max_message_bytes] == 1000
    assert pubsub[:token_provider] == {AnkusaServer.GcpToken, :metadata, []}
  end

  test "an sqs sink requires queue_url and names an unknown key" do
    path =
      tmp_config("""
      sources:
        a:
          sinks: [{type: sqs, region: us-east-1}]
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ "sources.a.sinks[0]"
    assert error.message =~ "queue_url"

    path =
      tmp_config("""
      sources:
        a:
          sinks: [{type: sqs, queue_url: "https://sqs.us-east-1.amazonaws.com/1/h", region: us-east-1, queue: x}]
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ ~s(unknown key "queue")
  end

  test "a google_pubsub sink requires project and topic, and auth: token requires a token" do
    path =
      tmp_config("""
      sources:
        a:
          sinks: [{type: google_pubsub, project: p}]
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message == ~s(sources.a.sinks[0]: missing required key "topic")

    path =
      tmp_config("""
      sources:
        a:
          sinks: [{type: google_pubsub, project: p, topic: t, auth: token}]
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message == ~s(sources.a.sinks[0]: missing required key "token")
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
    refute printed =~ "eyJacc"
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

  test "tolerance range: 0..3600 loads, 3601 and negatives are a config error naming the key" do
    load = fn type, seconds ->
      path =
        tmp_config("""
        sources:
          signed:
            verify: {type: #{type}, secret: "whsec_YWJj", tolerance_seconds: #{seconds}}
            sinks: [{type: log}]
        """)

      Config.load!(path: path, env: %{})
    end

    for seconds <- [0, 300, 3600] do
      config = load.("stripe", seconds).config
      assert {Ankusa.Verifier.Hmac, opts} = source_from(config, "signed").verifier
      assert opts[:tolerance] == seconds
    end

    for type <- ~w(stripe standard_webhooks slack), seconds <- [3601, 86_400, -1] do
      error = assert_raise ConfigError, fn -> load.(type, seconds) end

      assert error.message ==
               "sources.signed.verify.tolerance_seconds: must be between 0 and 3600 seconds, " <>
                 "got #{seconds}"
    end

    # The same bound for an inline `hmac` scheme, and a string that parses past it.
    path =
      tmp_config("""
      sources:
        signed:
          verify:
            type: hmac
            secret: "k"
            signature_header: "X-Sig"
            tolerance_seconds: "3601"
          sinks: [{type: log}]
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ "sources.signed.verify.tolerance_seconds: must be between 0 and 3600"
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

  # ── asymmetric, token and client-certificate verifiers ─────────────────────

  @asym_pub "whpk_11qYAYKxCrfVS/7TyWQHOg7hcvPapiMlrwIaaPcHURo="
  @asym_other_pub "whpk_PUAXw+hDiVqStwqnTRt+vJyYLM8uxJaMwM1V8Sr0Zgw="

  defp verify_yaml(verify, extra) do
    tmp_config("""
    #{extra}
    sources:
      x:
        verify: #{verify}
        sinks: [{type: log}]
    """)
  end

  defp verifier_of(verify, env \\ %{}, extra \\ "") do
    config = Config.load!(path: verify_yaml(verify, extra), env: env).config
    source_from(config, "x").verifier
  end

  defp verify_error(verify, env \\ %{}, extra \\ "") do
    error =
      assert_raise ConfigError, fn ->
        Config.load!(path: verify_yaml(verify, extra), env: env)
      end

    error.message
  end

  test "type standard_webhooks_asym takes a public key or a rotation list, and names a bad one" do
    assert verifier_of(
             ~s({type: standard_webhooks_asym, public_key: "#{@asym_pub}", tolerance_seconds: 60})
           ) ==
             {Ankusa.Verifier.Asymmetric,
              [scheme: :standard_webhooks_asym, public_key: @asym_pub, tolerance: 60]}

    assert verifier_of(
             ~s({type: standard_webhooks_asym, public_key: ["#{@asym_pub}", "#{@asym_other_pub}"]})
           ) ==
             {Ankusa.Verifier.Asymmetric,
              [scheme: :standard_webhooks_asym, public_key: [@asym_pub, @asym_other_pub]]}

    assert verifier_of(~s({type: standard_webhooks_asym, public_key: "${PUB}"}), %{
             "PUB" => @asym_pub
           }) ==
             {Ankusa.Verifier.Asymmetric,
              [scheme: :standard_webhooks_asym, public_key: @asym_pub]}

    assert verify_error("{type: standard_webhooks_asym}") ==
             ~s(sources.x.verify: missing required key "public_key")

    assert verify_error(~s({type: standard_webhooks_asym, public_key: ""})) ==
             "sources.x.verify.public_key: must not be empty"

    assert verify_error("{type: standard_webhooks_asym, public_key: nope}") =~
             "sources.x.verify.public_key: not a public key for the scheme's algorithm"

    assert verify_error(~s({type: standard_webhooks_asym, public_key: ["#{@asym_pub}", nope]})) =~
             "sources.x.verify.public_key[1]: not a public key for the scheme's algorithm"

    assert verify_error(~s({type: standard_webhooks_asym, public_key: "#{@asym_pub}", secret: k})) ==
             ~s(sources.x.verify: unknown key "secret")

    assert verify_error(
             ~s({type: standard_webhooks_asym, public_key: "#{@asym_pub}", tolerance_seconds: 3601})
           ) ==
             "sources.x.verify.tolerance_seconds: must be between 0 and 3600 seconds, got 3601"
  end

  test "type asymmetric builds an inline scheme, and checks each key against its algorithm" do
    assert {Ankusa.Verifier.Asymmetric, opts} =
             verifier_of(
               ~s({type: asymmetric, public_key: "#{@asym_pub}", signature_header: "X-Sig", ) <>
                 ~s(sig_prefix: "ed25519=", encoding: hex, timestamp_header: "X-Ts", ) <>
                 ~s(signed: "{ts}.{body}", tolerance_seconds: 120})
             )

    assert opts[:public_key] == @asym_pub
    assert opts[:tolerance] == 120

    assert opts[:scheme] == %Ankusa.Verifier.Asymmetric.Scheme{
             signature_header: "X-Sig",
             parse: :whole,
             sig_prefix: "ed25519=",
             signed: "{ts}.{body}",
             algorithm: :ed25519,
             encoding: :hex,
             timestamp: {:header, "X-Ts"}
           }

    rsa = :public_key.generate_key({:rsa, 2048, 65_537})
    {:RSAPrivateKey, _, n, e, _, _, _, _, _, _, _} = rsa

    pem =
      [:public_key.pem_entry_encode(:SubjectPublicKeyInfo, {:RSAPublicKey, n, e})]
      |> :public_key.pem_encode()
      |> JSON.encode!()

    assert {Ankusa.Verifier.Asymmetric, rsa_opts} =
             verifier_of(
               "{type: asymmetric, algorithm: rsa_sha256, signature_header: X-Sig, public_key: #{pem}}"
             )

    assert rsa_opts[:scheme].algorithm == :rsa_sha256
    assert rsa_opts[:scheme].encoding == :base64

    # The default algorithm is ed25519: an RSA key under it is named, not loaded.
    assert verify_error("{type: asymmetric, signature_header: X-Sig, public_key: #{pem}}") =~
             "sources.x.verify.public_key: not a public key for the scheme's algorithm"

    assert verify_error(
             ~s({type: asymmetric, signature_header: X-Sig, public_key: "#{@asym_pub}", algorithm: dsa})
           ) =~
             ~s(sources.x.verify.algorithm: unknown value "dsa")

    assert verify_error(~s({type: asymmetric, public_key: "#{@asym_pub}"})) ==
             ~s(sources.x.verify: missing required key "signature_header")
  end

  test "type token: bearer by default, basic, a header or a query parameter, each secret named when wrong" do
    assert verifier_of("{type: token, secret: tok}") ==
             {Ankusa.Verifier.Token, [secret: "tok", location: :bearer]}

    assert verifier_of(~s({type: token, secret: "${TOKEN}"}), %{"TOKEN" => "from-env"}) ==
             {Ankusa.Verifier.Token, [secret: "from-env", location: :bearer]}

    assert verifier_of("{type: token, secret: [new, old]}") ==
             {Ankusa.Verifier.Token, [secret: ["new", "old"], location: :bearer]}

    assert verifier_of("{type: token, secret: \"svc:pw\", location: basic}") ==
             {Ankusa.Verifier.Token, [secret: "svc:pw", location: :basic]}

    assert verifier_of("{type: token, secret: tok, location: header, header: X-Hook}") ==
             {Ankusa.Verifier.Token, [secret: "tok", location: {:header, "X-Hook"}]}

    assert verifier_of("{type: token, secret: tok, location: query, param: token}") ==
             {Ankusa.Verifier.Token, [secret: "tok", location: {:query, "token"}]}

    assert verify_error("{type: token}") == ~s(sources.x.verify: missing required key "secret")

    assert verify_error(~s({type: token, secret: ""})) ==
             "sources.x.verify.secret: must not be empty"

    assert verify_error("{type: token, secret: [ok, \"\"]}") ==
             "sources.x.verify.secret[1]: must not be empty"

    assert verify_error("{type: token, secret: nocolon, location: basic}") ==
             "sources.x.verify.secret: a basic secret must be user:password"

    assert verify_error("{type: token, secret: tok, location: header}") ==
             ~s(sources.x.verify: missing required key "header")

    assert verify_error("{type: token, secret: tok, location: query}") ==
             ~s(sources.x.verify: missing required key "param")

    assert verify_error(~s({type: token, secret: tok, location: header, header: ""})) ==
             "sources.x.verify.header: must not be empty"

    assert verify_error("{type: token, secret: tok, header: X-Hook}") ==
             ~s(sources.x.verify: unknown key "header" for location "bearer")

    assert verify_error("{type: token, secret: tok, location: header, header: X, param: p}") ==
             ~s(sources.x.verify: unknown key "param" for location "header")

    assert verify_error("{type: token, secret: tok, location: cookie}") ==
             ~s(sources.x.verify.location: unknown value "cookie"; expected one of bearer, basic, header, query)

    assert verify_error("{type: token, secret: tok, tolerance_seconds: 5}") ==
             ~s(sources.x.verify: unknown key "tolerance_seconds")
  end

  describe "type client_cert" do
    @mtls "listener: {tls: {certfile: #{@tls_dir}/server.pem, keyfile: #{@tls_dir}/server-key.pem, cacertfile: #{@tls_dir}/ca.pem, verify: verify_peer}}"

    test "pins fingerprints (normalised) and subjects, on a listener that asks for a certificate" do
      colons = String.duplicate("AB:", 31) <> "AB"
      normal = String.duplicate("ab", 32)

      assert verifier_of("{type: client_cert, fingerprints: [\"#{colons}\"]}", %{}, @mtls) ==
               {Ankusa.Verifier.ClientCert, [fingerprints: [normal]]}

      assert verifier_of("{type: client_cert, subjects: \"dns:a.example, cn:b\"}", %{}, @mtls) ==
               {Ankusa.Verifier.ClientCert, [subjects: ["dns:a.example", "cn:b"]]}

      assert verifier_of(
               "{type: client_cert, fingerprints: [#{normal}], subjects: [\"cn:b\"]}",
               %{},
               @mtls
             ) ==
               {Ankusa.Verifier.ClientCert, [fingerprints: [normal], subjects: ["cn:b"]]}
    end

    test "names a missing or malformed pin" do
      assert verify_error("{type: client_cert}", %{}, @mtls) ==
               ~s(sources.x.verify: client_cert needs "fingerprints" or "subjects")

      assert verify_error("{type: client_cert, fingerprints: [abc]}", %{}, @mtls) =~
               "sources.x.verify.fingerprints[0]: expected a SHA-256 fingerprint"

      assert verify_error("{type: client_cert, subjects: [\"cn:a\", bob]}", %{}, @mtls) =~
               "sources.x.verify.subjects[1]: expected dns:<name>, email:<address>"

      assert verify_error("{type: client_cert, subjects: [\"cn:a\"], secret: k}", %{}, @mtls) ==
               ~s(sources.x.verify: unknown key "secret")
    end

    test "refuses to load on a listener that never asks for a client certificate" do
      plain = verify_error("{type: client_cert, subjects: [\"cn:a\"]}")

      assert plain =~
               "source x: client_cert verification needs listener.tls with verify: verify_peer"

      tls =
        "listener: {tls: {certfile: #{@tls_dir}/server.pem, keyfile: #{@tls_dir}/server-key.pem}}"

      assert verify_error("{type: client_cert, subjects: [\"cn:a\"]}", %{}, tls) =~
               "client_cert verification needs listener.tls with verify: verify_peer"
    end
  end

  test "/v1/config shows where a credential travels and which keys are pinned, never the credential or a public key" do
    mtls =
      "listener: {tls: {certfile: #{@tls_dir}/server.pem, keyfile: #{@tls_dir}/server-key.pem, cacertfile: #{@tls_dir}/ca.pem, verify: verify_peer}}"

    path =
      tmp_config("""
      #{mtls}
      sources:
        bearer: {verify: {type: token, secret: "tok-canary-1"}, sinks: [{type: log}]}
        header: {verify: {type: token, secret: "tok-canary-2", location: header, header: X-Hook}, sinks: [{type: log}]}
        query: {verify: {type: token, secret: "tok-canary-3", location: query, param: token}, sinks: [{type: log}]}
        asym: {verify: {type: standard_webhooks_asym, public_key: "#{@asym_pub}"}, sinks: [{type: log}]}
        cert: {verify: {type: client_cert, fingerprints: ["#{String.duplicate("ab", 32)}"], subjects: ["cn:ops"]}, sinks: [{type: log}]}
      """)

    printed = print_config(path)

    for canary <- ["tok-canary-1", "tok-canary-2", "tok-canary-3", @asym_pub] do
      refute printed =~ canary
    end

    sources = JSON.decode!(printed)["source_store"]["opts"]["sources"]
    opts = fn id -> sources[id]["verifier"]["opts"] end

    assert opts.("bearer") == %{"secret" => "[REDACTED]", "location" => "bearer"}
    assert opts.("header")["location"] == ~s({:header, "X-Hook"})
    assert opts.("header")["secret"] == "[REDACTED]"
    assert opts.("query")["location"] == ~s({:query, "token"})
    assert opts.("asym")["public_key"] == "[REDACTED]"

    assert opts.("cert") == %{
             "fingerprints" => [String.duplicate("ab", 32)],
             "subjects" => ["cn:ops"]
           }
  end

  test "routing tenant_path uses the tenant resolver" do
    path =
      tmp_config("""
      http: {routing: tenant_path, prefix: /hooks}
      rate_limits: {default: {rate: 50, burst: 100}}
      sources: {demo: {verify: {type: none}, trust_url_tenant: true, sinks: [{type: log}]}}
      """)

    assert Config.load!(path: path, env: %{}).config.route_resolver ==
             {Ankusa.RouteResolver.TenantPath, [prefix: ["hooks"]]}
  end

  test "routing tenant_path needs a finite rate_limits.default: the URL names the tenant" do
    path =
      tmp_config("""
      http: {routing: tenant_path}
      rate_limits: {tenants: {acme: {rate: 5, burst: 5}}}
      sources: {demo: {verify: {type: none}, trust_url_tenant: true, sinks: [{type: log}]}}
      """)

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ "rate_limits.default is required with routing: tenant_path"
  end

  test "routing tenant_path refuses an unverified shared source unless it trusts the URL's tenant" do
    open =
      tmp_config("""
      http: {routing: tenant_path}
      rate_limits: {default: {rate: 50, burst: 100}}
      sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
      """)

    assert_raise ConfigError,
                 ~r/source demo is shared \(tenant_id "default"\) and has no verifier/,
                 fn -> Config.load!(path: open, env: %{}) end

    trusted =
      tmp_config("""
      http: {routing: tenant_path}
      rate_limits: {default: {rate: 50, burst: 100}}
      sources: {demo: {verify: {type: none}, trust_url_tenant: true, sinks: [{type: log}]}}
      """)

    config = Config.load!(path: trusted, env: %{}).config
    assert source_from(config, "demo").trust_url_tenant == true
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

  test "source_store: redis carries the URL, namespace, tick, seeds and decoder" do
    path =
      tmp_config("""
      source_store: {type: redis, url: "redis://cache:6379", namespace: "ankusa:sources", tick_ms: 5000}
      sources: {demo: {verify: {type: none}, sinks: [{type: log}]}}
      """)

    config = Config.load!(path: path, env: %{}).config

    assert {Ankusa.SourceStore.Redis, opts} = config.source_store
    assert opts[:url] == "redis://cache:6379"
    assert opts[:namespace] == "ankusa:sources"
    assert opts[:tick_ms] == 5000
    assert Map.keys(opts[:sources]) == ["demo"]
    assert is_function(opts[:decoder], 2)
  end

  test "source_store: a URL alone, from the file or ANKUSA_SOURCE_STORE_URL, means redis" do
    path = tmp_config("source_store: {url: \"redis://cache:6379\"}")
    assert {Ankusa.SourceStore.Redis, _} = Config.load!(path: path, env: %{}).config.source_store

    path = tmp_config("sources: {}")
    env = %{"ANKUSA_SOURCE_STORE_URL" => "redis://from-env:6379"}

    assert {Ankusa.SourceStore.Redis, opts} =
             Config.load!(path: path, env: env).config.source_store

    assert opts[:url] == "redis://from-env:6379"
  end

  test "source_store: Redis keys on another type are an error, not dropped" do
    path = tmp_config("source_store: {type: persistent, url: \"redis://cache:6379\"}")
    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ ~s(source_store: "url" is only valid with type: redis)

    path = tmp_config("source_store: {type: static}")
    env = %{"ANKUSA_SOURCE_STORE_URL" => "redis://from-env:6379"}
    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: env) end
    assert error.message =~ ~s(source_store: "url" is only valid with type: redis)

    path = tmp_config("source_store: {type: redis}")
    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ ~s(source_store: missing required key "url")
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
        "source_store: {type: postgres}\nsources: {demo: {verify: {type: none}, sinks: [{type: log}]}}"
      )

    error = assert_raise ConfigError, fn -> Config.load!(path: path, env: %{}) end
    assert error.message =~ "source_store.type"
    assert error.message =~ ~s(unknown value "postgres")
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
            - {type: http, url: "https://sink.invalid", allow_unsigned: true, headers: {authorization: "Bearer leak"}}
      """)

    refute print_config(path) =~ "Bearer leak"
  end

  # ── egress ──────────────────────────────────────────────────────────────────

  describe "egress" do
    test "defaults: https only, every private range denied" do
      config = Config.load!(path: tmp_config("sources: {}\n"), env: %{}).config

      assert config.egress == Ankusa.HttpClient.Egress.default_policy()
      assert config.egress.allow_insecure_http == false
      assert "169.254.0.0/16" in config.egress.deny_cidrs
      assert config.egress.deny_hosts == ["metadata.google.internal"]
    end

    test "the section is read key by key over the defaults" do
      path =
        tmp_config("""
        egress:
          allow_insecure_http: true
          allow_cidrs: ["10.20.0.0/16", "fd00:1::/32"]
          allow_hosts: worker, jobs.internal
          deny_hosts: []
        """)

      egress = Config.load!(path: path, env: %{}).config.egress

      assert egress.allow_insecure_http
      assert egress.allow_cidrs == ["10.20.0.0/16", "fd00:1::/32"]
      assert egress.allow_hosts == ["worker", "jobs.internal"]
      assert egress.deny_hosts == []
      assert egress.deny_cidrs == Ankusa.HttpClient.Egress.default_policy().deny_cidrs
    end

    test "a bad value names its key" do
      assert_raise ConfigError, ~r/egress.allow_cidrs: invalid CIDR "10.0.0.0\/99"/, fn ->
        Config.load!(path: tmp_config("egress: {allow_cidrs: [\"10.0.0.0/99\"]}\n"), env: %{})
      end

      assert_raise ConfigError, ~r/egress.allow_insecure_http/, fn ->
        Config.load!(path: tmp_config("egress: {allow_insecure_http: \"yes\"}\n"), env: %{})
      end

      assert_raise ConfigError, ~r/egress.*allow_everything/, fn ->
        Config.load!(path: tmp_config("egress: {allow_everything: true}\n"), env: %{})
      end
    end

    test "an http sink must be https, unless egress.allow_insecure_http" do
      yaml = """
      sources:
        a:
          verify: {type: none}
          sinks: [{type: http, url: "http://hooks.example/h?token=sekrit", allow_unsigned: true}]
      """

      error =
        assert_raise ConfigError, ~r/sources.a.sinks\[0\].url: must be an https:\/\/ URL/, fn ->
          Config.load!(path: tmp_config(yaml), env: %{})
        end

      refute error.message =~ "sekrit", "the URL may hold credentials"

      allowed = tmp_config("egress: {allow_insecure_http: true}\n" <> yaml)
      assert %Ankusa.Config{} = Config.load!(path: allowed, env: %{}).config
    end

    test "a lifecycle http sink is held to the same rule" do
      yaml = """
      lifecycle:
        sinks: [{type: http, url: "http://events.example/", allow_unsigned: true}]
      """

      assert_raise ConfigError, ~r/lifecycle.sinks\[0\].url: must be an https:\/\/ URL/, fn ->
        Config.load!(path: tmp_config(yaml), env: %{})
      end

      assert %Ankusa.Config{} =
               Config.load!(
                 path: tmp_config("egress: {allow_insecure_http: true}\n" <> yaml),
                 env: %{}
               ).config
    end

    test "a sink URL that is not an absolute http(s) URL is refused" do
      for url <- ["ftp://hooks.example/", "hooks.example/path", "/just/a/path"] do
        yaml =
          "sources: {a: {verify: {type: none}, sinks: [{type: http, allow_unsigned: true, url: \"#{url}\"}]}}\n"

        assert_raise ConfigError,
                     ~r/sources.a.sinks\[0\].url: must be an? (absolute )?http\(s\) URL/,
                     fn ->
                       Config.load!(path: tmp_config(yaml), env: %{})
                     end
      end
    end

    test "API-submitted sources are held to the configured policy when written" do
      spec = %{
        "verify" => %{"type" => "none"},
        "sinks" => [
          %{"type" => "http", "url" => "http://hooks.example/h", "allow_unsigned" => true}
        ]
      }

      assert_raise ConfigError,
                   ~r/sources.acme.new.sinks\[0\].url: must be an https:\/\/ URL/,
                   fn ->
                     Config.source_from_map!("acme.new", spec)
                   end

      strict = "source_store: {type: persistent}\n"
      loose = strict <> "egress: {allow_insecure_http: true}\n"

      {Ankusa.SourceStore.Persistent, opts} =
        Config.load!(path: tmp_config(strict), env: %{}).config.source_store

      assert_raise ConfigError, ~r/must be an https:\/\/ URL/, fn ->
        opts[:decoder].("acme.new", spec)
      end

      {Ankusa.SourceStore.Persistent, opts} =
        Config.load!(path: tmp_config(loose), env: %{}).config.source_store

      assert [sinks: [{Ankusa.Sink.Http, [url: "http://hooks.example/h", allow_unsigned: true]}]] =
               Keyword.take(opts[:decoder].("acme.new", spec), [:sinks])
    end

    test "an HTTP sink is signed by default: no secret and no allow_unsigned fails the load" do
      yaml = fn sink ->
        "sources: {a: {verify: {type: none}, sinks: [{type: http, url: \"https://h.example/\"#{sink}}]}}\n"
      end

      assert_raise ConfigError,
                   ~r/sources.a.sinks\[0\]: Sink.Http requires secret.*allow_unsigned: true/,
                   fn -> Config.load!(path: tmp_config(yaml.("")), env: %{}) end

      assert_raise ConfigError, ~r/sources.a.sinks\[0\]: Sink.Http requires secret/, fn ->
        Config.load!(path: tmp_config(yaml.(", allow_unsigned: false")), env: %{})
      end

      assert_raise ConfigError, ~r/sources.a.sinks\[0\].allow_unsigned/, fn ->
        Config.load!(path: tmp_config(yaml.(", allow_unsigned: \"maybe\"")), env: %{})
      end

      for ok <- [", secret: whsec_YWJj", ", allow_unsigned: true"] do
        assert %Ankusa.Config{} = Config.load!(path: tmp_config(yaml.(ok)), env: %{}).config
      end

      lifecycle = "lifecycle: {sinks: [{type: http, url: \"https://events.example/\"}]}\n"

      assert_raise ConfigError, ~r/lifecycle.sinks\[0\]: Sink.Http requires secret/, fn ->
        Config.load!(path: tmp_config(lifecycle), env: %{})
      end
    end

    test "an API-submitted source with an unsigned HTTP sink is refused when written" do
      spec = %{
        "verify" => %{"type" => "none"},
        "sinks" => [%{"type" => "http", "url" => "https://hooks.example/h"}]
      }

      assert_raise ConfigError, ~r/sources.acme.new.sinks\[0\]: Sink.Http requires secret/, fn ->
        Config.source_from_map!("acme.new", spec)
      end

      signed = put_in(spec, ["sinks", Access.at(0), "secret"], "whsec_YWJj")

      assert [sinks: [{Ankusa.Sink.Http, [url: _, secret: "whsec_YWJj"]}]] =
               Keyword.take(Config.source_from_map!("acme.new", signed), [:sinks])
    end

    test "per-sink TLS keys reach the sink; verification is on unless the sink says otherwise" do
      ca = tmp_config("not a real bundle, but a file\n")

      path =
        tmp_config("""
        sources:
          a:
            verify: {type: none}
            sinks:
              - {type: http, url: "https://private-ca.example/h", cacertfile: "#{ca}", allow_unsigned: true}
              - {type: http, url: "https://self-signed.example/h", allow_insecure_tls: true, allow_unsigned: true}
              - {type: http, url: "https://default.example/h", allow_unsigned: true}
        """)

      source = source_from(Config.load!(path: path, env: %{}).config, "a")

      assert [
               {Ankusa.Sink.Http, private},
               {Ankusa.Sink.Http, insecure},
               {Ankusa.Sink.Http, default}
             ] = source.sinks

      assert private[:cacertfile] == ca
      assert insecure[:allow_insecure_tls] == true
      refute Keyword.has_key?(default, :allow_insecure_tls)
      refute Keyword.has_key?(default, :cacertfile)
    end

    test "a cacertfile that is not a file, or a non-boolean allow_insecure_tls, fails the load" do
      sink = fn extra ->
        tmp_config("""
        sources:
          a:
            verify: {type: none}
            sinks: [{type: http, url: "https://h.example/", allow_unsigned: true, #{extra}}]
        """)
      end

      assert_raise ConfigError,
                   ~r/sources.a.sinks\[0\]: Sink.Http cacertfile "\/nope\/ca.pem" is not a readable file/,
                   fn ->
                     Config.load!(path: sink.("cacertfile: /nope/ca.pem"), env: %{})
                   end

      assert_raise ConfigError, ~r/sources.a.sinks\[0\].allow_insecure_tls/, fn ->
        Config.load!(path: sink.("allow_insecure_tls: \"yes\""), env: %{})
      end
    end

    test "the printed config shows the policy, and a sink's TLS opt-out but not its CA path" do
      ca = tmp_config("a file\n")

      path =
        tmp_config("""
        egress: {allow_cidrs: ["10.20.0.0/16"]}
        sources:
          a:
            verify: {type: none}
            sinks: [{type: http, url: "https://h.example/", cacertfile: "#{ca}", allow_insecure_tls: true, allow_unsigned: true}]
        """)

      printed = print_config(path)

      assert printed =~ "10.20.0.0/16"
      assert printed =~ "metadata.google.internal"
      assert printed =~ ~s("allow_insecure_tls":true)
      refute printed =~ ca
    end
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
