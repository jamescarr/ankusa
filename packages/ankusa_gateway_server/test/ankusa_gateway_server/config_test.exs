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

  test "a full image's file is refused by its section, before its unset variables" do
    yaml = """
    config_version: 2
    sources:
      stripe: {verify: {type: hmac, secret: "${STRIPE_WHSEC}"}}
    """

    assert error!(yaml) ==
             "sources: not available in the gateway image (jamescarr/ankusa-gateway); use jamescarr/ankusa"
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

  # ── log / tracing ───────────────────────────────────────────────────────────

  test "log.level and log.format load, and a level that is not one is named" do
    assert load!("log: {level: warning, format: json}\n").log == %{level: :warning, format: :json}
    assert load!("log: {}\n").log == %{level: :info, format: :text}

    assert error!("log: {level: trace}\n") ==
             ~s(log.level: unknown value "trace"; expected one of debug, info, warning, error)

    assert error!("log: {format: xml}\n") ==
             ~s(log.format: unknown value "xml"; expected one of text, json)
  end

  test "an https OTLP endpoint and a ratio load; a ratio may arrive as a string" do
    loaded =
      load!("""
      tracing: {enabled: true, otlp_endpoint: "https://collector.example:4318/otlp", sample_ratio: 0.05}
      """)

    assert loaded.tracing == %{
             enabled: true,
             otlp_endpoint: "https://collector.example:4318/otlp",
             sample_ratio: 0.05
           }

    assert load!("tracing: {sample_ratio: \"0.25\"}\n").tracing == %{
             enabled: false,
             otlp_endpoint: "http://localhost:4318",
             sample_ratio: 0.25
           }
  end

  test "a tracing endpoint that is not an http(s) URL with a host, or carries credentials, is refused" do
    for endpoint <- [
          "collector:4318",
          "https://user:pw@collector.example",
          "ftp://collector.example/otlp",
          "https:///otlp"
        ] do
      assert error!("tracing: {otlp_endpoint: \"#{endpoint}\"}\n") =~
               "tracing.otlp_endpoint: expected an http:// or https:// URL with a host and " <>
                 "no user:password"
    end
  end

  test "a ratio outside 0..1, or not a number, is named" do
    assert error!("tracing: {sample_ratio: 2}\n") ==
             "tracing.sample_ratio: expected a number from 0 to 1, got 2"

    assert error!("tracing: {sample_ratio: [1]}\n") ==
             "tracing.sample_ratio: expected a number, got [1]"

    assert error!("tracing: {sample_ratio: one}\n") ==
             ~s(tracing.sample_ratio: expected a number, got "one")
  end

  # ── listener, metrics, TLS ──────────────────────────────────────────────────

  test "the listener's TLS block lands, with versions, ciphers and verify_peer" do
    loaded =
      load!("""
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
        drain_delay_ms: 1
        drain_timeout_ms: 2
        tls:
          certfile: #{@tls_dir}/server.pem
          keyfile: #{@tls_dir}/server-key.pem
          cacertfile: #{@tls_dir}/ca.pem
          verify: verify_peer
          versions: [tlsv1.3, "tlsv1.2"]
          ciphers: [TLS_AES_256_GCM_SHA384]
      """)

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
             drain_delay_ms: 1,
             drain_timeout_ms: 2,
             tls: %{
               verify: :verify_peer,
               versions: [:"tlsv1.3", :"tlsv1.2"],
               ciphers: ["TLS_AES_256_GCM_SHA384"],
               cacertfile: cacertfile
             }
           } = loaded.config.listener

    assert cacertfile == Path.join(@tls_dir, "ca.pem")
  end

  test "metrics land, with a TLS block of their own and the gateway's series in the scrape" do
    loaded =
      load!("""
      metrics:
        enabled: true
        port: 9100
        ip: 0.0.0.0
        detail: source
        max_series_per_metric: 100
        tls: {certfile: #{@tls_dir}/server.pem, keyfile: #{@tls_dir}/server-key.pem}
      """)

    assert %{
             enabled: true,
             port: 9100,
             ip: "0.0.0.0",
             detail: :source,
             max_series_per_metric: 100,
             tls: %{verify: :verify_none, versions: [:"tlsv1.2", :"tlsv1.3"]},
             extra: [Ankusa.Gateway.Metrics]
           } = loaded.config.metrics
  end

  test "a bad listener, metrics or TLS key fails the load naming it" do
    tls = "certfile: #{@tls_dir}/server.pem, keyfile: #{@tls_dir}/server-key.pem"

    for {yaml, message} <- [
          {"listener: [x]", ~s(listener: expected a mapping, got ["x"])},
          {"listener: {timeout: 1}", ~s(listener: unknown key "timeout")},
          {"listener: {tls: {#{tls}, verify: maybe}}",
           ~s(listener.tls.verify: unknown value "maybe")},
          {"listener: {tls: {#{tls}, versions: [ssl3]}}",
           ~s(listener.tls.versions: unknown value "ssl3")},
          {"listener: {tls: {#{tls}, ciphers: 5}}",
           "listener.tls.ciphers: expected a list or a comma-separated string"},
          {"listener: {read_timeout_ms: fast}", "listener.read_timeout_ms: expected an integer"},
          {"listener: {http2: sure}", "listener.http2: expected a boolean"},
          {"metrics: {port: most}", "metrics.port: expected an integer"},
          {"metrics: {enabled: \"yes\"}", ~s(metrics.enabled: expected a boolean, got "yes")},
          {"metrics: {enabled: 1}", "metrics.enabled: expected a boolean, got 1"},
          {"metrics: {detail: everything}", ~s(metrics.detail: unknown value "everything")},
          {"metrics: {tls: {verify: verify_peer}}",
           "metrics.tls.certfile is required when metrics.tls is set"}
        ] do
      assert error!(yaml <> "\n") =~ message, yaml
    end
  end

  # ── node ────────────────────────────────────────────────────────────────────

  test "node carries the data dir and the region, and nothing else" do
    loaded = load!("node: {data_dir: /tmp/gw-node, region: eu-west-1}\n")

    assert loaded.config.data_dir == "/tmp/gw-node"
    assert loaded.config.dispatch.region == "eu-west-1"

    assert error!("node: {data_dir: 1}\n") == "node.data_dir: expected a string, got 1"
    assert error!("node: {roles: []}\n") =~ "node.roles: must not be empty"
    assert error!("node: {zone: a}\n") == ~s(node: unknown key "zone")
  end

  # ── storage and claim stores ────────────────────────────────────────────────

  test "an s3 store takes its credentials, its sse and its transport options" do
    loaded =
      load!("""
      storage:
        type: s3
        s3:
          bucket: segs
          region: us-east-1
          endpoint: https://s3.example
          access_key_id: AKIDEXAMPLE
          secret_access_key: secret-canary
          session_token: session-canary
          sse: {type: kms, key_id: "arn:aws:kms:us-east-1:1:key/a"}
          op_deadline_ms: 90000
          upload_timeout_ms: 45000
          multipart_threshold_bytes: 16777216
          pool_size: 64
          breaker_failures: 3
      """)

    assert {Ankusa.BlobStore.S3, s3} = loaded.config.storage.blob_store

    assert %{
             bucket: "segs",
             region: "us-east-1",
             endpoint: "https://s3.example",
             access_key_id: "AKIDEXAMPLE",
             secret_access_key: "secret-canary",
             session_token: "session-canary",
             op_deadline_ms: 90_000,
             upload_timeout_ms: 45_000,
             multipart_threshold_bytes: 16_777_216,
             pool_size: 64,
             breaker_failures: 3
           } = Map.new(s3)

    assert s3[:sse] == %{type: :kms, key_id: "arn:aws:kms:us-east-1:1:key/a"}

    # print-config's view hides the credentials.
    refute JSON.encode!(AnkusaGatewayServer.CLI.config_view(loaded)) =~ "secret-canary"
  end

  test "a gcs store takes a metadata or static token and sse; auth is checked" do
    for {yaml, expectation} <- [
          {"gcs: {bucket: g}", &match?({AnkusaGatewayServer.GcpToken, :metadata, []}, &1)},
          {"gcs: {bucket: g, auth: token, token: t0k}",
           &match?({AnkusaGatewayServer.GcpToken, :static, ["t0k"]}, &1)},
          {"gcs: {bucket: g, auth: none, sse: {type: aes256}}", &(&1 == nil)}
        ] do
      loaded = load!("storage: {type: gcs, #{yaml}}\n")

      assert {Ankusa.BlobStore.GCS, gcs} = loaded.config.storage.blob_store
      assert gcs[:bucket] == "g"
      assert expectation.(gcs[:token_provider])
    end

    loaded = load!("storage: {type: gcs, gcs: {bucket: g, auth: none, sse: {type: aes256}}}\n")
    assert {Ankusa.BlobStore.GCS, gcs} = loaded.config.storage.blob_store
    assert gcs[:sse] == %{type: :aes256}

    assert error!("storage: {type: gcs, gcs: {bucket: g, auth: oauth}}\n") ==
             ~s(storage.gcs.auth: unknown value "oauth"; expected one of metadata, token, none)

    assert error!("storage: {type: gcs, gcs: {bucket: g, auth: token}}\n") ==
             ~s(storage.gcs: missing required key "token")
  end

  test "a claim store may carry a local root; stores is a failover list; store and stores are exclusive" do
    loaded = load!("claim_check: {store: {type: local, root: /var/lib/ankusa/claims}}\n")

    assert [{Ankusa.BlobStore.LocalFS, [root: "/var/lib/ankusa/claims"]}] =
             loaded.config.claim_check.blob_stores

    loaded =
      load!("""
      claim_check:
        stores:
          - {type: s3, s3: {bucket: c1, region: us-east-1, sse: {type: aes256}}}
          - {type: local}
      """)

    assert [{Ankusa.BlobStore.S3, s3}, {Ankusa.BlobStore.LocalFS, []}] =
             loaded.config.claim_check.blob_stores

    assert s3[:sse] == %{type: :aes256}

    assert error!("claim_check: {stores: []}\n") == "claim_check.stores: must not be empty"

    assert error!("claim_check: {stores: 5}\n") ==
             "claim_check.stores: expected a list of stores, got 5"

    assert error!("claim_check: {store: {type: local}, stores: [{type: local}]}\n") ==
             "claim_check: store and stores are exclusive; list every store under stores"

    assert error!("claim_check: {stores: [5]}\n") ==
             "claim_check.stores[0]: expected a mapping, got 5"
  end

  test "a store's sse and required keys are named" do
    for {yaml, message} <- [
          {"storage: {type: s3, s3: {region: r}}", ~s(storage.s3: missing required key "bucket")},
          {"storage: {type: s3, s3: {bucket: b, region: r, sse: {type: rot13}}}",
           ~s(storage.s3.sse.type: unknown value "rot13"; expected one of aes256, kms)},
          {"storage: {type: s3, s3: {bucket: b, region: r, sse: {key_id: k}}}",
           ~s(storage.s3.sse: missing required key "type")},
          {"storage: {type: s3, s3: {bucket: b, region: r, sse: {type: kms, kms: k}}}",
           ~s(storage.s3.sse: unknown key "kms")},
          {"storage: {type: s3, s3: {bucket: b, region: r, sse: kms}}",
           "storage.s3.sse: expected a mapping"},
          {"storage: {type: azure}", ~s(storage.type: unknown value "azure")},
          {"claim_check: {store: {type: gcs, gcs: {bucket: b, auth: token}}}",
           ~s(claim_check.store.gcs: missing required key "token")}
        ] do
      assert error!(yaml <> "\n") =~ message, yaml
    end
  end

  # ── claim_check ─────────────────────────────────────────────────────────────

  test "claim_check's keys land in core's config" do
    loaded =
      load!("""
      claim_check:
        retention_days: 3
        pack_max_bytes: 1024
        redeem_grace_ms: 1000
        sweep_max_objects: 10
        bucket_expiry_days: 5
        placement: hash
        key_layout: sharded
        store_cooldown_ms: 250
        read_deadline_ms: 2500
        read_timeout_ms: 500
        read_retries: 2
        read_max_inflight: 8
        read_queue_max: 16
        read_cache_bytes: 0
        read_cache_max_entry_bytes: 4096
        ref_signing_keys: "#{String.duplicate("a", 16)}, #{String.duplicate("b", 16)}"
        ref_ttl_ms: 3600000
        accept_unsigned_refs: true
      """)

    assert %{
             retention_days: 3,
             pack_max_bytes: 1024,
             redeem_grace_ms: 1000,
             sweep_max_objects: 10,
             bucket_expiry_days: 5,
             placement: :hash,
             key_layout: :sharded,
             store_cooldown_ms: 250,
             read_deadline_ms: 2500,
             read_timeout_ms: 500,
             read_retries: 2,
             read_max_inflight: 8,
             read_queue_max: 16,
             read_cache_bytes: 0,
             read_cache_max_entry_bytes: 4096,
             ref_ttl_ms: 3_600_000,
             accept_unsigned_refs: true
           } = loaded.config.claim_check

    assert loaded.config.claim_check.ref_signing_keys == [
             String.duplicate("a", 16),
             String.duplicate("b", 16)
           ]
  end

  test "an empty signing key list means no signing; claim_check names a bad key" do
    assert load!("claim_check: {ref_signing_keys: \"\"}\n").config.claim_check.ref_signing_keys ==
             []

    for {yaml, message} <- [
          {"claim_check: {placement: random}", ~s(claim_check.placement: unknown value "random")},
          {"claim_check: {key_layout: nested}",
           ~s(claim_check.key_layout: unknown value "nested")},
          {"claim_check: {read_timeout_ms: 0}",
           "claim_check.read_timeout_ms must be a positive integer, got 0"},
          {"claim_check: {nope: 1}", ~s(claim_check: unknown key "nope")}
        ] do
      assert error!(yaml <> "\n") =~ message, yaml
    end
  end

  # ── egress ──────────────────────────────────────────────────────────────────

  test "the egress policy and its pools land in core's config" do
    loaded =
      load!("""
      egress:
        allow_insecure_http: true
        allow_cidrs: ["10.20.0.0/16", "fd00:1::/32"]
        allow_hosts: [worker, jobs.internal]
        deny_hosts: []
        deny_cidrs: ["169.254.0.0/16"]
        pools:
          pool_size: 8
          pool_count: 2
          pool_timeout: 1000
          pool_max_idle_ms: 60000
          conn_max_idle_ms: 30000
          max_pools: 16
          max_pools_per_tenant: 4
          protocols: [http1, "http2"]
      """)

    assert %{
             allow_insecure_http: true,
             allow_cidrs: ["10.20.0.0/16", "fd00:1::/32"],
             allow_hosts: ["worker", "jobs.internal"],
             deny_hosts: [],
             deny_cidrs: ["169.254.0.0/16"]
           } = loaded.config.egress

    assert %{
             pool_size: 8,
             pool_count: 2,
             pool_timeout: 1000,
             pool_max_idle_ms: 60_000,
             conn_max_idle_ms: 30_000,
             max_pools: 16,
             max_pools_per_tenant: 4,
             protocols: [:http1, :http2]
           } = loaded.config.egress.pools
  end

  test "an egress list may be emptied; a bad key, pool or protocol is named" do
    assert load!("egress: {allow_hosts: []}\n").config.egress.allow_hosts == []

    for {yaml, message} <- [
          {"egress: {nope: 1}", ~s(egress: unknown key "nope")},
          {"egress: {pools: {pool_sizes: 1}}", ~s(egress.pools: unknown key "pool_sizes")},
          {"egress: {pools: {pool_size: big}}", "egress.pools.pool_size: expected an integer"},
          {"egress: {pools: {pool_size: 0}}",
           "egress.pools.pool_size must be an integer from 1 to 1024, got 0"},
          {"egress: {pools: {protocols: [http3]}}",
           ~s(egress.pools.protocols: unknown value "http3"; expected one of http1, http2)},
          {"egress: {allow_cidrs: [\"10.0.0.0/99\"]}", "egress.allow_cidrs: invalid CIDR"}
        ] do
      assert error!(yaml <> "\n") =~ message, yaml
    end
  end

  # ── the document: versions, overrides, interpolation ────────────────────────

  test "a version this image does not read, or not a number, is refused" do
    assert error!("config_version: 3\n") =~
             "config_version: 3 is newer than this image reads (up to 2)"

    assert error!("config_version: 0\n") == "config_version: expected a positive integer, got 0"

    assert error!("config_version: \"two\"\n") ==
             ~s(config_version: expected a positive integer, got "two")
  end

  test "a version-1 gateway file migrates every listener key, the request cap and the integrity check" do
    loaded =
      load!("""
      claim_check:
        max_concurrent_reads: 7
        integrity_check: off
      """)

    assert loaded.gateway.max_concurrent_requests == 7
    assert loaded.gateway.claims.integrity_check == :off
    assert loaded.doc["config_version"] == 2
  end

  test "migration: mode none empties the chain, allow_insecure is kept, jwt and mtls carry their block" do
    loaded =
      load!("""
      claim_check:
        auth:
          mode: none
          allow_insecure: true
      """)

    assert %{allow_insecure: true, authenticators: []} = loaded.gateway.auth

    loaded =
      load!("""
      claim_check:
        auth:
          mode: jwt
          jwt: {issuer: https://login.example.com/, audience: ankusa, jwks_url: https://login.example.com/jwks.json}
      """)

    assert [%{type: :jwt, config: %{issuer: "https://login.example.com/"}}] =
             loaded.gateway.auth.authenticators
  end

  test "migration: an mtls block needs the gateway's TLS, and a templated or unknown mode is refused" do
    message =
      error!("""
      claim_check:
        auth:
          mode: mtls
          mtls:
            subject_map:
              "cn:a": [claims:read]
      """)

    assert message =~ "mtls needs gateway.tls with verify: :verify_peer"

    assert error!("""
           claim_check:
             auth:
               mode: ${AUTH_MODE}
           """) ==
             "claim_check.auth.mode: a templated mode cannot be migrated; write gateway.auth " <>
               "and set config_version: 2"

    assert error!("""
           claim_check:
             auth: {mode: basic}
           """) ==
             "claim_check.auth.mode: unknown value \"basic\"; expected one of none, token, jwt, mtls"
  end

  test "migration: a target the file already sets, or a gateway section that is not a mapping, is refused" do
    assert error!("""
           claim_check: {port: 4100}
           gateway: {port: 4101}
           """) == "claim_check.port and gateway.port are both set"

    assert error!("""
           claim_check: {port: 4100}
           gateway: [x]
           """) == ~s(gateway: expected a mapping, got ["x"])
  end

  test "a version-1 claim_check section with nothing moved is left as it is" do
    loaded = load!("claim_check: {read_cache_bytes: 0}\n")

    assert loaded.doc["claim_check"] == %{"read_cache_bytes" => 0}
    assert loaded.config.claim_check.read_cache_bytes == 0
  end

  test "an unset variable may take the file's default; a substituted value may hold ${" do
    assert load!("node: {region: \"${REGION:-eu-west-1}\"}\n").config.dispatch.region ==
             "eu-west-1"

    loaded = load!("node: {region: \"${REGION}\"}\n", %{"REGION" => "${kept}"})
    assert loaded.config.dispatch.region == "${kept}"
  end

  test "an unresolved ${…} names the dotted key and the forms that are allowed" do
    expected =
      "metrics.ip: unresolved ${…} (use ${NAME}, ${NAME:-default}, " <>
        "${file:/abs/path} or ${sm:<provider>:<ref>}; variable names are [A-Z0-9_]+; " <>
        "use ${NAME:-} for an empty default)"

    assert error!("metrics: {ip: \"${lower_case}\"}\n") == expected

    assert error!("metrics: {ip: \"${file:${DIR}/key}\"}\n", %{"DIR" => "/tmp"}) == expected
  end

  test "ANKUSA_CLAIM_CHECK_TOKEN set but empty is refused" do
    assert error!("config_version: 2\n", %{"ANKUSA_CLAIM_CHECK_TOKEN" => ""}) ==
             "ANKUSA_CLAIM_CHECK_TOKEN is set but empty"
  end

  test "the token override keeps the rest of gateway.auth as the file has it" do
    loaded =
      load!(
        """
        config_version: 2
        gateway:
          auth:
            allow_insecure: true
            authenticators:
              - type: token
                tokens: [{id: old, sha256: "#{Token.hash("old")}", scopes: [claims:read]}]
        """,
        %{"ANKUSA_CLAIM_CHECK_TOKEN" => "new-token"}
      )

    assert loaded.gateway.auth.allow_insecure

    assert [%{config: %{tokens: [%{id: "ANKUSA_CLAIM_CHECK_TOKEN"}]}}] =
             loaded.gateway.auth.authenticators
  end

  test "an env override on a section that is not a mapping is left for the walker to refuse" do
    assert error!("node: [a]\n", %{"ANKUSA_DATA_DIR" => "/tmp/x"}) ==
             ~s(node: expected a mapping, got ["a"])
  end

  test "a config file named explicitly that is not there is an error" do
    missing =
      Path.join(System.tmp_dir!(), "gateway_missing_#{System.unique_integer([:positive])}.yml")

    assert assert_raise(ConfigError, fn -> Config.load!(path: missing, env: %{}) end).message ==
             "config file #{missing} not found"

    assert assert_raise(ConfigError, fn -> Config.load!(env: %{"ANKUSA_CONFIG" => missing}) end).message ==
             "config file #{missing} not found (ANKUSA_CONFIG)"
  end

  test "a YAML error names the file; a document that is not a mapping is refused" do
    path = Path.join(System.tmp_dir!(), "gateway_yaml_#{System.unique_integer([:positive])}.yml")
    File.write!(path, "gateway: [unclosed\n")
    on_exit(fn -> File.rm(path) end)

    assert assert_raise(ConfigError, fn -> Config.load!(path: path, env: %{}) end).message =~ path

    File.write!(path, "- one\n- two\n")

    assert assert_raise(ConfigError, fn -> Config.load!(path: path, env: %{}) end).message ==
             "#{path}: expected a YAML mapping at the top level, got [\"one\", \"two\"]"
  end

  # ── gateway authenticators ──────────────────────────────────────────────────

  test "a gateway auth block may be allow_insecure alone; its shapes are checked" do
    loaded = load!("gateway: {auth: {allow_insecure: true}}\n")

    assert %{allow_insecure: true, authenticators: []} = loaded.gateway.auth

    for {yaml, message} <- [
          {"gateway: {auth: {authenticators: 5}}", "expected a list of authenticators"},
          {"gateway: {auth: {authenticators: [{type: basic}]}}",
           ~s(gateway.auth.authenticators[0].type: unknown value "basic")},
          {"gateway: {auth: {authenticators: [{type: token, tokens: 5}]}}",
           "gateway.auth.authenticators[0].tokens: expected a list of token entries"},
          {"gateway: {auth: {authenticators: [{type: token, tokens: [{id: a, token: t, scopes: 5}]}]}}",
           "gateway.auth.authenticators[0].tokens[0].scopes: expected a list or a comma-separated string"},
          {"gateway: {auth: {authenticators: [{type: mtls, subject_map: [a]}]}}",
           "gateway.auth.authenticators[0].subject_map: expected a mapping"},
          {"gateway: {claims: {integrity_check: sometimes}}",
           ~s(gateway.claims.integrity_check: unknown value "sometimes")},
          {"gateway: {nope: 1}", ~s(gateway: unknown key "nope")}
        ] do
      assert error!("config_version: 2\n" <> yaml <> "\n") =~ message, yaml
    end
  end

  test "a jwt authenticator takes a list of audiences and a list of keys" do
    jwk =
      JOSE.JWK.generate_key({:ec, "P-256"})
      |> JOSE.JWK.to_public()
      |> JOSE.JWK.to_map()
      |> elem(1)

    key = Enum.map_join(jwk, ", ", fn {k, v} -> ~s("#{k}": "#{v}") end)

    loaded =
      load!("""
      config_version: 2
      gateway:
        auth:
          authenticators:
            - type: jwt
              issuer: https://login.example.com/
              audience: [ankusa, other]
              keys: [{#{key}}]
              leeway_s: 5
              scope_claim: scp
              tenants_claim: orgs
              groups_claim: roles
              refresh_ms: 60000
              min_refresh_ms: 5000
              header: x-id-token
              subject_prefix: "idp|"
              name: idp
      """)

    assert [
             %{
               type: :jwt,
               name: "idp",
               config: %{
                 issuer: "https://login.example.com/",
                 audience: ["ankusa", "other"],
                 keys: [^jwk],
                 leeway_s: 5,
                 scope_claim: "scp",
                 tenants_claim: "orgs",
                 groups_claim: "roles",
                 refresh_ms: 60_000,
                 min_refresh_ms: 5000,
                 header: "x-id-token",
                 subject_prefix: "idp|"
               }
             }
           ] = loaded.gateway.auth.authenticators

    for {yaml, message} <- [
          {"gateway: {auth: {authenticators: [{type: jwt, issuer: i, audience: 5, jwks_url: https://x/jwks}]}}",
           "gateway.auth.authenticators[0].audience: expected a string or a list, got 5"},
          {"gateway: {auth: {authenticators: [{type: jwt, issuer: i, audience: a, keys: 5}]}}",
           "gateway.auth.authenticators[0].keys: expected a list of keys, got 5"},
          {"gateway: {auth: {authenticators: [{type: jwt, issuer: i, audience: a, keys: [5]}]}}",
           "gateway.auth.authenticators[0].keys[0]: expected a JWK mapping or a PEM string, got 5"}
        ] do
      assert error!("config_version: 2\n" <> yaml <> "\n") =~ message, yaml
    end
  end

  test "an mtls subject map takes a scope list, or a map with scopes and groups" do
    loaded =
      load!("""
      config_version: 2
      gateway:
        tls: {certfile: #{@tls_dir}/server.pem, keyfile: #{@tls_dir}/server-key.pem, cacertfile: #{@tls_dir}/ca.pem, verify: verify_peer}
        auth:
          authenticators:
            - type: mtls
              subject_map:
                "cn:a": [claims:read]
                "cn:b": {scopes: [claims:read], groups: [ops]}
      """)

    assert [%{type: :mtls, config: %{subject_map: subject_map}}] =
             loaded.gateway.auth.authenticators

    assert subject_map["cn:a"] == %{scopes: ["claims:read"], groups: []}
    assert subject_map["cn:b"] == %{scopes: ["claims:read"], groups: ["ops"]}
  end

  test "a core validation that raises ArgumentError becomes a ConfigError naming the key" do
    assert error!("claim_check: {read_timeout_ms: 0}\n") ==
             "claim_check.read_timeout_ms must be a positive integer, got 0"
  end

  describe "gateway.authz and gateway.auth.cache" do
    test "a rules authorizer, a policy over it, and the cache keys load" do
      loaded =
        load!("""
        config_version: 2
        gateway:
          auth:
            cache: {ttl_ms: 30000, negative_ttl_ms: 1000}
          authz:
            deadline_ms: 100
            cache: {allow_ttl_ms: 2000, refresh_ahead: false, serve_stale_ms: 5000}
            authorizers:
              teams:
                type: rules
                rules:
                  - effect: allow
                    actions: [read, list]
                    spaces: [docs]
                    keys: ["teams/{team}/**"]
                    when: {groups_any: ["team-{{ vars.team }}"]}
            policies:
              docs: {mode: first, steps: [teams, {use: scopes, on_unavailable: abstain}]}
        """)

      gateway = loaded.gateway
      assert gateway.auth.cache.ttl_ms == 30_000
      assert gateway.auth.cache.negative_ttl_ms == 1_000
      assert gateway.auth.cache.max_entries == 100_000
      assert gateway.authz.deadline_ms == 100

      assert %{allow_ttl_ms: 2_000, refresh_ahead: false, serve_stale_ms: 5_000} =
               gateway.authz.cache

      assert %{module: Ankusa.Gateway.Authz.Rules} = gateway.authz.authorizers["teams"]

      assert gateway.authz.policies["docs"] == %{
               mode: :first,
               steps: [
                 %{use: "teams", on_unavailable: :fail},
                 %{use: "scopes", on_unavailable: :abstain}
               ]
             }
    end

    test "every error names its key" do
      for {yaml, message} <- [
            {"gateway: {authz: {authorizers: {x: {type: opa}}}}",
             ~s[gateway.authz.authorizers.x.type: unknown type "opa" (known: rules)]},
            {"gateway: {authz: {authorizers: {x: {type: rules, rules: [{effect: allow, verbs: [read]}]}}}}",
             "gateway.authz.authorizers.x.rules[0]"},
            {"gateway: {authz: {authorizers: {x: {type: rules, rules: [{effect: maybe, actions: [read]}]}}}}",
             "gateway.authz.authorizers.x.rules[0].effect"},
            {"gateway: {authz: {authorizers: {x: {type: rules, rules: [{effect: allow, actions: [read], when: {teams_any: [a]}}]}}}}",
             "gateway.authz.authorizers.x.rules[0].when"},
            {"gateway: {authz: {policies: {p: {mode: most, steps: [scopes]}}}}",
             "gateway.authz.policies.p.mode"},
            {"gateway: {authz: {policies: {p: {steps: [nope]}}}}",
             "gateway.authz.policies.p.steps[0].use names nope"},
            {"gateway: {authz: {deadline_ms: 5}}", "gateway.authz.deadline_ms"},
            {"gateway: {authz: {cache: {deny_ttl_ms: -1}}}", "gateway.authz.cache.deny_ttl_ms"},
            {"gateway: {auth: {cache: {negative_ttl_ms: 600000}}}",
             "gateway.auth.cache.negative_ttl_ms"}
          ] do
        assert error!("config_version: 2\n" <> yaml <> "\n") =~ message, yaml
      end
    end
  end
end
