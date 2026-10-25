defmodule AnkusaServer.Config do
  @moduledoc """
  The operator's config file, turned into an `Ankusa.Config{}`.

  A pipeline, in order, so precedence is never a guessing game:

    1. **Locate.** `ANKUSA_CONFIG` (or the `:path` option), else
       `/etc/ankusa/ankusa.yml`, else `./ankusa.yml`. No file at all is legal:
       the server starts on defaults with no sources and says so.
    2. **Interpolate.** Every string value is walked and `${NAME}` /
       `${NAME:-default}` replaced from the environment. This is how secrets get
       in: the file holds `${STRIPE_WHSEC}`, not the secret. An unset variable
       with no default is a startup failure naming the dotted key, not a
       silently empty secret; `${NAME:-}` is how a file asks for empty.
       `${file:/abs/path}` and `${sm:<provider>:<ref>}` (a secret manager under
       `secrets.providers`) are resolved by `AnkusaServer.Secrets` once the
       environment overrides are in, and a source's `secret_file:` is
       `secret:` read from a file; see that module for the outage rule and
       `reload/2`.
    3. **Env overrides.** A fixed set of `ANKUSA_*` variables (below) override
       the file — env wins, because that is how containers get reconfigured
       without a new image.
    4. **Validate and translate.** A hand-written walker rejects any key not in
       the schema (`node.roles`, not `nodes.roles`), any wrong type, and any bad
       enum value, with the dotted path in the message. Then it builds the
       `Ankusa.Config` the framework runs on and runs core's own validation.

  Every failure raises `AnkusaServer.ConfigError`, which `load_or_halt!/0` turns
  into an exit-78 (`EX_CONFIG`) instead of a crash dump, on boot and in
  `docker run jamescarr/ankusa check-config` alike — the latter prints the same
  message without starting anything.

  ## Env overrides

  | Variable | Field |
  | --- | --- |
  | `ANKUSA_ROLES` | `node.roles` (comma-separated) |
  | `ANKUSA_DATA_DIR` | `node.data_dir` |
  | `ANKUSA_LOG_LEVEL` | `log.level` |
  | `ANKUSA_BANNER` | `log.banner` |
  | `ANKUSA_HTTP_PORT`, else `PORT` | `http.port` |
  | `ANKUSA_ADMIN_PORT` | `admin.port` |
  | `ANKUSA_CLAIM_CHECK_PORT` | `claim_check.port` |
  | `ANKUSA_ADMIN_IP` | `admin.ip` |
  | `ANKUSA_CLAIM_CHECK_IP` | `claim_check.ip` |
  | `ANKUSA_ADMIN_TOKEN` | `admin.auth`: one bearer token holding every scope and `tenant:*` (see below) |
  | `ANKUSA_ROUTES_ADMIN_TOKEN` | `routes.admin.auth`: one token holding `routes:read`, `routes:write`, `tenant:*` |
  | `ANKUSA_CLAIM_CHECK_TOKEN` | `claim_check.auth`: one token holding `claims:read`, `tenant:*` |
  | `ANKUSA_ROUTES_ENABLED` | `routes.enabled` |
  | `ANKUSA_ROUTES_STORE_URL` | `routes.store.url` |
  | `ANKUSA_SOURCE_STORE_URL` | `source_store.url` |
  | `ANKUSA_WAL_TYPE` | `wal.type` (`disk`, `none` or `dual`) |
  | `ANKUSA_INTAKE_NODE_ID` | `intake.node_id`: the node's stable identity under `wal.type: dual`, the same on a replacement |
  | `ANKUSA_STORAGE_TYPE` | `storage.type` |
  | `ANKUSA_S3_BUCKET`, `ANKUSA_S3_REGION`, `ANKUSA_S3_ENDPOINT` | `storage.s3.bucket/region/endpoint` |
  | `ANKUSA_GCS_BUCKET` | `storage.gcs.bucket` |
  | `ANKUSA_BACKUP_ENABLED` | `backup.enabled` |
  | `ANKUSA_METRICS_ENABLED`, `ANKUSA_METRICS_PORT`, `ANKUSA_METRICS_IP` | `metrics.enabled/port/ip` |

  Sources are deliberately **not** env-overridable: they carry behaviour
  (verifier, sinks), and behaviour in an env var is unreadable in review.
  Operators inject their secrets through `${VAR}` instead.

  The full annotated schema, with every default, is
  `config-examples/reference.yml` — this module is the code that enforces it.
  """

  require Logger

  alias AnkusaServer.ConfigError

  @default_path "/etc/ankusa/ankusa.yml"
  @fallback_path "./ankusa.yml"

  @root_keys ~w(node log http listener admin metrics routes rate_limits dedupe fleet quarantine batcher dispatch wal intake storage claim_check backup sources source_store secrets lifecycle egress audit tenants queue archive health)
  @node_keys ~w(roles data_dir min_free_bytes)
  @health_keys ~w(sink_error_window_ms)
  @log_keys ~w(level banner)
  @http_keys ~w(port max_body_bytes routing prefix store_headers)
  @store_headers_keys ~w(drop hash)
  @store_headers_fields %{"drop" => :drop, "hash" => :hash}
  @admin_keys ~w(enabled port ip gauge_interval_ms tls auth)
  @listener_keys ~w(tls max_connections num_acceptors read_timeout_ms body_read_timeout_ms request_deadline_ms max_requests_per_connection max_header_count max_header_length http2)
  @metrics_keys ~w(enabled port ip tls detail max_series_per_metric)
  @metrics_details ~w(tenant source none)
  @tls_keys ~w(certfile keyfile cacertfile verify versions ciphers)
  @auth_keys ~w(mode allow_insecure tokens jwt mtls)
  @auth_modes ~w(none token jwt mtls)
  @auth_token_keys ~w(id token sha256 scopes)
  @auth_jwt_keys ~w(issuer audience jwks_url keys refresh_ms min_refresh_ms leeway_s scope_claim tenants_claim)
  @auth_mtls_keys ~w(subject_map)
  @tls_verifies ~w(verify_none verify_peer)
  @tls_versions %{"tlsv1.2" => :"tlsv1.2", "tlsv1.3" => :"tlsv1.3"}
  @batcher_keys ~w(partitions max_batch max_commits group_commit_max_bytes group_commit_max_records group_commit_delay_ms max_delay_ms max_queue max_queue_bytes)
  @dispatch_keys ~w(batch concurrency max_inflight max_inflight_bytes attempt_timeout_ms sink_concurrency breaker_failures breaker_open_ms breaker_max_open_ms max_retry_after_ms retry tenants)
  @dispatch_tenant_keys ~w(max_inflight max_inflight_bytes concurrency)
  @retry_keys ~w(base_ms max_ms max_attempts jitter max_elapsed_ms)
  @wal_keys ~w(type publish_timeout_ms)
  @intake_types ~w(kafka)
  @intake_keys ~w(type node_id fleet topic kafka publish_timeout_ms reconcile_deadline_ms breaker_failures probe_ms retry_after_s offset_flush_ms pending max_record_bytes recover_on_boot verify_topic)
  @intake_pending_keys ~w(max_records max_bytes)
  @intake_kafka_keys ~w(brokers ssl sasl)
  @storage_keys ~w(type roll_bytes roll_ms max_segments_per_tick key_prefix s3 gcs catalogue_retention_days)
  @blob_store_keys ~w(type root s3 gcs)
  # The transport options of `Ankusa.BlobStore.Options`, in milliseconds or bytes
  # as named; the core range-checks them when the instance starts.
  @blob_transport_keys ~w(timeout_ms upload_timeout_ms op_deadline_ms multipart_threshold_bytes
                          multipart_part_bytes retries breaker_failures breaker_open_ms
                          breaker_slow_ms pool_size pool_count pool_timeout)
  @blob_transport_options Map.new(@blob_transport_keys, &{&1, String.to_atom(&1)})
  @s3_keys ~w(bucket region endpoint access_key_id secret_access_key session_token sse) ++
             @blob_transport_keys
  @gcs_keys ~w(bucket endpoint auth token sse) ++ @blob_transport_keys
  @sse_keys ~w(type key_id)
  @sse_types %{"aes256" => :aes256, "kms" => :kms}
  @secrets_keys ~w(kek kek_previous providers cache_ttl_ms)
  @claim_check_keys ~w(port retention_days redeem_grace_ms sweep_max_objects bucket_expiry_days
                       pack_max_bytes ip store stores placement key_layout store_cooldown_ms
                       read_deadline_ms read_timeout_ms read_retries read_max_inflight read_queue_max
                       read_cache_bytes read_cache_max_entry_bytes max_concurrent_reads
                       ref_signing_keys ref_ttl_ms accept_unsigned_refs integrity_check tls auth)
  @backup_keys ~w(enabled interval_ms keep store secondary timeout_ms verify_interval_ms
                  signing_key final_timeout_ms max_age_ms require_fresh)
  @source_store_keys ~w(type url namespace tick_ms timeout_ms load_timeout_ms)
  @lifecycle_keys ~w(sinks)
  @egress_keys ~w(allow_insecure_http deny_cidrs allow_cidrs allow_hosts deny_hosts pools)
  @routes_keys ~w(enabled max_routes memory_budget_bytes store cache trusted_proxies ip_rules admin log_sample ip_denied_status seed)
  @routes_store_keys ~w(type url namespace tick_ms timeout_ms load_timeout_ms)
  # Keys a Redis store owns (`routes.store`, `source_store`); the other store
  # types must not silently drop them.
  @redis_only_keys ~w(url namespace tick_ms timeout_ms load_timeout_ms)
  @routes_cache_keys ~w(max_size ttl_ms negative_ttl_ms gc_interval_ms)
  @routes_ip_rules_keys ~w(default rules)
  @routes_rule_keys ~w(action cidr)
  @routes_admin_keys ~w(port ip tls auth)
  @routes_seed_keys ~w(id tenant_id source_id path methods enabled ip_rules metadata)
  @rate_limits_keys ~w(default tenants per_ip node sources scope)
  @scopes ~w(node fleet)
  @dedupe_scope_keys ~w(scope max_entries max_entries_per_tenant)
  @fleet_keys ~w(store timeout_ms pending_ttl_ms breaker_window_ms breaker_min_calls breaker_failure_rate breaker_open_ms)
  @fleet_store_keys ~w(type url namespace pool_size)
  @fleet_store_types ~w(redis)
  @rate_limit_keys ~w(rate burst bytes_per_second burst_bytes max_body_bytes max_queue max_queue_bytes)
  @hook_limit_keys ~w(rate burst)
  @rate_limit_sources_keys ~w(default)
  @source_limit_keys ~w(bytes_per_second burst_bytes)
  @quarantine_keys ~w(burst rate max_bytes retention_days)
  @egress_pools_keys ~w(pool_size pool_count pool_timeout pool_max_idle_ms conn_max_idle_ms max_pools max_pools_per_tenant protocols)
  @audit_keys ~w(retention_days max_rows max_bytes outbox_max_backlog)
  @queue_keys ~w(dlq rocksdb max_oldest_due_ms)
  @dlq_keys ~w(retention_days max_rows max_bytes)
  @archive_keys ~w(retention_days)
  @rocksdb_keys ~w(block_cache_bytes bloom_bits_per_key max_open_files compaction_style rate_limiter_bytes_per_sec max_background_jobs write_buffer_bytes)
  # The column families whose memtable `queue.rocksdb.write_buffer_bytes` sizes.
  @write_buffer_families %{
    "default" => :default,
    "hooks" => :hooks,
    "deliveries" => :deliveries,
    "index" => :index,
    "archive" => :archive,
    "quarantine" => :quarantine,
    "audit" => :audit,
    "tenants" => :tenants,
    "counters" => :counters
  }
  @tenants_keys ~w(max)
  # A rotation window needs two keys; a handful covers any overlap a provider
  # forces. More is a misconfiguration (each key costs an HMAC per hook).
  @max_secrets 8
  @source_keys ~w(tenant on_verify_failure verify sinks dedupe forward_headers max_headers forward_headers_max_bytes store_headers trust_url_tenant retry)
  @dedupe_keys ~w(preset header json ttl_seconds)
  @dedupe_presets %{
    "github" => :github,
    "standard_webhooks" => :standard_webhooks,
    "svix" => :svix,
    "shopify" => :shopify,
    "stripe" => :stripe
  }
  @verify_keys ~w(type secret tolerance_seconds)
  @verify_hmac_keys ~w(type secret tolerance_seconds signature_header parse sig_prefix sig_key version signed hash encoding secret_decode timestamp_header)
  @verify_asym_preset_keys ~w(type public_key tolerance_seconds)
  @verify_asym_keys ~w(type public_key tolerance_seconds signature_header parse sig_prefix sig_key version signed algorithm encoding timestamp_header)
  @verify_token_keys ~w(type secret location header param)
  @verify_client_cert_keys ~w(type fingerprints subjects)
  @log_sink_keys ~w(type)
  @http_sink_keys ~w(type url method headers timeout_ms secret allow_unsigned max_response_bytes cacertfile allow_insecure_tls)
  @rabbitmq_sink_keys ~w(type url exchange exchange_type routing_key inline_max_bytes max_message_bytes max_inflight idle_timeout_ms)
  @kafka_sink_keys ~w(type brokers topic key inline_max_bytes max_record_bytes ssl sasl idle_timeout_ms)
  @sasl_keys ~w(mechanism username password)
  @nats_sink_keys ~w(type servers subject inline_max_bytes max_message_bytes publish_timeout_ms tls auth idle_timeout_ms)
  @nats_auth_keys ~w(username password token nkey_seed jwt)
  @redis_sink_keys ~w(type url channel inline_max_bytes max_message_bytes publish_timeout_ms idle_timeout_ms)
  @batch_sink_keys ~w(batch batch_linger_ms batch_max_entries batch_max_bytes batch_queue_max batch_queue_max_bytes batch_queue_tenant_max batch_max_inflight)
  @sqs_sink_keys ~w(type queue_url region endpoint message_group_id inline_max_bytes max_message_bytes timeout_ms access_key_id secret_access_key session_token) ++
                   @batch_sink_keys
  @google_pubsub_sink_keys ~w(type project topic endpoint ordering_key inline_max_bytes max_message_bytes timeout_ms auth token) ++
                             @batch_sink_keys

  @roles ~w(edge dispatch storage claim_check)
  @routes_store_types ~w(ets redis)
  @ip_rule_actions ~w(allow deny)
  @verify_types ~w(none stripe github standard_webhooks shopify slack hmac standard_webhooks_asym asymmetric token client_cert)
  @token_locations ~w(bearer basic header query)
  @scheme_algorithms ~w(ed25519 rsa_sha256)
  @scheme_parses ~w(whole csv_pairs space_versions)
  @scheme_hashes ~w(sha256 sha512 sha1)
  @scheme_encodings ~w(hex base64)
  @scheme_secret_decodes ~w(raw whsec_base64)
  @sink_types ~w(log http rabbitmq kafka nats redis sqs google_pubsub)
  @policies ~w(reject quarantine accept_flag)
  @routings ~w(path tenant_path)
  @log_levels ~w(debug info warning error)
  @exchange_types ~w(topic direct fanout headers)

  @typedoc """
  A loaded config plus the log level and banner switch the file asked for, and
  what its `${file:}` / `${sm:}` references resolved to (`AnkusaServer.Secrets`).
  """
  @type loaded :: %{
          config: Ankusa.Config.t(),
          log_level: Logger.level(),
          banner: boolean(),
          secrets: AnkusaServer.Secrets.t()
        }

  # `${NAME}`, `${NAME:-default}`, `${file:/path}`, `${sm:provider:ref}`. A file
  # path or a reference holds no `$` or `}`: `${file:${DIR}/key}` is spelled
  # `secret_file: ${DIR}/key`.
  @var_re ~r/\$\{(?:file:([^}$]+)|sm:([a-z0-9_-]+):([^}$]+)|([A-Z0-9_]+)(?::-([^}]*))?)\}/

  @doc """
  Read, interpolate, validate, and translate the config file.

  Options: `:env` (an environment map, default `System.get_env/0`), `:path`
  (overrides `ANKUSA_CONFIG`, used by tests and `--config`-style callers),
  `:secrets` (the `AnkusaServer.Secrets` of the load this one follows: a
  reference that fails keeps its value from there, see `AnkusaServer.Secrets.reload/2`)
  and `refresh: true` (ignore `secrets.cache_ttl_ms`).
  """
  @spec load!(keyword()) :: loaded()
  def load!(opts \\ []) do
    env = Keyword.get(opts, :env, System.get_env())
    raw = read!(Keyword.get(opts, :path), env)

    {doc, secrets} =
      raw
      |> interpolate(env, [])
      |> apply_env_overrides(env)
      |> AnkusaServer.Secrets.resolve(
        Keyword.get(opts, :secrets),
        Keyword.take(opts, [:refresh])
      )

    %{config: config!(doc), log_level: log_level!(doc), banner: banner?(doc), secrets: secrets}
  end

  @doc """
  `load!/0` for the callers that run on the file: boot, and the `check-config`
  and `print-config` commands. A `ConfigError` is printed to stderr as
  `ankusa: invalid configuration` plus the message, and the VM halts with 78
  (`EX_CONFIG`), so every entry point fails the same way.
  """
  @spec load_or_halt!() :: loaded()
  def load_or_halt! do
    load!()
  rescue
    error in ConfigError ->
      IO.write(:stderr, "ankusa: invalid configuration\n  " <> error.message <> "\n")
      System.halt(78)
  end

  # ── 1. locate + parse ───────────────────────────────────────────────────────

  defp read!(path_override, env) do
    case path_override || env["ANKUSA_CONFIG"] do
      nil -> read_any!(env)
      path -> read_yaml!(path, env)
    end
  end

  defp read_any!(env) do
    cond do
      File.exists?(@default_path) ->
        read_yaml!(@default_path, env)

      File.exists?(@fallback_path) ->
        read_yaml!(@fallback_path, env)

      true ->
        Logger.warning("[ankusa] no config file found; starting with defaults and no sources")
        %{}
    end
  end

  defp read_yaml!(path, env) do
    case YamlElixir.read_from_file(path) do
      {:ok, nil} ->
        %{}

      {:ok, doc} when is_map(doc) ->
        doc

      {:ok, other} ->
        raise ConfigError,
          message: "#{path}: expected a YAML mapping at the top level, got #{inspect(other)}"

      {:error, %YamlElixir.FileNotFoundError{}} ->
        raise ConfigError, message: "config file #{path} not found#{configured_by(env, path)}"

      {:error, error} ->
        raise ConfigError, message: "#{path}: #{Exception.message(error)}"
    end
  end

  defp configured_by(env, path) do
    if env["ANKUSA_CONFIG"] == path, do: " (ANKUSA_CONFIG)", else: ""
  end

  # ── 2. interpolate ──────────────────────────────────────────────────────────

  defp interpolate(value, env, path) when is_map(value) do
    value = Map.new(value, fn {k, v} -> {k, interpolate(v, env, path ++ [k])} end)
    if secret_file_site?(path), do: expand_secret_file(value, path), else: value
  end

  defp interpolate(value, env, path) when is_list(value) do
    value
    |> Enum.with_index()
    |> Enum.map(fn {v, i} -> interpolate(v, env, path ++ [i]) end)
  end

  defp interpolate(value, env, path) when is_binary(value) do
    # Whatever in the file still reads `${` once the references are taken out
    # did not match the grammar (`${stripe_secret}`, `${A-B}`): left alone it
    # would become a literal secret or URL, so it is an error. Only the template
    # is checked: a substituted value may contain `${` itself.
    if value |> then(&Regex.replace(@var_re, &1, "")) |> String.contains?("${") do
      raise ConfigError,
        message:
          "#{render_path(path)}: unresolved ${…} (use ${NAME}, ${NAME:-default}, " <>
            "${file:/abs/path} or ${sm:<provider>:<ref>}; variable names are [A-Z0-9_]+; " <>
            "use ${NAME:-} for an empty default)"
    end

    value
    |> parts()
    |> Enum.map(&substitute(&1, env, path))
    |> AnkusaServer.Secrets.template(path)
  end

  defp interpolate(value, _env, _path), do: value

  # The value as literal text and references, in order. A variable is
  # substituted here; a file or secret manager reference stays for
  # `AnkusaServer.Secrets`, so a file that cannot be read fails with the
  # same dotted key as an unset variable.
  defp parts(value) do
    {parts, position} =
      @var_re
      |> Regex.scan(value, return: :index)
      |> Enum.reduce({[], 0}, fn [{start, length} | groups], {parts, position} ->
        literal = binary_part(value, position, start - position)
        {[reference(value, groups) | [literal | parts]], start + length}
      end)

    tail = binary_part(value, position, byte_size(value) - position)
    Enum.reverse([tail | parts])
  end

  defp reference(value, groups) do
    case Enum.map(groups, &group(value, &1)) do
      [file | _] when is_binary(file) -> {:file, file}
      [_file, name, ref | _] when is_binary(name) -> {:sm, name, ref}
      [_file, _name, _ref, var, default] -> {:var, var, default}
      [_file, _name, _ref, var] -> {:var, var, nil}
    end
  end

  # An optional group that did not take part is `{-1, 0}` (or is cut off the end).
  defp group(_value, {-1, _length}), do: nil
  defp group(value, {start, length}), do: binary_part(value, start, length)

  defp substitute({:var, name, default}, env, path) do
    case Map.fetch(env, name) do
      {:ok, replacement} ->
        replacement

      # An empty default is still a default: `${NAME:-}` is how a file asks for empty.
      :error when is_binary(default) ->
        default

      :error ->
        raise ConfigError, message: "#{render_path(path)}: ${#{name}} is not set"
    end
  end

  defp substitute(part, _env, _path), do: part

  # `secret_file:` is `secret:` read from a file (one path, or a list for a
  # rotation window). It means something only where a `secret` does: a verifier,
  # and an HTTP sink of a source or of `lifecycle`. A spec an API client
  # submits never goes through here, so it cannot name a file of this host.
  defp secret_file_site?(["sources", _id, "verify"]), do: true
  defp secret_file_site?(["sources", _id, "sinks", index]) when is_integer(index), do: true
  defp secret_file_site?(["lifecycle", "sinks", index]) when is_integer(index), do: true
  defp secret_file_site?(_path), do: false

  defp expand_secret_file(%{"secret_file" => files} = map, path) do
    if Map.has_key?(map, "secret") do
      raise ConfigError,
        message: "#{render_path(path)}: secret and secret_file are mutually exclusive"
    end

    key_path = path ++ ["secret_file"]

    secret =
      case files do
        [_ | _] ->
          files
          |> Enum.with_index()
          |> Enum.map(fn {file, i} -> file_ref!(file, key_path ++ [i]) end)

        file ->
          file_ref!(file, key_path)
      end

    map |> Map.delete("secret_file") |> Map.put("secret", secret)
  end

  defp expand_secret_file(map, _path), do: map

  defp file_ref!(file, path) when is_binary(file) and file != "",
    do: AnkusaServer.Secrets.file_template(file, path)

  defp file_ref!(other, path),
    do:
      raise(ConfigError, message: type_error(path, "a file path or a list of file paths", other))

  # ── 3. env overrides ────────────────────────────────────────────────────────

  @env_overrides [
    {"ANKUSA_ROLES", ["node", "roles"]},
    {"ANKUSA_DATA_DIR", ["node", "data_dir"]},
    {"ANKUSA_LOG_LEVEL", ["log", "level"]},
    {"ANKUSA_BANNER", ["log", "banner"]},
    {"ANKUSA_ADMIN_PORT", ["admin", "port"]},
    {"ANKUSA_ADMIN_IP", ["admin", "ip"]},
    {"ANKUSA_CLAIM_CHECK_PORT", ["claim_check", "port"]},
    {"ANKUSA_CLAIM_CHECK_IP", ["claim_check", "ip"]},
    {"ANKUSA_ROUTES_ENABLED", ["routes", "enabled"]},
    {"ANKUSA_ROUTES_STORE_URL", ["routes", "store", "url"]},
    {"ANKUSA_SOURCE_STORE_URL", ["source_store", "url"]},
    {"ANKUSA_WAL_TYPE", ["wal", "type"]},
    {"ANKUSA_INTAKE_NODE_ID", ["intake", "node_id"]},
    {"ANKUSA_STORAGE_TYPE", ["storage", "type"]},
    {"ANKUSA_S3_BUCKET", ["storage", "s3", "bucket"]},
    {"ANKUSA_S3_REGION", ["storage", "s3", "region"]},
    {"ANKUSA_GCS_BUCKET", ["storage", "gcs", "bucket"]},
    {"ANKUSA_STORAGE_KEY_PREFIX", ["storage", "key_prefix"]},
    {"ANKUSA_BACKUP_ENABLED", ["backup", "enabled"]},
    {"ANKUSA_METRICS_ENABLED", ["metrics", "enabled"]},
    {"ANKUSA_METRICS_PORT", ["metrics", "port"]},
    {"ANKUSA_METRICS_IP", ["metrics", "ip"]}
  ]

  # A token in the environment is how a container gets operable without a
  # mounted config file. It replaces that listener's whole `auth` block, and is
  # hashed when the config loads: the plaintext never reaches `Ankusa.Config`.
  @token_overrides [
    {"ANKUSA_ADMIN_TOKEN", ["admin"], Ankusa.Principal.scopes() ++ ["tenant:*"]},
    {"ANKUSA_ROUTES_ADMIN_TOKEN", ["routes", "admin"],
     ["routes:read", "routes:write", "tenant:*"]},
    {"ANKUSA_CLAIM_CHECK_TOKEN", ["claim_check"], ["claims:read", "tenant:*"]}
  ]

  defp apply_env_overrides(doc, env) do
    doc = Enum.reduce(@env_overrides, doc, &override(&2, &1, env))
    doc = Enum.reduce(@token_overrides, doc, &token_override(&2, &1, env))

    # `PORT` is the fallback every container platform sets; `ANKUSA_HTTP_PORT`
    # is the explicit one and wins.
    case {env["ANKUSA_HTTP_PORT"], env["PORT"]} do
      {nil, nil} -> doc
      {nil, port} -> put_path(doc, ["http", "port"], port)
      {port, _} -> put_path(doc, ["http", "port"], port)
    end
  end

  defp override(doc, {var, path}, env) do
    case Map.fetch(env, var) do
      {:ok, value} -> put_path(doc, path, value)
      :error -> doc
    end
  end

  defp token_override(doc, {var, path, scopes}, env) do
    case Map.fetch(env, var) do
      {:ok, ""} ->
        raise ConfigError, message: "#{var} is set but empty"

      {:ok, token} ->
        auth = %{
          "mode" => "token",
          "tokens" => [%{"id" => var, "token" => token, "scopes" => scopes}]
        }

        put_path(doc, path ++ ["auth"], auth)

      :error ->
        doc
    end
  end

  # A section that is present but empty (`http:` with every child commented
  # out) parses as nil and takes the override like a missing one. Any other
  # non-map is left alone for the walker to reject with its type error.
  defp put_path(nil, path, value), do: put_path(%{}, path, value)
  defp put_path(doc, [key], value) when is_map(doc), do: Map.put(doc, key, value)

  defp put_path(doc, [key | rest], value) when is_map(doc),
    do: Map.put(doc, key, put_path(doc[key], rest, value))

  defp put_path(doc, _path, _value), do: doc

  # ── 4. validate + translate ─────────────────────────────────────────────────

  defp log_level!(doc) do
    log = section!(doc, "log", @log_keys, [])
    level = enum!(log["level"] || "info", @log_levels, ["log", "level"])
    String.to_existing_atom(level)
  end

  defp banner?(doc) do
    log = section!(doc, "log", @log_keys, [])
    bool_opt(log, "banner", ["log"]) != false
  end

  defp config!(doc) do
    check_keys!(doc, @root_keys, [])

    egress = egress_policy!(doc)

    opts =
      node_section(doc) ++
        health_section(doc) ++
        http_section(doc) ++
        listener_section(doc) ++
        admin_section(doc) ++
        metrics_section(doc) ++
        routes_section(doc) ++
        rate_limits_section(doc) ++
        fleet_section(doc) ++
        quarantine_section(doc) ++
        audit_section(doc) ++
        queue_section(doc) ++
        archive_section(doc) ++
        tenants_section(doc) ++
        batcher_section(doc) ++
        dispatch_section(doc) ++
        wal_section(doc) ++
        intake_section(doc) ++
        storage_section(doc) ++
        claim_check_section(doc) ++
        backup_section(doc) ++
        egress_section(doc) ++
        source_store_section(doc, egress) ++
        secrets_section(doc) ++
        lifecycle_section(doc, egress)

    try do
      config = Ankusa.Config.new(opts)
      Ankusa.ClaimCheck.validate_config!(config)
      Ankusa.Verifier.validate_config!(config)
      Ankusa.Verifier.validate_shared!(config)
      # The image validates what core validates, at the same moment: a route
      # config that would refuse to boot must fail `check-config` too, with the
      # same message.
      Ankusa.Routes.validate_config!(config)
      Ankusa.Edge.RateLimiter.validate_config!(config)
      Ankusa.Edge.Quarantine.validate_config!(config)
      Ankusa.Dispatch.Pipeline.validate_config!(config)
      Ankusa.Queue.validate_config!(config)
      Ankusa.Store.Backup.validate_config!(config)
      Ankusa.BlobStore.validate_config!(config)
      Ankusa.Lifecycle.validate_config!(config)
      Ankusa.HttpClient.Egress.validate_config!(config)
      config
    rescue
      error in ArgumentError ->
        reraise ConfigError, [message: error.message], __STACKTRACE__
    end
  end

  # ── node / http / admin ─────────────────────────────────────────────────────

  defp node_section(doc) do
    node = section!(doc, "node", @node_keys, [])

    []
    |> put_opt(:roles, node["roles"] && roles!(node["roles"], ["node", "roles"]))
    |> put_opt(
      :data_dir,
      node["data_dir"] && expect_string(node["data_dir"], ["node", "data_dir"])
    )
  end

  # Types only; the ranges are core's (`Ankusa.Config`). `node.min_free_bytes` and
  # `health.sink_error_window_ms` are the two thresholds of what `GET /ready`
  # checks beyond the store (`Ankusa.Health`); core keeps them in its `listener`
  # section (`ready_min_free_bytes`, `ready_sink_error_window_ms`), because
  # `Ankusa.Config` has no field to spare.
  defp health_section(doc) do
    node = section!(doc, "node", @node_keys, [])
    health = section!(doc, "health", @health_keys, [])

    [
      listener:
        []
        |> put_opt(:ready_min_free_bytes, int_opt(node, "min_free_bytes", ["node"]))
        |> put_opt(
          :ready_sink_error_window_ms,
          int_opt(health, "sink_error_window_ms", ["health"])
        )
    ]
  end

  # `to_atom` after `enum!`, not `to_existing_atom`: under `bin/ankusa eval`
  # (check-config) modules load on demand, so `:edge` may not exist yet.
  # The string is checked against a closed list first (`enum!/3`), so the atoms are a fixed set.
  # sobelow_skip ["DOS.StringToAtom"]
  defp roles!(value, path) do
    value
    |> string_list!(path)
    |> Enum.map(&String.to_atom(enum!(&1, @roles, path)))
  end

  defp http_section(doc) do
    http = section!(doc, "http", @http_keys, [])

    []
    |> put_opt(:port, int_opt(http, "port", ["http"]))
    |> put_opt(:max_body_bytes, int_opt(http, "max_body_bytes", ["http"]))
    |> put_opt(:store_headers, store_headers(http, ["http"]))
    |> put_opt(:route_resolver, route_resolver(http))
  end

  defp route_resolver(http) do
    routing = enum!(http["routing"] || "path", @routings, ["http", "routing"])

    module =
      case routing do
        "path" -> Ankusa.RouteResolver.Path
        "tenant_path" -> Ankusa.RouteResolver.TenantPath
      end

    case http["prefix"] do
      nil -> {module, []}
      prefix -> {module, [prefix: prefix!(prefix, ["http", "prefix"])]}
    end
  end

  defp prefix!(prefix, path) do
    case expect_string(prefix, path) do
      "/" -> []
      p -> String.split(p, "/", trim: true)
    end
  end

  defp admin_section(doc) do
    admin = section!(doc, "admin", @admin_keys, [])

    # The image's whole point is being operable, so the admin API is on unless
    # an operator turns it off — the opposite of core's default, which must not
    # bind a port behind an embedded user's back.
    [
      admin:
        [enabled: bool!(Map.get(admin, "enabled", true), ["admin", "enabled"])]
        |> put_opt(:port, int_opt(admin, "port", ["admin"]))
        |> put_opt(:ip, string_opt(admin, "ip", ["admin"]))
        |> put_opt(:gauge_interval_ms, int_opt(admin, "gauge_interval_ms", ["admin"]))
        |> put_opt(:tls, tls_opt(admin, ["admin"]))
        |> put_opt(:auth, auth_opt(admin, ["admin"]))
    ]
  end

  # ── listener / metrics / tls ────────────────────────────────────────────────

  defp listener_section(doc) do
    listener = section!(doc, "listener", @listener_keys, [])
    path = ["listener"]

    [
      listener:
        []
        |> put_opt(:tls, tls_opt(listener, path))
        |> put_opt(:max_connections, int_opt(listener, "max_connections", path))
        |> put_opt(:num_acceptors, int_opt(listener, "num_acceptors", path))
        |> put_opt(:read_timeout_ms, int_opt(listener, "read_timeout_ms", path))
        |> put_opt(:body_read_timeout_ms, int_opt(listener, "body_read_timeout_ms", path))
        |> put_opt(:request_deadline_ms, int_opt(listener, "request_deadline_ms", path))
        |> put_opt(
          :max_requests_per_connection,
          int_opt(listener, "max_requests_per_connection", path)
        )
        |> put_opt(:max_header_count, int_opt(listener, "max_header_count", path))
        |> put_opt(:max_header_length, int_opt(listener, "max_header_length", path))
        |> put_opt(:http2, bool_opt(listener, "http2", path))
    ]
  end

  defp metrics_section(doc) do
    metrics = section!(doc, "metrics", @metrics_keys, [])
    path = ["metrics"]

    [
      metrics:
        []
        |> put_opt(:enabled, bool_opt(metrics, "enabled", path))
        |> put_opt(:port, int_opt(metrics, "port", path))
        |> put_opt(:ip, string_opt(metrics, "ip", path))
        |> put_opt(:tls, tls_opt(metrics, path))
        |> put_opt(:detail, atom_enum_opt(metrics, "detail", @metrics_details, path))
        |> put_opt(:max_series_per_metric, int_opt(metrics, "max_series_per_metric", path))
    ]
  end

  # A listener's `tls` block, or nil (plain HTTP). Core checks the files and
  # the combination (`verify_peer` needs `cacertfile`).
  defp tls_opt(parent, path) do
    case parent["tls"] do
      nil ->
        nil

      value ->
        path = path ++ ["tls"]
        tls = section!(value, @tls_keys, path)

        []
        |> put_opt(:certfile, string_opt(tls, "certfile", path))
        |> put_opt(:keyfile, string_opt(tls, "keyfile", path))
        |> put_opt(:cacertfile, string_opt(tls, "cacertfile", path))
        |> put_opt(:verify, atom_enum_opt(tls, "verify", @tls_verifies, path))
        |> put_opt(:versions, tls_versions(tls, path))
        |> put_opt(:ciphers, tls["ciphers"] && string_list!(tls["ciphers"], path ++ ["ciphers"]))
    end
  end

  defp tls_versions(tls, path) do
    case tls["versions"] do
      nil ->
        nil

      value ->
        path = path ++ ["versions"]

        value
        |> string_list!(path)
        |> Enum.map(&Map.fetch!(@tls_versions, enum!(&1, Map.keys(@tls_versions), path)))
    end
  end

  # ── control-plane auth ──────────────────────────────────────────────────────

  # A control plane's `auth` block (`Ankusa.Admin.Auth`), or nil. Core checks
  # what has to go together: a mode needs its own block, and `mode: none` is
  # only accepted on loopback.
  defp auth_opt(parent, path) do
    case parent["auth"] do
      nil ->
        nil

      value ->
        path = path ++ ["auth"]
        auth = section!(value, @auth_keys, path)

        []
        |> put_opt(:mode, atom_enum_opt(auth, "mode", @auth_modes, path))
        |> put_opt(:allow_insecure, bool_opt(auth, "allow_insecure", path))
        |> put_opt(:tokens, auth_tokens(auth, path))
        |> put_opt(:jwt, auth_jwt(auth, path))
        |> put_opt(:mtls, auth_mtls(auth, path))
    end
  end

  defp auth_tokens(auth, path) do
    case auth["tokens"] do
      nil ->
        nil

      list when is_list(list) ->
        list
        |> Enum.with_index()
        |> Enum.map(fn {entry, index} -> auth_token!(entry, path ++ ["tokens", index]) end)

      other ->
        raise ConfigError,
          message: type_error(path ++ ["tokens"], "a list of token entries", other)
    end
  end

  # A token is given as itself (`token: ${CI_TOKEN}`), hashed here, or as the
  # hash of one (`sha256:`): the plaintext never reaches `Ankusa.Config`.
  defp auth_token!(entry, path) do
    token = section!(entry, @auth_token_keys, path)

    sha256 =
      case {token["token"], token["sha256"]} do
        {nil, nil} ->
          raise ConfigError, message: "#{render_path(path)}: needs token or sha256"

        {plain, nil} ->
          plain = expect_string(plain, path ++ ["token"])

          if plain == "",
            do:
              raise(ConfigError, message: "#{render_path(path ++ ["token"])}: must not be empty")

          Ankusa.Admin.Auth.Token.hash(plain)

        {nil, hex} ->
          expect_string(hex, path ++ ["sha256"])

        {_plain, _hex} ->
          raise ConfigError,
            message: "#{render_path(path)}: token and sha256 are alternatives; give one"
      end

    scopes =
      case token["scopes"] do
        nil -> raise ConfigError, message: "#{render_path(path)}: missing required key \"scopes\""
        value -> string_list!(value, path ++ ["scopes"])
      end

    %{id: required_string!(token, "id", path), sha256: sha256, scopes: scopes}
  end

  defp auth_jwt(auth, path) do
    case auth["jwt"] do
      nil ->
        nil

      value ->
        path = path ++ ["jwt"]
        jwt = section!(value, @auth_jwt_keys, path)

        []
        |> put_opt(:issuer, string_opt(jwt, "issuer", path))
        |> put_opt(:audience, jwt_audience(jwt, path))
        |> put_opt(:jwks_url, string_opt(jwt, "jwks_url", path))
        |> put_opt(:keys, jwt_keys(jwt, path))
        |> put_opt(:refresh_ms, int_opt(jwt, "refresh_ms", path))
        |> put_opt(:min_refresh_ms, int_opt(jwt, "min_refresh_ms", path))
        |> put_opt(:leeway_s, int_opt(jwt, "leeway_s", path))
        |> put_opt(:scope_claim, string_opt(jwt, "scope_claim", path))
        |> put_opt(:tenants_claim, string_opt(jwt, "tenants_claim", path))
    end
  end

  # One audience, or several (a token carrying any of them is accepted).
  defp jwt_audience(jwt, path) do
    case jwt["audience"] do
      nil ->
        nil

      audience when is_binary(audience) ->
        audience

      list when is_list(list) ->
        string_list!(list, path ++ ["audience"])

      other ->
        raise ConfigError, message: type_error(path ++ ["audience"], "a string or a list", other)
    end
  end

  # Public keys: JWK mappings, or PEM strings. Core checks each one.
  defp jwt_keys(jwt, path) do
    case jwt["keys"] do
      nil ->
        nil

      list when is_list(list) ->
        list
        |> Enum.with_index()
        |> Enum.map(fn
          {key, _index} when is_map(key) or is_binary(key) ->
            key

          {other, index} ->
            raise ConfigError,
              message: type_error(path ++ ["keys", index], "a JWK mapping or a PEM string", other)
        end)

      other ->
        raise ConfigError, message: type_error(path ++ ["keys"], "a list of keys", other)
    end
  end

  # `subject_map` keys name a certificate identity: `cn:<name>`, `dns:<name>`,
  # `email:<address>`, `uri:<uri>` or `ip:<address>` (quote them in YAML).
  defp auth_mtls(auth, path) do
    case auth["mtls"] do
      nil ->
        nil

      value ->
        path = path ++ ["mtls"]
        mtls = section!(value, @auth_mtls_keys, path)

        case mtls["subject_map"] do
          nil ->
            []

          map when is_map(map) ->
            [
              subject_map:
                Map.new(map, fn {subject, scopes} ->
                  {to_string(subject),
                   string_list!(scopes, path ++ ["subject_map", to_string(subject)])}
                end)
            ]

          other ->
            raise ConfigError, message: type_error(path ++ ["subject_map"], "a mapping", other)
        end
    end
  end

  # ── batcher / dispatch ──────────────────────────────────────────────────────

  defp batcher_section(doc) do
    batcher = section!(doc, "batcher", @batcher_keys, [])

    [
      batcher:
        []
        |> put_opt(:partitions, int_opt(batcher, "partitions", ["batcher"]))
        |> put_opt(:max_batch, int_opt(batcher, "max_batch", ["batcher"]))
        |> put_opt(:max_commits, int_opt(batcher, "max_commits", ["batcher"]))
        |> put_opt(
          :group_commit_max_bytes,
          int_opt(batcher, "group_commit_max_bytes", ["batcher"])
        )
        |> put_opt(
          :group_commit_max_records,
          int_opt(batcher, "group_commit_max_records", ["batcher"])
        )
        |> put_opt(:max_delay_ms, int_opt(batcher, "max_delay_ms", ["batcher"]))
        |> put_opt(
          :group_commit_delay_ms,
          int_opt(batcher, "group_commit_delay_ms", ["batcher"])
        )
        |> put_opt(:max_queue, int_opt(batcher, "max_queue", ["batcher"]))
        |> put_opt(:max_queue_bytes, int_opt(batcher, "max_queue_bytes", ["batcher"]))
    ]
  end

  defp dispatch_section(doc) do
    dispatch = section!(doc, "dispatch", @dispatch_keys, [])
    retry = section!(dispatch, "retry", @retry_keys, ["dispatch"])

    [
      dispatch:
        []
        |> put_opt(:batch, int_opt(dispatch, "batch", ["dispatch"]))
        |> put_opt(:concurrency, int_opt(dispatch, "concurrency", ["dispatch"]))
        |> put_opt(:max_inflight, int_opt(dispatch, "max_inflight", ["dispatch"]))
        |> put_opt(:max_inflight_bytes, int_opt(dispatch, "max_inflight_bytes", ["dispatch"]))
        |> put_opt(:attempt_timeout_ms, int_opt(dispatch, "attempt_timeout_ms", ["dispatch"]))
        |> put_sink_concurrency(dispatch)
        |> put_opt(:breaker_failures, int_opt(dispatch, "breaker_failures", ["dispatch"]))
        |> put_opt(:breaker_open_ms, int_opt(dispatch, "breaker_open_ms", ["dispatch"]))
        |> put_opt(:breaker_max_open_ms, int_opt(dispatch, "breaker_max_open_ms", ["dispatch"]))
        |> put_opt(:retry, retry_policy(retry))
        |> put_opt(:max_retry_after_ms, int_opt(dispatch, "max_retry_after_ms", ["dispatch"]))
        |> put_opt(:tenants, dispatch_tenants(dispatch))
    ]
  end

  # Absent keeps core's default (8); an explicit `null` switches the per-sink
  # cap off, which `put_opt/3` would drop as "not given".
  defp put_sink_concurrency(kw, dispatch) do
    case Map.fetch(dispatch, "sink_concurrency") do
      {:ok, nil} -> kw ++ [sink_concurrency: nil]
      _ -> put_opt(kw, :sink_concurrency, int_opt(dispatch, "sink_concurrency", ["dispatch"]))
    end
  end

  # `dispatch.tenants`: `default` and one entry per tenant id, each any of
  # `max_inflight`, `max_inflight_bytes` and `concurrency`. Types only; the
  # ranges are core's (`Ankusa.Dispatch.Pipeline.validate_config!/1`). A tenant
  # named `default` cannot be given limits of its own: the key is the default.
  defp dispatch_tenants(dispatch) do
    path = ["dispatch", "tenants"]

    case Map.get(dispatch, "tenants") do
      nil ->
        nil

      tenants when is_map(tenants) ->
        Map.new(tenants, fn {tenant, limits} ->
          tenant = to_string(tenant)
          limits_path = path ++ [tenant]
          limits = section!(limits, @dispatch_tenant_keys, limits_path)

          limits =
            %{}
            |> put_key(:max_inflight, int_opt(limits, "max_inflight", limits_path))
            |> put_key(:max_inflight_bytes, int_opt(limits, "max_inflight_bytes", limits_path))
            |> put_key(:concurrency, int_opt(limits, "concurrency", limits_path))

          {if(tenant == "default", do: :default, else: tenant), limits}
        end)

      other ->
        raise ConfigError, message: type_error(path, "a mapping of tenant id to limits", other)
    end
  end

  defp retry_policy(retry) do
    path = ["dispatch", "retry"]

    if retry == %{} do
      nil
    else
      {Ankusa.RetryPolicy.Exponential, retry_opts(retry, path)}
    end
  end

  # `sources.<id>.retry`: options laid over the instance's `dispatch.retry`.
  # Absent is no override; the ranges are core's (`Ankusa.RetryPolicy`), reported
  # here with the key's path.
  defp source_retry(source, path) do
    case source["retry"] do
      nil ->
        nil

      value ->
        path = path ++ ["retry"]
        opts = retry_opts(section!(value, @retry_keys, path), path)

        try do
          for {key, value} <- opts,
              do: Ankusa.RetryPolicy.validate_opt!(key, value, render_path(path))

          opts
        rescue
          error in ArgumentError ->
            reraise ConfigError, [message: Exception.message(error)], __STACKTRACE__
        end
    end
  end

  defp retry_opts(retry, path) do
    []
    |> put_opt(:base_ms, int_opt(retry, "base_ms", path))
    |> put_opt(:max_ms, int_opt(retry, "max_ms", path))
    |> put_opt(:max_attempts, int_opt(retry, "max_attempts", path))
    |> put_opt(:jitter, bool_opt(retry, "jitter", path))
    |> put_opt(:max_elapsed_ms, int_opt(retry, "max_elapsed_ms", path))
  end

  # ── wal ─────────────────────────────────────────────────────────────────────

  defp wal_section(doc) do
    wal = section!(doc, "wal", @wal_keys, [])

    case enum!(wal["type"] || "disk", ~w(disk none dual), ["wal", "type"]) do
      "disk" ->
        [wal: :disk]

      "dual" ->
        [wal: :dual]

      "none" ->
        [wal: :none]
        |> put_opt(:direct_publish_timeout_ms, int_opt(wal, "publish_timeout_ms", ["wal"]))
    end
  end

  # ── intake ──────────────────────────────────────────────────────────────────

  # The intake log behind `wal.type: dual` (ADR 0002). Read whatever `wal.type` is:
  # core checks every range, and the cross-key rules (a type and a node id, the
  # backup, the deadlines) only under `dual`.
  defp intake_section(doc) do
    intake = section!(doc, "intake", @intake_keys, [])
    path = ["intake"]

    if map_size(intake) == 0 do
      []
    else
      pending = section!(intake, "pending", @intake_pending_keys, path)

      [
        intake:
          %{}
          |> put_map(:type, atom_enum_opt(intake, "type", @intake_types, path))
          |> put_map(:node_id, string_opt(intake, "node_id", path))
          |> put_map(:fleet, string_opt(intake, "fleet", path))
          |> put_map(:topic, string_opt(intake, "topic", path))
          |> put_map(:kafka, intake_kafka(intake, path))
          |> put_map(:publish_timeout_ms, int_opt(intake, "publish_timeout_ms", path))
          |> put_map(:reconcile_deadline_ms, int_opt(intake, "reconcile_deadline_ms", path))
          |> put_map(:breaker_failures, int_opt(intake, "breaker_failures", path))
          |> put_map(:probe_ms, int_opt(intake, "probe_ms", path))
          |> put_map(:retry_after_s, int_opt(intake, "retry_after_s", path))
          |> put_map(:offset_flush_ms, int_opt(intake, "offset_flush_ms", path))
          |> put_map(:max_record_bytes, int_opt(intake, "max_record_bytes", path))
          |> put_map(:recover_on_boot, bool_opt(intake, "recover_on_boot", path))
          |> put_map(:verify_topic, bool_opt(intake, "verify_topic", path))
          |> put_map(:pending, intake_pending(pending, path ++ ["pending"]))
      ]
    end
  end

  defp intake_pending(pending, _path) when map_size(pending) == 0, do: nil

  defp intake_pending(pending, path) do
    %{}
    |> put_map(:max_records, int_opt(pending, "max_records", path))
    |> put_map(:max_bytes, int_opt(pending, "max_bytes", path))
  end

  defp intake_kafka(intake, path) do
    case section!(intake, "kafka", @intake_kafka_keys, path) do
      kafka when map_size(kafka) == 0 ->
        nil

      kafka ->
        path = path ++ ["kafka"]

        [brokers: string_list!(kafka["brokers"], path ++ ["brokers"])]
        |> put_opt(:ssl, bool_opt(kafka, "ssl", path))
        |> put_opt(:sasl, sasl(kafka["sasl"], path ++ ["sasl"]))
    end
  end

  defp put_map(map, _key, nil), do: map
  defp put_map(map, key, value), do: Map.put(map, key, value)

  # ── storage ─────────────────────────────────────────────────────────────────

  defp storage_section(doc) do
    storage = section!(doc, "storage", @storage_keys, [])

    [
      storage:
        [blob_store: blob_store!(storage, ["storage"], false)]
        |> put_opt(:roll_bytes, int_opt(storage, "roll_bytes", ["storage"]))
        |> put_opt(:roll_ms, int_opt(storage, "roll_ms", ["storage"]))
        |> put_opt(
          :max_segments_per_tick,
          int_opt(storage, "max_segments_per_tick", ["storage"])
        )
        |> put_opt(:key_prefix, string_opt(storage, "key_prefix", ["storage"]))
        |> put_opt(
          :catalogue_retention_days,
          int_opt(storage, "catalogue_retention_days", ["storage"])
        )
    ]
  end

  # `{module, opts}` from a `type: local|s3|gcs` section with its `s3:`/`gcs:`
  # children: `storage` itself, `claim_check.store` and each entry of
  # `claim_check.stores`. `root?` allows a LocalFS `root` (only a dedicated
  # claim store has one; segments live under `data_dir`).
  defp blob_store!(section, path, root?) do
    s3 = section!(section, "s3", @s3_keys, path)
    gcs = section!(section, "gcs", @gcs_keys, path)

    case enum!(section["type"] || "local", ~w(local s3 gcs), path ++ ["type"]) do
      "local" ->
        case root? && string_opt(section, "root", path) do
          root when is_binary(root) -> {Ankusa.BlobStore.LocalFS, [root: root]}
          _ -> {Ankusa.BlobStore.LocalFS, []}
        end

      "s3" ->
        {Ankusa.BlobStore.S3, s3_opts!(s3, path ++ ["s3"])}

      "gcs" ->
        {Ankusa.BlobStore.GCS, gcs_opts!(gcs, path ++ ["gcs"])}
    end
  end

  defp s3_opts!(s3, path) do
    [
      bucket: required_string!(s3, "bucket", path),
      region: required_string!(s3, "region", path)
    ]
    |> put_opt(:endpoint, string_opt(s3, "endpoint", path))
    |> put_opt(:access_key_id, string_opt(s3, "access_key_id", path))
    |> put_opt(:secret_access_key, string_opt(s3, "secret_access_key", path))
    |> put_opt(:session_token, string_opt(s3, "session_token", path))
    |> put_opt(:sse, sse_opt(s3, path))
    |> Kernel.++(blob_transport_opts(s3, path))
  end

  defp gcs_opts!(gcs, path) do
    bucket = required_string!(gcs, "bucket", path)

    ([bucket: bucket]
     |> put_opt(:endpoint, string_opt(gcs, "endpoint", path))
     |> put_opt(:sse, sse_opt(gcs, path))
     |> Kernel.++(blob_transport_opts(gcs, path))) ++
      gcp_auth!(gcs, path)
  end

  # `timeout_ms`, `op_deadline_ms`, `multipart_*`, `breaker_*`, `pool_*` … of an
  # `s3` or `gcs` section, as the adapter options of the same names.
  defp blob_transport_opts(section, path) do
    for {key, option} <- @blob_transport_options, value = int_opt(section, key, path) do
      {option, value}
    end
  end

  # `sse: {type: aes256 | kms, key_id: …}` on an `s3` or `gcs` section: the
  # object store encrypts every object this store writes
  # (`Ankusa.BlobStore.SSE` says what each provider does with it).
  defp sse_opt(section, path) do
    if section["sse"] == nil do
      nil
    else
      sse_path = path ++ ["sse"]
      sse = section!(section, "sse", @sse_keys, path)
      type = required_string!(sse, "type", sse_path)
      type = enum!(type, Map.keys(@sse_types), sse_path ++ ["type"])

      [type: Map.fetch!(@sse_types, type)]
      |> put_opt(:key_id, string_opt(sse, "key_id", sse_path))
      |> Map.new()
    end
  end

  # The key-encryption keys for secrets at rest (`Ankusa.Crypto.Envelope`).
  # Both are operator-supplied through `${VAR}`: the file holds the reference,
  # never the key. `kek_previous` is a list, or one comma-separated string, and
  # may be empty (`${PREVIOUS_KEKS:-}`).
  defp secrets_section(doc) do
    path = ["secrets"]
    secrets = section!(doc, "secrets", @secrets_keys, [])

    previous =
      case secrets["kek_previous"] do
        empty when empty in [nil, [], ""] -> nil
        keys -> string_list!(keys, path ++ ["kek_previous"])
      end

    [
      secrets:
        []
        |> put_opt(:kek, string_opt(secrets, "kek", path))
        |> put_opt(:kek_previous, previous)
    ]
  end

  # `auth: metadata | token | none` (default metadata) on any GCP section.
  defp gcp_auth!(section, path) do
    case enum!(section["auth"] || "metadata", ~w(metadata token none), path ++ ["auth"]) do
      "none" ->
        []

      "metadata" ->
        [token_provider: {AnkusaServer.GcpToken, :metadata, []}]

      "token" ->
        [
          token_provider:
            {AnkusaServer.GcpToken, :static, [required_string!(section, "token", path)]}
        ]
    end
  end

  # ── claim check ─────────────────────────────────────────────────────────────

  defp claim_check_section(doc) do
    claim_check = section!(doc, "claim_check", @claim_check_keys, [])
    path = ["claim_check"]

    [
      claim_check:
        []
        |> put_opt(:port, int_opt(claim_check, "port", path))
        |> put_opt(:pack_max_bytes, int_opt(claim_check, "pack_max_bytes", path))
        |> put_opt(:retention_days, int_opt(claim_check, "retention_days", path))
        |> put_opt(:redeem_grace_ms, int_opt(claim_check, "redeem_grace_ms", path))
        |> put_opt(:sweep_max_objects, int_opt(claim_check, "sweep_max_objects", path))
        |> put_opt(:bucket_expiry_days, int_opt(claim_check, "bucket_expiry_days", path))
        |> put_opt(:ip, string_opt(claim_check, "ip", path))
        |> put_opt(:blob_stores, claim_stores!(claim_check))
        |> put_opt(:placement, atom_enum_opt(claim_check, "placement", ~w(ordered hash), path))
        |> put_opt(:key_layout, atom_enum_opt(claim_check, "key_layout", ~w(flat sharded), path))
        |> put_opt(:store_cooldown_ms, int_opt(claim_check, "store_cooldown_ms", path))
        |> put_opt(:read_deadline_ms, int_opt(claim_check, "read_deadline_ms", path))
        |> put_opt(:read_timeout_ms, int_opt(claim_check, "read_timeout_ms", path))
        |> put_opt(:read_retries, int_opt(claim_check, "read_retries", path))
        |> put_opt(:read_max_inflight, int_opt(claim_check, "read_max_inflight", path))
        |> put_opt(:read_queue_max, int_opt(claim_check, "read_queue_max", path))
        |> put_opt(:read_cache_bytes, int_opt(claim_check, "read_cache_bytes", path))
        |> put_opt(
          :read_cache_max_entry_bytes,
          int_opt(claim_check, "read_cache_max_entry_bytes", path)
        )
        |> put_opt(:max_concurrent_reads, int_opt(claim_check, "max_concurrent_reads", path))
        |> put_opt(
          :integrity_check,
          atom_enum_opt(claim_check, "integrity_check", ~w(off manifest), path)
        )
        |> put_opt(:ref_signing_keys, ref_signing_keys(claim_check, path))
        |> put_opt(:ref_ttl_ms, int_opt(claim_check, "ref_ttl_ms", path))
        |> put_opt(
          :accept_unsigned_refs,
          nullable_bool_opt(claim_check, "accept_unsigned_refs", path)
        )
        |> put_opt(:tls, tls_opt(claim_check, path))
        |> put_opt(:auth, auth_opt(claim_check, path))
    ]
  end

  # `[current, previous...]`: a list, or one comma-separated string (`${VAR}`).
  # Empty or null leaves the key unset, which means no signing.
  defp ref_signing_keys(claim_check, path) do
    case claim_check["ref_signing_keys"] do
      empty when empty in [nil, [], ""] -> nil
      keys -> string_list!(keys, path ++ ["ref_signing_keys"])
    end
  end

  defp nullable_bool_opt(map, key, path) do
    if map[key] == nil, do: nil, else: bool_opt(map, key, path)
  end

  # `store` (one mapping) and `stores` (a list of them, in failover order) both
  # become `claim_check.blob_stores`; neither (or null) = nil, the storage
  # bucket.
  defp claim_stores!(claim_check) do
    case {claim_check["store"], claim_check["stores"]} do
      {nil, nil} ->
        nil

      {_store, nil} ->
        store = section!(claim_check, "store", @blob_store_keys, ["claim_check"])
        [blob_store!(store, ["claim_check", "store"], true)]

      {nil, [_ | _] = stores} ->
        stores
        |> Enum.with_index()
        |> Enum.map(fn {entry, index} ->
          claim_store!(entry, ["claim_check", "stores", index])
        end)

      {nil, []} ->
        raise ConfigError, message: "claim_check.stores: must not be empty"

      {nil, other} ->
        raise ConfigError,
          message: type_error(["claim_check", "stores"], "a list of stores", other)

      {_store, _stores} ->
        raise ConfigError,
          message: "claim_check: store and stores are exclusive; list every store under stores"
    end
  end

  defp claim_store!(entry, path) when is_map(entry) do
    entry |> section!(@blob_store_keys, path) |> blob_store!(path, true)
  end

  defp claim_store!(entry, path),
    do: raise(ConfigError, message: type_error(path, "a mapping", entry))

  # ── backup ──────────────────────────────────────────────────────────────────

  # `store` takes the claim store's shape (`root` allowed for a local store); unset
  # means the storage bucket, under `storage.key_prefix` either way.
  defp backup_section(doc) do
    backup = section!(doc, "backup", @backup_keys, [])

    [
      backup:
        []
        |> put_opt(:enabled, bool_opt(backup, "enabled", ["backup"]))
        |> put_opt(:interval_ms, int_opt(backup, "interval_ms", ["backup"]))
        |> put_opt(:keep, int_opt(backup, "keep", ["backup"]))
        |> put_opt(:blob_store, backup_store(backup, "store"))
        |> put_opt(:secondary, backup_store(backup, "secondary"))
        |> put_opt(:timeout_ms, int_opt(backup, "timeout_ms", ["backup"]))
        |> put_opt(:verify_interval_ms, int_opt(backup, "verify_interval_ms", ["backup"]))
        |> put_opt(:final_timeout_ms, int_opt(backup, "final_timeout_ms", ["backup"]))
        |> put_opt(:max_age_ms, int_opt(backup, "max_age_ms", ["backup"]))
        |> put_opt(:require_fresh, bool_opt(backup, "require_fresh", ["backup"]))
        |> put_opt(:signing_key, backup_signing_keys(backup))
    ]
  end

  # `store` and `secondary` take the claim store's shape; unset leaves the key
  # to core's default.
  defp backup_store(backup, key) do
    case backup[key] do
      nil ->
        nil

      _store ->
        store = section!(backup, key, @blob_store_keys, ["backup"])
        blob_store!(store, ["backup", key], true)
    end
  end

  # `[current, previous...]`: a list, or one comma-separated string (`${VAR}`).
  # Empty or null leaves the manifests unsigned.
  defp backup_signing_keys(backup) do
    case backup["signing_key"] do
      empty when empty in [nil, [], ""] -> nil
      keys -> string_list!(keys, ["backup", "signing_key"])
    end
  end

  # ── routes ──────────────────────────────────────────────────────────────────

  # Route management is off unless the file says otherwise, and every key is
  # optional: an enabled section with just a token is a working config (deny
  # everything, until an operator posts a route).
  defp routes_section(doc) do
    path = ["routes"]
    routes = section!(doc, "routes", @routes_keys, [])

    [
      routes:
        []
        |> put_opt(:enabled, bool_opt(routes, "enabled", path))
        |> put_opt(:max_routes, int_opt(routes, "max_routes", path))
        |> put_opt(:memory_budget_bytes, int_opt(routes, "memory_budget_bytes", path))
        |> put_opt(:log_sample, int_opt(routes, "log_sample", path))
        |> put_opt(:ip_denied_status, int_opt(routes, "ip_denied_status", path))
        |> put_opt(:trusted_proxies, trusted_proxies(routes, path))
        |> put_opt(:store, routes_store(routes, path))
        |> put_opt(:cache, routes_cache(routes, path))
        |> put_opt(:ip_rules, routes_ip_rules(routes, path))
        |> put_opt(:admin, routes_admin(routes, path))
        |> put_opt(:seed, routes_seed(routes, path))
    ]
  end

  # An empty list is "no proxies" — the default — not the empty-list error the
  # shared helper raises for roles and broker lists.
  defp trusted_proxies(routes, path) do
    case Map.get(routes, "trusted_proxies") do
      nil -> nil
      [] -> nil
      value -> string_list!(value, path ++ ["trusted_proxies"])
    end
  end

  # A URL with no `type` means Redis: the ETS store has no URL, so a file that
  # sets one has said what it wants. The other way round is a config error, not
  # a silent drop: with `type: ets` live (as the shipped reference.yml has it) a
  # fleet that only set a URL would leave every node with its own definitions
  # while its operator believed they were shared.
  defp routes_store(routes, path) do
    store = section!(routes, "store", @routes_store_keys, path)
    path = path ++ ["store"]

    type =
      enum!(
        store["type"] || (store["url"] && "redis") || "ets",
        @routes_store_types,
        path ++ ["type"]
      )

    case type do
      "ets" ->
        reject_redis_only_keys!(store, path)
        {Ankusa.Routes.Store.ETS, []}

      "redis" ->
        {Ankusa.Routes.Store.Redis,
         [url: required_string!(store, "url", path)]
         |> put_opt(:namespace, string_opt(store, "namespace", path))
         |> put_opt(:tick_ms, int_opt(store, "tick_ms", path))
         |> put_opt(:timeout_ms, int_opt(store, "timeout_ms", path))
         |> put_opt(:load_timeout_ms, int_opt(store, "load_timeout_ms", path))}
    end
  end

  defp reject_redis_only_keys!(store, path) do
    case Enum.filter(@redis_only_keys, &(Map.get(store, &1) != nil)) do
      [] ->
        :ok

      [key] ->
        raise ConfigError,
          message:
            "#{render_path(path)}: #{inspect(key)} is only valid with type: redis; " <>
              "set type: redis or remove it"

      keys ->
        raise ConfigError,
          message:
            "#{render_path(path)}: #{Enum.map_join(keys, ", ", &inspect/1)} are only valid " <>
              "with type: redis; set type: redis or remove them"
    end
  end

  defp routes_cache(routes, path) do
    cache = section!(routes, "cache", @routes_cache_keys, path)
    path = path ++ ["cache"]

    []
    |> put_opt(:max_size, int_opt(cache, "max_size", path))
    |> put_opt(:ttl_ms, int_opt(cache, "ttl_ms", path))
    |> put_opt(:negative_ttl_ms, int_opt(cache, "negative_ttl_ms", path))
    |> put_opt(:gc_interval_ms, int_opt(cache, "gc_interval_ms", path))
  end

  defp routes_admin(routes, path) do
    admin = section!(routes, "admin", @routes_admin_keys, path)
    path = path ++ ["admin"]

    []
    |> put_opt(:port, int_opt(admin, "port", path))
    |> put_opt(:ip, string_opt(admin, "ip", path))
    |> put_opt(:tls, tls_opt(admin, path))
    |> put_opt(:auth, auth_opt(admin, path))
  end

  defp routes_ip_rules(routes, path) do
    ip_rules = section!(routes, "ip_rules", @routes_ip_rules_keys, path)
    path = path ++ ["ip_rules"]

    []
    |> put_opt(:default, atom_enum_opt(ip_rules, "default", @ip_rule_actions, path))
    |> put_opt(:rules, rules_list(ip_rules, path))
  end

  defp rules_list(ip_rules, path) do
    case Map.get(ip_rules, "rules") do
      nil ->
        nil

      list when is_list(list) ->
        list
        |> Enum.with_index()
        |> Enum.map(fn {rule, index} -> rule!(rule, path ++ ["rules", index]) end)

      other ->
        raise ConfigError,
          message: type_error(path ++ ["rules"], "a list of rules", other)
    end
  end

  # The string is checked against a closed list first (`enum!/3`), so the atoms are a fixed set.
  # sobelow_skip ["DOS.StringToAtom"]
  defp rule!(rule, path) do
    rule = section!(rule, @routes_rule_keys, path)

    %{
      action:
        String.to_atom(
          enum!(required_string!(rule, "action", path), @ip_rule_actions, path ++ ["action"])
        ),
      cidr: required_string!(rule, "cidr", path)
    }
  end

  # A seed entry is a route definition; its own fields are validated by core
  # (`Ankusa.Routes.validate_config!/1`), which owns the route grammar. Here it
  # only has to be a mapping of known keys.
  defp routes_seed(routes, path) do
    case Map.get(routes, "seed") do
      nil ->
        nil

      list when is_list(list) ->
        list
        |> Enum.with_index()
        |> Enum.map(fn {seed, index} ->
          section!(seed, @routes_seed_keys, path ++ ["seed", index])
        end)

      other ->
        raise ConfigError, message: type_error(path ++ ["seed"], "a list of routes", other)
    end
  end

  # ── rate limits ─────────────────────────────────────────────────────────────

  # The server checks shape and types only — that the required keys are present,
  # that `rate` is a number and `burst` an integer, and so on. Ranges, tenant ids
  # and the rule that `routing: tenant_path` needs a `default` are core's
  # (`Ankusa.Edge.RateLimiter.validate_config!/1`), so the message an operator
  # sees is the same whether they run the image or embed the library.
  #
  # No env override: a per-tenant map is not env-shaped. `${VAR}` inside the
  # values is how a deployment parameterizes one.
  defp rate_limits_section(doc) do
    path = ["rate_limits"]
    rate_limits = section!(doc, "rate_limits", @rate_limits_keys, [])

    [
      rate_limits:
        []
        |> put_opt(:default, rate_limit_opt(rate_limits, "default", path))
        |> put_opt(:tenants, rate_limit_tenants(rate_limits, path))
        |> put_opt(:per_ip, hook_limit_opt(rate_limits, "per_ip", path))
        |> put_opt(:node, hook_limit_opt(rate_limits, "node", path))
        |> put_opt(:sources, rate_limit_sources(rate_limits, path))
        |> put_opt(:scope, atom_enum_opt(rate_limits, "scope", @scopes, path))
    ]
  end

  # `dedupe.{scope, max_entries, max_entries_per_tenant}` and the `fleet` block behind both fleet scopes
  # (`Ankusa.Edge.Fleet`). A store with a `url` and no `type` is Redis, the one
  # fleet store there is.
  defp fleet_section(doc) do
    dedupe = section!(doc, "dedupe", @dedupe_scope_keys, [])
    fleet = section!(doc, "fleet", @fleet_keys, [])
    path = ["fleet"]

    [
      dedupe:
        []
        |> put_opt(:scope, atom_enum_opt(dedupe, "scope", @scopes, ["dedupe"]))
        |> put_opt(:max_entries, int_opt(dedupe, "max_entries", ["dedupe"]))
        |> put_opt(:max_entries_per_tenant, int_opt(dedupe, "max_entries_per_tenant", ["dedupe"])),
      fleet:
        []
        |> put_opt(:store, fleet_store(fleet, path))
        |> put_opt(:timeout_ms, int_opt(fleet, "timeout_ms", path))
        |> put_opt(:pending_ttl_ms, int_opt(fleet, "pending_ttl_ms", path))
        |> put_opt(:breaker_window_ms, int_opt(fleet, "breaker_window_ms", path))
        |> put_opt(:breaker_min_calls, int_opt(fleet, "breaker_min_calls", path))
        |> put_opt(:breaker_failure_rate, number_opt(fleet, "breaker_failure_rate", path))
        |> put_opt(:breaker_open_ms, int_opt(fleet, "breaker_open_ms", path))
    ]
  end

  defp fleet_store(fleet, path) do
    case section!(fleet, "store", @fleet_store_keys, path) do
      store when map_size(store) == 0 ->
        nil

      store ->
        path = path ++ ["store"]

        url = required_string!(store, "url", path)
        enum!(store["type"] || "redis", @fleet_store_types, path ++ ["type"])

        {Ankusa.RateLimiter.Store.Redis,
         [url: url]
         |> put_opt(:namespace, string_opt(store, "namespace", path))
         |> put_opt(:pool_size, int_opt(store, "pool_size", path))}
    end
  end

  defp rate_limit_opt(map, key, path) do
    case map[key] do
      nil -> nil
      value -> rate_limit!(value, path ++ [key])
    end
  end

  defp rate_limit_tenants(rate_limits, path) do
    case Map.get(rate_limits, "tenants") do
      nil ->
        nil

      tenants when is_map(tenants) ->
        Map.new(tenants, fn {tenant, limit} ->
          tenant = to_string(tenant)
          {tenant, rate_limit!(limit, path ++ ["tenants", tenant])}
        end)

      other ->
        raise ConfigError,
          message: type_error(path ++ ["tenants"], "a mapping of tenant id to limit", other)
    end
  end

  # A tenant's (or the default) limit: `rate` and `burst`, plus any of the
  # optional byte, body and queue keys.
  defp rate_limit!(value, path) do
    limit = section!(value, @rate_limit_keys, path)

    %{rate: required_number!(limit, "rate", path), burst: required_int!(limit, "burst", path)}
    |> put_key(:bytes_per_second, number_opt(limit, "bytes_per_second", path))
    |> put_key(:burst_bytes, int_opt(limit, "burst_bytes", path))
    |> put_key(:max_body_bytes, int_opt(limit, "max_body_bytes", path))
    |> put_key(:max_queue, int_opt(limit, "max_queue", path))
    |> put_key(:max_queue_bytes, int_opt(limit, "max_queue_bytes", path))
  end

  # `per_ip` and `node`: hooks per second and a burst, nothing more.
  defp hook_limit_opt(map, key, path) do
    case map[key] do
      nil ->
        nil

      value ->
        limit = section!(value, @hook_limit_keys, path ++ [key])

        %{
          rate: required_number!(limit, "rate", path ++ [key]),
          burst: required_int!(limit, "burst", path ++ [key])
        }
    end
  end

  # `sources.default` only. Absent leaves core's 100 MB/s; an explicit
  # `default: null` switches the ceiling off.
  defp rate_limit_sources(rate_limits, path) do
    case rate_limits["sources"] do
      nil ->
        nil

      value ->
        sources = section!(value, @rate_limit_sources_keys, path ++ ["sources"])

        case Map.fetch(sources, "default") do
          :error -> nil
          {:ok, nil} -> %{default: nil}
          {:ok, limit} -> %{default: source_limit!(limit, path ++ ["sources", "default"])}
        end
    end
  end

  defp source_limit!(value, path) do
    limit = section!(value, @source_limit_keys, path)

    %{bytes_per_second: required_number!(limit, "bytes_per_second", path)}
    |> put_key(:burst_bytes, int_opt(limit, "burst_bytes", path))
  end

  defp put_key(map, _key, nil), do: map
  defp put_key(map, key, value), do: Map.put(map, key, value)

  # ── quarantine ──────────────────────────────────────────────────────────────

  # Types only; ranges are core's (`Ankusa.Edge.Quarantine.validate_config!/1`),
  # so `check-config` and an embedded boot fail with the same message.
  defp quarantine_section(doc) do
    path = ["quarantine"]
    quarantine = section!(doc, "quarantine", @quarantine_keys, [])

    [
      quarantine:
        []
        |> put_opt(:burst, int_opt(quarantine, "burst", path))
        |> put_opt(:rate, number_opt(quarantine, "rate", path))
        |> put_opt(:max_bytes, int_opt(quarantine, "max_bytes", path))
        |> put_opt(:retention_days, int_opt(quarantine, "retention_days", path))
    ]
  end

  # ── egress.pools ────────────────────────────────────────────────────────────

  # Types only; the ranges are core's (`Ankusa.Config`), so `check-config` and an
  # embedded boot fail with the same message.
  defp egress_pools(egress) do
    path = ["egress", "pools"]
    pools = section!(egress, "pools", @egress_pools_keys, ["egress"])

    []
    |> put_opt(:pool_size, int_opt(pools, "pool_size", path))
    |> put_opt(:pool_count, int_opt(pools, "pool_count", path))
    |> put_opt(:pool_timeout, int_opt(pools, "pool_timeout", path))
    |> put_opt(:pool_max_idle_ms, int_opt(pools, "pool_max_idle_ms", path))
    |> put_opt(:conn_max_idle_ms, int_opt(pools, "conn_max_idle_ms", path))
    |> put_opt(:max_pools, int_opt(pools, "max_pools", path))
    |> put_opt(:max_pools_per_tenant, int_opt(pools, "max_pools_per_tenant", path))
    |> put_opt(:protocols, protocols_opt(pools, path))
  end

  # `http1` and `http2`, a list or one comma-separated string; the closed list is
  # checked before any atom is made.
  defp protocols_opt(pools, path) do
    case pools["protocols"] do
      nil ->
        nil

      value ->
        value
        |> string_list!(path ++ ["protocols"])
        |> Enum.map(&String.to_existing_atom(enum!(&1, ~w(http1 http2), path ++ ["protocols"])))
    end
  end

  # ── audit ───────────────────────────────────────────────────────────────────

  # Types only; the range is core's (`Ankusa.Config`), so `check-config` and an
  # embedded boot fail with the same message.
  defp audit_section(doc) do
    path = ["audit"]
    audit = section!(doc, "audit", @audit_keys, [])

    [
      audit:
        []
        |> put_opt(:retention_days, int_opt(audit, "retention_days", path))
        |> put_opt(:max_rows, int_opt(audit, "max_rows", path))
        |> put_opt(:max_bytes, int_opt(audit, "max_bytes", path))
        |> put_opt(:outbox_max_backlog, int_opt(audit, "outbox_max_backlog", path))
    ]
  end

  # ── queue (dlq, rocksdb) and archive ────────────────────────────────────────

  # Types only; the ranges are core's (`Ankusa.Config`).
  defp queue_section(doc) do
    queue = section!(doc, "queue", @queue_keys, [])
    dlq = section!(queue, "dlq", @dlq_keys, ["queue"])

    [
      queue:
        [
          dlq:
            []
            |> put_opt(:retention_days, int_opt(dlq, "retention_days", ["queue", "dlq"]))
            |> put_opt(:max_rows, int_opt(dlq, "max_rows", ["queue", "dlq"]))
            |> put_opt(:max_bytes, int_opt(dlq, "max_bytes", ["queue", "dlq"])),
          rocksdb: rocksdb_opts(queue)
        ]
        |> put_opt(:max_oldest_due_ms, int_opt(queue, "max_oldest_due_ms", ["queue"]))
    ]
  end

  defp rocksdb_opts(queue) do
    path = ["queue", "rocksdb"]
    rocksdb = section!(queue, "rocksdb", @rocksdb_keys, ["queue"])
    buffers_path = path ++ ["write_buffer_bytes"]
    sizes = section!(rocksdb, "write_buffer_bytes", Map.keys(@write_buffer_families), path)

    buffers =
      Enum.reduce(@write_buffer_families, [], fn {name, cf}, acc ->
        put_opt(acc, cf, int_opt(sizes, name, buffers_path))
      end)

    []
    |> put_opt(:block_cache_bytes, int_opt(rocksdb, "block_cache_bytes", path))
    |> put_opt(:bloom_bits_per_key, int_opt(rocksdb, "bloom_bits_per_key", path))
    |> put_opt(:max_open_files, int_opt(rocksdb, "max_open_files", path))
    |> put_opt(
      :compaction_style,
      atom_enum_opt(rocksdb, "compaction_style", ~w(level universal), path)
    )
    |> put_opt(:rate_limiter_bytes_per_sec, int_opt(rocksdb, "rate_limiter_bytes_per_sec", path))
    |> put_opt(:max_background_jobs, int_opt(rocksdb, "max_background_jobs", path))
    |> put_opt(:write_buffer_bytes, if(buffers == [], do: nil, else: buffers))
  end

  defp archive_section(doc) do
    archive = section!(doc, "archive", @archive_keys, [])
    [archive: [] |> put_opt(:retention_days, int_opt(archive, "retention_days", ["archive"]))]
  end

  # ── tenants ─────────────────────────────────────────────────────────────────

  # Types only; the range is core's (`Ankusa.Config`).
  defp tenants_section(doc) do
    path = ["tenants"]
    tenants = section!(doc, "tenants", @tenants_keys, [])

    [tenants: [] |> put_opt(:max, int_opt(tenants, "max", path))]
  end

  # ── egress ──────────────────────────────────────────────────────────────────

  # The keys the file sets; `Ankusa.Config.new/1` merges them over the defaults.
  defp egress_given(doc) do
    egress = section!(doc, "egress", @egress_keys, [])
    path = ["egress"]

    []
    |> put_opt(:allow_insecure_http, bool_opt(egress, "allow_insecure_http", path))
    |> put_opt(:deny_cidrs, egress_list(egress, "deny_cidrs", path))
    |> put_opt(:allow_cidrs, egress_list(egress, "allow_cidrs", path))
    |> put_opt(:allow_hosts, egress_list(egress, "allow_hosts", path))
    |> put_opt(:deny_hosts, egress_list(egress, "deny_hosts", path))
  end

  # Unlike `string_list!/2`, an empty list is meaningful here (`allow_cidrs: []`,
  # or `deny_cidrs: []` to take the deny list away).
  defp egress_list(egress, key, path) do
    case egress[key] do
      nil -> nil
      [] -> []
      value -> string_list!(value, path ++ [key])
    end
  end

  # `egress.pools` is core's, not part of the policy the sinks' URLs are judged
  # by: it goes to `Ankusa.Config` only, which merges it over its defaults.
  defp egress_section(doc) do
    given = Map.new(egress_given(doc))

    case egress_pools(section!(doc, "egress", @egress_keys, [])) do
      [] -> [egress: given]
      pools -> [egress: Map.put(given, :pools, Map.new(pools))]
    end
  end

  defp egress_policy!(doc) do
    Map.merge(Ankusa.HttpClient.Egress.default_policy(), Map.new(egress_given(doc)))
  end

  # `https` only, unless `egress.allow_insecure_http`. Judged on the URL's shape,
  # not its address: a name resolves at delivery, where the egress policy decides.
  # The message never echoes the URL, which may carry credentials.
  defp check_sink_schemes!(sinks, egress, path) do
    sinks
    |> Enum.with_index()
    |> Enum.each(fn
      {{Ankusa.Sink.Http, opts}, index} ->
        check_sink_scheme!(opts[:url], egress, path ++ ["sinks", index, "url"])

      {_other, _index} ->
        :ok
    end)
  end

  defp check_sink_scheme!(url, egress, path) do
    case URI.parse(url) do
      %URI{scheme: scheme, host: host}
      when is_binary(scheme) and is_binary(host) and host != "" ->
        case String.downcase(scheme) do
          "https" ->
            :ok

          "http" when egress.allow_insecure_http ->
            :ok

          "http" ->
            raise ConfigError,
              message:
                "#{render_path(path)}: must be an https:// URL " <>
                  "(plain http needs egress.allow_insecure_http: true)"

          _other ->
            raise ConfigError, message: "#{render_path(path)}: must be an http(s) URL"
        end

      _no_host ->
        raise ConfigError, message: "#{render_path(path)}: must be an absolute http(s) URL"
    end
  end

  defp validate_http_sink!(opts, path) do
    Ankusa.Sink.Http.validate_opts!(opts)
  rescue
    error in ArgumentError ->
      reraise ConfigError, [message: "#{render_path(path)}: #{error.message}"], __STACKTRACE__
  end

  # ── sources ─────────────────────────────────────────────────────────────────

  # A URL with no `type` means Redis, as for `routes.store`: a file that sets
  # one has said it wants sources shared, and the other types have no URL.
  defp source_store_section(doc, egress) do
    sources = sources!(doc, egress)
    store = section!(doc, "source_store", @source_store_keys, [])
    path = ["source_store"]
    decoder = &AnkusaServer.Config.source_from_map!(&1, &2, egress)

    type =
      enum!(
        store["type"] || (store["url"] && "redis") || "static",
        ~w(static persistent redis),
        path ++ ["type"]
      )

    case type do
      "static" ->
        reject_redis_only_keys!(store, path)
        [source_store: {Ankusa.SourceStore.Static, sources: sources}]

      "persistent" ->
        reject_redis_only_keys!(store, path)
        [source_store: {Ankusa.SourceStore.Persistent, sources: sources, decoder: decoder}]

      "redis" ->
        opts =
          [url: required_string!(store, "url", path), sources: sources, decoder: decoder]
          |> put_opt(:namespace, string_opt(store, "namespace", path))
          |> put_opt(:tick_ms, int_opt(store, "tick_ms", path))
          |> put_opt(:timeout_ms, int_opt(store, "timeout_ms", path))
          |> put_opt(:load_timeout_ms, int_opt(store, "load_timeout_ms", path))

        [source_store: {Ankusa.SourceStore.Redis, opts}]
    end
  end

  # Lifecycle events are delivered to the same kind of sinks a source's hooks
  # are, so the sink walker is shared: every sink type, its key check, and its
  # error message carry over to `lifecycle.sinks` unchanged.
  defp lifecycle_section(doc, egress) do
    case doc["lifecycle"] do
      nil ->
        []

      _ ->
        lifecycle = section!(doc, "lifecycle", @lifecycle_keys, [])
        sinks = sinks!(lifecycle, ["lifecycle"])
        check_sink_schemes!(sinks, egress, ["lifecycle"])
        [lifecycle: %{sinks: sinks}]
    end
  end

  defp sources!(doc, egress) do
    case doc["sources"] do
      nil ->
        warn_no_sources()
        %{}

      sources when is_map(sources) ->
        if sources == %{}, do: warn_no_sources()

        Map.new(sources, fn {source_id, source} ->
          {source_id, source_opts!(source, ["sources", source_id], egress)}
        end)

      other ->
        raise ConfigError,
          message: type_error(["sources"], "a mapping of source_id to source", other)
    end
  end

  defp warn_no_sources,
    do: Logger.warning("[ankusa] no sources configured; every POST will return 404")

  @doc """
  Translate one source spec — the map an admin API client submits, the same keys
  as a YAML `sources.<id>` entry — into `Ankusa.Source.new/2` options.

  This is the decoder `Ankusa.SourceStore.Persistent` runs over every write, so
  validation is identical to the file's: a bad spec raises `AnkusaServer.ConfigError`
  with the same dotted-path message the YAML walker produces.
  """
  @spec source_from_map!(String.t(), map()) :: keyword()
  def source_from_map!(source_id, spec),
    do: source_from_map!(source_id, spec, Ankusa.HttpClient.Egress.default_policy())

  @doc false
  # The decoder the image gives the source stores: `source_from_map!/2` under the
  # configured egress policy, so an API-submitted `http://` sink is refused when
  # it is written, with the same message the file gets.
  @spec source_from_map!(String.t(), map(), Ankusa.HttpClient.Egress.policy()) :: keyword()
  def source_from_map!(source_id, spec, egress) do
    source_opts!(spec, ["sources", source_id], egress)
  end

  defp source_opts!(source, path, egress) do
    source = section!(source, @source_keys, path)

    opts =
      []
      |> put_opt(:tenant_id, string_opt(source, "tenant", path))
      |> put_opt(:on_verify_failure, atom_enum_opt(source, "on_verify_failure", @policies, path))
      |> put_opt(:verifier, verifier(source, path))
      |> Keyword.put(:sinks, sinks!(source, path))
      |> put_opt(:dedupe, dedupe(source, path))
      |> put_opt(:forward_headers, forward_headers(source, path))
      |> put_opt(:max_headers, positive_int_opt(source, "max_headers", path))
      |> put_opt(
        :forward_headers_max_bytes,
        positive_int_opt(source, "forward_headers_max_bytes", path)
      )
      |> put_opt(:store_headers, store_headers(source, path))
      |> put_opt(:trust_url_tenant, bool_opt(source, "trust_url_tenant", path))
      |> put_opt(:retry, source_retry(source, path))

    check_sink_schemes!(opts[:sinks], egress, path)
    opts
  end

  # `nil`, a preset name (`github`, `stripe`, …), or a map with exactly one of
  # preset/header/json and an optional `ttl_seconds`. Returns a value
  # `Ankusa.Dedupe.new!/1` accepts: a preset atom, or a `ttl_ms`-carrying map.
  defp dedupe(source, path) do
    case source["dedupe"] do
      nil ->
        nil

      preset when is_binary(preset) ->
        enum!(preset, Map.keys(@dedupe_presets), path ++ ["dedupe"])
        Map.fetch!(@dedupe_presets, preset)

      value ->
        dedupe_section!(value, path ++ ["dedupe"])
    end
  end

  defp dedupe_section!(value, path) do
    spec = section!(value, @dedupe_keys, path)

    from =
      case {spec["preset"], spec["header"], spec["json"]} do
        {nil, nil, nil} ->
          raise ConfigError,
            message: "#{render_path(path)}: set exactly one of preset, header, json"

        {preset, nil, nil} ->
          {:preset,
           Map.fetch!(
             @dedupe_presets,
             enum!(preset, Map.keys(@dedupe_presets), path ++ ["preset"])
           )}

        {nil, _header, nil} ->
          {:header, string_opt(spec, "header", path)}

        {nil, nil, _json} ->
          {:json, string_opt(spec, "json", path)}

        _other ->
          raise ConfigError,
            message: "#{render_path(path)}: set exactly one of preset, header, json"
      end

    # A blank header or dot path would pass `new!/1`'s pattern but silently
    # never dedupe: refuse it here with the config's own error shape.
    case from do
      {:header, ""} ->
        raise ConfigError, message: "#{render_path(path ++ ["header"])}: must not be empty"

      {:json, ""} ->
        raise ConfigError, message: "#{render_path(path ++ ["json"])}: must not be empty"

      _ ->
        :ok
    end

    ttl_ms =
      case int_opt(spec, "ttl_seconds", path) do
        nil ->
          nil

        s when s > 0 ->
          {:ttl_ms, s * 1_000}

        s ->
          raise ConfigError,
            message: "#{render_path(path ++ ["ttl_seconds"])}: must be > 0, got #{s}"
      end

    [from, ttl_ms] |> Enum.reject(&is_nil/1) |> Map.new()
  end

  defp forward_headers(source, path) do
    case source["forward_headers"] do
      nil ->
        nil

      headers when is_list(headers) ->
        Enum.map(headers, fn header ->
          if is_binary(header) and header != "" do
            String.downcase(header)
          else
            raise ConfigError,
              message: type_error(path ++ ["forward_headers"], "a list of header names", headers)
          end
        end)

      other ->
        raise ConfigError,
          message: type_error(path ++ ["forward_headers"], "a list of header names", other)
    end
  end

  # Like `int_opt/3`, for keys that name a size or a count: zero and below are
  # a misconfiguration, refused with the key's dotted path.
  defp positive_int_opt(map, key, path) do
    case int_opt(map, key, path) do
      nil ->
        nil

      n when n > 0 ->
        n

      n ->
        raise ConfigError, message: "#{render_path(path ++ [key])}: must be > 0, got #{n}"
    end
  end

  # `nil` (absent), or `%{drop: [...], hash: [...]}` with each key optional: an
  # absent key inherits (the default list for `http`, the instance's for a
  # source). Names are lowercased; `Ankusa.Edge.StoredHeaders` re-validates.
  defp store_headers(section, path) do
    case section["store_headers"] do
      nil ->
        nil

      value ->
        path = path ++ ["store_headers"]
        spec = section!(value, @store_headers_keys, path)

        for {key, names} <- spec, into: %{} do
          {Map.fetch!(@store_headers_fields, key), header_names!(names, path ++ [key])}
        end
    end
  end

  defp header_names!(names, path) do
    if is_list(names) and Enum.all?(names, &(is_binary(&1) and &1 != "")) do
      Enum.map(names, &String.downcase/1)
    else
      raise ConfigError, message: type_error(path, "a list of header names", names)
    end
  end

  defp verifier(source, path) do
    path = path ++ ["verify"]

    case expect_map(source["verify"], path) do
      verify when map_size(verify) == 0 ->
        nil

      verify ->
        type = enum!(required_string!(verify, "type", path), @verify_types, path ++ ["type"])

        # Typo rejection: the key set is fixed per type (named schemes take
        # only `secret`/`tolerance_seconds`; `hmac` takes the descriptor keys).
        check_keys!(verify, verify_keys(type), path)

        verifier_for(type, verify, path)
    end
  end

  # The replay window, in seconds, from 0 to `Ankusa.Verifier.max_tolerance_seconds/0`
  # (an hour). Core refuses the same range at boot; checking here names the YAML key.
  defp tolerance_opt(verify, path) do
    max = Ankusa.Verifier.max_tolerance_seconds()

    case int_opt(verify, "tolerance_seconds", path) do
      nil ->
        nil

      seconds when seconds >= 0 and seconds <= max ->
        seconds

      seconds ->
        raise ConfigError,
          message:
            "#{render_path(path ++ ["tolerance_seconds"])}: must be between 0 and " <>
              "#{max} seconds, got #{seconds}"
    end
  end

  defp verifier_for("standard_webhooks_asym", verify, path),
    do: asymmetric_verifier(:standard_webhooks_asym, verify, path)

  defp verifier_for("asymmetric", verify, path),
    do: asymmetric_verifier(asymmetric_scheme(verify, path), verify, path)

  defp verifier_for("token", verify, path), do: token_verifier(verify, path)
  defp verifier_for("client_cert", verify, path), do: client_cert_verifier(verify, path)

  defp verifier_for(type, verify, path) do
    opts =
      []
      |> put_opt(:secret, secret_opt(verify, path))
      |> put_opt(:tolerance, tolerance_opt(verify, path))

    {verify_module(type), verify_opts(type, verify, path, opts)}
  end

  # The provider's public key, or keys newest first. Each is checked against the
  # scheme's algorithm here, so a bad one names its YAML key. `/v1/config` shows
  # it as `[REDACTED]` like any adapter option not on the allowlist: a private
  # key pasted by mistake must not be echoed back.
  defp asymmetric_verifier(scheme, verify, path) do
    value = verify["public_key"]

    if is_nil(value),
      do: raise(ConfigError, message: "#{render_path(path)}: missing required key \"public_key\"")

    keys = secret!(value, path ++ ["public_key"])
    non_empty_secret!(keys, path ++ ["public_key"])

    keys
    |> List.wrap()
    |> Enum.with_index()
    |> Enum.each(fn {key, index} ->
      case Ankusa.Verifier.Asymmetric.validate_opts(scheme: scheme, public_key: key) do
        :ok ->
          :ok

        {:error, _reason} ->
          segments = if is_list(keys), do: ["public_key", index], else: ["public_key"]

          raise ConfigError,
            message:
              "#{render_path(path ++ segments)}: not a public key for the scheme's " <>
                "algorithm (whpk_<base64> or the base64 of 32 bytes for ed25519; a PEM " <>
                "PUBLIC KEY, or an RSA PUBLIC KEY of at least 2048 bits, for rsa_sha256)"
      end
    end)

    opts = put_opt([scheme: scheme, public_key: keys], :tolerance, tolerance_opt(verify, path))
    {Ankusa.Verifier.Asymmetric, opts}
  end

  # The string is checked against a closed list first (`enum!/3`), so the atoms are a fixed set.
  # sobelow_skip ["DOS.StringToAtom"]
  defp asymmetric_scheme(verify, path) do
    timestamp_header = string_opt(verify, "timestamp_header", path)

    scheme = %Ankusa.Verifier.Asymmetric.Scheme{
      signature_header: required_string!(verify, "signature_header", path),
      parse:
        String.to_atom(
          enum!(string_opt(verify, "parse", path) || "whole", @scheme_parses, path ++ ["parse"])
        ),
      sig_prefix: string_opt(verify, "sig_prefix", path),
      sig_key: string_opt(verify, "sig_key", path),
      version: string_opt(verify, "version", path),
      signed: string_opt(verify, "signed", path) || "{body}",
      algorithm:
        String.to_atom(
          enum!(
            string_opt(verify, "algorithm", path) || "ed25519",
            @scheme_algorithms,
            path ++ ["algorithm"]
          )
        ),
      encoding:
        String.to_atom(
          enum!(
            string_opt(verify, "encoding", path) || "base64",
            @scheme_encodings,
            path ++ ["encoding"]
          )
        ),
      timestamp: (timestamp_header && {:header, timestamp_header}) || nil
    }

    Ankusa.Verifier.Schemes.validate!(scheme)
  end

  defp token_verifier(verify, path) do
    required_secret!(verify, path)
    opts = [secret: secret_opt(verify, path), location: token_location(verify, path)]

    case Ankusa.Verifier.Token.validate_opts(opts) do
      :ok ->
        {Ankusa.Verifier.Token, opts}

      {:error, _reason} ->
        raise ConfigError,
          message: "#{render_path(path ++ ["secret"])}: a basic secret must be user:password"
    end
  end

  # `header` names the header of `location: header`, `param` the query parameter
  # of `location: query`; each is required there and refused anywhere else.
  defp token_location(verify, path) do
    location =
      enum!(
        string_opt(verify, "location", path) || "bearer",
        @token_locations,
        path ++ ["location"]
      )

    case location do
      "bearer" -> token_name!(verify, nil, path, :bearer)
      "basic" -> token_name!(verify, nil, path, :basic)
      "header" -> token_name!(verify, "header", path, :header)
      "query" -> token_name!(verify, "param", path, :query)
    end
  end

  defp token_name!(verify, key, path, location) do
    for extra <- ~w(header param), extra != key, Map.has_key?(verify, extra) do
      raise ConfigError,
        message:
          "#{render_path(path)}: unknown key \"#{extra}\" for location #{inspect(to_string(location))}"
    end

    case key do
      nil ->
        location

      key ->
        case required_string!(verify, key, path) do
          "" -> raise ConfigError, message: "#{render_path(path ++ [key])}: must not be empty"
          name -> {location, name}
        end
    end
  end

  defp client_cert_verifier(verify, path) do
    opts =
      []
      |> put_opt(:fingerprints, fingerprints(verify, path))
      |> put_opt(:subjects, subjects(verify, path))

    if opts == [] do
      raise ConfigError,
        message: "#{render_path(path)}: client_cert needs \"fingerprints\" or \"subjects\""
    end

    {Ankusa.Verifier.ClientCert, opts}
  end

  # Normalised (lowercase hex, no `:`), so `/v1/config` and the verifier agree.
  defp fingerprints(verify, path) do
    case verify["fingerprints"] do
      nil ->
        nil

      value ->
        path = path ++ ["fingerprints"]

        value
        |> string_list!(path)
        |> Enum.with_index()
        |> Enum.map(fn {fingerprint, index} ->
          case Ankusa.Verifier.ClientCert.normalize_fingerprint(fingerprint) do
            {:ok, hex} ->
              hex

            :error ->
              raise ConfigError,
                message:
                  "#{render_path(path ++ [index])}: expected a SHA-256 fingerprint " <>
                    "(64 hex characters, optional : separators), got #{inspect(fingerprint)}"
          end
        end)
    end
  end

  defp subjects(verify, path) do
    case verify["subjects"] do
      nil ->
        nil

      value ->
        path = path ++ ["subjects"]
        subjects = string_list!(value, path)

        subjects
        |> Enum.with_index()
        |> Enum.each(fn {subject, index} ->
          if Ankusa.Verifier.ClientCert.validate_opts(subjects: [subject]) != :ok do
            raise ConfigError,
              message:
                "#{render_path(path ++ [index])}: expected dns:<name>, email:<address>, " <>
                  "uri:<uri>, ip:<address> or cn:<common name>, got #{inspect(subject)}"
          end
        end)

        subjects
    end
  end

  defp verify_keys("hmac"), do: @verify_hmac_keys
  defp verify_keys("standard_webhooks_asym"), do: @verify_asym_preset_keys
  defp verify_keys("asymmetric"), do: @verify_asym_keys
  defp verify_keys("token"), do: @verify_token_keys
  defp verify_keys("client_cert"), do: @verify_client_cert_keys
  defp verify_keys(_), do: @verify_keys

  defp verify_module("none"), do: Ankusa.Verifier.None
  defp verify_module(_), do: Ankusa.Verifier.Hmac

  defp verify_opts("none", _verify, _path, opts), do: opts

  # The string is checked against a closed list first (`enum!/3`), so the atoms are a fixed set.
  # sobelow_skip ["DOS.StringToAtom"]
  defp verify_opts(type, verify, path, opts) do
    # The signature verifiers are useless without a secret, and a silently-empty
    # HMAC key would accept everything an attacker signs.
    required_secret!(verify, path)

    scheme =
      case type do
        "hmac" -> hmac_scheme(verify, path)
        _ -> String.to_atom(type)
      end

    [{:scheme, scheme} | opts]
  end

  # `secret` is a string, or a list of strings for a rotation window (newest
  # first; `Ankusa.Verifier.Hmac` tries every one).
  defp secret_opt(verify, path) do
    case verify["secret"] do
      nil -> nil
      value -> secret!(value, path ++ ["secret"])
    end
  end

  # An empty string or element is refused, never dropped: `${OLD:-}` for an
  # unset rotation slot is the easy way to write an empty HMAC key.
  defp required_secret!(verify, path) do
    case verify["secret"] do
      nil ->
        raise ConfigError, message: "#{render_path(path)}: missing required key \"secret\""

      value ->
        value |> secret!(path ++ ["secret"]) |> non_empty_secret!(path ++ ["secret"])
    end
  end

  defp non_empty_secret!(secrets, path) when is_list(secrets) do
    secrets
    |> Enum.with_index()
    |> Enum.each(fn {secret, i} -> non_empty_secret!(secret, path ++ [i]) end)
  end

  defp non_empty_secret!("", path),
    do: raise(ConfigError, message: "#{render_path(path)}: must not be empty")

  defp non_empty_secret!(_secret, _path), do: :ok

  defp secret!(secret, _path) when is_binary(secret), do: secret

  defp secret!([_ | _] = secrets, path) when length(secrets) <= @max_secrets do
    if Enum.all?(secrets, &is_binary/1) do
      secrets
    else
      raise ConfigError, message: type_error(path, "a string or a list of strings", secrets)
    end
  end

  defp secret!(secrets, path) when is_list(secrets) and secrets != [] do
    raise ConfigError,
      message: "#{render_path(path)}: at most #{@max_secrets} secrets, got #{length(secrets)}"
  end

  defp secret!(value, path),
    do: raise(ConfigError, message: type_error(path, "a string or a list of strings", value))

  # The string is checked against a closed list first (`enum!/3`), so the atoms are a fixed set.
  # sobelow_skip ["DOS.StringToAtom"]
  defp hmac_scheme(verify, path) do
    timestamp_header = string_opt(verify, "timestamp_header", path)

    scheme = %Ankusa.Verifier.Hmac.Scheme{
      signature_header: required_string!(verify, "signature_header", path),
      parse:
        String.to_atom(
          enum!(string_opt(verify, "parse", path) || "whole", @scheme_parses, path ++ ["parse"])
        ),
      sig_prefix: string_opt(verify, "sig_prefix", path),
      sig_key: string_opt(verify, "sig_key", path),
      version: string_opt(verify, "version", path),
      signed: string_opt(verify, "signed", path) || "{body}",
      hash:
        String.to_atom(
          enum!(
            string_opt(verify, "hash", path) || "sha256",
            @scheme_hashes,
            path ++ ["hash"]
          )
        ),
      encoding:
        String.to_atom(
          enum!(
            string_opt(verify, "encoding", path) || "hex",
            @scheme_encodings,
            path ++ ["encoding"]
          )
        ),
      secret_decode:
        String.to_atom(
          enum!(
            string_opt(verify, "secret_decode", path) || "raw",
            @scheme_secret_decodes,
            path ++ ["secret_decode"]
          )
        ),
      timestamp: (timestamp_header && {:header, timestamp_header}) || nil
    }

    Ankusa.Verifier.Schemes.validate!(scheme)
  end

  defp sinks!(source, path) do
    case source["sinks"] do
      list when is_list(list) and list != [] ->
        list
        |> Enum.with_index()
        |> Enum.map(fn {sink, index} -> sink!(sink, path ++ ["sinks", index]) end)

      [] ->
        raise ConfigError, message: "#{render_path(path ++ ["sinks"])}: must not be empty"

      nil ->
        raise ConfigError,
          message: "#{render_path(path ++ ["sinks"])}: missing required key \"sinks\""

      other ->
        raise ConfigError, message: type_error(path ++ ["sinks"], "a list of sinks", other)
    end
  end

  defp sink!(sink, path) do
    sink = expect_map(sink, path)
    type = enum!(required_string!(sink, "type", path), @sink_types, path ++ ["type"])

    case type do
      "log" ->
        check_keys!(sink, @log_sink_keys, path)
        {Ankusa.Sink.Log, []}

      "http" ->
        check_keys!(sink, @http_sink_keys, path)

        secret =
          case sink["secret"] do
            nil ->
              nil

            value ->
              secrets = secret!(value, path ++ ["secret"])
              non_empty_secret!(secrets, path ++ ["secret"])
              secrets
          end

        opts =
          [url: required_string!(sink, "url", path)]
          |> put_opt(:method, atom_enum_opt(sink, "method", ~w(post put patch), path))
          |> put_opt(:headers, headers(sink["headers"], path ++ ["headers"]))
          |> put_opt(:timeout_ms, int_opt(sink, "timeout_ms", path))
          |> put_opt(:secret, secret)
          |> put_opt(:max_response_bytes, int_opt(sink, "max_response_bytes", path))
          |> put_opt(:cacertfile, string_opt(sink, "cacertfile", path))
          |> put_opt(:allow_insecure_tls, bool_opt(sink, "allow_insecure_tls", path))
          |> put_opt(:allow_unsigned, bool_opt(sink, "allow_unsigned", path))

        validate_http_sink!(opts, path)
        {Ankusa.Sink.Http, opts}

      "rabbitmq" ->
        check_keys!(sink, @rabbitmq_sink_keys, path)

        {Ankusa.Sink.RabbitMQ,
         [exchange: required_string!(sink, "exchange", path)]
         |> put_opt(:url, string_opt(sink, "url", path))
         |> put_opt(:exchange_type, atom_enum_opt(sink, "exchange_type", @exchange_types, path))
         |> put_opt(:routing_key, string_opt(sink, "routing_key", path))
         |> put_opt(:inline_max_bytes, int_opt(sink, "inline_max_bytes", path))
         |> put_opt(:max_message_bytes, positive_int_opt(sink, "max_message_bytes", path))
         |> put_opt(:max_inflight, int_opt(sink, "max_inflight", path))
         |> put_opt(:idle_timeout_ms, int_opt(sink, "idle_timeout_ms", path))}

      "kafka" ->
        check_keys!(sink, @kafka_sink_keys, path)

        {Ankusa.Sink.Kafka,
         [
           brokers: string_list!(sink["brokers"], path ++ ["brokers"]),
           topic: required_string!(sink, "topic", path)
         ]
         |> put_opt(:key, string_opt(sink, "key", path))
         |> put_opt(:inline_max_bytes, int_opt(sink, "inline_max_bytes", path))
         |> put_opt(:max_record_bytes, int_opt(sink, "max_record_bytes", path))
         |> put_opt(:ssl, bool_opt(sink, "ssl", path))
         |> put_opt(:sasl, sasl(sink["sasl"], path ++ ["sasl"]))
         |> put_opt(:idle_timeout_ms, int_opt(sink, "idle_timeout_ms", path))}

      "nats" ->
        check_keys!(sink, @nats_sink_keys, path)

        connection =
          [
            servers: string_list!(sink["servers"], path ++ ["servers"]),
            subject: required_string!(sink, "subject", path)
          ]
          |> put_opt(:inline_max_bytes, int_opt(sink, "inline_max_bytes", path))
          |> put_opt(:max_message_bytes, positive_int_opt(sink, "max_message_bytes", path))
          |> put_opt(:publish_timeout_ms, int_opt(sink, "publish_timeout_ms", path))
          |> put_opt(:tls, bool_opt(sink, "tls", path))
          |> put_opt(:idle_timeout_ms, int_opt(sink, "idle_timeout_ms", path))

        {Ankusa.Sink.NATS, connection ++ nats_auth(sink["auth"], path ++ ["auth"])}

      "redis" ->
        check_keys!(sink, @redis_sink_keys, path)

        {Ankusa.Sink.Redis,
         [
           url: required_string!(sink, "url", path),
           channel: required_string!(sink, "channel", path)
         ]
         |> put_opt(:inline_max_bytes, int_opt(sink, "inline_max_bytes", path))
         |> put_opt(:max_message_bytes, positive_int_opt(sink, "max_message_bytes", path))
         |> put_opt(:publish_timeout_ms, int_opt(sink, "publish_timeout_ms", path))
         |> put_opt(:idle_timeout_ms, int_opt(sink, "idle_timeout_ms", path))}

      "sqs" ->
        check_keys!(sink, @sqs_sink_keys, path)

        opts =
          [
            queue_url: required_string!(sink, "queue_url", path),
            region: required_string!(sink, "region", path)
          ]
          |> put_opt(:endpoint, string_opt(sink, "endpoint", path))
          |> put_opt(:message_group_id, string_opt(sink, "message_group_id", path))
          |> put_opt(:inline_max_bytes, int_opt(sink, "inline_max_bytes", path))
          |> put_opt(:max_message_bytes, int_opt(sink, "max_message_bytes", path))
          |> put_opt(:timeout_ms, int_opt(sink, "timeout_ms", path))
          |> put_opt(:access_key_id, string_opt(sink, "access_key_id", path))
          |> put_opt(:secret_access_key, string_opt(sink, "secret_access_key", path))
          |> put_opt(:session_token, string_opt(sink, "session_token", path))
          |> Kernel.++(batch_opts(sink, path))

        validate_batch_sink!(Ankusa.Sink.SQS, opts, path)
        {Ankusa.Sink.SQS, opts}

      "google_pubsub" ->
        check_keys!(sink, @google_pubsub_sink_keys, path)

        opts =
          ([
             project: required_string!(sink, "project", path),
             topic: required_string!(sink, "topic", path)
           ]
           |> put_opt(:endpoint, string_opt(sink, "endpoint", path))
           |> put_opt(:ordering_key, string_opt(sink, "ordering_key", path))
           |> put_opt(:inline_max_bytes, int_opt(sink, "inline_max_bytes", path))
           |> put_opt(:max_message_bytes, int_opt(sink, "max_message_bytes", path))
           |> put_opt(:timeout_ms, int_opt(sink, "timeout_ms", path))) ++
            batch_opts(sink, path) ++ gcp_auth!(sink, path)

        validate_batch_sink!(Ankusa.Sink.GooglePubSub, opts, path)
        {Ankusa.Sink.GooglePubSub, opts}
    end
  end

  # The batching options of `Ankusa.Sink.SQS` and `Ankusa.Sink.GooglePubSub`
  # (`Ankusa.Sink.Batcher`). Types only here; the ranges are the sink's, checked
  # by `validate_batch_sink!/3`.
  defp batch_opts(sink, path) do
    [
      batch: bool_opt(sink, "batch", path),
      batch_linger_ms: int_opt(sink, "batch_linger_ms", path),
      batch_max_entries: int_opt(sink, "batch_max_entries", path),
      batch_max_bytes: int_opt(sink, "batch_max_bytes", path),
      batch_queue_max: int_opt(sink, "batch_queue_max", path),
      batch_queue_max_bytes: int_opt(sink, "batch_queue_max_bytes", path),
      batch_queue_tenant_max: int_opt(sink, "batch_queue_tenant_max", path),
      batch_max_inflight: int_opt(sink, "batch_max_inflight", path)
    ]
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end

  # A batch option out of range fails at load, naming the sink, like a bad
  # `Sink.Http` (`validate_http_sink!/2`), not at the first delivery.
  defp validate_batch_sink!(mod, opts, path) do
    mod.validate_opts!(opts)
  rescue
    error in ArgumentError ->
      reraise ConfigError, [message: "#{render_path(path)}: #{error.message}"], __STACKTRACE__
  end

  defp headers(nil, _path), do: nil

  defp headers(headers, path) when is_map(headers) do
    Enum.map(headers, fn {key, value} -> {to_string(key), expect_string(value, path ++ [key])} end)
  end

  defp headers(other, path), do: raise(ConfigError, message: type_error(path, "a mapping", other))

  defp sasl(nil, _path), do: nil

  defp sasl(sasl, path) do
    sasl = section!(sasl, @sasl_keys, path)
    required_string!(sasl, "mechanism", path)

    {atom_enum_opt(sasl, "mechanism", ~w(plain scram_sha_256 scram_sha_512), path),
     required_string!(sasl, "username", path), required_string!(sasl, "password", path)}
  end

  defp nats_auth(nil, _path), do: []

  defp nats_auth(auth, path) do
    auth = section!(auth, @nats_auth_keys, path)

    opts =
      []
      |> put_opt(:username, string_opt(auth, "username", path))
      |> put_opt(:password, string_opt(auth, "password", path))
      |> put_opt(:token, string_opt(auth, "token", path))
      |> put_opt(:nkey_seed, string_opt(auth, "nkey_seed", path))
      |> put_opt(:jwt, string_opt(auth, "jwt", path))

    validate_nats_auth!(opts, path)
    opts
  end

  # gnat sends exactly one scheme, picking username/password, then token, then
  # nkey_seed(+jwt). A file that declares two gets one silently ignored; a lone
  # `username` or a lone `jwt` is ignored outright and connects as nobody. Both
  # would surface as "authorization violation" on the first hook, so they are
  # boot errors instead.
  defp validate_nats_auth!(opts, path) do
    declared =
      [
        {Keyword.has_key?(opts, :username) or Keyword.has_key?(opts, :password),
         "username/password"},
        {Keyword.has_key?(opts, :token), "token"},
        {Keyword.has_key?(opts, :nkey_seed), "nkey_seed"}
      ]
      |> Enum.filter(&elem(&1, 0))
      |> Enum.map(&elem(&1, 1))

    cond do
      length(declared) > 1 ->
        raise ConfigError,
          message:
            "#{render_path(path)}: #{Enum.join(declared, ", ")} are mutually exclusive; " <>
              "declare one scheme"

      Keyword.has_key?(opts, :username) != Keyword.has_key?(opts, :password) ->
        raise ConfigError, message: "#{render_path(path)}: username and password go together"

      Keyword.has_key?(opts, :jwt) and not Keyword.has_key?(opts, :nkey_seed) ->
        raise ConfigError, message: "#{render_path(path)}: jwt requires nkey_seed"

      true ->
        :ok
    end
  end

  # ── schema helpers ──────────────────────────────────────────────────────────

  # A nested mapping of known keys. nil — absent, or present with every child
  # commented out — is an empty section.
  defp section!(parent, key, allowed, path), do: section!(parent[key], allowed, path ++ [key])

  defp section!(value, allowed, path) do
    section = expect_map(value, path)
    check_keys!(section, allowed, path)
    section
  end

  defp check_keys!(map, allowed, path) do
    Enum.each(map, fn {key, _value} ->
      unless key in allowed do
        raise ConfigError, message: "#{render_path(path)}: unknown key \"#{key}\""
      end
    end)
  end

  defp expect_map(nil, _path), do: %{}
  defp expect_map(value, _path) when is_map(value), do: value

  defp expect_map(value, path),
    do: raise(ConfigError, message: type_error(path, "a mapping", value))

  defp expect_string(value, _path) when is_binary(value), do: value

  defp expect_string(value, path),
    do: raise(ConfigError, message: type_error(path, "a string", value))

  defp required_string!(map, key, path) do
    case Map.fetch(map, key) do
      {:ok, value} ->
        expect_string(value, path ++ [key])

      :error ->
        raise ConfigError, message: "#{render_path(path)}: missing required key \"#{key}\""
    end
  end

  defp string_opt(map, key, path) do
    case map[key] do
      nil -> nil
      value -> expect_string(value, path ++ [key])
    end
  end

  defp int_opt(map, key, path) do
    case map[key] do
      nil -> nil
      # Env overrides and YAML quoted values both arrive as strings; a digit
      # string is an integer as far as an operator is concerned.
      value when is_integer(value) -> value
      value when is_binary(value) -> parse_int!(value, path ++ [key])
      value -> raise ConfigError, message: type_error(path ++ [key], "an integer", value)
    end
  end

  # Like `int_opt/3`, for keys that take fractions (`quarantine.rate`).
  defp number_opt(map, key, path) do
    case map[key] do
      nil -> nil
      value when is_number(value) -> value
      value when is_binary(value) -> parse_number!(value, path ++ [key])
      value -> raise ConfigError, message: type_error(path ++ [key], "a number", value)
    end
  end

  defp parse_int!(value, path) do
    case Integer.parse(value) do
      {int, ""} -> int
      _ -> raise ConfigError, message: type_error(path, "an integer", value)
    end
  end

  # A required integer, where absent and explicit-nil are both "you didn't say".
  defp required_int!(map, key, path) do
    case map[key] do
      nil -> raise ConfigError, message: "#{render_path(path)}: missing required key \"#{key}\""
      _value -> int_opt(map, key, path)
    end
  end

  defp required_number!(map, key, path) do
    case map[key] do
      nil ->
        raise ConfigError, message: "#{render_path(path)}: missing required key \"#{key}\""

      value when is_number(value) ->
        value

      # Quoted YAML and `${VAR}` interpolation both arrive as strings.
      value when is_binary(value) ->
        parse_number!(value, path ++ [key])

      value ->
        raise ConfigError, message: type_error(path ++ [key], "a number", value)
    end
  end

  # Integer first, so `"100"` stays an integer; then float, so `"12.5"` does
  # not come back as `12` with a trailing ".5" nobody checked.
  defp parse_number!(value, path) do
    case Integer.parse(value) do
      {int, ""} ->
        int

      _ ->
        case Float.parse(value) do
          {float, ""} -> float
          _ -> raise ConfigError, message: type_error(path, "a number", value)
        end
    end
  end

  defp bool_opt(map, key, path) do
    case Map.fetch(map, key) do
      :error -> nil
      {:ok, value} -> bool!(value, path ++ [key])
    end
  end

  defp bool!(value, _path) when is_boolean(value), do: value

  defp bool!(value, path) when is_binary(value) do
    case String.downcase(value) do
      "true" -> true
      "false" -> false
      _ -> raise ConfigError, message: type_error(path, "a boolean", value)
    end
  end

  defp bool!(value, path), do: raise(ConfigError, message: type_error(path, "a boolean", value))

  defp enum!(value, allowed, path) do
    if value in allowed do
      value
    else
      raise ConfigError,
        message:
          "#{render_path(path)}: unknown value #{inspect(value)}; " <>
            "expected one of #{Enum.join(allowed, ", ")}"
    end
  end

  # The string is checked against a closed list first (`enum!/3`), so the atoms are a fixed set.
  # sobelow_skip ["DOS.StringToAtom"]
  defp atom_enum_opt(map, key, allowed, path) do
    case map[key] do
      nil -> nil
      value -> String.to_atom(enum!(value, allowed, path ++ [key]))
    end
  end

  # A list of strings, or one comma-separated string: the form a single env var
  # (`ANKUSA_ROLES`, `"${KAFKA_BROKERS}"`) can carry.
  defp string_list!(value, path) do
    strings =
      case value do
        list when is_list(list) ->
          Enum.map(list, &expect_string(&1, path))

        string when is_binary(string) ->
          string |> String.split(",") |> Enum.map(&String.trim/1) |> Enum.reject(&(&1 == ""))

        other ->
          raise ConfigError,
            message: type_error(path, "a list or a comma-separated string", other)
      end

    if strings == [], do: raise(ConfigError, message: "#{render_path(path)}: must not be empty")
    strings
  end

  defp type_error(path, expected, value) do
    "#{render_path(path)}: expected #{expected}, got #{inspect(value)}"
  end

  @doc false
  @spec render_path([term()]) :: String.t()
  def render_path([]), do: "config"

  def render_path(segments) do
    Enum.reduce(segments, "", fn
      index, "" when is_integer(index) -> "[#{index}]"
      index, acc when is_integer(index) -> acc <> "[#{index}]"
      segment, "" -> to_string(segment)
      segment, acc -> acc <> "." <> to_string(segment)
    end)
  end

  defp put_opt(kw, _key, nil), do: kw
  defp put_opt(kw, key, value), do: kw ++ [{key, value}]
end
