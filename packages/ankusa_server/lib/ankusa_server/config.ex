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
  | `ANKUSA_HTTP_PORT`, else `PORT` | `http.port` |
  | `ANKUSA_ADMIN_PORT` | `admin.port` |
  | `ANKUSA_CLAIM_CHECK_PORT` | `claim_check.port` |
  | `ANKUSA_ADMIN_IP` | `admin.ip` |
  | `ANKUSA_CLAIM_CHECK_IP` | `claim_check.ip` |
  | `ANKUSA_ROUTES_ENABLED` | `routes.enabled` |
  | `ANKUSA_ROUTES_STORE_URL` | `routes.store.url` |
  | `ANKUSA_SOURCE_STORE_URL` | `source_store.url` |
  | `ANKUSA_WAL_TYPE` | `wal.type` |
  | `ANKUSA_STORAGE_TYPE` | `storage.type` |
  | `ANKUSA_S3_BUCKET`, `ANKUSA_S3_REGION`, `ANKUSA_S3_ENDPOINT` | `storage.s3.bucket/region/endpoint` |
  | `ANKUSA_GCS_BUCKET` | `storage.gcs.bucket` |
  | `ANKUSA_BACKUP_ENABLED` | `backup.enabled` |

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

  @root_keys ~w(node log http admin routes rate_limits quarantine batcher dispatch wal storage claim_check backup sources source_store lifecycle)
  @node_keys ~w(roles data_dir)
  @log_keys ~w(level)
  @http_keys ~w(port max_body_bytes routing prefix)
  @admin_keys ~w(enabled port ip gauge_interval_ms)
  @batcher_keys ~w(partitions max_batch max_delay_ms max_queue max_queue_bytes)
  @dispatch_keys ~w(batch concurrency max_inflight max_inflight_bytes attempt_timeout_ms sink_concurrency breaker_failures breaker_open_ms breaker_max_open_ms retry)
  @retry_keys ~w(base_ms max_ms max_attempts jitter)
  @wal_keys ~w(type publish_timeout_ms)
  @storage_keys ~w(type roll_bytes roll_ms key_prefix s3 gcs)
  @blob_store_keys ~w(type root s3 gcs)
  @s3_keys ~w(bucket region endpoint access_key_id secret_access_key session_token)
  @gcs_keys ~w(bucket endpoint auth token)
  @claim_check_keys ~w(port retention_days pack_max_bytes ip store)
  @backup_keys ~w(enabled interval_ms keep store)
  @source_store_keys ~w(type url namespace tick_ms)
  @lifecycle_keys ~w(sinks)
  @routes_keys ~w(enabled max_routes store cache trusted_proxies ip_rules admin log_sample ip_denied_status seed)
  @routes_store_keys ~w(type url namespace tick_ms)
  # Keys a Redis store owns (`routes.store`, `source_store`); the other store
  # types must not silently drop them.
  @redis_only_keys ~w(url namespace tick_ms)
  @routes_cache_keys ~w(max_size ttl_ms negative_ttl_ms gc_interval_ms)
  @routes_ip_rules_keys ~w(default rules)
  @routes_rule_keys ~w(action cidr)
  @routes_admin_keys ~w(port ip)
  @routes_seed_keys ~w(id path methods enabled ip_rules metadata)
  @rate_limits_keys ~w(default tenants)
  @rate_limit_keys ~w(rate burst)
  @quarantine_keys ~w(burst rate max_bytes)
  # A rotation window needs two keys; a handful covers any overlap a provider
  # forces. More is a misconfiguration (each key costs an HMAC per hook).
  @max_secrets 8
  @source_keys ~w(tenant on_verify_failure verify sinks dedupe forward_headers)
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
  @log_sink_keys ~w(type)
  @http_sink_keys ~w(type url method headers timeout_ms secret max_response_bytes)
  @rabbitmq_sink_keys ~w(type url exchange exchange_type routing_key inline_max_bytes max_inflight)
  @kafka_sink_keys ~w(type brokers topic key inline_max_bytes max_record_bytes ssl sasl)
  @sasl_keys ~w(mechanism username password)
  @nats_sink_keys ~w(type servers subject inline_max_bytes publish_timeout_ms tls auth)
  @nats_auth_keys ~w(username password token nkey_seed jwt)
  @redis_sink_keys ~w(type url channel inline_max_bytes publish_timeout_ms)
  @sqs_sink_keys ~w(type queue_url region endpoint message_group_id inline_max_bytes max_message_bytes timeout_ms access_key_id secret_access_key session_token)
  @google_pubsub_sink_keys ~w(type project topic endpoint ordering_key inline_max_bytes max_message_bytes timeout_ms auth token)

  @roles ~w(edge dispatch storage claim_check)
  @routes_store_types ~w(ets redis)
  @ip_rule_actions ~w(allow deny)
  @verify_types ~w(none stripe github standard_webhooks shopify slack hmac)
  @scheme_parses ~w(whole csv_pairs space_versions)
  @scheme_hashes ~w(sha256 sha512 sha1)
  @scheme_encodings ~w(hex base64)
  @scheme_secret_decodes ~w(raw whsec_base64)
  @sink_types ~w(log http rabbitmq kafka nats redis sqs google_pubsub)
  @policies ~w(reject quarantine accept_flag)
  @routings ~w(path tenant_path)
  @log_levels ~w(debug info warning error)
  @exchange_types ~w(topic direct fanout headers)

  @typedoc "A loaded config plus the log level the file asked for."
  @type loaded :: %{config: Ankusa.Config.t(), log_level: Logger.level()}

  @var_re ~r/\$\{([A-Z0-9_]+)(:-([^}]*))?\}/

  @doc """
  Read, interpolate, validate, and translate the config file.

  Options: `:env` (an environment map, default `System.get_env/0`) and `:path`
  (overrides `ANKUSA_CONFIG`, used by tests and `--config`-style callers).
  """
  @spec load!(keyword()) :: loaded()
  def load!(opts \\ []) do
    env = Keyword.get(opts, :env, System.get_env())
    raw = read!(Keyword.get(opts, :path), env)

    doc =
      raw
      |> interpolate(env, [])
      |> apply_env_overrides(env)

    %{config: config!(doc), log_level: log_level!(doc)}
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
    Map.new(value, fn {k, v} -> {k, interpolate(v, env, path ++ [k])} end)
  end

  defp interpolate(value, env, path) when is_list(value) do
    value
    |> Enum.with_index()
    |> Enum.map(fn {v, i} -> interpolate(v, env, path ++ [i]) end)
  end

  defp interpolate(value, env, path) when is_binary(value) do
    # Whatever in the file still reads `${` once the variable references are
    # taken out did not match the grammar (`${stripe_secret}`, `${A-B}`): left
    # alone it would become a literal secret or URL, so it is an error. Only
    # the template is checked: a substituted value may contain `${` itself.
    if value |> then(&Regex.replace(@var_re, &1, "")) |> String.contains?("${") do
      raise ConfigError,
        message:
          "#{render_path(path)}: unresolved ${…} (variable names are [A-Z0-9_]+; " <>
            "use ${NAME:-} for an empty default)"
    end

    Regex.replace(@var_re, value, fn _match, name, marker, default ->
      case Map.fetch(env, name) do
        {:ok, replacement} ->
          replacement

        # The marker, not the default, decides: `${NAME:-}` has an empty one.
        :error when marker != "" ->
          default

        :error ->
          raise ConfigError, message: "#{render_path(path)}: ${#{name}} is not set"
      end
    end)
  end

  defp interpolate(value, _env, _path), do: value

  # ── 3. env overrides ────────────────────────────────────────────────────────

  @env_overrides [
    {"ANKUSA_ROLES", ["node", "roles"]},
    {"ANKUSA_DATA_DIR", ["node", "data_dir"]},
    {"ANKUSA_LOG_LEVEL", ["log", "level"]},
    {"ANKUSA_ADMIN_PORT", ["admin", "port"]},
    {"ANKUSA_ADMIN_IP", ["admin", "ip"]},
    {"ANKUSA_CLAIM_CHECK_PORT", ["claim_check", "port"]},
    {"ANKUSA_CLAIM_CHECK_IP", ["claim_check", "ip"]},
    {"ANKUSA_ROUTES_ENABLED", ["routes", "enabled"]},
    {"ANKUSA_ROUTES_STORE_URL", ["routes", "store", "url"]},
    {"ANKUSA_SOURCE_STORE_URL", ["source_store", "url"]},
    {"ANKUSA_WAL_TYPE", ["wal", "type"]},
    {"ANKUSA_STORAGE_TYPE", ["storage", "type"]},
    {"ANKUSA_S3_BUCKET", ["storage", "s3", "bucket"]},
    {"ANKUSA_S3_REGION", ["storage", "s3", "region"]},
    {"ANKUSA_GCS_BUCKET", ["storage", "gcs", "bucket"]},
    {"ANKUSA_STORAGE_KEY_PREFIX", ["storage", "key_prefix"]},
    {"ANKUSA_BACKUP_ENABLED", ["backup", "enabled"]}
  ]

  defp apply_env_overrides(doc, env) do
    doc = Enum.reduce(@env_overrides, doc, &override(&2, &1, env))

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

  defp config!(doc) do
    check_keys!(doc, @root_keys, [])

    opts =
      node_section(doc) ++
        http_section(doc) ++
        admin_section(doc) ++
        routes_section(doc) ++
        rate_limits_section(doc) ++
        quarantine_section(doc) ++
        batcher_section(doc) ++
        dispatch_section(doc) ++
        wal_section(doc) ++
        storage_section(doc) ++
        claim_check_section(doc) ++
        backup_section(doc) ++
        source_store_section(doc) ++
        lifecycle_section(doc)

    try do
      config = Ankusa.Config.new(opts)
      Ankusa.ClaimCheck.validate_config!(config)
      Ankusa.Verifier.validate_config!(config)
      # The image validates what core validates, at the same moment: a route
      # config that would refuse to boot must fail `check-config` too, with the
      # same message.
      Ankusa.Routes.validate_config!(config)
      Ankusa.Edge.RateLimiter.validate_config!(config)
      Ankusa.Edge.Quarantine.validate_config!(config)
      Ankusa.Dispatch.Pipeline.validate_config!(config)
      Ankusa.Queue.validate_config!(config)
      Ankusa.Store.Backup.validate_config!(config)
      Ankusa.Lifecycle.validate_config!(config)
      config
    rescue
      error in ArgumentError -> raise ConfigError, message: error.message
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

  # `to_atom` after `enum!`, not `to_existing_atom`: under `bin/ankusa eval`
  # (check-config) modules load on demand, so `:edge` may not exist yet.
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
    ]
  end

  # ── batcher / dispatch ──────────────────────────────────────────────────────

  defp batcher_section(doc) do
    batcher = section!(doc, "batcher", @batcher_keys, [])

    [
      batcher:
        []
        |> put_opt(:partitions, int_opt(batcher, "partitions", ["batcher"]))
        |> put_opt(:max_batch, int_opt(batcher, "max_batch", ["batcher"]))
        |> put_opt(:max_delay_ms, int_opt(batcher, "max_delay_ms", ["batcher"]))
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
        |> put_opt(:sink_concurrency, int_opt(dispatch, "sink_concurrency", ["dispatch"]))
        |> put_opt(:breaker_failures, int_opt(dispatch, "breaker_failures", ["dispatch"]))
        |> put_opt(:breaker_open_ms, int_opt(dispatch, "breaker_open_ms", ["dispatch"]))
        |> put_opt(:breaker_max_open_ms, int_opt(dispatch, "breaker_max_open_ms", ["dispatch"]))
        |> put_opt(:retry, retry_policy(retry))
    ]
  end

  defp retry_policy(retry) do
    path = ["dispatch", "retry"]

    if retry == %{} do
      nil
    else
      {Ankusa.RetryPolicy.Exponential,
       []
       |> put_opt(:base_ms, int_opt(retry, "base_ms", path))
       |> put_opt(:max_ms, int_opt(retry, "max_ms", path))
       |> put_opt(:max_attempts, int_opt(retry, "max_attempts", path))
       |> put_opt(:jitter, bool_opt(retry, "jitter", path))}
    end
  end

  # ── wal ─────────────────────────────────────────────────────────────────────

  defp wal_section(doc) do
    wal = section!(doc, "wal", @wal_keys, [])

    case enum!(wal["type"] || "disk", ~w(disk none), ["wal", "type"]) do
      "disk" ->
        [wal: :disk]

      "none" ->
        [wal: :none]
        |> put_opt(:direct_publish_timeout_ms, int_opt(wal, "publish_timeout_ms", ["wal"]))
    end
  end

  # ── storage ─────────────────────────────────────────────────────────────────

  defp storage_section(doc) do
    storage = section!(doc, "storage", @storage_keys, [])

    [
      storage:
        [blob_store: blob_store!(storage, ["storage"], false)]
        |> put_opt(:roll_bytes, int_opt(storage, "roll_bytes", ["storage"]))
        |> put_opt(:roll_ms, int_opt(storage, "roll_ms", ["storage"]))
        |> put_opt(:key_prefix, string_opt(storage, "key_prefix", ["storage"]))
    ]
  end

  # `{module, opts}` from a `type: local|s3|gcs` section with its `s3:`/`gcs:`
  # children: `storage` itself, and `claim_check.store`. `root?` allows a
  # LocalFS `root` (only a dedicated claim store has one; segments live under
  # `data_dir`).
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
  end

  defp gcs_opts!(gcs, path) do
    bucket = required_string!(gcs, "bucket", path)

    ([bucket: bucket] |> put_opt(:endpoint, string_opt(gcs, "endpoint", path))) ++
      gcp_auth!(gcs, path)
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

    blob_store =
      case claim_check["store"] do
        nil ->
          nil

        _store ->
          store = section!(claim_check, "store", @blob_store_keys, ["claim_check"])
          blob_store!(store, ["claim_check", "store"], true)
      end

    [
      claim_check:
        []
        |> put_opt(:port, int_opt(claim_check, "port", ["claim_check"]))
        |> put_opt(:pack_max_bytes, int_opt(claim_check, "pack_max_bytes", ["claim_check"]))
        |> put_opt(:retention_days, int_opt(claim_check, "retention_days", ["claim_check"]))
        |> put_opt(:ip, string_opt(claim_check, "ip", ["claim_check"]))
        |> put_opt(:blob_store, blob_store)
    ]
  end

  # ── backup ──────────────────────────────────────────────────────────────────

  # `store` takes the claim store's shape (`root` allowed for a local store); unset
  # means the storage bucket, under `storage.key_prefix` either way.
  defp backup_section(doc) do
    backup = section!(doc, "backup", @backup_keys, [])

    blob_store =
      case backup["store"] do
        nil ->
          nil

        _store ->
          store = section!(backup, "store", @blob_store_keys, ["backup"])
          blob_store!(store, ["backup", "store"], true)
      end

    [
      backup:
        []
        |> put_opt(:enabled, bool_opt(backup, "enabled", ["backup"]))
        |> put_opt(:interval_ms, int_opt(backup, "interval_ms", ["backup"]))
        |> put_opt(:keep, int_opt(backup, "keep", ["backup"]))
        |> put_opt(:blob_store, blob_store)
    ]
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
         |> put_opt(:tick_ms, int_opt(store, "tick_ms", path))}
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

  # The server checks shape and types only — that both keys are present, and
  # that `rate` is a number and `burst` an integer. Ranges and tenant ids are
  # core's (`Ankusa.Edge.RateLimiter.validate_config!/1`), so the message an
  # operator sees is the same whether they run the image or embed the library.
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
    ]
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

  defp rate_limit!(value, path) do
    limit = section!(value, @rate_limit_keys, path)
    %{rate: required_number!(limit, "rate", path), burst: required_int!(limit, "burst", path)}
  end

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
    ]
  end

  # ── sources ─────────────────────────────────────────────────────────────────

  # A URL with no `type` means Redis, as for `routes.store`: a file that sets
  # one has said it wants sources shared, and the other types have no URL.
  defp source_store_section(doc) do
    sources = sources!(doc)
    store = section!(doc, "source_store", @source_store_keys, [])
    path = ["source_store"]
    decoder = &AnkusaServer.Config.source_from_map!/2

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

        [source_store: {Ankusa.SourceStore.Redis, opts}]
    end
  end

  # Lifecycle events are delivered to the same kind of sinks a source's hooks
  # are, so the sink walker is shared: every sink type, its key check, and its
  # error message carry over to `lifecycle.sinks` unchanged.
  defp lifecycle_section(doc) do
    case doc["lifecycle"] do
      nil ->
        []

      _ ->
        lifecycle = section!(doc, "lifecycle", @lifecycle_keys, [])
        [lifecycle: %{sinks: sinks!(lifecycle, ["lifecycle"])}]
    end
  end

  defp sources!(doc) do
    case doc["sources"] do
      nil ->
        warn_no_sources()
        %{}

      sources when is_map(sources) ->
        if sources == %{}, do: warn_no_sources()

        Map.new(sources, fn {source_id, source} ->
          {source_id, source_opts!(source, ["sources", source_id])}
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
  def source_from_map!(source_id, spec) do
    source_opts!(spec, ["sources", source_id])
  end

  defp source_opts!(source, path) do
    source = section!(source, @source_keys, path)

    []
    |> put_opt(:tenant_id, string_opt(source, "tenant", path))
    |> put_opt(:on_verify_failure, atom_enum_opt(source, "on_verify_failure", @policies, path))
    |> put_opt(:verifier, verifier(source, path))
    |> Keyword.put(:sinks, sinks!(source, path))
    |> put_opt(:dedupe, dedupe(source, path))
    |> put_opt(:forward_headers, forward_headers(source, path))
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

        opts =
          []
          |> put_opt(:secret, secret_opt(verify, path))
          |> put_opt(:tolerance, int_opt(verify, "tolerance_seconds", path))

        {verify_module(type), verify_opts(type, verify, path, opts)}
    end
  end

  defp verify_keys("hmac"), do: @verify_hmac_keys
  defp verify_keys(_), do: @verify_keys

  defp verify_module("none"), do: Ankusa.Verifier.None
  defp verify_module(_), do: Ankusa.Verifier.Hmac

  defp verify_opts("none", _verify, _path, opts), do: opts

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

        {Ankusa.Sink.Http,
         [url: required_string!(sink, "url", path)]
         |> put_opt(:method, atom_enum_opt(sink, "method", ~w(post put patch), path))
         |> put_opt(:headers, headers(sink["headers"], path ++ ["headers"]))
         |> put_opt(:timeout_ms, int_opt(sink, "timeout_ms", path))
         |> put_opt(:secret, secret)
         |> put_opt(:max_response_bytes, int_opt(sink, "max_response_bytes", path))}

      "rabbitmq" ->
        check_keys!(sink, @rabbitmq_sink_keys, path)

        {Ankusa.Sink.RabbitMQ,
         [exchange: required_string!(sink, "exchange", path)]
         |> put_opt(:url, string_opt(sink, "url", path))
         |> put_opt(:exchange_type, atom_enum_opt(sink, "exchange_type", @exchange_types, path))
         |> put_opt(:routing_key, string_opt(sink, "routing_key", path))
         |> put_opt(:inline_max_bytes, int_opt(sink, "inline_max_bytes", path))
         |> put_opt(:max_inflight, int_opt(sink, "max_inflight", path))}

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
         |> put_opt(:sasl, sasl(sink["sasl"], path ++ ["sasl"]))}

      "nats" ->
        check_keys!(sink, @nats_sink_keys, path)

        connection =
          [
            servers: string_list!(sink["servers"], path ++ ["servers"]),
            subject: required_string!(sink, "subject", path)
          ]
          |> put_opt(:inline_max_bytes, int_opt(sink, "inline_max_bytes", path))
          |> put_opt(:publish_timeout_ms, int_opt(sink, "publish_timeout_ms", path))
          |> put_opt(:tls, bool_opt(sink, "tls", path))

        {Ankusa.Sink.NATS, connection ++ nats_auth(sink["auth"], path ++ ["auth"])}

      "redis" ->
        check_keys!(sink, @redis_sink_keys, path)

        {Ankusa.Sink.Redis,
         [
           url: required_string!(sink, "url", path),
           channel: required_string!(sink, "channel", path)
         ]
         |> put_opt(:inline_max_bytes, int_opt(sink, "inline_max_bytes", path))
         |> put_opt(:publish_timeout_ms, int_opt(sink, "publish_timeout_ms", path))}

      "sqs" ->
        check_keys!(sink, @sqs_sink_keys, path)

        {Ankusa.Sink.SQS,
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
         |> put_opt(:session_token, string_opt(sink, "session_token", path))}

      "google_pubsub" ->
        check_keys!(sink, @google_pubsub_sink_keys, path)

        {Ankusa.Sink.GooglePubSub,
         ([
            project: required_string!(sink, "project", path),
            topic: required_string!(sink, "topic", path)
          ]
          |> put_opt(:endpoint, string_opt(sink, "endpoint", path))
          |> put_opt(:ordering_key, string_opt(sink, "ordering_key", path))
          |> put_opt(:inline_max_bytes, int_opt(sink, "inline_max_bytes", path))
          |> put_opt(:max_message_bytes, int_opt(sink, "max_message_bytes", path))
          |> put_opt(:timeout_ms, int_opt(sink, "timeout_ms", path))) ++ gcp_auth!(sink, path)}
    end
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

  defp render_path([]), do: "config"

  defp render_path(segments) do
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
