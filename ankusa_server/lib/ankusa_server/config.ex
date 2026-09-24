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
  | `ANKUSA_WAL_TYPE` | `wal.type` |
  | `ANKUSA_WAL_POSTGRES_URL` | `wal.postgres.url` |
  | `ANKUSA_STORAGE_TYPE` | `storage.type` |
  | `ANKUSA_S3_BUCKET`, `ANKUSA_S3_REGION`, `ANKUSA_S3_ENDPOINT` | `storage.s3.bucket/region/endpoint` |
  | `ANKUSA_GCS_BUCKET` | `storage.gcs.bucket` |

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

  @root_keys ~w(node log http admin batcher dispatch wal storage claim_check sources)
  @node_keys ~w(roles data_dir)
  @log_keys ~w(level)
  @http_keys ~w(port max_body_bytes routing prefix)
  @admin_keys ~w(enabled port)
  @batcher_keys ~w(partitions max_batch max_delay_ms max_queue)
  @dispatch_keys ~w(poll_ms batch concurrency max_inflight max_inflight_bytes retry)
  @retry_keys ~w(base_ms max_ms max_attempts jitter)
  @wal_keys ~w(type postgres)
  @postgres_keys ~w(url host port username password database pool_size ssl migrate)
  @postgres_discrete_keys ~w(host port username password database)
  @storage_keys ~w(type roll_bytes roll_ms s3 gcs)
  @s3_keys ~w(bucket region endpoint access_key_id secret_access_key)
  @gcs_keys ~w(bucket endpoint auth token)
  @claim_check_keys ~w(port max_bytes retention_days tokens remote)
  @token_keys ~w(token tenants)
  @remote_keys ~w(url token)
  @source_keys ~w(tenant on_verify_failure verify dedup sinks)
  @verify_keys ~w(type secret tolerance_seconds)
  @verify_hmac_keys ~w(type secret tolerance_seconds signature_header parse sig_prefix sig_key version signed hash encoding secret_decode timestamp_header)
  @dedup_keys ~w(type header json_path)
  @log_sink_keys ~w(type)
  @http_sink_keys ~w(type url method headers timeout_ms ordered)
  @rabbitmq_sink_keys ~w(type url exchange exchange_type routing_key inline_max_bytes)
  @kafka_sink_keys ~w(type brokers topic key inline_max_bytes ssl sasl)
  @sasl_keys ~w(mechanism username password)
  @nats_sink_keys ~w(type servers subject inline_max_bytes publish_timeout_ms tls auth)
  @nats_auth_keys ~w(username password token nkey_seed jwt)

  @roles ~w(edge dispatch storage claim_check)
  @verify_types ~w(none stripe github standard_webhooks shopify slack hmac)
  @scheme_parses ~w(whole csv_pairs space_versions)
  @scheme_hashes ~w(sha256 sha512 sha1)
  @scheme_encodings ~w(hex base64)
  @scheme_secret_decodes ~w(raw whsec_base64)
  @dedup_types ~w(rules stripe github)
  @sink_types ~w(log http rabbitmq kafka nats)
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
    {"ANKUSA_CLAIM_CHECK_PORT", ["claim_check", "port"]},
    {"ANKUSA_WAL_TYPE", ["wal", "type"]},
    {"ANKUSA_WAL_POSTGRES_URL", ["wal", "postgres", "url"]},
    {"ANKUSA_STORAGE_TYPE", ["storage", "type"]},
    {"ANKUSA_S3_BUCKET", ["storage", "s3", "bucket"]},
    {"ANKUSA_S3_REGION", ["storage", "s3", "region"]},
    {"ANKUSA_S3_ENDPOINT", ["storage", "s3", "endpoint"]},
    {"ANKUSA_GCS_BUCKET", ["storage", "gcs", "bucket"]}
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
        batcher_section(doc) ++
        dispatch_section(doc) ++
        wal_section(doc) ++
        storage_section(doc) ++
        claim_check_section(doc) ++
        source_store_section(doc)

    try do
      config = Ankusa.Config.new(opts)
      Ankusa.ClaimCheck.validate_config!(config)
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
    ]
  end

  defp dispatch_section(doc) do
    dispatch = section!(doc, "dispatch", @dispatch_keys, [])
    retry = section!(dispatch, "retry", @retry_keys, ["dispatch"])

    [
      dispatch:
        []
        |> put_opt(:poll_ms, int_opt(dispatch, "poll_ms", ["dispatch"]))
        |> put_opt(:batch, int_opt(dispatch, "batch", ["dispatch"]))
        |> put_opt(:concurrency, int_opt(dispatch, "concurrency", ["dispatch"]))
        |> put_opt(:max_inflight, int_opt(dispatch, "max_inflight", ["dispatch"]))
        |> put_opt(:max_inflight_bytes, int_opt(dispatch, "max_inflight_bytes", ["dispatch"]))
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
    postgres = section!(wal, "postgres", @postgres_keys, ["wal"])

    case enum!(wal["type"] || "disk", ~w(disk postgres), ["wal", "type"]) do
      "disk" ->
        [wal: {Ankusa.WAL.DiskLog, []}]

      "postgres" ->
        if postgres == %{} do
          raise ConfigError,
            message: "wal.postgres: required when wal.type is \"postgres\""
        end

        [wal: {Ankusa.WAL.Postgres, postgres_opts!(postgres)}]
    end
  end

  defp postgres_opts!(postgres) do
    path = ["wal", "postgres"]

    if postgres["url"] && Enum.any?(@postgres_discrete_keys, &Map.has_key?(postgres, &1)) do
      raise ConfigError,
        message:
          "#{render_path(path)}: \"url\" and the discrete connection keys are mutually exclusive"
    end

    url_opts =
      case postgres["url"] do
        nil ->
          []

        url ->
          url_path = path ++ ["url"]
          postgres_url_opts!(expect_string(url, url_path), url_path)
      end

    url_opts
    |> put_opt(:hostname, string_opt(postgres, "host", path))
    |> put_opt(:username, string_opt(postgres, "username", path))
    |> put_opt(:password, string_opt(postgres, "password", path))
    |> put_opt(:database, string_opt(postgres, "database", path))
    |> put_opt(:port, int_opt(postgres, "port", path))
    |> put_opt(:pool_size, int_opt(postgres, "pool_size", path))
    |> put_opt(:ssl, bool_opt(postgres, "ssl", path))
    |> put_opt(:migrate, bool_opt(postgres, "migrate", path))
  end

  # `is_binary/1` is not decoration: it narrows the argument so `URI.parse/1`
  # has a concrete input type, which keeps the struct fields statically known.
  defp postgres_url_opts!(url, path) when is_binary(url) do
    uri = URI.parse(url)

    cond do
      uri.scheme not in ["postgres", "postgresql"] ->
        raise ConfigError,
          message: "#{render_path(path)}: expected a postgres:// URL, got #{inspect(url)}"

      is_nil(uri.host) ->
        raise ConfigError, message: "#{render_path(path)}: no host in #{inspect(url)}"

      true ->
        database = uri.path |> to_string() |> String.trim_leading("/")
        {username, password} = userinfo_parts(uri.userinfo)

        [
          hostname: uri.host,
          port: uri.port || 5432,
          username: username,
          password: password,
          database: if(database == "", do: nil, else: database)
        ]
        |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    end
  end

  # The struct has `userinfo`, not `user`/`password` (`URI.user/1` is a
  # function). URL-encoded values are decoded here, because Postgrex wants the
  # password, not its percent-encoding — `p%40ss` is the password `p@ss`.
  defp userinfo_parts(nil), do: {nil, nil}

  defp userinfo_parts(userinfo) do
    case String.split(userinfo, ":", parts: 2) do
      [user] -> {URI.decode(user), nil}
      [user, password] -> {URI.decode(user), URI.decode(password)}
    end
  end

  # ── storage ─────────────────────────────────────────────────────────────────

  defp storage_section(doc) do
    storage = section!(doc, "storage", @storage_keys, [])
    s3 = section!(storage, "s3", @s3_keys, ["storage"])
    gcs = section!(storage, "gcs", @gcs_keys, ["storage"])

    blob_store =
      case enum!(storage["type"] || "local", ~w(local s3 gcs), ["storage", "type"]) do
        "local" -> {Ankusa.BlobStore.LocalFS, []}
        "s3" -> {Ankusa.BlobStore.S3, s3_opts!(s3)}
        "gcs" -> {Ankusa.BlobStore.GCS, gcs_opts!(gcs)}
      end

    [
      storage:
        [blob_store: blob_store]
        |> put_opt(:roll_bytes, int_opt(storage, "roll_bytes", ["storage"]))
        |> put_opt(:roll_ms, int_opt(storage, "roll_ms", ["storage"]))
    ]
  end

  defp s3_opts!(s3) do
    path = ["storage", "s3"]

    [
      bucket: required_string!(s3, "bucket", path),
      region: required_string!(s3, "region", path)
    ]
    |> put_opt(:endpoint, string_opt(s3, "endpoint", path))
    |> put_opt(:access_key_id, string_opt(s3, "access_key_id", path))
    |> put_opt(:secret_access_key, string_opt(s3, "secret_access_key", path))
  end

  defp gcs_opts!(gcs) do
    path = ["storage", "gcs"]
    bucket = required_string!(gcs, "bucket", path)

    opts = [bucket: bucket] |> put_opt(:endpoint, string_opt(gcs, "endpoint", path))

    case enum!(gcs["auth"] || "metadata", ~w(metadata token none), path ++ ["auth"]) do
      "none" ->
        opts

      "metadata" ->
        opts ++ [token_provider: {AnkusaServer.GcsToken, :metadata, []}]

      "token" ->
        token = required_string!(gcs, "token", path)
        opts ++ [token_provider: {AnkusaServer.GcsToken, :static, [token]}]
    end
  end

  # ── claim check ─────────────────────────────────────────────────────────────

  defp claim_check_section(doc) do
    claim_check = section!(doc, "claim_check", @claim_check_keys, [])

    [
      claim_check:
        []
        |> put_opt(:port, int_opt(claim_check, "port", ["claim_check"]))
        |> put_opt(:max_bytes, int_opt(claim_check, "max_bytes", ["claim_check"]))
        |> put_opt(:retention_days, int_opt(claim_check, "retention_days", ["claim_check"]))
        |> put_opt(:api_tokens, api_tokens(claim_check))
        |> put_opt(:adapter, claim_check_adapter(claim_check))
    ]
  end

  defp api_tokens(claim_check) do
    case claim_check["tokens"] do
      nil ->
        nil

      tokens when is_list(tokens) ->
        tokens
        |> Enum.with_index()
        |> Map.new(fn {token, index} ->
          path = ["claim_check", "tokens", index]
          token = section!(token, @token_keys, path)

          {required_string!(token, "token", path), token_scope(token["tenants"], path)}
        end)

      other ->
        raise ConfigError, message: type_error(["claim_check", "tokens"], "a list", other)
    end
  end

  defp token_scope(nil, _path), do: :all
  defp token_scope("all", _path), do: :all

  defp token_scope(tenants, path) when is_list(tenants) do
    Enum.map(tenants, &expect_string(&1, path ++ ["tenants"]))
  end

  defp token_scope(other, path) do
    raise ConfigError,
      message: type_error(path ++ ["tenants"], "a list of tenants or \"all\"", other)
  end

  defp claim_check_adapter(claim_check) do
    case claim_check["remote"] do
      nil ->
        nil

      remote ->
        path = ["claim_check", "remote"]
        remote = section!(remote, @remote_keys, path)

        {Ankusa.ClaimCheck.Remote,
         [url: required_string!(remote, "url", path)]
         |> put_opt(:token, string_opt(remote, "token", path))}
    end
  end

  # ── sources ─────────────────────────────────────────────────────────────────

  defp source_store_section(doc) do
    case doc["sources"] do
      nil ->
        warn_no_sources()
        [source_store: {Ankusa.SourceStore.Static, sources: %{}}]

      sources when is_map(sources) ->
        if sources == %{}, do: warn_no_sources()

        translated =
          Map.new(sources, fn {source_id, source} ->
            {source_id, source_opts!(source, ["sources", source_id])}
          end)

        [source_store: {Ankusa.SourceStore.Static, sources: translated}]

      other ->
        raise ConfigError,
          message: type_error(["sources"], "a mapping of source_id to source", other)
    end
  end

  defp warn_no_sources,
    do: Logger.warning("[ankusa] no sources configured; every POST will return 404")

  defp source_opts!(source, path) do
    source = section!(source, @source_keys, path)

    []
    |> put_opt(:tenant_id, string_opt(source, "tenant", path))
    |> put_opt(:on_verify_failure, atom_enum_opt(source, "on_verify_failure", @policies, path))
    |> put_opt(:verifier, verifier(source, path))
    |> put_opt(:dedup, dedup_key(source, path))
    |> Keyword.put(:sinks, sinks!(source, path))
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
          |> put_opt(:secret, string_opt(verify, "secret", path))
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
    required_string!(verify, "secret", path)

    scheme =
      case type do
        "hmac" -> hmac_scheme(verify, path)
        _ -> String.to_atom(type)
      end

    [{:scheme, scheme} | opts]
  end

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

  defp dedup_key(source, path) do
    path = path ++ ["dedup"]

    case section!(source["dedup"], @dedup_keys, path) do
      dedup when map_size(dedup) == 0 ->
        nil

      dedup ->
        type = enum!(dedup["type"] || "rules", @dedup_types, path ++ ["type"])

        case type do
          "rules" ->
            {Ankusa.DedupKey.Rules,
             []
             |> put_opt(:header, string_opt(dedup, "header", path))
             |> put_opt(:json, json_path(dedup, path))}

          "stripe" ->
            {Ankusa.DedupKey.Stripe, []}

          "github" ->
            {Ankusa.DedupKey.GitHub, []}
        end
    end
  end

  defp json_path(dedup, path) do
    case dedup["json_path"] do
      nil -> nil
      value -> value |> expect_string(path ++ ["json_path"]) |> String.split(".")
    end
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

        {Ankusa.Sink.Http,
         [url: required_string!(sink, "url", path)]
         |> put_opt(:method, atom_enum_opt(sink, "method", ~w(post put patch), path))
         |> put_opt(:headers, headers(sink["headers"], path ++ ["headers"]))
         |> put_opt(:timeout_ms, int_opt(sink, "timeout_ms", path))
         |> put_opt(:ordered, bool_opt(sink, "ordered", path))}

      "rabbitmq" ->
        check_keys!(sink, @rabbitmq_sink_keys, path)

        {Ankusa.Sink.RabbitMQ,
         [exchange: required_string!(sink, "exchange", path)]
         |> put_opt(:url, string_opt(sink, "url", path))
         |> put_opt(:exchange_type, atom_enum_opt(sink, "exchange_type", @exchange_types, path))
         |> put_opt(:routing_key, string_opt(sink, "routing_key", path))
         |> put_opt(:inline_max_bytes, int_opt(sink, "inline_max_bytes", path))}

      "kafka" ->
        check_keys!(sink, @kafka_sink_keys, path)

        {Ankusa.Sink.Kafka,
         [
           brokers: string_list!(sink["brokers"], path ++ ["brokers"]),
           topic: required_string!(sink, "topic", path)
         ]
         |> put_opt(:key, string_opt(sink, "key", path))
         |> put_opt(:inline_max_bytes, int_opt(sink, "inline_max_bytes", path))
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

  defp parse_int!(value, path) do
    case Integer.parse(value) do
      {int, ""} -> int
      _ -> raise ConfigError, message: type_error(path, "an integer", value)
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
