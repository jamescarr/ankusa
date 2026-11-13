defmodule AnkusaGatewayServer.Sections do
  @moduledoc """
  The sections of the config file both images read, each turned into the
  options of `Ankusa.Config.new/1` (or, for `log` and `tracing`, the image's own
  settings): `log`, `tracing`, `listener`, `metrics`, a listener's `tls` and
  `auth`, `storage` and its blob stores, `secrets`, `claim_check` and `egress`.
  Types only, mostly: core checks the ranges and what has to go together.
  """

  import AnkusaGatewayServer.Schema

  alias AnkusaGatewayServer.ConfigError

  @log_keys ~w(level banner format)
  @tracing_keys ~w(enabled otlp_endpoint sample_ratio)
  @default_otlp_endpoint "http://localhost:4318"
  @listener_keys ~w(tls max_connections num_acceptors read_timeout_ms body_read_timeout_ms request_deadline_ms max_requests_per_connection max_header_count max_header_length http2 drain_delay_ms drain_timeout_ms)
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
  @claim_check_keys ~w(retention_days redeem_grace_ms sweep_max_objects bucket_expiry_days
                       pack_max_bytes store stores placement key_layout store_cooldown_ms
                       read_deadline_ms read_timeout_ms read_retries read_max_inflight read_queue_max
                       read_cache_bytes read_cache_max_entry_bytes
                       ref_signing_keys ref_ttl_ms accept_unsigned_refs)
  @egress_keys ~w(allow_insecure_http deny_cidrs allow_cidrs allow_hosts deny_hosts pools)
  @egress_pools_keys ~w(pool_size pool_count pool_timeout pool_max_idle_ms conn_max_idle_ms max_pools max_pools_per_tenant protocols)
  @log_levels ~w(debug info warning error)
  @log_formats ~w(text json)

  @typedoc """
  The `tracing` section: whether the image runs an OpenTelemetry SDK
  (`AnkusaGatewayServer.Tracing`), where it exports to, and the share of root
  traces it keeps. Not part of `Ankusa.Config`: it configures the image, not an
  instance.
  """
  @type tracing :: %{
          enabled: boolean(),
          otlp_endpoint: String.t(),
          sample_ratio: float()
        }

  # ── 4. validate + translate ─────────────────────────────────────────────────

  # The log section: the level the node starts at (`Ankusa.LogLevel`, also
  # changeable at runtime) and the format of every line (`AnkusaGatewayServer.LogFormatter`).
  def log!(doc) do
    log = section!(doc, "log", @log_keys, [])
    level = enum!(log["level"] || "info", @log_levels, ["log", "level"])
    format = enum!(log["format"] || "text", @log_formats, ["log", "format"])

    %{
      format: if(format == "json", do: :json, else: :text),
      level: String.to_existing_atom(level)
    }
  end

  # `tracing:` configures the image's OpenTelemetry SDK, not an instance, so it
  # is read beside `log:` rather than into `Ankusa.Config`.
  def tracing!(doc) do
    path = ["tracing"]
    tracing = section!(doc, "tracing", @tracing_keys, [])

    %{
      enabled: bool_opt(tracing, "enabled", path) == true,
      otlp_endpoint: otlp_endpoint!(tracing),
      sample_ratio: sample_ratio!(tracing)
    }
  end

  # The value is never echoed: an endpoint may be pasted with credentials in it,
  # and the collector's own credentials belong in `OTEL_EXPORTER_OTLP_HEADERS`.
  def otlp_endpoint!(tracing) do
    case string_opt(tracing, "otlp_endpoint", ["tracing"]) do
      nil ->
        @default_otlp_endpoint

      endpoint ->
        case URI.parse(endpoint) do
          %URI{scheme: scheme, host: host, userinfo: nil}
          when scheme in ["http", "https"] and is_binary(host) and host != "" ->
            endpoint

          _ ->
            raise ConfigError,
              message:
                "tracing.otlp_endpoint: expected an http:// or https:// URL with a host and " <>
                  "no user:password (the collector's credentials go in OTEL_EXPORTER_OTLP_HEADERS)"
        end
    end
  end

  def sample_ratio!(tracing) do
    case number_opt(tracing, "sample_ratio", ["tracing"]) do
      nil ->
        1.0

      ratio when ratio >= 0 and ratio <= 1 ->
        ratio / 1

      other ->
        raise ConfigError,
          message: "tracing.sample_ratio: expected a number from 0 to 1, got #{inspect(other)}"
    end
  end

  # ── listener / metrics / tls ────────────────────────────────────────────────

  def listener_section(doc) do
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
        |> put_opt(:drain_delay_ms, int_opt(listener, "drain_delay_ms", path))
        |> put_opt(:drain_timeout_ms, int_opt(listener, "drain_timeout_ms", path))
    ]
  end

  def metrics_section(doc) do
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
  def tls_opt(parent, path) do
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

  def tls_versions(tls, path) do
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
  def auth_opt(parent, path) do
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

  def auth_tokens(auth, path) do
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
  def auth_token!(entry, path) do
    token = section!(entry, @auth_token_keys, path)
    sha256 = token_sha256!(token, path)

    scopes =
      case token["scopes"] do
        nil -> raise ConfigError, message: "#{render_path(path)}: missing required key \"scopes\""
        value -> string_list!(value, path ++ ["scopes"])
      end

    %{id: required_string!(token, "id", path), sha256: sha256, scopes: scopes}
  end

  def token_sha256!(token, path) do
    case {token["token"], token["sha256"]} do
      {nil, nil} ->
        raise ConfigError, message: "#{render_path(path)}: needs token or sha256"

      {plain, nil} ->
        plain = expect_string(plain, path ++ ["token"])

        if plain == "",
          do: raise(ConfigError, message: "#{render_path(path ++ ["token"])}: must not be empty")

        Ankusa.Admin.Auth.Token.hash(plain)

      {nil, hex} ->
        expect_string(hex, path ++ ["sha256"])

      {_plain, _hex} ->
        raise ConfigError,
          message: "#{render_path(path)}: token and sha256 are alternatives; give one"
    end
  end

  def auth_jwt(auth, path) do
    case auth["jwt"] do
      nil ->
        nil

      value ->
        path = path ++ ["jwt"]
        value |> section!(@auth_jwt_keys, path) |> jwt_opts(path)
    end
  end

  # The keys of a `jwt` block, typed; core checks the ranges and what has to
  # go together.
  def jwt_opts(jwt, path) do
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

  # One audience, or several (a token carrying any of them is accepted).
  def jwt_audience(jwt, path) do
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
  def jwt_keys(jwt, path) do
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
  def auth_mtls(auth, path) do
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

  # ── storage ─────────────────────────────────────────────────────────────────

  def storage_section(doc) do
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
  def blob_store!(section, path, root?) do
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

  def s3_opts!(s3, path) do
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

  def gcs_opts!(gcs, path) do
    bucket = required_string!(gcs, "bucket", path)

    ([bucket: bucket]
     |> put_opt(:endpoint, string_opt(gcs, "endpoint", path))
     |> put_opt(:sse, sse_opt(gcs, path))
     |> Kernel.++(blob_transport_opts(gcs, path))) ++
      gcp_auth!(gcs, path)
  end

  # `timeout_ms`, `op_deadline_ms`, `multipart_*`, `breaker_*`, `pool_*` … of an
  # `s3` or `gcs` section, as the adapter options of the same names.
  def blob_transport_opts(section, path) do
    for {key, option} <- @blob_transport_options, value = int_opt(section, key, path) do
      {option, value}
    end
  end

  # `sse: {type: aes256 | kms, key_id: …}` on an `s3` or `gcs` section: the
  # object store encrypts every object this store writes
  # (`Ankusa.BlobStore.SSE` says what each provider does with it).
  def sse_opt(section, path) do
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

  # `auth: metadata | token | none` (default metadata) on any GCP section.
  def gcp_auth!(section, path) do
    case enum!(section["auth"] || "metadata", ~w(metadata token none), path ++ ["auth"]) do
      "none" ->
        []

      "metadata" ->
        [token_provider: {AnkusaGatewayServer.GcpToken, :metadata, []}]

      "token" ->
        [
          token_provider:
            {AnkusaGatewayServer.GcpToken, :static, [required_string!(section, "token", path)]}
        ]
    end
  end

  # The key-encryption keys for secrets at rest (`Ankusa.Crypto.Envelope`).
  # Both are operator-supplied through `${VAR}`: the file holds the reference,
  # never the key. `kek_previous` is a list, or one comma-separated string, and
  # may be empty (`${PREVIOUS_KEKS:-}`).
  def secrets_section(doc) do
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

  # ── claim check ─────────────────────────────────────────────────────────────

  def claim_check_section(doc) do
    moved!(doc["claim_check"])
    claim_check = section!(doc, "claim_check", @claim_check_keys, [])
    path = ["claim_check"]

    [
      claim_check:
        []
        |> put_opt(:pack_max_bytes, int_opt(claim_check, "pack_max_bytes", path))
        |> put_opt(:retention_days, int_opt(claim_check, "retention_days", path))
        |> put_opt(:redeem_grace_ms, int_opt(claim_check, "redeem_grace_ms", path))
        |> put_opt(:sweep_max_objects, int_opt(claim_check, "sweep_max_objects", path))
        |> put_opt(:bucket_expiry_days, int_opt(claim_check, "bucket_expiry_days", path))
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
        |> put_opt(:ref_signing_keys, ref_signing_keys(claim_check, path))
        |> put_opt(:ref_ttl_ms, int_opt(claim_check, "ref_ttl_ms", path))
        |> put_opt(
          :accept_unsigned_refs,
          nullable_bool_opt(claim_check, "accept_unsigned_refs", path)
        )
    ]
  end

  # A version-2 file (a version-1 one was migrated) that still names a key the
  # gateway took: say where it went.
  def moved!(%{} = claim_check) do
    case Enum.find(
           Map.keys(claim_check),
           &Map.has_key?(AnkusaGatewayServer.Document.moved_keys(), &1)
         ) do
      nil ->
        :ok

      key ->
        raise ConfigError,
          message:
            "claim_check.#{key}: moved to #{AnkusaGatewayServer.Document.moved_keys()[key]} (config_version 2)"
    end
  end

  def moved!(_other), do: :ok

  # `[current, previous...]`: a list, or one comma-separated string (`${VAR}`).
  # Empty or null leaves the key unset, which means no signing.
  def ref_signing_keys(claim_check, path) do
    case claim_check["ref_signing_keys"] do
      empty when empty in [nil, [], ""] -> nil
      keys -> string_list!(keys, path ++ ["ref_signing_keys"])
    end
  end

  # `store` (one mapping) and `stores` (a list of them, in failover order) both
  # become `claim_check.blob_stores`; neither (or null) = nil, the storage
  # bucket.
  def claim_stores!(claim_check) do
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

  def claim_store!(entry, path) when is_map(entry) do
    entry |> section!(@blob_store_keys, path) |> blob_store!(path, true)
  end

  def claim_store!(entry, path),
    do: raise(ConfigError, message: type_error(path, "a mapping", entry))

  # `egress.pools` is core's, not part of the policy the sinks' URLs are judged
  # by: it goes to `Ankusa.Config` only, which merges it over its defaults.
  def egress_section(doc) do
    given = Map.new(egress_given(doc))

    case egress_pools(section!(doc, "egress", @egress_keys, [])) do
      [] -> [egress: given]
      pools -> [egress: Map.put(given, :pools, Map.new(pools))]
    end
  end

  # ── egress ──────────────────────────────────────────────────────────────────

  # The keys the file sets; `Ankusa.Config.new/1` merges them over the defaults.
  def egress_given(doc) do
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
  def egress_list(egress, key, path) do
    case egress[key] do
      nil -> nil
      [] -> []
      value -> string_list!(value, path ++ [key])
    end
  end

  # ── egress.pools ────────────────────────────────────────────────────────────

  # Types only; the ranges are core's (`Ankusa.Config`), so `check-config` and an
  # embedded boot fail with the same message.
  def egress_pools(egress) do
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
  def protocols_opt(pools, path) do
    case pools["protocols"] do
      nil ->
        nil

      value ->
        value
        |> string_list!(path ++ ["protocols"])
        |> Enum.map(&String.to_existing_atom(enum!(&1, ~w(http1 http2), path ++ ["protocols"])))
    end
  end

  def egress_policy!(doc) do
    Map.merge(Ankusa.HttpClient.Egress.default_policy(), Map.new(egress_given(doc)))
  end
end
