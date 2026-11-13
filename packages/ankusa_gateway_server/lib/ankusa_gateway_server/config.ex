defmodule AnkusaGatewayServer.Config do
  @moduledoc """
  The gateway image's config file (`jamescarr/ankusa-gateway`), turned into a
  core `%Ankusa.Config{}` with no role and an `%Ankusa.Gateway.Config{}`.

  The format is `jamescarr/ankusa`'s (`config_version: 2`), restricted to the
  sections a gateway reads: `config_version node log listener metrics tracing
  secrets egress storage claim_check gateway`. A section only the full image
  reads (`sources`, `dispatch`, `admin`, …) is refused by name; anything else
  is an unknown key. The pipeline is the full image's
  (`AnkusaGatewayServer.Document`): locate (`ANKUSA_CONFIG`, else
  `/etc/ankusa/gateway.yml`, else `./gateway.yml`, else defaults with a
  warning), migrate a version-1 file, substitute `${…}`, apply the environment
  overrides below, resolve `${file:}` / `${sm:}` (`AnkusaGatewayServer.Secrets`),
  validate.

  `node.roles`, when present, must be `[claim_check]`: this image runs only the
  gateway.

  ## Env overrides

  | Variable | Field |
  | --- | --- |
  | `ANKUSA_ROLES` | `node.roles` |
  | `ANKUSA_DATA_DIR` | `node.data_dir` |
  | `ANKUSA_REGION` | `node.region` |
  | `ANKUSA_LOG_LEVEL`, `ANKUSA_LOG_FORMAT` | `log.level`, `log.format` |
  | `ANKUSA_CLAIM_CHECK_PORT`, `ANKUSA_CLAIM_CHECK_IP` | `gateway.port`, `gateway.ip` |
  | `ANKUSA_CLAIM_CHECK_TOKEN` | `gateway.auth.authenticators`: one token holding `claims:read`, `tenant:*` |
  | `ANKUSA_STORAGE_TYPE` | `storage.type` |
  | `ANKUSA_S3_BUCKET`, `ANKUSA_S3_REGION` | `storage.s3.bucket`, `storage.s3.region` |
  | `ANKUSA_GCS_BUCKET` | `storage.gcs.bucket` |
  | `ANKUSA_METRICS_ENABLED`, `ANKUSA_METRICS_PORT`, `ANKUSA_METRICS_IP` | `metrics.enabled`, `metrics.port`, `metrics.ip` |

  Every failure raises `AnkusaGatewayServer.ConfigError`; `load_or_halt!/0`
  prints it and exits 78.
  """

  require Logger

  import AnkusaGatewayServer.Schema
  import AnkusaGatewayServer.Sections

  alias AnkusaGatewayServer.{ConfigError, Document, GatewaySection, Secrets}

  @default_path "/etc/ankusa/gateway.yml"
  @fallback_path "./gateway.yml"
  @config_version 2
  @migrations %{1 => &AnkusaGatewayServer.Document.migrate_gateway_keys/1}

  @root_keys ~w(config_version node log listener metrics tracing secrets egress storage claim_check gateway)
  # What the full image reads and this one does not.
  @full_image_keys ~w(http admin routes rate_limits dedupe fleet quarantine batcher dispatch wal
                      intake backup sources source_store lifecycle audit tenants queue archive health)
  @node_keys ~w(roles data_dir region)

  @env_overrides [
    {"ANKUSA_ROLES", ["node", "roles"]},
    {"ANKUSA_DATA_DIR", ["node", "data_dir"]},
    {"ANKUSA_REGION", ["node", "region"]},
    {"ANKUSA_LOG_LEVEL", ["log", "level"]},
    {"ANKUSA_LOG_FORMAT", ["log", "format"]},
    {"ANKUSA_CLAIM_CHECK_PORT", ["gateway", "port"]},
    {"ANKUSA_CLAIM_CHECK_IP", ["gateway", "ip"]},
    {"ANKUSA_STORAGE_TYPE", ["storage", "type"]},
    {"ANKUSA_S3_BUCKET", ["storage", "s3", "bucket"]},
    {"ANKUSA_S3_REGION", ["storage", "s3", "region"]},
    {"ANKUSA_GCS_BUCKET", ["storage", "gcs", "bucket"]},
    {"ANKUSA_METRICS_ENABLED", ["metrics", "enabled"]},
    {"ANKUSA_METRICS_PORT", ["metrics", "port"]},
    {"ANKUSA_METRICS_IP", ["metrics", "ip"]}
  ]

  @typedoc """
  The core config (no role), the gateway's, the document they were built from
  with every substitution made, the log and tracing settings, and what the
  `${file:}` / `${sm:}` references resolved to.
  """
  @type loaded :: %{
          config: Ankusa.Config.t(),
          gateway: Ankusa.Gateway.Config.t(),
          doc: map(),
          log: %{format: :text | :json, level: Logger.level()},
          tracing: AnkusaGatewayServer.Sections.tracing(),
          secrets: Secrets.t()
        }

  @doc """
  Read, migrate, interpolate, validate and translate the config file.

  Options: `:env` (default `System.get_env/0`), `:path` (overrides
  `ANKUSA_CONFIG`), `:secrets` and `refresh: true` as for
  `AnkusaGatewayServer.Secrets.resolve/3`.
  """
  @spec load!(keyword()) :: loaded()
  def load!(opts \\ []) do
    env = Keyword.get(opts, :env, System.get_env())

    raw =
      opts
      |> Keyword.get(:path)
      |> Document.read!(env,
        default_path: @default_path,
        fallback_path: @fallback_path,
        on_missing: fn ->
          Logger.warning("[ankusa] no config file found; starting the gateway on defaults")
          %{}
        end
      )
      |> Document.migrate!(@config_version, @migrations)

    {doc, secrets} =
      raw
      |> Document.interpolate(env, [])
      |> Document.apply_overrides(env, @env_overrides)
      |> Document.claim_check_token(env)
      |> Secrets.resolve(Keyword.get(opts, :secrets), Keyword.take(opts, [:refresh]))

    %{
      config: config!(doc),
      gateway: GatewaySection.parse!(doc, check_exposure: true),
      doc: doc,
      log: log!(doc),
      tracing: tracing!(doc),
      secrets: secrets
    }
  end

  @doc """
  `load!/0` for boot and the `check-config` and `print-config` commands: a
  `ConfigError` is printed to stderr as `ankusa-gateway: invalid
  configuration` plus the message, and the VM halts with 78 (`EX_CONFIG`).
  """
  @spec load_or_halt!() :: loaded()
  def load_or_halt! do
    load!()
  rescue
    error in ConfigError ->
      IO.write(:stderr, "ankusa-gateway: invalid configuration\n  " <> error.message <> "\n")
      System.halt(78)
  end

  defp config!(doc) do
    root_keys!(doc)

    opts =
      [instance: :default, roles: [], source_store: {Ankusa.SourceStore.Static, sources: %{}}] ++
        node_section(doc) ++
        listener_section(doc) ++
        metrics_extra(metrics_section(doc)) ++
        storage_section(doc) ++
        claim_check_section(doc) ++
        egress_section(doc) ++
        secrets_section(doc)

    try do
      config = Ankusa.Config.new(opts)
      Ankusa.ClaimCheck.validate_config!(config)
      Ankusa.BlobStore.validate_config!(config)
      Ankusa.HttpClient.Egress.validate_config!(config)
      config
    rescue
      error in ArgumentError ->
        reraise ConfigError, [message: error.message], __STACKTRACE__
    end
  end

  defp root_keys!(doc) do
    case Enum.find(Map.keys(doc), &(&1 in @full_image_keys)) do
      nil ->
        check_keys!(doc, @root_keys, [])

      key ->
        raise ConfigError,
          message:
            "#{key}: not available in the gateway image (jamescarr/ankusa-gateway); use jamescarr/ankusa"
    end
  end

  defp node_section(doc) do
    node = section!(doc, "node", @node_keys, [])

    case node["roles"] do
      nil ->
        :ok

      roles ->
        unless string_list!(roles, ["node", "roles"]) == ["claim_check"] do
          raise ConfigError,
            message:
              "node.roles: this image runs only the gateway; remove it or set [claim_check]"
        end
    end

    []
    |> put_opt(:data_dir, string_opt(node, "data_dir", ["node"]))
    # Core keeps the region in its `dispatch` section (`dispatch.region`).
    |> put_opt(
      :dispatch,
      node["region"] && [region: expect_string(node["region"], ["node", "region"])]
    )
  end

  # The gateway's series join the node's scrape.
  defp metrics_extra(metrics: metrics),
    do: [metrics: metrics ++ [extra: [Ankusa.Gateway.Metrics]]]
end
