defmodule AnkusaExample.Ingest.Application do
  @moduledoc """
  The whole point of this example: a `Ankusa.Instance` configured entirely from
  environment variables. Nothing here is example-specific machinery — this
  is what "the same code runs as one binary on a laptop or as separate
  fleets" looks like when you actually deploy it: swap env vars, not code.

  Topology:

      curl -> Ankusa (edge/dispatch/storage) -> Ankusa.Sink.RabbitMQ -> exchange
                                            \\-> Ankusa.Storage.Compactor -> S3 (segments)

  Two independent things land in the object store under different prefixes:
  the async segment compactor (`seg/...`, always, every hook) and the
  RabbitMQ sink's fat-payload claim check (`claims/...`, only when a body
  exceeds `INLINE_MAX_BYTES`). No collision, no coordination needed between
  them.

  The same image also runs as the standalone `:claim_check`-role gateway
  (`ANKUSA_ROLES=claim_check` — see `docker-compose.yml`'s `claim-check`
  service): both containers point `storage.blob_store` at the same S3
  bucket, so a claim ref checked in by the ingest node (using its own S3
  credentials) redeems through the claim-check node's HTTP API — the surface
  the worker actually calls, since the worker holds no S3 credentials of its
  own.
  """

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    config = build_config()

    Logger.info(
      "[ankusa-example] starting: roles=#{inspect(config.roles)} port=#{config.port} " <>
        "claim_check_port=#{config.claim_check.port} exchange=#{exchange()} " <>
        "s3_bucket=#{env("S3_BUCKET", "ankusa-example")} s3_endpoint=#{env("S3_ENDPOINT", "http://localhost:4566")}"
    )

    Supervisor.start_link([{Ankusa.Instance, config}], strategy: :one_for_one, name: __MODULE__)
  end

  defp build_config do
    Ankusa.Config.new(
      instance: :default,
      port: env_int("PORT", 4000),
      data_dir: env("DATA_DIR", "./data"),
      roles: roles(),
      storage: %{blob_store: {Ankusa.BlobStore.S3, s3_opts()}},
      source_store: {Ankusa.SourceStore.Static, sources: %{"demo" => source()}},
      claim_check: %{
        port: env_int("CLAIM_CHECK_PORT", 4001)
      }
    )
  end

  defp roles do
    Ankusa.Config.parse_roles!(System.get_env("ANKUSA_ROLES", "edge,dispatch,storage"))
  end

  # A single zero-config `demo` source, matching the root project's own
  # quickstart — this example is about the pipeline, not verification, so
  # `Verifier.None` here. Swap in `{Ankusa.Verifier.Hmac, scheme: :stripe}` etc.
  # for a real provider exactly as documented in the root README.
  defp source do
    [
      verifier: {Ankusa.Verifier.None, []},
      dedup_key: {Ankusa.DedupKey.Rules, []},
      on_verify_failure: :accept_flag,
      sinks: [
        {Ankusa.Sink.RabbitMQ,
         exchange: exchange(),
         url: env("RABBITMQ_URL", "amqp://guest:guest@localhost:5672"),
         inline_max_bytes: env_int("INLINE_MAX_BYTES", 8_192)}
      ]
    ]
  end

  defp s3_opts do
    [
      bucket: env("S3_BUCKET", "ankusa-example"),
      region: env("S3_REGION", "us-east-1"),
      endpoint: env("S3_ENDPOINT", "http://localhost:4566"),
      access_key_id: env("S3_ACCESS_KEY_ID", "test"),
      secret_access_key: env("S3_SECRET_ACCESS_KEY", "test")
    ]
  end

  defp exchange, do: env("RABBITMQ_EXCHANGE", "ankusa.events")

  defp env(key, default), do: System.get_env(key, default)
  defp env_int(key, default), do: key |> System.get_env() |> parse_int(default)

  defp parse_int(nil, default), do: default

  defp parse_int(v, default) do
    case Integer.parse(v) do
      {n, _} -> n
      :error -> default
    end
  end
end
