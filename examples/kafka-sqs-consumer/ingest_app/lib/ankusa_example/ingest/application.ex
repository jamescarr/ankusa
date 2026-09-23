defmodule AnkusaExample.Ingest.Application do
  @moduledoc """
  A `Ankusa.Instance` configured entirely from environment variables — the
  same wrapper as `examples/rabbitmq-consumer/ingest_app`, with
  `Ankusa.Sink.Kafka` in place of `Ankusa.Sink.RabbitMQ`.

      curl -> Ankusa (edge/dispatch/storage) -> Ankusa.Sink.Kafka -> topic
                                            \\-> Ankusa.Storage.Compactor -> S3 (segments)

  The same image runs the `:claim_check`-role gateway
  (`ANKUSA_ROLES=claim_check`, see `docker-compose.yml`): both point
  `storage.blob_store` at the same bucket, so a ticket checked in by ingest
  redeems through the gateway's HTTP API, which is all the worker can reach.
  """

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    config = build_config()

    Logger.info(
      "[ankusa-example] starting: roles=#{inspect(config.roles)} port=#{config.port} " <>
        "claim_check_port=#{config.claim_check.port} topic=#{topic()} " <>
        "brokers=#{Enum.join(brokers(), ",")}"
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
        port: env_int("CLAIM_CHECK_PORT", 4001),
        api_tokens: %{env("CLAIM_CHECK_TOKEN", "dev-claim-check-token") => :all}
      }
    )
  end

  defp roles do
    Ankusa.Config.parse_roles!(System.get_env("ANKUSA_ROLES", "edge,dispatch,storage"))
  end

  defp source do
    [
      verifier: {Ankusa.Verifier.None, []},
      dedup: {Ankusa.DedupKey.Rules, []},
      on_verify_failure: :accept_flag,
      sinks: [
        {Ankusa.Sink.Kafka,
         brokers: brokers(), topic: topic(), inline_max_bytes: env_int("INLINE_MAX_BYTES", 8_192)}
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

  defp brokers, do: "KAFKA_BROKERS" |> env("localhost:19092") |> String.split(",", trim: true)
  defp topic, do: env("KAFKA_TOPIC", "ankusa.events")

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
