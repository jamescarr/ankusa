defmodule AnkusaExample.Ingest.Application do
  @moduledoc """
  A `Ankusa.Instance` configured entirely from environment variables — the
  same wrapper shape as `examples/rabbitmq-consumer/ingest_app`, with
  `Ankusa.WAL.Postgres` in place of the default disk-backed WAL and
  `Ankusa.Sink.Http` handing deliveries straight to `consumer_app`'s
  `POST /deliveries` endpoint instead of a broker.

      curl -> Ankusa (edge/dispatch/storage) -> Ankusa.Sink.Http -> consumer_app

  No job framework lives here or anywhere in Ankusa core: this process
  knows nothing about Oban. It durably logs and dispatches webhooks over
  plain HTTP, exactly as it would to any other consumer.
  """

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    config = build_config()

    Logger.info(
      "[ankusa-example] starting: roles=#{inspect(config.roles)} port=#{config.port} " <>
        "wal_host=#{env("WAL_DB_HOST", "localhost")} consumer_url=#{env("CONSUMER_URL", "http://localhost:4200/deliveries")}"
    )

    Supervisor.start_link([{Ankusa.Instance, config}], strategy: :one_for_one, name: __MODULE__)
  end

  defp build_config do
    Ankusa.Config.new(
      instance: :default,
      port: env_int("PORT", 4000),
      data_dir: env("DATA_DIR", "./data"),
      roles: Ankusa.Config.parse_roles!(env("ANKUSA_ROLES", "edge,dispatch,storage")),
      wal: {Ankusa.WAL.Postgres, wal_opts() ++ [migrate: false]},
      source_store: {Ankusa.SourceStore.Static, sources: %{"demo" => source()}}
    )
  end

  # Public: the release task (`AnkusaExample.Ingest.Release.migrate/0`) reuses
  # this exact connection config to run the WAL DDL against the same
  # database, before the application (and its `migrate: false` pool) starts.
  @doc false
  def wal_opts do
    [
      hostname: env("WAL_DB_HOST", "localhost"),
      port: env_int("WAL_DB_PORT", 5432),
      username: env("WAL_DB_USER", "ankusa"),
      password: env("WAL_DB_PASSWORD", "ankusa"),
      database: env("WAL_DB_NAME", "ankusa"),
      pool_size: env_int("WAL_DB_POOL_SIZE", 10)
    ]
  end

  # A single zero-config `demo` source, matching the root project's own
  # quickstart — this example is about the pipeline, not verification, so
  # `Verifier.None` here. Swap in `{Ankusa.Verifier.Hmac, scheme: :stripe}` etc.
  # for a real provider exactly as documented in the root README.
  defp source do
    [
      verifier: {Ankusa.Verifier.None, []},
      dedup: {Ankusa.DedupKey.Rules, json: ["id"]},
      on_verify_failure: :accept_flag,
      sinks: [
        {Ankusa.Sink.Http,
         url: env("CONSUMER_URL", "http://localhost:4200/deliveries"), timeout_ms: 5_000}
      ]
    ]
  end

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
