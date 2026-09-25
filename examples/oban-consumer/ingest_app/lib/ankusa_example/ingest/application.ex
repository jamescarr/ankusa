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
        "wal=#{elem(config.wal, 0)} consumer_url=#{env("CONSUMER_URL", "http://localhost:4200/deliveries")}"
    )

    Supervisor.start_link([{Ankusa.Instance, config}], strategy: :one_for_one, name: __MODULE__)
  end

  # Public: the release helpers (`AnkusaExample.Ingest.Release`) need the same
  # config the application runs on — same WAL, same members, same data dir.
  @doc false
  def build_config do
    Ankusa.Config.new(
      instance: :default,
      port: env_int("PORT", 4000),
      data_dir: env("DATA_DIR", "./data"),
      roles: Ankusa.Config.parse_roles!(env("ANKUSA_ROLES", "edge,dispatch,storage")),
      wal: wal(),
      storage: storage(),
      source_store: {Ankusa.SourceStore.Static, sources: %{"demo" => source()}}
    )
  end

  # `ANKUSA_STORAGE_TYPE=s3` points segments at an S3-compatible endpoint — the
  # chaos harness's `floci`, or MinIO, or S3 itself. Default stays local disk,
  # so the laptop shape needs no bucket.
  defp storage do
    # How often the compactor may tick — roll a segment, upload it, and truncate
    # behind the readers. The chaos harness sets this far out so that a run's
    # records are all still in the log when it takes its final scan; left alone,
    # a compactor that keeps up with dispatch reclaims them within seconds, which
    # is correct behaviour and an unreadable log to check an ack against.
    interval_ms = env_int("ANKUSA_STORAGE_INTERVAL_MS", 1_000)

    roll_and_retention = [interval_ms: interval_ms]

    case env("ANKUSA_STORAGE_TYPE", "local") do
      "s3" ->
        roll_and_retention ++
          [
            blob_store:
              {Ankusa.BlobStore.S3,
               [
                 bucket: env("S3_BUCKET", "ankusa-segments"),
                 region: env("S3_REGION", "us-east-1"),
                 endpoint: env("S3_ENDPOINT", "https://s3.us-east-1.amazonaws.com"),
                 access_key_id: env("AWS_ACCESS_KEY_ID", ""),
                 secret_access_key: env("AWS_SECRET_ACCESS_KEY", "")
               ]}
          ]

      _ ->
        roll_and_retention ++ [blob_store: {Ankusa.BlobStore.LocalFS, []}]
    end
  end

  # `WAL=ra` selects the replicated WAL: this node runs the roles it was told
  # to, and talks to the Ra cluster named by `WAL_RA_MEMBERS` (a node with
  # `ANKUSA_ROLES` containing `wal` also hosts a member). Everything else —
  # the roles, the source, the sink — is identical, which is the point of the
  # adapter being a WAL and not a fork.
  defp wal do
    case env("WAL", "postgres") do
      "ra" -> {Ankusa.WAL.Ra, ra_opts()}
      _ -> {Ankusa.WAL.Postgres, wal_opts() ++ [migrate: false]}
    end
  end

  defp ra_opts do
    cluster = :ankusa_wal_default

    members =
      env("WAL_RA_MEMBERS", "")
      |> String.split(",", trim: true)
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.map(&{cluster, String.to_atom(&1)})

    # A single node with no members configured is the laptop shape: one member,
    # the node itself.
    opts = [members: if(members == [], do: [{cluster, node()}], else: members)]

    # `WAL_RA_TIME_OFFSET_MS` shifts the machine's own view of the clock, which
    # is how the chaos harness's `clock-skew` scenario makes a member's clock
    # wrong for real: fencing decisions must not depend on it.
    case env_int("WAL_RA_TIME_OFFSET_MS", 0) do
      0 -> opts
      offset -> Keyword.put(opts, :time_offset_ms, offset)
    end
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
