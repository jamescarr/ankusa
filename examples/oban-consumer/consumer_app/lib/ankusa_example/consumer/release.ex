defmodule AnkusaExample.Consumer.Release do
  @moduledoc """
  Standard Ecto release-migration helper: a release has no `mix` executable,
  so `bin/consumer eval "AnkusaExample.Consumer.Release.migrate()"` is how
  `run.sh` applies `priv/repo/migrations` (Oban's tables plus
  `processed_webhooks`) against a fresh cluster.
  """

  @app :ankusa_example_consumer

  def migrate do
    Application.load(@app)

    for repo <- Application.fetch_env!(@app, :ecto_repos) do
      {:ok, _, _} = Ecto.Migrator.with_repo(repo, &Ecto.Migrator.run(&1, :up, all: true))
    end
  end
end
