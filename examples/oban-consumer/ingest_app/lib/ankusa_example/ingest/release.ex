defmodule AnkusaExample.Ingest.Release do
  @moduledoc """
  Release tasks invoked via `bin/ingest eval "AnkusaExample.Ingest.Release.migrate()"`.

  A production release has no Mix (and thus no `mix ankusa.wal.migrate`-style
  task), so the WAL DDL bootstrap has to run through a plain module function
  instead, using the exact same connection config the application itself
  connects with — see `AnkusaExample.Ingest.Application.wal_opts/0`.
  """

  @doc "Runs the `Ankusa.WAL.Postgres` DDL migration against the configured database."
  def migrate do
    {:ok, _} = Application.ensure_all_started(:postgrex)
    {:ok, conn} = Postgrex.start_link(AnkusaExample.Ingest.Application.wal_opts())
    Ankusa.WAL.Postgres.Migration.run!(conn)
  end
end
