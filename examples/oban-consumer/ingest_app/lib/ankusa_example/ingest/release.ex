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

  @doc """
  Boots this node's Raft member on a `wal` node before the fleet starts, so the
  cluster elects a leader once instead of racing every client's first append.

  Safe to call on every node: it is the same bootstrap `Ankusa.WAL.Ra` does at
  startup, and a member that is already running is left alone.
  """
  def wal_members do
    config = AnkusaExample.Ingest.Application.build_config()
    Ankusa.WAL.Ra.start_link(instance: config.instance, config: config)
  end
end
