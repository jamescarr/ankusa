defmodule Ankusa.WAL.Postgres.Migration do
  @moduledoc """
  Idempotent DDL bootstrap for `Ankusa.WAL.Postgres`, run once at connection
  startup. Three tables:

    * `ankusa_wal`        — the log. `seq` is a `BIGSERIAL`; may have small gaps
      (a rolled-back transaction consumes sequence values it never keeps).
      Strictly increasing *allocation* alone is not enough to make it usable as
      a cursor — commits can land out of allocation order and a reader would
      skip the lower seq — so `c:Ankusa.WAL.append/2` holds a per-instance
      advisory lock from before the insert until COMMIT, making commit order
      equal allocation order. See `Ankusa.WAL.Postgres`'s `## Seq order`.
    * `ankusa_wal_cursors` — one row per named reader cursor (`:dispatch`,
      `:compactor`), scoped by instance.
    * `ankusa_wal_leases`  — one row per lease name (`:dispatch`, `:storage`),
      scoped by instance. `token` climbs on every acquisition; `expires_at` is
      compared against the database clock (`now()`), so every node agrees on
      whether a lease is live regardless of its own wall clock. A cursor write
      or truncation must carry the live token for the row it touches.

  The log has **no uniqueness constraint**, so no ledger table exists. An
  `ankusa_wal_dedup` table and a `dedup_key` column from a Postgres-era schema
  are left alone if a database already has them: nothing reads or writes either
  one, and dropping a table an operator's database still holds is not this
  module's call to make.

  Every table is scoped by an `instance` column so one Postgres database can
  back multiple `Ankusa.Instance`s — including the same instance name running on
  many independent BEAM nodes at once, which is the whole point: coordination
  happens through this shared, durable state, never through BEAM distribution.
  """

  @ddl """
  CREATE TABLE IF NOT EXISTS ankusa_wal (
    seq BIGSERIAL PRIMARY KEY,
    -- Not unique: the log has no uniqueness constraint, so the same event
    -- appended twice is two rows. (Older deployments have
    -- `ankusa_wal_event_id_key`; `@drop_event_id_unique` removes it.)
    event_id TEXT NOT NULL,
    instance TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    envelope BYTEA NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );
  """

  # The uniqueness that the ledger-era schema put on `event_id` is exactly what
  # this log must not have. Idempotent, so it runs on every boot. Guarded so a
  # boot against an already-migrated schema does not take an ACCESS EXCLUSIVE
  # lock to run a DROP that would be a no-op.
  @drop_event_id_unique """
  DO $$
  BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ankusa_wal_event_id_key') THEN
      ALTER TABLE ankusa_wal DROP CONSTRAINT ankusa_wal_event_id_key;
    END IF;
  END $$;
  """

  @wal_index """
  CREATE INDEX IF NOT EXISTS ankusa_wal_instance_seq_idx ON ankusa_wal (instance, seq);
  """

  @cursors_ddl """
  CREATE TABLE IF NOT EXISTS ankusa_wal_cursors (
    instance TEXT NOT NULL,
    name TEXT NOT NULL,
    seq BIGINT NOT NULL,
    PRIMARY KEY (instance, name)
  );
  """

  @leases_ddl """
  CREATE TABLE IF NOT EXISTS ankusa_wal_leases (
    instance TEXT NOT NULL,
    name TEXT NOT NULL,
    holder TEXT NOT NULL,
    token BIGINT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (instance, name)
  );
  """

  @spec run!(GenServer.server()) :: :ok
  def run!(conn) do
    for stmt <- [@ddl, @drop_event_id_unique, @wal_index, @cursors_ddl, @leases_ddl] do
      Postgrex.query!(conn, stmt, [])
    end

    :ok
  end
end
