defmodule Ankusa.WAL.Postgres.Migration do
  @moduledoc """
  Idempotent DDL bootstrap for `Ankusa.WAL.Postgres`, run once at connection
  startup. Three tables:

    * `ankusa_wal`        — the log. `seq` is a `BIGSERIAL`; may have small gaps
      (a deduped or rolled-back row consumes a sequence value it never keeps).
      Strictly increasing *allocation* alone is not enough to make it usable as
      a cursor — commits can land out of allocation order and a reader would
      skip the lower seq — so `c:Ankusa.WAL.append/2` holds a per-instance
      advisory lock from before the insert until COMMIT, making commit order
      equal allocation order. See `Ankusa.WAL.Postgres`'s `## Seq order`.
    * `ankusa_wal_dedup`  — a **permanent** ledger, separate from `ankusa_wal` and
      never touched by `truncate_through/2`. This is what makes dedup survive
      WAL truncation: `ankusa_wal` rows get deleted once compacted to a segment,
      but the dedup key that row claimed is remembered forever, exactly
      mirroring `WAL.DiskLog`'s persisted `.dedup` snapshot. Carries its own
      `seq` column (filled in by the same transaction that writes `ankusa_wal`)
      rather than looking it up via a join to `ankusa_wal` — that join would stop
      finding a key once its owning row gets truncated.
    * `ankusa_wal_cursors` — one row per named reader cursor (`:dispatch`,
      `:compactor`), scoped by instance.

  Every table is scoped by an `instance` column so one Postgres database can
  back multiple `Ankusa.Instance`s — including the same instance name running on
  many independent BEAM nodes at once, which is the whole point: coordination
  happens through this shared, durable state, never through BEAM distribution.
  """

  @ddl """
  CREATE TABLE IF NOT EXISTS ankusa_wal (
    seq BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    instance TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    dedup_key TEXT,
    envelope BYTEA NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );
  """

  @wal_index """
  CREATE INDEX IF NOT EXISTS ankusa_wal_instance_seq_idx ON ankusa_wal (instance, seq);
  """

  @dedup_ddl """
  CREATE TABLE IF NOT EXISTS ankusa_wal_dedup (
    instance TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    dedup_key TEXT NOT NULL,
    event_id TEXT NOT NULL,
    seq BIGINT,
    PRIMARY KEY (instance, tenant_id, source_id, dedup_key)
  );
  """

  @cursors_ddl """
  CREATE TABLE IF NOT EXISTS ankusa_wal_cursors (
    instance TEXT NOT NULL,
    name TEXT NOT NULL,
    seq BIGINT NOT NULL,
    PRIMARY KEY (instance, name)
  );
  """

  @spec run!(GenServer.server()) :: :ok
  def run!(conn) do
    for stmt <- [@ddl, @wal_index, @dedup_ddl, @cursors_ddl] do
      Postgrex.query!(conn, stmt, [])
    end

    :ok
  end
end
