defmodule Ankusa.WAL.Postgres do
  @moduledoc """
  Shared, multi-node `Ankusa.WAL` backed by Postgres. This is the adapter that
  turns "many ingest servers" into a real fleet: every BEAM node runs its own
  local `Postgrex` connection pool (registered under the same
  `Ankusa.via(instance, :wal)` name DiskLog would use), and coordination between
  nodes happens entirely through the shared database — no BEAM distribution,
  no RPC between edge nodes, matching the framework's existing rule that
  components hand off through durable state, never through each other.

  ## Group commit, translated to SQL

  `append/2` runs inside one `Postgrex.transaction/2` (still exactly one
  `COMMIT`, i.e. one fsync, per batch — matching the core invariant):

    1. **Claim dedup keys.** A single `INSERT ... ON CONFLICT DO NOTHING`
       against `ankusa_wal_dedup`, batched via `unnest/1`. Postgres takes a
       row lock on the conflicting index entry and blocks until the other
       writer's transaction resolves, so two nodes racing the same dedup key
       never double-claim it — the loser reliably sees the winner's committed
       row afterward.
    2. **Insert winners under the instance lock.** Rows that either had no
       dedup key or won their claim are inserted into `ankusa_wal` (again
       batched via `unnest/1`), `RETURNING event_id, seq`; the same statement
       backfills those rows' `ankusa_wal_dedup.seq` by primary key. A record
       whose dedup key collided is never written here — the same "duplicate
       absorbed, nothing extra stored" contract as `WAL.DiskLog`. Before the
       insert the transaction takes the instance's advisory lock (see
       `## Seq order`).
    3. **Resolve losers' seq.** For rows that lost their dedup claim, one
       lookup joins `ankusa_wal_dedup` back to `ankusa_wal` by `event_id` to find
       the seq of the row that already owns that key.

  Every row is correlated by the envelope's own `id` (a UUIDv7, always unique
  per envelope regardless of dedup key), never by array position — positional
  matching against `RETURNING` is not guaranteed to preserve input order.

  ## Seq order

  `seq` is a `BIGSERIAL`, so Postgres assigns it at INSERT time — but a
  transaction only becomes visible at COMMIT, and those two moments are not the
  same. Two writers can allocate 100 and 101 and commit in the opposite order.
  A reader that follows the log with `seq > cursor` would then read 101, move
  its cursor past it, and never see 100 when it lands — permanent, silent loss
  (the compactor truncates through the dispatch cursor too, so the row is gone
  even from the table).

  Rather than make readers tolerate that, **commit order is forced to equal
  allocation order**: every `append/2` that has winners takes
  `pg_advisory_xact_lock(hashtext("ankusa_wal:" <> instance))` before inserting,
  and the lock is held until the transaction ends. The holder's seqs are
  therefore committed and visible before the next holder can allocate, which is
  exactly the contract `Ankusa.WAL` states for readers: once you have seen seq
  N, no record with seq ≤ N can appear later. Seqs may still have gaps
  (rolled-back transactions consume values).

  The cost is real and deliberate: appends for one instance serialize
  fleet-wide, and the lock covers the whole `claim_dedup` → `COMMIT` window. Two
  instance names that hash to the same lock value simply share a lock — correct,
  marginally slower. Cross-instance appends are unaffected. Because Postgres
  allocates from the sequence *inside* the critical section, an allocation is
  never wasted by a lock wait.

  Deadlock-free: the lock is taken before any write to `ankusa_wal`, and the
  holder's only writes are rows it owns — fresh WAL rows and dedup rows it
  claimed itself. It never waits on rows a non-holder holds, so there is no
  cycle to break.

  ## Config

      config :ankusa,
        wal: {Ankusa.WAL.Postgres, hostname: "localhost", port: 5433,
              username: "ankusa", password: "ankusa", database: "ankusa_dev",
              pool_size: 10}

  Any `Postgrex.start_link/1` option is accepted and passed through verbatim
  (`:migrate false` skips the DDL bootstrap, e.g. if you run migrations
  separately in CI).
  """

  @behaviour Ankusa.WAL

  alias Ankusa.Envelope
  alias Ankusa.WAL.Postgres.Migration

  # ── lifecycle ───────────────────────────────────────────────────────────

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    config = Keyword.fetch!(opts, :config)
    {_mod, wal_opts} = config.wal
    {migrate?, postgrex_opts} = Keyword.pop(wal_opts, :migrate, true)

    name = Ankusa.via(instance, :wal)

    with {:ok, pid} <- Postgrex.start_link(Keyword.put(postgrex_opts, :name, name)) do
      if migrate?, do: Migration.run!(pid)
      {:ok, pid}
    end
  end

  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :instance)},
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  # ── behaviour ───────────────────────────────────────────────────────────

  @impl Ankusa.WAL
  def append(server, records) do
    instance = instance_key(server)
    rows = Enum.map(records, &row(&1.envelope, instance))

    Postgrex.transaction(server, fn conn ->
      claimed = claim_dedup(conn, rows)

      winners =
        Enum.filter(rows, fn r -> is_nil(r.dedup_key) or MapSet.member?(claimed, r.event_id) end)

      inserted = insert_winners(conn, instance, winners)

      losers = Enum.reject(rows, fn r -> Map.has_key?(inserted, r.event_id) end)
      loser_seqs = resolve_losers(conn, losers)

      Enum.map(rows, fn r ->
        case Map.fetch(inserted, r.event_id) do
          {:ok, seq} -> {:committed, %{r.env | seq: seq}}
          :error -> {:duplicate, Map.fetch!(loser_seqs, r.event_id)}
        end
      end)
    end)
  end

  @impl Ankusa.WAL
  def read(server, after_seq, limit) do
    instance = instance_key(server)

    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        server,
        "SELECT seq, envelope FROM ankusa_wal WHERE instance = $1 AND seq > $2 ORDER BY seq LIMIT $3",
        [instance, after_seq, limit]
      )

    Enum.map(rows, fn [seq, payload] -> %{Envelope.from_binary(payload) | seq: seq} end)
  end

  @impl Ankusa.WAL
  def get_cursor(server, name) do
    instance = instance_key(server)

    case Postgrex.query!(
           server,
           "SELECT seq FROM ankusa_wal_cursors WHERE instance = $1 AND name = $2",
           [instance, to_string(name)]
         ) do
      %Postgrex.Result{rows: [[seq]]} -> seq
      %Postgrex.Result{rows: []} -> 0
    end
  end

  @impl Ankusa.WAL
  def put_cursor(server, name, seq) do
    instance = instance_key(server)

    Postgrex.query!(
      server,
      """
      INSERT INTO ankusa_wal_cursors (instance, name, seq) VALUES ($1, $2, $3)
      ON CONFLICT (instance, name) DO UPDATE SET seq = EXCLUDED.seq
      """,
      [instance, to_string(name), seq]
    )

    :ok
  end

  @impl Ankusa.WAL
  def truncate_through(server, seq) do
    instance = instance_key(server)
    # `ankusa_wal_dedup` is untouched — dedup keys survive truncation.
    Postgrex.query!(server, "DELETE FROM ankusa_wal WHERE instance = $1 AND seq <= $2", [
      instance,
      seq
    ])

    :ok
  end

  @impl Ankusa.WAL
  def stats(server) do
    instance = instance_key(server)

    %Postgrex.Result{rows: [[records, min_seq, max_seq, bytes]]} =
      Postgrex.query!(
        server,
        """
        SELECT count(*), min(seq), max(seq), COALESCE(pg_total_relation_size('ankusa_wal'), 0)
        FROM ankusa_wal WHERE instance = $1
        """,
        [instance]
      )

    %Postgrex.Result{rows: cursor_rows} =
      Postgrex.query!(server, "SELECT name, seq FROM ankusa_wal_cursors WHERE instance = $1", [
        instance
      ])

    %{
      records: records,
      bytes: bytes,
      next_seq: (max_seq || 0) + 1,
      min_seq: min_seq,
      max_seq: max_seq,
      cursors: Map.new(cursor_rows, fn [name, seq] -> {name, seq} end)
    }
  end

  # ── append internals ───────────────────────────────────────────────────

  defp row(%Envelope{} = env, instance) do
    %{
      env: env,
      event_id: env.id,
      instance: instance,
      # NOT NULL at the storage boundary — don't trust an upstream nil.
      tenant_id: env.tenant_id || "",
      source_id: env.source_id,
      dedup_key: env.dedup_key,
      # `seq` isn't known until Postgres assigns it; the stored payload's own
      # `seq` field is irrelevant — `read/3` always overwrites it from the
      # `seq` column.
      envelope: Envelope.to_binary(%{env | seq: nil})
    }
  end

  defp claim_dedup(conn, rows) do
    case Enum.filter(rows, & &1.dedup_key) do
      [] ->
        MapSet.new()

      dedup_rows ->
        %Postgrex.Result{rows: claimed} =
          Postgrex.query!(
            conn,
            """
            INSERT INTO ankusa_wal_dedup (instance, tenant_id, source_id, dedup_key, event_id)
            SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[])
            ON CONFLICT (instance, tenant_id, source_id, dedup_key) DO NOTHING
            RETURNING event_id
            """,
            [
              Enum.map(dedup_rows, & &1.instance),
              Enum.map(dedup_rows, & &1.tenant_id),
              Enum.map(dedup_rows, & &1.source_id),
              Enum.map(dedup_rows, & &1.dedup_key),
              Enum.map(dedup_rows, & &1.event_id)
            ]
          )

        claimed |> List.flatten() |> MapSet.new()
    end
  end

  defp insert_winners(_conn, _instance, []), do: %{}

  defp insert_winners(conn, instance, winners) do
    # One lock per instance, held from before seq allocation until COMMIT (an
    # xact-scoped advisory lock is released when the transaction ends, and the
    # commit is visible by then). Without it `seq` would be *allocation*-ordered
    # while commits race: a later transaction could commit a higher seq first,
    # and a cursor-following reader (`seq > cursor`) would step over the lower
    # seq that lands afterwards — losing that hook silently. See `## Seq order`.
    Postgrex.query!(conn, "SELECT pg_advisory_xact_lock(hashtext($1))", [
      "ankusa_wal:" <> instance
    ])

    # One statement: insert the winners and backfill their dedup ledger rows in
    # the same round trip. The backfill keys on `ankusa_wal_dedup`'s primary key
    # (`event_id` + the dedup tuple) instead of matching on `event_id` alone, so
    # it is an index lookup, not a scan of the ever-growing ledger.
    %Postgrex.Result{rows: inserted} =
      Postgrex.query!(
        conn,
        """
        WITH ins AS (
          INSERT INTO ankusa_wal (event_id, instance, tenant_id, source_id, dedup_key, envelope)
          SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::bytea[])
          RETURNING event_id, instance, tenant_id, source_id, dedup_key, seq
        ), backfill AS (
          UPDATE ankusa_wal_dedup d SET seq = ins.seq FROM ins
          WHERE ins.dedup_key IS NOT NULL
            AND d.instance = ins.instance AND d.tenant_id = ins.tenant_id
            AND d.source_id = ins.source_id AND d.dedup_key = ins.dedup_key
            AND d.event_id = ins.event_id
        )
        SELECT event_id, seq FROM ins
        """,
        [
          Enum.map(winners, & &1.event_id),
          Enum.map(winners, & &1.instance),
          Enum.map(winners, & &1.tenant_id),
          Enum.map(winners, & &1.source_id),
          Enum.map(winners, & &1.dedup_key),
          Enum.map(winners, & &1.envelope)
        ]
      )

    Map.new(inserted, fn [event_id, seq] -> {event_id, seq} end)
  end

  defp resolve_losers(_conn, []), do: %{}

  defp resolve_losers(conn, losers) do
    %Postgrex.Result{rows: found} =
      Postgrex.query!(
        conn,
        """
        SELECT instance, tenant_id, source_id, dedup_key, seq
        FROM ankusa_wal_dedup
        WHERE (instance, tenant_id, source_id, dedup_key) IN (
          SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[])
        )
        """,
        [
          Enum.map(losers, & &1.instance),
          Enum.map(losers, & &1.tenant_id),
          Enum.map(losers, & &1.source_id),
          Enum.map(losers, & &1.dedup_key)
        ]
      )

    by_tuple =
      Map.new(found, fn [inst, tenant, src, dkey, seq] -> {{inst, tenant, src, dkey}, seq} end)

    Map.new(losers, fn r ->
      {r.event_id, Map.fetch!(by_tuple, {r.instance, r.tenant_id, r.source_id, r.dedup_key})}
    end)
  end

  # The facade always calls with `Ankusa.via(instance, :wal)` — pattern-match
  # the instance back out rather than threading a second argument through
  # every callback, and reuse it as the Postgrex conn target (same name the
  # pool was registered under in `start_link/1`).
  defp instance_key({:via, Registry, {Ankusa.Registry, {instance, :wal}}}),
    do: to_string(instance)
end
