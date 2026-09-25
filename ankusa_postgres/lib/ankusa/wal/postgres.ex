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
  `COMMIT`, i.e. one fsync, per batch — matching the core invariant): it takes
  the instance's advisory lock (see `## Seq order`) and inserts every row with a
  single `INSERT ... SELECT FROM unnest(...) RETURNING event_id, seq`. Every row
  is correlated by the envelope's own `id` (a UUIDv7, always unique per
  envelope), never by array position — positional matching against `RETURNING`
  is not guaranteed to preserve input order.

  There is no dedup step: the log has no uniqueness constraint, so a provider's
  retry is inserted again with its own seq, exactly as `WAL.DiskLog` and
  `WAL.Ra` append it. Deciding that a copy is a duplicate of an event already
  delivered happens at dispatch (`Ankusa.Dispatch.Receiver`), off the ack path.
  A Postgres-era `ankusa_wal_dedup` table in an existing database is left
  untouched — nothing here reads or writes it.

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
  fleet-wide from the moment the transaction starts inserting. Two instance
  names that hash to the same lock value simply share a lock — correct,
  marginally slower. Cross-instance appends are unaffected. Because Postgres
  allocates from the sequence *inside* the critical section, an allocation is
  never wasted by a lock wait. Deadlock-free: the lock is taken before any write
  to `ankusa_wal`, and the holder's only writes are rows it owns.

  ## Cursors, leases and fencing

  A cursor write must carry the token of a live lease for that cursor's lease
  name (`:dispatch`'s cursor by the `:dispatch` lease, `:compactor`'s by the
  `:storage` lease — see `Ankusa.WAL.lease_for_cursor/1`). The check and the
  write happen in one transaction, against the *database* clock (`now()`), so a
  zombie writer on any node is fenced the moment its lease expires:

      SELECT 1 FROM ankusa_wal_leases
       WHERE instance = $1 AND name = $2 AND token = $3 AND expires_at > now()

  Cursor writes are a maximum, not an assignment
  (`GREATEST(ankusa_wal_cursors.seq, EXCLUDED.seq)`), so even a fenced write
  that slipped through could not move a cursor backwards.

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

    # `Postgrex.transaction/2` returns `{:ok, value}`, which is exactly the
    # `{:ok, [result]}` shape `append/2` reports — no unwrapping here.
    Postgrex.transaction(server, fn conn ->
      seqs = insert_rows(conn, instance, rows)

      rows
      |> Enum.zip(seqs)
      |> Enum.map(fn {r, seq} -> {:committed, %{r.env | seq: seq}} end)
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
  def put_cursor(server, name, seq, token) do
    instance = instance_key(server)

    transaction(server, fn conn ->
      if live_lease?(conn, instance, Ankusa.WAL.lease_for_cursor(name), token) do
        # GREATEST: cursors are monotonic, never assigned.
        Postgrex.query!(
          conn,
          """
          INSERT INTO ankusa_wal_cursors (instance, name, seq) VALUES ($1, $2, $3)
          ON CONFLICT (instance, name)
            DO UPDATE SET seq = GREATEST(ankusa_wal_cursors.seq, EXCLUDED.seq)
          """,
          [instance, to_string(name), seq]
        )

        :ok
      else
        {:error, :fenced}
      end
    end)
  end

  @impl Ankusa.WAL
  def truncate_through(server, seq, token) do
    instance = instance_key(server)

    transaction(server, fn conn ->
      if live_lease?(conn, instance, :storage, token) do
        Postgrex.query!(conn, "DELETE FROM ankusa_wal WHERE instance = $1 AND seq <= $2", [
          instance,
          seq
        ])

        :ok
      else
        {:error, :fenced}
      end
    end)
  end

  @impl Ankusa.WAL
  def acquire_lease(server, name, holder, ttl_ms) do
    instance = instance_key(server)

    transaction(server, fn conn ->
      %Postgrex.Result{rows: rows} =
        Postgrex.query!(
          conn,
          """
          INSERT INTO ankusa_wal_leases (instance, name, holder, token, expires_at)
          VALUES ($1, $2, $3, 1, now() + ($4 || ' milliseconds')::interval)
          ON CONFLICT (instance, name) DO UPDATE
            SET holder = EXCLUDED.holder,
                token = ankusa_wal_leases.token + 1,
                expires_at = EXCLUDED.expires_at
            WHERE ankusa_wal_leases.expires_at <= now()
               OR ankusa_wal_leases.holder = EXCLUDED.holder
          RETURNING token, (extract(epoch FROM expires_at) * 1000)::bigint
          """,
          [instance, to_string(name), holder, Integer.to_string(ttl_ms)]
        )

      case rows do
        [[token, expires_at]] ->
          {:ok, lease(name, holder, token, ttl_ms, expires_at)}

        [] ->
          %Postgrex.Result{rows: [[existing]]} =
            Postgrex.query!(
              conn,
              "SELECT holder FROM ankusa_wal_leases WHERE instance = $1 AND name = $2",
              [instance, to_string(name)]
            )

          {:error, {:held, existing}}
      end
    end)
  end

  @impl Ankusa.WAL
  def renew_lease(server, %{name: name, holder: holder, token: token, ttl_ms: ttl_ms}) do
    instance = instance_key(server)

    transaction(server, fn conn ->
      %Postgrex.Result{rows: rows} =
        Postgrex.query!(
          conn,
          """
          UPDATE ankusa_wal_leases
             SET expires_at = now() + ($5 || ' milliseconds')::interval
           WHERE instance = $1 AND name = $2 AND holder = $3 AND token = $4
             AND expires_at > now()
          RETURNING (extract(epoch FROM expires_at) * 1000)::bigint
          """,
          [instance, to_string(name), holder, token, Integer.to_string(ttl_ms)]
        )

      case rows do
        [[expires_at]] -> {:ok, lease(name, holder, token, ttl_ms, expires_at)}
        [] -> {:error, :lost}
      end
    end)
  end

  @impl Ankusa.WAL
  def release_lease(server, %{name: name, holder: holder, token: token}) do
    instance = instance_key(server)

    transaction(server, fn conn ->
      # Expire, never delete: the token counter must only climb, so the next
      # holder gets a token the released one can never reuse.
      Postgrex.query!(
        conn,
        """
        UPDATE ankusa_wal_leases
           SET expires_at = to_timestamp(0)
         WHERE instance = $1 AND name = $2 AND holder = $3 AND token = $4
        """,
        [instance, to_string(name), holder, token]
      )

      :ok
    end)
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
      next_seq: next_seq(server),
      min_seq: min_seq,
      max_seq: max_seq,
      cursors: Map.new(cursor_rows, fn [name, seq] -> {name, seq} end)
    }
  end

  # The next seq the next append will get, read from the sequence itself rather
  # than from `max(seq) + 1`: after a full truncation `max(seq)` is NULL, and
  # "1" would be a seq every cursor is already past — the next append would be
  # invisible to every reader. The sequence is shared by *every instance* in the
  # database, so this is "the next seq the next append will get", not a
  # per-instance record count.
  defp next_seq(server) do
    # Resolved through the catalog rather than hard-coding `ankusa_wal_seq_seq`:
    # the name is derived from the table and column names, and asking is cheap.
    %Postgrex.Result{rows: [[sequence]]} =
      Postgrex.query!(server, "SELECT pg_get_serial_sequence('ankusa_wal', 'seq')", [])

    case sequence do
      nil ->
        1

      name ->
        %Postgrex.Result{rows: rows} =
          Postgrex.query!(server, "SELECT last_value, is_called FROM #{name}", [])

        case rows do
          [[last_value, true]] -> last_value + 1
          _ -> 1
        end
    end
  end

  defp live_lease?(conn, instance, name, token) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        conn,
        """
        SELECT 1 FROM ankusa_wal_leases
         WHERE instance = $1 AND name = $2 AND token = $3 AND expires_at > now()
        """,
        [instance, to_string(name), token]
      )

    rows != []
  end

  defp lease(name, holder, token, ttl_ms, expires_at) do
    %{name: name, holder: holder, token: token, ttl_ms: ttl_ms, expires_at: expires_at}
  end

  # `DBConnection.transaction/2` wraps the function's value in `{:ok, _}` (and
  # replaces it with `{:error, _}` when the transaction itself fails). The lease
  # and cursor callbacks report their own result, so unwrap.
  defp transaction(server, fun) do
    case Postgrex.transaction(server, fun) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
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
      # `seq` isn't known until Postgres assigns it; the stored payload's own
      # `seq` field is irrelevant — `read/3` always overwrites it from the
      # `seq` column.
      envelope: env |> Ankusa.WAL.stamp_commit() |> Map.put(:seq, nil) |> Envelope.to_binary()
    }
  end

  defp insert_rows(_conn, _instance, []), do: []

  defp insert_rows(conn, instance, rows) do
    # One lock per instance, held from before seq allocation until COMMIT (an
    # xact-scoped advisory lock is released when the transaction ends, and the
    # commit is visible by then). Without it `seq` would be *allocation*-ordered
    # while commits race: a later transaction could commit a higher seq first,
    # and a cursor-following reader (`seq > cursor`) would step over the lower
    # seq that lands afterwards — losing that hook silently. See `## Seq order`.
    Postgrex.query!(conn, "SELECT pg_advisory_xact_lock(hashtext($1))", [
      "ankusa_wal:" <> instance
    ])

    # Allocate the seqs *here* rather than letting the column default do it, so
    # each row's seq is known before the insert and the results can be paired
    # with the input by position. The log has no uniqueness constraint, so two
    # rows of one batch may be the same event with the same id — the id cannot
    # identify them, and `RETURNING` order is not specified.
    seqs = allocate_seqs(conn, length(rows))

    %Postgrex.Result{} =
      Postgrex.query!(
        conn,
        """
        INSERT INTO ankusa_wal (seq, event_id, instance, tenant_id, source_id, envelope)
        SELECT * FROM unnest($1::bigint[], $2::text[], $3::text[], $4::text[], $5::text[], $6::bytea[])
        """,
        [
          seqs,
          Enum.map(rows, & &1.event_id),
          Enum.map(rows, & &1.instance),
          Enum.map(rows, & &1.tenant_id),
          Enum.map(rows, & &1.source_id),
          Enum.map(rows, & &1.envelope)
        ]
      )

    seqs
  end

  # `nextval` is volatile, so it runs once per row of the series; the lock above
  # is what makes the allocation order the commit order.
  defp allocate_seqs(conn, count) do
    %Postgrex.Result{rows: [[seqs]]} =
      Postgrex.query!(
        conn,
        "SELECT array_agg(nextval(pg_get_serial_sequence('ankusa_wal', 'seq')::regclass)) " <>
          "FROM generate_series(1, $1::int)",
        [count]
      )

    seqs
  end

  # The facade always calls with `Ankusa.via(instance, :wal)` — pattern-match
  # the instance back out rather than threading a second argument through
  # every callback, and reuse it as the Postgrex conn target (same name the
  # pool was registered under in `start_link/1`).
  defp instance_key({:via, Registry, {Ankusa.Registry, {instance, :wal}}}),
    do: to_string(instance)
end
