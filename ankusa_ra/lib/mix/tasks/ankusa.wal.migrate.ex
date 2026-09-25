defmodule Mix.Tasks.Ankusa.Wal.Migrate do
  @shortdoc "Move a Postgres WAL's state into an Ankusa.WAL.Ra cluster"

  @moduledoc """
  One-shot, **offline** cutover from `Ankusa.WAL.Postgres` to `Ankusa.WAL.Ra`.

      mix ankusa.wal.migrate --from-postgres postgres://… --instance default \
        --members ankusa_wal_default@wal-0,ankusa_wal_default@wal-1,ankusa_wal_default@wal-2

  ## What is copied, and what is deliberately not

  Three things move:

    1. **The seq floor.** `{:import, max_seq, cursors}` sets the new cluster's
       `next_seq` to `max_seq + 1` and seeds the cursors, so no seq is reused and
       a compacted segment key (`seg/<seq>`) can never collide with a new record.
    2. **The cursors.** Dispatch and the compactor resume where they stopped
       instead of replaying the whole log.
    3. **The dedup ledger.** This is the part that is easy to forget and
       expensive to omit: `ankusa_wal_dedup` records every
       `(tenant, source, dedup_key)` the old WAL ever accepted. A provider that
       retries an event it already sent — which is *normal*, and the reason the
       ledger exists — would be accepted a second time by a cluster that had
       never seen the key, violating "one committed seq per dedup key, forever".

  The *records* are not copied, and must not be: the Log must be drained
  (`max(seq)` reached by both cursors, so everything is compacted into segments
  and readable through `Ankusa.Storage`) before this runs. The task refuses to
  start otherwise, naming the reader that is behind.

  Re-running is safe — `{:import_dedup, pairs}` ignores keys that already exist —
  so an interrupted run resumes from the beginning of the ledger without
  double-counting.

  ## Flags

    * `--from-postgres` — the Postgres URL to read from (required).
    * `--instance` — the Ankusa instance name (required).
    * `--members` — comma-separated `node` or `cluster@node` entries (required).
    * `--chunk` — rows per `{:import_dedup, …}` command (default `10000`).
    * `--timeout` — milliseconds for a Ra command (default `15000`).
  """

  use Mix.Task

  alias Ankusa.WAL.Ra

  @switches [
    from_postgres: :string,
    instance: :string,
    members: :string,
    chunk: :integer,
    timeout: :integer
  ]

  @impl true
  def run(args) do
    Mix.Task.run("app.start")
    {opts, _argv, invalid} = OptionParser.parse(args, strict: @switches)

    if invalid != [] do
      Mix.raise("unrecognised option(s): #{inspect(invalid)}")
    end

    url = opts[:from_postgres] || Mix.raise("--from-postgres is required")
    instance = opts[:instance] || Mix.raise("--instance is required")
    members = parse_members(opts[:members] || Mix.raise("--members is required"), instance)
    chunk = opts[:chunk] || 10_000
    timeout = opts[:timeout] || 15_000

    {:ok, conn} = connect(url)

    try do
      max_seq = table_high_water(conn, instance)
      cursors = cursors(conn, instance)

      # The cutover floor is the highest seq the old WAL ever handed out, from
      # *either* source: surviving rows, or a cursor that has already consumed
      # them. Taking only `max(seq)` would be wrong whenever the log is fully
      # compacted — which is exactly the state a cutover runs in — and would
      # restart the new cluster's seqs below cursors that are already past them.
      floor = max(max_seq, cursors |> Map.values() |> Enum.max(fn -> 0 end))

      IO.puts("#{instance}: drained at seq #{floor}, cursors #{inspect(cursors)}")

      raise_if_lagging!(cursors, floor)

      {:ok, :ok} = Ra.remote_command(members, {:import, floor, cursors}, timeout: timeout)

      IO.puts("imported seq floor #{floor} and #{map_size(cursors)} cursor(s)")

      {rows, inserted, skipped} = import_dedup(conn, instance, members, chunk, timeout)

      IO.puts(
        "dedup ledger: #{rows} row(s) read, #{inserted} imported, #{skipped} already present"
      )

      expected = ledger_count(conn, instance)

      if rows != expected do
        Mix.raise("ledger drifted during the copy: read #{rows}, Postgres now has #{expected}")
      end

      overview =
        case leader(members, timeout) do
          {:ok, leader} -> :ra.consistent_aux(leader, :overview, timeout)
          other -> Mix.raise("could not reach the cluster to verify: #{inspect(other)}")
        end

      IO.puts(
        "cluster now holds #{inspect(Map.take(elem(overview, 1), [:dedup_keys, :next_seq, :floor]))}"
      )

      :ok
    after
      GenServer.stop(conn, :normal)
    end
  end

  # ── postgres side ─────────────────────────────────────────────────────────

  defp connect(url) do
    uri = URI.parse(url)
    {user, password} = credentials(uri.userinfo)

    {:ok, _} =
      Postgrex.start_link(
        hostname: uri.host,
        port: uri.port || 5432,
        username: user,
        password: password,
        database: String.trim_leading(uri.path || "", "/")
      )
  end

  defp credentials(nil), do: {nil, nil}

  defp credentials(userinfo) do
    case String.split(userinfo, ":", parts: 2) do
      [user, password] -> {user, URI.decode_www_form(password)}
      [user] -> {user, nil}
    end
  end

  defp table_high_water(conn, instance) do
    %Postgrex.Result{rows: [[count, max_seq]]} =
      Postgrex.query!(conn, "SELECT count(*), max(seq) FROM ankusa_wal WHERE instance = $1", [
        instance
      ])

    if count > 0 do
      IO.puts(
        :stderr,
        "warning: #{count} record(s) still in ankusa_wal (max seq #{max_seq}); " <>
          "the records themselves are not copied, only the floor, cursors and dedup ledger"
      )
    end

    max_seq || 0
  end

  defp cursors(conn, instance) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(conn, "SELECT name, seq FROM ankusa_wal_cursors WHERE instance = $1", [
        instance
      ])

    Map.new(rows, fn [name, seq] -> {cursor_name(name), seq} end)
  end

  defp cursor_name(name) do
    String.to_existing_atom(name)
  rescue
    ArgumentError -> name
  end

  # The Log contract: records above min(dispatch, compactor) must stay readable,
  # and the cutover copies no records — so every reader must have consumed the
  # whole log, or what it has not read yet is lost. A reader that is behind gets
  # named, because "which one" is the only thing an operator needs to fix it.
  defp raise_if_lagging!(cursors, floor) do
    for name <- [:dispatch, :compactor] do
      at = Map.get(cursors, name, 0)

      if at < floor do
        IO.puts(:stderr, "#{name} cursor is at #{at}, but the WAL is at seq #{floor}")
        System.halt(1)
      end
    end
  end

  defp ledger_count(conn, instance) do
    %Postgrex.Result{rows: [[count]]} =
      Postgrex.query!(conn, "SELECT count(*) FROM ankusa_wal_dedup WHERE instance = $1", [
        instance
      ])

    count
  end

  # Keyset pagination over the ledger's primary key. Offsets would be wrong on a
  # table that may still be receiving rows, and a keyset cursor also makes a
  # resumed run simply continue rather than re-scan.
  defp import_dedup(conn, instance, members, chunk, timeout) do
    stream_dedup(conn, instance, members, chunk, timeout, {"", "", ""}, 0, 0, 0)
  end

  defp stream_dedup(conn, instance, members, chunk, timeout, after_key, rows, inserted, skipped) do
    {tenant, source, key} = after_key

    %Postgrex.Result{rows: batch} =
      Postgrex.query!(
        conn,
        """
        SELECT tenant_id, source_id, dedup_key, COALESCE(seq, 0)
        FROM ankusa_wal_dedup
        WHERE instance = $1 AND (tenant_id, source_id, dedup_key) > ($2, $3, $4)
        ORDER BY tenant_id, source_id, dedup_key
        LIMIT $5
        """,
        [instance, tenant, source, key, chunk]
      )

    case batch do
      [] ->
        {rows, inserted, skipped}

      batch ->
        pairs =
          Enum.map(batch, fn [t, s, k, seq] -> {{t, s, k}, seq} end)

        {:ok, {:ok, n}} =
          Ra.remote_command(members, {:import_dedup, pairs}, timeout: timeout)

        last = List.last(batch)
        [lt, ls, lk, _] = last

        rows = rows + length(batch)
        inserted = inserted + n
        skipped = skipped + (length(batch) - n)

        IO.puts("\r  #{rows} row(s)… (#{inserted} imported)")

        stream_dedup(
          conn,
          instance,
          members,
          chunk,
          timeout,
          {lt, ls, lk},
          rows,
          inserted,
          skipped
        )
    end
  end

  # ── ra side ───────────────────────────────────────────────────────────────

  defp parse_members(value, instance) do
    cluster = :"ankusa_wal_#{instance}"

    value
    |> String.split(",", trim: true)
    |> Enum.map(&String.trim/1)
    |> Enum.map(fn entry ->
      # A node name is `name@host`, and the host itself may contain dots (a
      # longname like `ankusa@wal-0.svc.cluster.local`), so only the first `@`
      # separates the parts.
      case String.split(entry, "@", parts: 2) do
        [node] -> {cluster, String.to_atom(node)}
        [_name, node] -> {cluster, String.to_atom(node)}
      end
    end)
  end

  defp leader(members, timeout) do
    Enum.find_value(members, :error, fn member ->
      case :ra.members(member, timeout) do
        {:ok, _members, leader} when leader != nil -> {:ok, leader}
        _ -> nil
      end
    end)
  end
end
