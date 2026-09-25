defmodule Mix.Tasks.Loadgen.Verify do
  @shortdoc "Polls processed_webhooks until every acked id from loadgen.run is drained"

  @moduledoc """
  See `tools/loadgen/README.md` for the full flag reference.
  """

  use Mix.Task

  @requirements ["app.start"]

  @poll_interval_ms 2_000
  @chunk_size 5_000

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start")

    {parsed, _rest} =
      OptionParser.parse!(args,
        strict: [
          acked: :string,
          database_url: :string,
          timeout: :integer,
          report: :string
        ]
      )

    acked_path =
      case Keyword.fetch(parsed, :acked) do
        {:ok, path} -> path
        :error -> Mix.raise("--acked is required")
      end

    database_url =
      case Keyword.fetch(parsed, :database_url) do
        {:ok, url} -> url
        :error -> Mix.raise("--database-url is required")
      end

    timeout_s = Keyword.get(parsed, :timeout, 300)
    report_path = Keyword.get(parsed, :report, "verify-report.json")

    # The sink is still being written to while this runs — the consumer is
    # upserting the very rows being polled for — so the pool must tolerate a
    # busy database rather than drop the request: a pool timeout is not evidence
    # of loss, and crashing on one fails a run that lost nothing.
    connect_opts =
      parse_database_url(database_url) ++
        [pool_size: 4, queue_target: 15_000, queue_interval: 30_000]

    {:ok, conn} = Postgrex.start_link(connect_opts)

    acked = read_acked_csv(acked_path)
    acked_ids = Enum.map(acked, fn {id, _sha} -> id end)
    expected_by_id = Map.new(acked, fn {id, sha} -> {id, sha} end)

    start_ms = System.monotonic_time(:millisecond)
    deadline_ms = start_ms + timeout_s * 1000

    {found_by_id, drain_s} = poll_until_drained(conn, acked_ids, start_ms, deadline_ms)

    missing_ids = Enum.reject(acked_ids, &Map.has_key?(found_by_id, &1))

    sha_mismatches =
      found_by_id
      |> Enum.count(fn {id, {sha, _deliveries}} -> Map.fetch!(expected_by_id, id) != sha end)

    extra_deliveries =
      found_by_id
      |> Enum.map(fn {_id, {_sha, deliveries}} -> max(deliveries - 1, 0) end)
      |> Enum.sum()

    total_processed = total_processed_count(conn)
    unacked_processed = max(total_processed - map_size(found_by_id), 0)

    report = %{
      acked: length(acked_ids),
      processed: map_size(found_by_id),
      missing: Enum.take(missing_ids, 10),
      sha_mismatches: sha_mismatches,
      extra_deliveries: extra_deliveries,
      drain_s: drain_s,
      unacked_processed: unacked_processed
    }

    File.write!(report_path, JSON.encode!(report))

    print_report(report, length(missing_ids))

    if length(missing_ids) > 0 or sha_mismatches > 0 do
      Mix.raise(
        "loadgen.verify: #{length(missing_ids)} missing, #{sha_mismatches} sha mismatches"
      )
    end

    :ok
  end

  defp parse_database_url(url) do
    uri = URI.parse(url)
    [userinfo_user, userinfo_pass] = split_userinfo(uri.userinfo)
    database = uri.path |> to_string() |> String.trim_leading("/")

    [
      hostname: uri.host,
      port: uri.port || 5432,
      username: userinfo_user,
      password: userinfo_pass,
      database: database
    ]
  end

  defp split_userinfo(nil), do: [nil, nil]

  defp split_userinfo(userinfo) do
    case String.split(userinfo, ":", parts: 2) do
      [user, pass] -> [user, pass]
      [user] -> [user, nil]
    end
  end

  defp read_acked_csv(path) do
    path
    |> File.stream!()
    |> Stream.map(&String.trim/1)
    |> Stream.reject(&(&1 == ""))
    |> Enum.map(fn line ->
      [id, sha] = String.split(line, ",", parts: 2)
      {id, sha}
    end)
  end

  # drain_s is approximated: each poll iteration re-queries the full acked-id set,
  # so we can only detect *when* the missing set first became empty, not the exact
  # timestamp any single id was written. That poll-iteration timestamp (minus the
  # start of polling) is used as the drain time.
  defp poll_until_drained(conn, acked_ids, start_ms, deadline_ms) do
    do_poll(conn, acked_ids, start_ms, deadline_ms, %{})
  end

  defp do_poll(conn, acked_ids, start_ms, deadline_ms, _prev_found) do
    found = query_found(conn, acked_ids)
    now_ms = System.monotonic_time(:millisecond)
    all_found? = Enum.all?(acked_ids, &Map.has_key?(found, &1))

    cond do
      all_found? ->
        {found, (now_ms - start_ms) / 1000}

      now_ms >= deadline_ms ->
        {found, (now_ms - start_ms) / 1000}

      true ->
        Process.sleep(@poll_interval_ms)
        do_poll(conn, acked_ids, start_ms, deadline_ms, found)
    end
  end

  # And if the pool still cannot answer, wait for it: the poll has a deadline of
  # its own, and that deadline is the one that decides whether a record is
  # missing.
  @query_attempts 10

  defp query_found(conn, acked_ids, attempts \\ @query_attempts) do
    do_query_found(conn, acked_ids)
  rescue
    e in DBConnection.ConnectionError ->
      if attempts > 1 do
        Process.sleep(500)
        query_found(conn, acked_ids, attempts - 1)
      else
        reraise e, __STACKTRACE__
      end
  end

  defp do_query_found(conn, acked_ids) do
    acked_ids
    |> Enum.chunk_every(@chunk_size)
    |> Enum.reduce(%{}, fn chunk, acc ->
      %Postgrex.Result{rows: rows} =
        Postgrex.query!(
          conn,
          "SELECT ankusa_id, body_sha256, deliveries FROM processed_webhooks WHERE ankusa_id = ANY($1)",
          [chunk]
        )

      Enum.reduce(rows, acc, fn [id, sha, deliveries], acc2 ->
        Map.put(acc2, id, {sha, deliveries})
      end)
    end)
  end

  defp total_processed_count(conn) do
    %Postgrex.Result{rows: [[count]]} =
      Postgrex.query!(conn, "SELECT count(*) FROM processed_webhooks", [])

    count
  end

  defp print_report(report, missing_count) do
    status = if missing_count > 0 or report.sha_mismatches > 0, do: "FAIL", else: "PASS"

    IO.puts("""

    loadgen verify report [#{status}]
    ---------------------
    acked             #{report.acked}
    processed         #{report.processed}
    missing           #{missing_count} (showing up to 10: #{inspect(report.missing)})
    sha_mismatches    #{report.sha_mismatches}
    extra_deliveries  #{report.extra_deliveries}
    drain_s           #{report.drain_s}
    unacked_processed #{report.unacked_processed}
    """)
  end
end
