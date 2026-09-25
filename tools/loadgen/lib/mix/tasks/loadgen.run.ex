defmodule Mix.Tasks.Loadgen.Run do
  @shortdoc "Fires a load test of webhook POSTs against --url and records acked ids"

  @moduledoc """
  See `tools/loadgen/README.md` for the full flag reference.
  """

  use Mix.Task

  @requirements ["app.start"]

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start")

    {parsed, _rest} =
      OptionParser.parse!(args,
        strict: [
          url: :string,
          concurrency: :integer,
          duration: :integer,
          rate: :integer,
          dup_ratio: :float,
          nil_key_ratio: :float,
          body_bytes: :integer,
          out: :string,
          report: :string,
          events: :string
        ]
      )

    url =
      case Keyword.fetch(parsed, :url) do
        {:ok, url} -> url
        :error -> Mix.raise("--url is required")
      end

    concurrency = Keyword.get(parsed, :concurrency, 64)
    duration = Keyword.get(parsed, :duration, 60)
    rate = Keyword.get(parsed, :rate)
    dup_ratio = Keyword.get(parsed, :dup_ratio, 0.05)
    nil_key_ratio = Keyword.get(parsed, :nil_key_ratio, 0.2)
    body_bytes = Keyword.get(parsed, :body_bytes, 512)
    out_path = Keyword.get(parsed, :out, "acked.csv")
    report_path = Keyword.get(parsed, :report, "loadgen-report.json")
    events_path = Keyword.get(parsed, :events)

    case Finch.start_link(name: Loadgen.Finch, pools: %{default: [size: concurrency]}) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, reason} -> Mix.raise("failed to start Finch pool: #{inspect(reason)}")
    end

    opts = %{
      url: url,
      concurrency: concurrency,
      duration: duration,
      rate: rate,
      dup_ratio: dup_ratio,
      nil_key_ratio: nil_key_ratio,
      body_bytes: body_bytes,
      # Per-request edge events in `Ankusa.WAL.Checker`'s shape. The aggregate
      # report cannot answer "did any edge ack while there was no quorum?" —
      # only a per-request status and timing can.
      events_path: events_path
    }

    wall_start = System.monotonic_time(:millisecond)
    t0_ms = wall_start
    deadline_ms = t0_ms + duration * 1000

    tasks =
      for i <- 0..(concurrency - 1) do
        Task.async(fn -> worker_loop(i, opts, t0_ms, deadline_ms, init_acc()) end)
      end

    await_timeout = duration * 1000 + 30_000
    worker_results = Enum.map(tasks, &Task.await(&1, await_timeout))

    wall_end = System.monotonic_time(:millisecond)
    duration_s = (wall_end - wall_start) / 1000

    merged = merge_results(worker_results)

    write_csv(out_path, merged.accepted_list)

    sorted_latencies = Enum.sort(merged.latencies)
    accepted_per_s = if duration_s > 0, do: merged.accepted / duration_s, else: 0.0
    sent_per_s = if duration_s > 0, do: merged.sent / duration_s, else: 0.0

    report = %{
      sent: merged.sent,
      accepted: merged.accepted,
      quarantined: merged.quarantined,
      shed: merged.shed,
      errors: merged.errors,
      duration_s: duration_s,
      sent_per_s: sent_per_s,
      accepted_per_s: accepted_per_s,
      latency_ms: %{
        p50: percentile_ms(sorted_latencies, 0.50),
        p95: percentile_ms(sorted_latencies, 0.95),
        p99: percentile_ms(sorted_latencies, 0.99),
        max: percentile_ms(sorted_latencies, 1.00)
      }
    }

    File.write!(report_path, JSON.encode!(report))

    if events_path do
      # Oldest first: the events are accumulated newest-first per worker.
      lines = merged.events |> Enum.reverse() |> Enum.map(&JSON.encode!/1)
      File.write!(events_path, Enum.map(lines, &[&1, "\n"]))
    end

    print_report(report)

    if is_integer(rate) and rate > 0 and sent_per_s < 0.95 * rate do
      IO.write(
        :stderr,
        "loadgen: generator fell behind (sent_per_s=#{Float.round(sent_per_s * 1.0, 2)} < " <>
          "rate=#{rate}); raise --concurrency\n"
      )
    end

    if merged.accepted == 0 do
      Mix.raise("loadgen: zero requests were accepted (202) -- refusing to report success")
    end

    :ok
  end

  defp init_acc do
    %{
      sent: 0,
      accepted: 0,
      quarantined: 0,
      shed: 0,
      errors: 0,
      k: 0,
      bodies: [],
      accepted_list: [],
      latencies: [],
      events: []
    }
  end

  defp worker_loop(i, opts, t0_ms, deadline_ms, acc) do
    now = System.monotonic_time(:millisecond)

    if now >= deadline_ms do
      acc
    else
      # Open-loop pacing: request number `n = k*C + i` is *scheduled* at
      # `t0 + n/rate`, so the offered rate is flat from the first request
      # instead of ramping up as each worker takes its own staggered start.
      intended_start_us = maybe_pace(opts.rate, opts.concurrency, i, acc.k, t0_ms)

      if System.monotonic_time(:millisecond) >= deadline_ms do
        acc
      else
        acc
        |> perform_request(opts, intended_start_us)
        |> then(&worker_loop(i, opts, t0_ms, deadline_ms, &1))
      end
    end
  end

  # Returns the µs timestamp this request was *scheduled* to start at, or `nil`
  # when unpaced (closed loop), so latency is never measured from a late send.
  defp maybe_pace(nil, _concurrency, _i, _k, _t0_ms), do: nil

  defp maybe_pace(rate, concurrency, i, k, t0_ms) when is_integer(rate) and rate > 0 do
    n = k * concurrency + i
    intended_start_us = t0_ms * 1000 + div(n * 1_000_000, rate)
    target_ms = t0_ms + round(n * 1000 / rate)
    sleep_ms = target_ms - System.monotonic_time(:millisecond)
    if sleep_ms > 0, do: Process.sleep(sleep_ms)
    intended_start_us
  end

  defp perform_request(acc, opts, intended_start_us) do
    {kind, body} = build_body(acc, opts.dup_ratio, opts.nil_key_ratio, opts.body_bytes)

    t_start = System.monotonic_time(:microsecond)
    started_ms = System.system_time(:millisecond)
    result = send_one(opts.url, body)
    t_end = System.monotonic_time(:microsecond)
    ended_ms = System.system_time(:millisecond)

    # Coordinated omission: when paced, the clock starts at the scheduled send
    # time, so a stalled generator shows up as latency instead of vanishing.
    latency_us = if intended_start_us, do: t_end - intended_start_us, else: t_end - t_start

    acc = %{
      acc
      | sent: acc.sent + 1,
        k: acc.k + 1,
        latencies: [latency_us | acc.latencies],
        events: [edge_event(result, started_ms, ended_ms) | acc.events]
    }

    classify(result, kind, body, acc)
  end

  # One `Ankusa.WAL.Checker` edge event per request. Timestamps are epoch
  # milliseconds on purpose: the fault windows the nemesis records are epoch
  # milliseconds too, so I8 can line them up.
  defp edge_event(result, started_ms, ended_ms) do
    {status, id} =
      case result do
        {:ok, %Req.Response{status: status} = response} when status in 200..299 ->
          {"2xx", extract_id(response.body)}

        {:ok, %Req.Response{status: status}} ->
          {to_string(status), nil}

        {:error, _reason} ->
          {"error", nil}
      end

    %{
      "client" => "loadgen",
      "op" => %{"tag" => "edge", "0" => status, "1" => id},
      "invoked_at" => started_ms,
      "completed_at" => ended_ms,
      "result" => nil
    }
  end

  # The traffic mix the chaos harness runs: mostly keyed (an `id` the edge turns
  # into a dedup key), a fifth with *no* key at all — which is a different code
  # path (`nil` never dedups) and the one a provider that sends no event id takes
  # — and a tenth a resend of an earlier body, which must be absorbed.
  defp build_body(acc, dup_ratio, nil_key_ratio, body_bytes) do
    cond do
      :rand.uniform() < dup_ratio and acc.bodies != [] ->
        {:dup, Enum.random(acc.bodies)}

      :rand.uniform() < nil_key_ratio ->
        body = JSON.encode!(%{"n" => acc.k, "pad" => String.duplicate("x", body_bytes)})
        {:nil_key, body}

      true ->
        id = :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)

        body =
          JSON.encode!(%{"id" => id, "n" => acc.k, "pad" => String.duplicate("x", body_bytes)})

        {:fresh, body}
    end
  end

  defp send_one(url, body) do
    Req.post(url,
      headers: [{"content-type", "application/json"}],
      body: body,
      finch: [name: Loadgen.Finch],
      receive_timeout: 10_000
    )
  rescue
    e -> {:error, e}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  # 202 is the durable ack: the record is committed and will be dispatched. The
  # edge commits *every* copy a provider sends — there is no duplicate reply to
  # classify — so a resend is counted as accepted like any other copy, and the
  # event id it carries is what lets `mix loadgen.verify` tell that this
  # particular ack was deduplicated on the way to a sink.
  defp classify({:ok, %Req.Response{status: 202, body: resp_body}}, _kind, req_body, acc) do
    id = extract_id(resp_body)
    sha = sha256_hex(req_body)

    case resp_body do
      %{"status" => "quarantined"} ->
        # Accepted, but held back from delivery: not an ack a sink can be asked
        # about.
        %{acc | quarantined: acc.quarantined + 1}

      _ ->
        %{
          acc
          | accepted: acc.accepted + 1,
            # Bounded pool: only the 1024 most recent acked bodies are dedup
            # sources, so a long run can't grow this list without limit.
            bodies: Enum.take([req_body | acc.bodies], 1024),
            accepted_list: [{id, sha, event_key(req_body)} | acc.accepted_list]
        }
    end
  end

  defp classify({:ok, %Req.Response{status: 503}}, _kind, _req_body, acc) do
    %{acc | shed: acc.shed + 1}
  end

  defp classify({:ok, %Req.Response{}}, _kind, _req_body, acc) do
    %{acc | errors: acc.errors + 1}
  end

  defp classify({:error, _reason}, _kind, _req_body, acc) do
    %{acc | errors: acc.errors + 1}
  end

  defp extract_id(body) when is_map(body) do
    Map.get(body, "id")
  end

  defp extract_id(body) when is_binary(body) do
    case JSON.decode(body) do
      {:ok, %{"id" => id}} -> id
      _ -> nil
    end
  end

  defp extract_id(_body), do: nil

  defp sha256_hex(body) do
    :crypto.hash(:sha256, body) |> Base.encode16(case: :lower)
  end

  defp merge_results(worker_results) do
    Enum.reduce(
      worker_results,
      %{
        sent: 0,
        accepted: 0,
        quarantined: 0,
        shed: 0,
        errors: 0,
        accepted_list: [],
        latencies: [],
        events: []
      },
      fn r, acc ->
        %{
          sent: acc.sent + r.sent,
          accepted: acc.accepted + r.accepted,
          quarantined: acc.quarantined + r.quarantined,
          shed: acc.shed + r.shed,
          errors: acc.errors + r.errors,
          accepted_list: r.accepted_list ++ acc.accepted_list,
          latencies: r.latencies ++ acc.latencies,
          events: r.events ++ acc.events
        }
      end
    )
  end

  # `id,sha256,event_key`: the third column is the provider's own event id, or
  # empty for a body that has none. `mix loadgen.verify` needs it to tell an ack
  # that was deduplicated at dispatch from an ack that was lost.
  defp write_csv(path, accepted_list) do
    contents =
      Enum.map_join(accepted_list, "", fn {id, sha, key} -> "#{id},#{sha},#{key || ""}\n" end)

    File.write!(path, contents)
  end

  # The provider event id the source's `DedupKey` rule extracts — the `json:
  # ["id"]` the harness configures. `nil` for a body that carries none.
  defp event_key(body) do
    case JSON.decode(body) do
      {:ok, %{"id" => id}} when is_binary(id) -> id
      _ -> nil
    end
  rescue
    _ -> nil
  end

  defp percentile_ms([], _p), do: 0.0

  defp percentile_ms(sorted_us, p) do
    count = length(sorted_us)
    idx = max(0, min(count - 1, ceil(p * count) - 1))
    Enum.at(sorted_us, idx) / 1000
  end

  defp print_report(report) do
    IO.puts("""

    loadgen report
    --------------
    sent            #{report.sent}
    accepted        #{report.accepted}
    quarantined     #{report.quarantined}
    shed            #{report.shed}
    errors          #{report.errors}
    duration_s      #{Float.round(report.duration_s * 1.0, 3)}
    sent_per_s      #{Float.round(report.sent_per_s * 1.0, 2)}
    accepted_per_s  #{Float.round(report.accepted_per_s * 1.0, 2)}
    latency p50 ms  #{report.latency_ms.p50}
    latency p95 ms  #{report.latency_ms.p95}
    latency p99 ms  #{report.latency_ms.p99}
    latency max ms  #{report.latency_ms.max}
    """)
  end
end
