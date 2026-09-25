defmodule Mix.Tasks.Ankusa.Chaos.Verify do
  @shortdoc "Check a chaos run's evidence against the WAL invariants"

  @moduledoc """
  Runs `Ankusa.WAL.Checker` over one chaos scenario's evidence and writes a
  report.

      mix ankusa.chaos.verify \
        --events out/kill-leader-events.jsonl \
        --acked out/kill-leader.csv \
        --final out/kill-leader-final.json \
        --report out/kill-leader-verify.json

  Exit status is non-zero when any invariant is violated or a `2xx`-acked id is
  not readable — that is what makes `chaos/run.sh` a gate rather than a log.

  ## Evidence

    * `--events` — JSONL, one `Ankusa.WAL.Checker` event per line (what each
      client tried, when, and what it got back).
    * `--acked` — the load generator's acked CSV (`id,sha256` per line): the
      only acks that have to be honest.
    * `--final` — JSON, the final scan: `[%{"id","seq","sha256"}]` covering the
      WAL *and* the compacted segments.
    * `--fault` — JSON, optional: `[{"from": ms, "to": ms}]` windows during
      which no quorum existed, for I8.
  """

  use Mix.Task

  alias Ankusa.WAL.Checker

  @switches [
    events: :string,
    acked: :string,
    final: :string,
    fault: :string,
    report: :string,
    "append-timeout-ms": :integer
  ]

  @impl true
  def run(args) do
    Mix.Task.run("app.start")
    {opts, _rest, invalid} = OptionParser.parse(args, strict: @switches)

    if invalid != [], do: Mix.raise("unrecognised option(s): #{inspect(invalid)}")

    events = read_jsonl(fetch!(opts, :events))
    acked = read_acked(fetch!(opts, :acked))
    final = read_json(fetch!(opts, :final))

    check_opts =
      [quorum_down: (opts[:fault] && read_windows(opts[:fault])) || []] ++
        if(opts[:"append-timeout-ms"],
          do: [append_timeout_ms: opts[:"append-timeout-ms"]],
          else: []
        )

    report = Checker.check(events, acked, final, check_opts)

    not_exercised =
      for {invariant, count} <- report.evaluated, count == 0, do: to_string(invariant)

    out = %{
      "events" => length(events),
      "acked" => map_size(acked),
      "readable" => length(final),
      "missing" => report.missing,
      "extra" => report.extra,
      "violations" =>
        Enum.map(report.violations, fn {invariant, detail} ->
          %{"invariant" => to_string(invariant), "detail" => inspect(detail)}
        end),
      "evaluated" =>
        Map.new(report.evaluated, fn {invariant, count} ->
          {to_string(invariant), count}
        end),
      "not_exercised" => not_exercised,
      "passed" => report.missing == [] and report.violations == []
    }

    if path = opts[:report] do
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, JSON.encode!(out))
    end

    IO.puts("""
    chaos verify
      events        #{out["events"]}
      acked         #{out["acked"]}
      readable      #{out["readable"]}
      missing       #{length(out["missing"])}
      violations    #{length(out["violations"])}
      not exercised #{if not_exercised == [], do: "none", else: Enum.join(not_exercised, ", ")}
    """)

    # Not a failure: a scenario that drives the edge over HTTP has no `append`
    # events to offer, so I4/I5/I7/I9 are legitimately silent there. Saying so
    # out loud is the difference between a gate that passed and a gate that
    # never ran.
    if not_exercised != [] do
      IO.puts("  (invariants with no evidence in this run are unchecked here, not satisfied)")
    end

    if out["passed"] do
      :ok
    else
      Mix.raise("chaos verify: invariant violation(s); see #{opts[:report]}")
    end
  end

  defp fetch!(opts, key) do
    opts[key] || Mix.raise("--#{key} is required")
  end

  defp read_jsonl(path) do
    path
    |> File.stream!()
    |> Stream.map(&String.trim/1)
    |> Stream.reject(&(&1 == ""))
    |> Enum.map(&(&1 |> JSON.decode!() |> to_atom_term()))
  end

  defp read_json(path), do: JSON.decode!(File.read!(path)) |> to_atom_term()

  # Outage windows, as epoch-millisecond pairs. Read by string key — they are
  # plain objects, not tagged operations — and rejected loudly if malformed: a
  # window the checker cannot parse would otherwise disable I8 silently, which
  # is worse than failing the run.
  defp read_windows(path) do
    path
    |> File.read!()
    |> JSON.decode!()
    |> Enum.map(fn
      %{"from" => from, "to" => to} when is_integer(from) and is_integer(to) ->
        {from, to}

      other ->
        Mix.raise("fault window must be {\"from\": ms, \"to\": ms}, got: #{inspect(other)}")
    end)
  end

  # The load generator's acked CSV: `id,sha256hex` per `201`. Both columns are
  # evidence — the id for I1, the digest for I2 and I10 — so both are kept.
  defp read_acked(path) do
    path
    |> File.stream!()
    |> Stream.map(&String.trim/1)
    |> Stream.reject(&(&1 == ""))
    |> Map.new(fn line ->
      case String.split(line, ",") do
        [id, sha | _] -> {id, String.trim(sha)}
        [id] -> {id, nil}
      end
    end)
  end

  # The evidence files are JSON, so a tagged operation arrives as
  # `{"tag":"put_cursor","0":"dispatch","1":5,"2":1}`; the checker wants
  # `{:put_cursor, "dispatch", 5, 1}`. Keys that are digits give the tuple's
  # order, plain objects become atom-keyed maps (`meta`, a read row), and a
  # string with a leading colon is an atom (`":fenced"`), which is how a script
  # writes one.
  defp to_atom_term(%{"tag" => tag} = object) do
    args =
      object
      |> Map.delete("tag")
      |> Enum.sort_by(fn {key, _value} -> String.to_integer(key) end)
      |> Enum.map(fn {_key, value} -> to_atom_term(value) end)

    List.to_tuple([String.to_atom(tag) | args])
  end

  defp to_atom_term(object) when is_map(object) do
    Map.new(object, fn {key, value} -> {String.to_atom(key), to_atom_term(value)} end)
  end

  defp to_atom_term(list) when is_list(list), do: Enum.map(list, &to_atom_term/1)
  defp to_atom_term(":" <> name), do: String.to_atom(name)
  defp to_atom_term(other), do: other
end
