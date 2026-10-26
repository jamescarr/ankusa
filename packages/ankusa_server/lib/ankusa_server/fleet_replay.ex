defmodule AnkusaServer.FleetReplay do
  @moduledoc """
  `docker-entrypoint replay`: one replay job, started on every node of a fleet
  and followed until each node has finished it.

  Replay jobs live on one node (`Ankusa.Replay`): they re-send that node's dead
  rows, and a node's store is its own. This command is the fleet's view of them,
  with no clustering inside the nodes: it is a client of each node's admin API.

      docker-entrypoint replay --nodes <list> [--kind dlq] [--filter k=v[,k=v]]...
                               [--rate N] [--max-lag-ms N] [--job-id UUID]
                               [--token T] [--timeout S] [--interval S]
                               [--cacertfile PATH]

    * `--nodes` — `[scheme://]host[:port][,…]` (port `4002`, scheme `http`). A name
      that resolves to several addresses (a headless service, a round-robin DNS
      name) is one node per address, over `http`; with `https` list the nodes.
    * `--kind` — `dlq` (default), `archive`, `quarantine` or `lifecycle`.
    * `--filter` — `key=value` pairs, comma-separated or repeated; the keys of the
      `POST /v1/replays` body: `tenant`, `source_id`, `id`, `error_class`
      (`permanent`, `throttled`, `transient`) as text; `sink`, `since`, `until`,
      `from`, `to` as integers; `sinks` as integers joined by `+` (`0+2`).
    * `--job-id` — the job's id on every node (default: a new UUIDv7, printed
      first). Every node gets the same `POST /v1/replays` with that id, and a node
      that already has the job answers with it (`200`), so this command can be
      started again with the same `--job-id` after it, or a node, was killed.
    * `--token` — the admin bearer token (default: `ANKUSA_ADMIN_TOKEN`); needs
      `replays:write`, `dlq:write` for a `dlq` job, and `tenant:*` (or the tenants
      the filter names).
    * `--timeout` — seconds to wait for every node to finish (default `3600`);
      `--interval` — seconds between polls (default `1`).

  A node that does not answer is asked again every interval until the timeout, and
  a node that restarts resumes its job from the cursor it committed. When the
  command stops it prints one line:

      replay <id> kind=dlq nodes=3 done=3 running=0 paused=0 cancelled=0 failed=0 unreachable=0 rejected=0 moved=… scanned=… skipped=… delivered=… dead=…

  and exits `0` when every node is `done`, `1` otherwise (a `paused` job, which an
  operator or the auto-pause stopped, needs attention: `PATCH /v1/replays/<id>`),
  `64` for a bad option.
  """

  alias Ankusa.HttpClient

  @default_port 4002
  @request_timeout_ms 10_000
  @progress_every_ms 10_000
  @kinds ~w(dlq archive quarantine lifecycle)
  @string_keys ~w(tenant source_id id error_class)
  @int_keys ~w(sink since until from to)
  @switches [
    nodes: :string,
    kind: :string,
    filter: :keep,
    rate: :integer,
    max_lag_ms: :integer,
    job_id: :string,
    token: :string,
    timeout: :integer,
    interval: :integer,
    cacertfile: :string
  ]
  @job_id_re ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/
  # What a job's `state` ends the wait for: a `paused` job moves only when an
  # operator resumes it.
  @finished ~w(done cancelled failed paused)
  @counters ~w(moved scanned skipped delivered dead)

  @type options :: %{
          nodes: [{String.t(), String.t(), pos_integer()}],
          kind: String.t(),
          body: %{optional(String.t()) => term()},
          job_id: String.t(),
          token: String.t() | nil,
          timeout_s: pos_integer(),
          interval_s: pos_integer(),
          cacertfile: String.t() | nil
        }

  @type node_status :: %{
          label: String.t(),
          state:
            :pending
            | :running
            | :done
            | :paused
            | :cancelled
            | :failed
            | :unreachable
            | :rejected,
          job_id: String.t() | nil,
          counters: %{optional(String.t()) => integer()},
          detail: String.t() | nil
        }

  @typedoc "What `run/2` returns: the job id, the kind, and each node's last known status."
  @type result :: %{job_id: String.t(), kind: String.t(), nodes: [node_status()]}

  @doc """
  The options of `docker-entrypoint replay` as a map, or `{:error, message}`.
  """
  @spec parse_args([String.t()]) :: {:ok, options()} | {:error, String.t()}
  def parse_args(args) do
    case OptionParser.parse(args, strict: @switches) do
      {opts, [], []} -> build(opts)
      {_opts, [arg | _], []} -> {:error, "unexpected argument #{inspect(arg)}"}
      {_opts, _args, [{flag, _} | _]} -> {:error, "unknown or malformed option #{flag}"}
    end
  end

  defp build(opts) do
    kind = Keyword.get(opts, :kind, "dlq")
    job_id = Keyword.get(opts, :job_id) || Ankusa.UUIDv7.generate()

    with {:ok, nodes} <- fetch_nodes(opts),
         :ok <- check(kind in @kinds, "--kind must be one of #{Enum.join(@kinds, ", ")}"),
         :ok <- check(Regex.match?(@job_id_re, job_id), "--job-id must be a lowercase UUID"),
         {:ok, filter} <- parse_filters(Keyword.get_values(opts, :filter)),
         {:ok, timeout_s} <- positive(opts, :timeout, 3600),
         {:ok, interval_s} <- positive(opts, :interval, 1) do
      body =
        filter
        |> Map.merge(%{"kind" => kind, "job_id" => job_id})
        |> put_opt("rate", opts[:rate])
        |> put_opt("max_lag_ms", opts[:max_lag_ms])

      {:ok,
       %{
         nodes: nodes,
         kind: kind,
         body: body,
         job_id: job_id,
         token: Keyword.get(opts, :token) || System.get_env("ANKUSA_ADMIN_TOKEN"),
         timeout_s: timeout_s,
         interval_s: interval_s,
         cacertfile: opts[:cacertfile]
       }}
    end
  end

  defp check(true, _message), do: :ok
  defp check(false, message), do: {:error, message}

  defp positive(opts, key, default) do
    value = Keyword.get(opts, key, default)
    flag = "--" <> String.replace(to_string(key), "_", "-")
    if value >= 1, do: {:ok, value}, else: {:error, "#{flag} must be at least 1"}
  end

  defp put_opt(map, _key, nil), do: map
  defp put_opt(map, key, value), do: Map.put(map, key, value)

  defp fetch_nodes(opts) do
    case Keyword.fetch(opts, :nodes) do
      {:ok, list} -> parse_nodes(list)
      :error -> {:error, "--nodes is required"}
    end
  end

  # `{scheme, host, port}` per entry.
  defp parse_nodes(list) do
    list
    |> String.split(",", trim: true)
    |> Enum.map(&String.trim/1)
    |> Enum.reduce_while({:ok, []}, fn entry, {:ok, acc} ->
      case parse_node(entry) do
        {:ok, node} -> {:cont, {:ok, [node | acc]}}
        {:error, _message} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, []} -> {:error, "--nodes names no node"}
      {:ok, nodes} -> {:ok, nodes |> Enum.reverse() |> Enum.uniq()}
      error -> error
    end
  end

  defp parse_node(entry) do
    with {:ok, scheme, rest} <- split_scheme(entry),
         {:ok, host, port} <- split_port(rest) do
      if Regex.match?(~r/\A[A-Za-z0-9._-]+\z/, host),
        do: {:ok, {scheme, host, port}},
        else: {:error, "--nodes: #{inspect(entry)} is not [scheme://]host[:port]"}
    else
      :error -> {:error, "--nodes: #{inspect(entry)} is not [scheme://]host[:port]"}
    end
  end

  defp split_scheme(entry) do
    case String.split(entry, "://", parts: 2) do
      [scheme, rest] when scheme in ["http", "https"] -> {:ok, scheme, rest}
      [_no_scheme] -> {:ok, "http", entry}
      _ -> :error
    end
  end

  defp split_port(rest) do
    case String.split(rest, ":") do
      [host] ->
        {:ok, host, @default_port}

      [host, port] ->
        case Integer.parse(port) do
          {n, ""} when n in 1..65_535 -> {:ok, host, n}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp parse_filters(values) do
    values
    |> Enum.flat_map(&String.split(&1, ",", trim: true))
    |> Enum.reduce_while({:ok, %{}}, fn pair, {:ok, acc} ->
      case parse_filter(pair) do
        {:ok, key, value} -> {:cont, {:ok, Map.put(acc, key, value)}}
        {:error, _message} = error -> {:halt, error}
      end
    end)
  end

  defp parse_filter(pair) do
    case String.split(pair, "=", parts: 2) do
      [key, value] when key in @string_keys and value != "" ->
        {:ok, key, value}

      [key, value] when key in @int_keys ->
        int_filter(key, value, &Integer.parse/1)

      ["sinks", value] ->
        sinks_filter(value)

      [key, _value] ->
        {:error, "--filter: unknown key or empty value for #{inspect(key)}"}

      _ ->
        {:error, "--filter: #{inspect(pair)} is not key=value"}
    end
  end

  defp int_filter(key, value, parse) do
    case parse.(value) do
      {n, ""} -> {:ok, key, n}
      _ -> {:error, "--filter: #{key} must be an integer"}
    end
  end

  defp sinks_filter(value) do
    ints = value |> String.split("+", trim: true) |> Enum.map(&Integer.parse/1)

    if ints != [] and Enum.all?(ints, &match?({_n, ""}, &1)),
      do: {:ok, "sinks", Enum.map(ints, &elem(&1, 0))},
      else: {:error, "--filter: sinks must be integers joined by +, e.g. 0+2"}
  end

  @doc """
  Starts the job on every node and follows it until each is finished or
  `timeout_s` passes. Prints through `report` (default `IO.puts/1`).

  `opts` (all optional, for tests and embedding): `:resolver` (`fn host ->
  {:ok, [address]} | {:error, reason}`), `:http` (options handed to
  `Ankusa.HttpClient.request/6`, e.g. `plug:`), `:interval_ms`, `:report`.

  Returns `{:ok, %{job_id, kind, nodes: [node_status]}}`, or `{:error, message}`
  when a node name does not resolve.
  """
  @spec run(options(), keyword()) :: {:ok, result()} | {:error, String.t()}
  def run(options, opts \\ []) do
    resolver = Keyword.get(opts, :resolver, &resolve/1)

    with {:ok, targets} <- resolve_nodes(options.nodes, resolver) do
      ctx = %{
        token: options.token,
        body: JSON.encode!(options.body),
        interval_ms: Keyword.get(opts, :interval_ms, options.interval_s * 1000),
        deadline: System.monotonic_time(:millisecond) + options.timeout_s * 1000,
        http:
          Keyword.get(opts, :http, []) ++
            if(options.cacertfile, do: [cacertfile: options.cacertfile], else: []),
        report: Keyword.get(opts, :report, &IO.puts/1)
      }

      ctx.report.(
        "replay #{options.job_id} kind=#{options.kind}: #{length(targets)} node(s): " <>
          Enum.map_join(targets, " ", & &1.label)
      )

      nodes =
        targets
        |> Task.async_stream(&drive(&1, ctx),
          max_concurrency: length(targets),
          timeout: :infinity
        )
        |> Enum.map(fn {:ok, status} -> status end)

      {:ok, %{job_id: options.job_id, kind: options.kind, nodes: nodes}}
    end
  end

  # A name with one address is used as the name (so a node that restarts on
  # another address is still found); a name with several is one node per address.
  defp resolve_nodes(nodes, resolver) do
    nodes
    |> Enum.reduce_while({:ok, []}, fn {scheme, host, port}, {:ok, acc} ->
      case expand(scheme, host, port, resolver) do
        {:ok, targets} -> {:cont, {:ok, [targets | acc]}}
        {:error, _message} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, groups} -> {:ok, groups |> Enum.reverse() |> List.flatten() |> Enum.uniq_by(& &1.url)}
      error -> error
    end
  end

  defp expand("http", host, port, resolver) do
    case resolver.(host) do
      {:ok, [_one]} ->
        {:ok, [target("http", host, host, port)]}

      {:ok, [_ | _] = addresses} ->
        {:ok,
         for address <- addresses do
           ip = address |> :inet.ntoa() |> to_string()
           target("http", "#{host}@#{ip}", ip, port)
         end}

      {:ok, []} ->
        {:error, "cannot resolve #{host}: no address"}

      {:error, reason} ->
        {:error, "cannot resolve #{host}: #{inspect(reason)}"}
    end
  end

  defp expand(scheme, host, port, _resolver), do: {:ok, [target(scheme, host, host, port)]}

  defp target(scheme, label, host, port),
    do: %{label: "#{label}:#{port}", url: "#{scheme}://#{host}:#{port}"}

  defp resolve(host) do
    :inet.getaddrs(String.to_charlist(host), :inet)
  end

  # ── one node ──────────────────────────────────────────────────────────────

  defp drive(target, ctx) do
    status = %{label: target.label, state: :pending, job_id: nil, counters: %{}, detail: nil}
    drive(target, ctx, status, nil, 0)
  end

  defp drive(target, ctx, status, last_printed, printed_at) do
    {status, finished?} = step(target, ctx, status)
    {last_printed, printed_at} = progress(ctx, status, last_printed, printed_at)

    cond do
      finished? ->
        status

      System.monotonic_time(:millisecond) + ctx.interval_ms >= ctx.deadline ->
        status

      true ->
        Process.sleep(ctx.interval_ms)
        drive(target, ctx, status, last_printed, printed_at)
    end
  end

  # No job on the node yet (or it lost it): `POST`, which answers the job that
  # has this id when there is one. Otherwise `GET` it.
  defp step(target, ctx, %{job_id: nil} = status) do
    case call(ctx, :post, target.url <> "/v1/replays", ctx.body) do
      {:ok, code, job} when code in [200, 202] and is_map(job) ->
        job_status(status, job)

      {:ok, code, body} when code in [429, 500, 502, 503, 504] ->
        {unreachable(status, "#{code} #{describe(body)}"), false}

      {:ok, code, body} ->
        {rejected(status, code, body), true}

      {:error, reason} ->
        {unreachable(status, inspect(reason)), false}
    end
  end

  defp step(target, ctx, %{job_id: job_id} = status) do
    case call(ctx, :get, target.url <> "/v1/replays/" <> job_id, nil) do
      {:ok, 200, job} when is_map(job) ->
        job_status(status, job)

      # The node came back without the job (an empty store): start it again.
      {:ok, 404, _body} ->
        {%{status | job_id: nil, state: :pending, detail: "job not found, starting it again"},
         false}

      {:ok, code, body} when code in [429, 500, 502, 503, 504] ->
        {unreachable(status, "#{code} #{describe(body)}"), false}

      {:ok, code, body} ->
        {rejected(status, code, body), true}

      {:error, reason} ->
        {unreachable(status, inspect(reason)), false}
    end
  end

  defp job_status(status, job) do
    state = job_state(job["state"])

    counters =
      Map.new(@counters, fn key -> {key, if(is_integer(job[key]), do: job[key], else: 0)} end)

    status = %{
      status
      | state: state,
        job_id: job["id"],
        counters: counters,
        detail: job["error"]
    }

    {status, job["state"] in @finished}
  end

  defp job_state("running"), do: :running
  defp job_state("done"), do: :done
  defp job_state("paused"), do: :paused
  defp job_state("cancelled"), do: :cancelled
  defp job_state("failed"), do: :failed
  defp job_state(_other), do: :running

  # The last counters stay: a node that stops answering has still moved what it
  # reported.
  defp unreachable(status, detail), do: %{status | state: :unreachable, detail: detail}

  defp rejected(status, code, body),
    do: %{status | state: :rejected, detail: "#{code} #{describe(body)}"}

  defp describe(%{"error" => error, "field" => field}), do: "#{error} field=#{field}"
  defp describe(%{"error" => error}), do: to_string(error)
  defp describe(_other), do: ""

  defp call(ctx, method, url, body) do
    headers =
      [{"accept", "application/json"}] ++
        if(body, do: [{"content-type", "application/json"}], else: []) ++
        if(ctx.token, do: [{"authorization", "Bearer " <> ctx.token}], else: [])

    case HttpClient.request(method, url, headers, body, @request_timeout_ms, ctx.http) do
      {:ok, code, response} -> {:ok, code, decode(response)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode(body) when is_binary(body) do
    case JSON.decode(body) do
      {:ok, decoded} -> decoded
      {:error, _reason} -> nil
    end
  end

  defp decode(_body), do: nil

  # One line when a node's state changes, and one every 10 s while it runs.
  defp progress(ctx, status, last_printed, printed_at) do
    now = System.monotonic_time(:millisecond)
    key = {status.state, status.detail}

    if key != last_printed or
         (status.state == :running and now - printed_at >= @progress_every_ms) do
      ctx.report.(node_line(status))
      {key, now}
    else
      {last_printed, printed_at}
    end
  end

  defp node_line(status) do
    counters = Enum.map_join(@counters, " ", &"#{&1}=#{Map.get(status.counters, &1, 0)}")
    detail = if status.detail, do: " (#{status.detail})", else: ""
    "  #{status.label}: #{status.state} #{counters}#{detail}"
  end

  # ── the summary ───────────────────────────────────────────────────────────

  @states [:done, :running, :paused, :cancelled, :failed, :unreachable, :rejected, :pending]

  @doc "The one-line summary of `run/2`'s result."
  @spec format_summary(result()) :: String.t()
  def format_summary(%{job_id: job_id, kind: kind, nodes: nodes}) do
    by_state = Enum.frequencies_by(nodes, & &1.state)
    states = Enum.map_join(@states, " ", &"#{&1}=#{Map.get(by_state, &1, 0)}")

    totals =
      Enum.map_join(@counters, " ", fn key ->
        "#{key}=#{nodes |> Enum.map(&Map.get(&1.counters, key, 0)) |> Enum.sum()}"
      end)

    "replay #{job_id} kind=#{kind} nodes=#{length(nodes)} #{states} #{totals}"
  end

  @doc "`0` when every node finished the job (`done`), `1` otherwise."
  @spec exit_code(result()) :: 0 | 1
  def exit_code(%{nodes: nodes}), do: if(Enum.all?(nodes, &(&1.state == :done)), do: 0, else: 1)
end
