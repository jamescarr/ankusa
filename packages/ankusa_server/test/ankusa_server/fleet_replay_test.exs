defmodule AnkusaServer.FleetReplayTest do
  @moduledoc """
  `docker-entrypoint replay`: one job id fanned out to every node of a fleet, and
  followed to the end, against stub admin APIs (a function plug in place of each
  node's socket).
  """

  use ExUnit.Case, async: true

  alias AnkusaServer.FleetReplay

  @job_id "0192f1c0-1b2c-7d3e-8f4a-5b6c7d8e9f01"

  # ── the stub fleet ──────────────────────────────────────────────────────────

  defp job(state, moved, id \\ @job_id) do
    {200,
     %{
       "id" => id,
       "state" => state,
       "moved" => moved,
       "scanned" => moved * 2,
       "skipped" => 0,
       "delivered" => moved,
       "dead" => 0,
       "error" => nil
     }}
  end

  # `script`: `{method, host} => [{status, body}, ...]`, answered in order, the
  # last one repeating. A `POST` with no script creates the job (`202`, running).
  defp stub_fleet(script) do
    {:ok, agent} = Agent.start_link(fn -> %{script: script, calls: []} end)
    {agent, fn conn -> handle(agent, conn) end}
  end

  defp handle(agent, conn) do
    {:ok, body, conn} = Plug.Conn.read_body(conn)

    call = %{
      host: conn.host,
      method: conn.method,
      path: conn.request_path,
      body: body,
      auth: Plug.Conn.get_req_header(conn, "authorization")
    }

    {status, payload} =
      Agent.get_and_update(agent, fn state ->
        state = %{state | calls: [call | state.calls]}
        next(state, {conn.method, conn.host}, body)
      end)

    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, JSON.encode!(payload))
  end

  defp next(state, {method, _host} = key, body) do
    default =
      case method do
        "POST" ->
          {202, elem(job("running", 0, JSON.decode!(body)["job_id"]), 1)}

        "GET" ->
          job("running", 0)
      end

    case Map.get(state.script, key, [default]) do
      [last] -> {last, state}
      [head | rest] -> {head, %{state | script: Map.put(state.script, key, rest)}}
    end
  end

  defp calls(agent), do: agent |> Agent.get(& &1.calls) |> Enum.reverse()

  defp options(extra \\ []) do
    base = ["--nodes", "a.test,b.test", "--job-id", @job_id, "--token", "tok"]
    {:ok, options} = FleetReplay.parse_args(base ++ extra)
    options
  end

  defp single_address(_host), do: {:ok, [{10, 0, 0, 1}]}

  defp run(options, plug, extra \\ []) do
    {:ok, lines} = Agent.start_link(fn -> [] end)

    {:ok, result} =
      FleetReplay.run(
        options,
        Keyword.merge(
          [
            resolver: &single_address/1,
            http: [plug: plug],
            interval_ms: 5,
            report: fn line -> Agent.update(lines, &[line | &1]) end
          ],
          extra
        )
      )

    {result, lines |> Agent.get(& &1) |> Enum.reverse()}
  end

  defp state_of(result, label), do: Enum.find(result.nodes, &(&1.label == label)).state

  # ── the fan-out ─────────────────────────────────────────────────────────────

  test "every node gets the same POST with one job id, and the statuses are aggregated" do
    {agent, plug} =
      stub_fleet(%{
        {"GET", "a.test"} => [job("running", 5), job("done", 10)],
        # b is unreachable for a poll, then catches up
        {"GET", "b.test"} => [
          {503, %{"error" => "store_unavailable"}},
          job("running", 2),
          job("done", 7)
        ]
      })

    options = options(["--filter", "tenant=acme,error_class=transient", "--filter", "sink=1"])
    {result, lines} = run(options, plug)

    assert state_of(result, "a.test:4002") == :done
    assert state_of(result, "b.test:4002") == :done
    assert FleetReplay.exit_code(result) == 0

    posts = for %{method: "POST"} = call <- calls(agent), do: call
    assert Enum.map(posts, & &1.host) |> Enum.sort() == ["a.test", "b.test"]
    assert Enum.all?(posts, &(&1.path == "/v1/replays" and &1.auth == ["Bearer tok"]))

    bodies = posts |> Enum.map(&JSON.decode!(&1.body)) |> Enum.uniq()

    assert bodies == [
             %{
               "kind" => "dlq",
               "job_id" => @job_id,
               "tenant" => "acme",
               "error_class" => "transient",
               "sink" => 1
             }
           ]

    # the summary: both nodes done, the counters added up
    assert FleetReplay.format_summary(result) ==
             "replay #{@job_id} kind=dlq nodes=2 done=2 running=0 paused=0 cancelled=0 " <>
               "failed=0 unreachable=0 rejected=0 pending=0 moved=17 scanned=34 skipped=0 " <>
               "delivered=17 dead=0"

    assert Enum.any?(lines, &(&1 =~ "b.test:4002: unreachable"))
    assert Enum.any?(lines, &(&1 =~ "a.test:4002: done moved=10"))
  end

  test "a node that comes back without the job is started again with the same id" do
    {agent, plug} =
      stub_fleet(%{
        {"GET", "a.test"} => [job("done", 4)],
        {"POST", "b.test"} => [
          {202, elem(job("running", 0), 1)},
          {200, elem(job("running", 3), 1)}
        ],
        # b restarted with an empty store: its job is gone, the POST finds it again
        {"GET", "b.test"} => [
          {404, %{"error" => "replay_not_found"}},
          job("done", 9)
        ]
      })

    {result, _lines} = run(options(), plug)

    assert FleetReplay.exit_code(result) == 0
    b_posts = for %{method: "POST", host: "b.test"} = call <- calls(agent), do: call
    assert length(b_posts) == 2
    assert b_posts |> Enum.map(&JSON.decode!(&1.body)["job_id"]) |> Enum.uniq() == [@job_id]
  end

  test "a node that refuses the job is reported and the others finish" do
    {_agent, plug} =
      stub_fleet(%{
        {"POST", "a.test"} => [{403, %{"error" => "forbidden", "scope" => "tenant:*"}}],
        {"GET", "b.test"} => [job("done", 1)]
      })

    {result, _lines} = run(options(), plug)

    assert state_of(result, "a.test:4002") == :rejected
    assert state_of(result, "b.test:4002") == :done
    assert FleetReplay.exit_code(result) == 1

    assert FleetReplay.format_summary(result) =~
             "done=1 running=0 paused=0 cancelled=0 failed=0 unreachable=0 rejected=1"

    assert Enum.find(result.nodes, &(&1.state == :rejected)).detail == "403 forbidden"
  end

  test "a paused job ends the wait and is not a success" do
    {_agent, plug} =
      stub_fleet(%{
        {"GET", "a.test"} => [job("paused", 3)],
        {"GET", "b.test"} => [job("done", 1)]
      })

    {result, _lines} = run(options(), plug)

    assert state_of(result, "a.test:4002") == :paused
    assert FleetReplay.exit_code(result) == 1
  end

  test "a job still running at the timeout is reported running" do
    {_agent, plug} = stub_fleet(%{{"GET", "a.test"} => [job("running", 1)]})
    {result, _lines} = run(options(["--timeout", "1"]), plug, interval_ms: 20)

    assert state_of(result, "a.test:4002") == :running
    assert FleetReplay.exit_code(result) == 1
  end

  test "a name that resolves to several addresses is one node per address" do
    {agent, plug} =
      stub_fleet(%{
        {"GET", "10.0.0.1"} => [job("done", 2)],
        {"GET", "10.0.0.2"} => [job("done", 3)]
      })

    {:ok, options} = FleetReplay.parse_args(["--nodes", "ankusa", "--job-id", @job_id])

    {result, _lines} =
      run(options, plug, resolver: fn "ankusa" -> {:ok, [{10, 0, 0, 1}, {10, 0, 0, 2}]} end)

    assert Enum.map(result.nodes, & &1.label) == ["ankusa@10.0.0.1:4002", "ankusa@10.0.0.2:4002"]
    assert FleetReplay.exit_code(result) == 0

    assert agent |> calls() |> Enum.map(& &1.host) |> Enum.uniq() |> Enum.sort() == [
             "10.0.0.1",
             "10.0.0.2"
           ]
  end

  test "a name that does not resolve is an error before anything is sent" do
    {agent, plug} = stub_fleet(%{})

    assert {:error, "cannot resolve a.test: :nxdomain"} =
             FleetReplay.run(options(),
               resolver: fn _ -> {:error, :nxdomain} end,
               http: [plug: plug]
             )

    assert calls(agent) == []
  end

  # ── the command line ────────────────────────────────────────────────────────

  describe "parse_args/1" do
    test "nodes, kind, filters and defaults" do
      assert {:ok, options} =
               FleetReplay.parse_args([
                 "--nodes",
                 "http://a:4100, https://b , c:4005",
                 "--filter",
                 "tenant=acme,sinks=0+2",
                 "--filter",
                 "since=5",
                 "--rate",
                 "50",
                 "--max-lag-ms",
                 "500",
                 "--token",
                 "t"
               ])

      assert options.nodes == [{"http", "a", 4100}, {"https", "b", 4002}, {"http", "c", 4005}]
      assert options.kind == "dlq"
      assert options.token == "t"
      assert options.timeout_s == 3600 and options.interval_s == 1
      assert Regex.match?(~r/\A[0-9a-f-]{36}\z/, options.job_id)

      assert %{
               "kind" => "dlq",
               "job_id" => job_id,
               "tenant" => "acme",
               "sinks" => [0, 2],
               "since" => 5,
               "rate" => 50,
               "max_lag_ms" => 500
             } = options.body

      assert job_id == options.job_id
    end

    test "a bad command line is a message, never a job" do
      for {args, message} <- [
            {[], "--nodes is required"},
            {["--nodes", ""], "--nodes names no node"},
            {["--nodes", "a b"], "--nodes: \"a b\" is not [scheme://]host[:port]"},
            {["--nodes", "a:99999"], "--nodes: \"a:99999\" is not [scheme://]host[:port]"},
            {["--nodes", "a", "--kind", "all"],
             "--kind must be one of dlq, archive, quarantine, lifecycle"},
            {["--nodes", "a", "--job-id", "ABC"], "--job-id must be a lowercase UUID"},
            {["--nodes", "a", "--filter", "tenant"], "--filter: \"tenant\" is not key=value"},
            {["--nodes", "a", "--filter", "colour=red"],
             "--filter: unknown key or empty value for \"colour\""},
            {["--nodes", "a", "--filter", "sink=x"], "--filter: sink must be an integer"},
            {["--nodes", "a", "--filter", "sinks=a+b"],
             "--filter: sinks must be integers joined by +, e.g. 0+2"},
            {["--nodes", "a", "--timeout", "0"], "--timeout must be at least 1"},
            {["--nodes", "a", "extra"], "unexpected argument \"extra\""},
            {["--nodes", "a", "--nope"], "unknown or malformed option --nope"}
          ] do
        assert {:error, ^message} = FleetReplay.parse_args(args)
      end
    end
  end
end
