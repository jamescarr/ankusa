defmodule Ankusa.Admin.RouterTest do
  @moduledoc """
  Exercises `Ankusa.Admin.Router`'s HTTP semantics directly via
  `Plug.Test`/`Router.call`, the same pattern `Ankusa.ClaimCheck.RouterTest`
  uses — no real socket needed to prove status-code mapping, role gating, and
  redaction are correct.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Config
  alias Ankusa.Admin.Router
  alias Ankusa.Dispatch.Pipeline

  # Always fails, so with `max_attempts: 1` one attempt dead-letters the row.
  defmodule AlwaysFailSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, _opts), do: {:error, :always}
  end

  # Fails while its agent holds `false`; once it holds `true` it delivers the
  # body to the test pid.
  defmodule GatedSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, _ctx, opts) do
      if Agent.get(Keyword.fetch!(opts, :agent), & &1) do
        send(Keyword.fetch!(opts, :pid), {:delivered, env.id, env.body})
        :ok
      else
        {:error, :gated}
      end
    end
  end

  setup do
    config = test_config(roles: [:dispatch], admin: %{enabled: true})
    put_config(config)
    %{inst: config.instance, config: config}
  end

  defp call(inst, method, path, body \\ "", headers \\ []) do
    conn =
      Plug.Test.conn(method, path, body)
      |> then(fn c ->
        Enum.reduce(headers, c, fn {k, v}, c -> Plug.Conn.put_req_header(c, k, v) end)
      end)

    Router.call(conn, Router.init(instance: inst))
  end

  # Boots an edge+dispatch instance whose dispatch dead-letters on the first
  # failure (`max_attempts: 1`), so ingesting through it creates real dead rows.
  defp start_dlq_instance(sources) do
    config =
      test_config(
        roles: [:edge, :dispatch],
        admin: %{enabled: true, port: 0},
        source_store: {Ankusa.SourceStore.Static, sources: sources},
        dispatch: %{
          retry: {Ankusa.RetryPolicy.Exponential, base_ms: 0, max_attempts: 1, jitter: false}
        }
      )

    put_config(config)
    start_supervised!({Ankusa.Instance, config})
    config
  end

  defp ingest!(config, source_id, body) do
    {:ok, env} = Ankusa.Edge.Ingest.ingest(config.instance, request(source_id, body))
    env
  end

  # ── no auth ────────────────────────────────────────────────────────────────

  test "every route answers with no authorization header at all" do
    config = start_dlq_instance(%{"a" => [sinks: [{AlwaysFailSink, []}]]})

    for path <- ["/health", "/v1/config", "/v1/dlq", "/v1/dlq?source_id=x"] do
      conn = call(config.instance, :get, path)
      assert conn.status == 200, "#{path} returned #{conn.status}"
    end

    assert call(config.instance, :post, "/v1/replays", ~s({"kind":"dlq"})).status == 202
  end

  test "an unrouted path is 404", %{inst: inst} do
    conn = call(inst, :get, "/nope")
    assert conn.status == 404
    assert %{"error" => "not_found"} = JSON.decode!(conn.resp_body)
  end

  test "GET /health reports the instance, its roles, and the ankusa version", %{inst: inst} do
    assert %{"status" => "ok", "instance" => inst_name, "roles" => ["dispatch"], "version" => vsn} =
             JSON.decode!(call(inst, :get, "/health").resp_body)

    assert inst_name == to_string(inst)
    assert is_binary(vsn) and vsn != ""
    assert vsn == to_string(Application.spec(:ankusa, :vsn))
  end

  # ── store stats ────────────────────────────────────────────────────────────

  test "GET /v1/wal reports this node's store, 409 when this node has none" do
    config =
      test_config(
        roles: [:edge],
        admin: %{enabled: true, port: 0},
        source_store:
          {Ankusa.SourceStore.Static, sources: %{"a" => [sinks: [{Ankusa.Sink.Log, []}]]}}
      )

    put_config(config)
    start_supervised!({Ankusa.Instance, config})

    # Exactly one successful commit: its seq is 1, so the next is 2.
    _env = ingest!(config, "a", ~s({"id":"one"}))

    assert %{"instance" => inst_name, "wal" => wal} =
             JSON.decode!(call(config.instance, :get, "/v1/wal").resp_body)

    assert inst_name == to_string(config.instance)
    assert Enum.sort(Map.keys(wal)) == ["deliveries", "disk_bytes", "hooks", "next_seq"]
    assert wal["next_seq"] == 2
    # The store's own end-of-range sentinel keys are not hooks or deliveries.
    assert wal["hooks"] == 1
    assert wal["deliveries"] == 1

    none = test_config(roles: [:edge], admin: %{enabled: true}, wal: :none)
    put_config(none)

    conn = call(none.instance, :get, "/v1/wal")
    assert conn.status == 409
    assert %{"error" => "wal_disabled"} = JSON.decode!(conn.resp_body)
  end

  test "GET /v1/wal on an empty store reports zero hooks and deliveries" do
    config = test_config(roles: [:edge], admin: %{enabled: true, port: 0})
    put_config(config)
    start_supervised!({Ankusa.Instance, config})

    assert %{"wal" => wal} =
             JSON.decode!(call(config.instance, :get, "/v1/wal").resp_body)

    assert wal["next_seq"] == 1
    assert wal["hooks"] == 0
    assert wal["deliveries"] == 0
  end

  # ── role gating ────────────────────────────────────────────────────────────

  test "a route needing a role this node does not run is 409" do
    config = test_config(roles: [:edge], admin: %{enabled: true})
    put_config(config)

    conn = call(config.instance, :get, "/v1/dlq")

    assert conn.status == 409
    assert %{"error" => "role_not_enabled", "role" => "dispatch"} = JSON.decode!(conn.resp_body)
  end

  test "the rate-limit routes are edge-only, like the quarantine pen", %{inst: inst} do
    for {method, path} <- [
          get: "/v1/rate-limits",
          get: "/v1/quarantine",
          delete: "/v1/quarantine"
        ] do
      conn = call(inst, method, path)

      assert conn.status == 409
      assert %{"error" => "role_not_enabled", "role" => "edge"} = JSON.decode!(conn.resp_body)
    end
  end

  # ── DLQ ────────────────────────────────────────────────────────────────────

  test "GET /v1/dlq filters by source and never returns bodies" do
    config =
      start_dlq_instance(%{
        "a" => [sinks: [{AlwaysFailSink, []}]],
        "b" => [sinks: [{AlwaysFailSink, []}]]
      })

    a = ingest!(config, "a", ~s({"secret":"top-secret-payload"}))
    _b = ingest!(config, "b", ~s({"id":"b1"}))
    assert {:ok, _settled} = Pipeline.tick(config.instance)

    conn = call(config.instance, :get, "/v1/dlq?source_id=a")
    assert conn.status == 200

    assert %{"total" => 1, "entries" => [entry]} = JSON.decode!(conn.resp_body)
    assert entry["id"] == a.id
    assert entry["source_id"] == "a"
    assert entry["seq"] == 1
    assert entry["reason"] == inspect({:sink, AlwaysFailSink, :always})
    refute Map.has_key?(entry, "body")
    refute conn.resp_body =~ "top-secret-payload"
  end

  test "GET /v1/dlq returns the newest write first, up to limit" do
    config = start_dlq_instance(%{"a" => [sinks: [{AlwaysFailSink, []}]]})

    old = ingest!(config, "a", "{}")
    new = ingest!(config, "a", "{}")
    assert {:ok, _settled} = Pipeline.tick(config.instance)

    assert %{"total" => 2, "entries" => [only]} =
             JSON.decode!(call(config.instance, :get, "/v1/dlq?limit=1").resp_body)

    assert only["id"] == new.id

    assert %{"total" => 2, "entries" => [first, second]} =
             JSON.decode!(call(config.instance, :get, "/v1/dlq").resp_body)

    assert first["id"] == new.id
    assert second["id"] == old.id
  end

  test "GET /v1/dlq clamps limit to 1000" do
    config = start_dlq_instance(%{"a" => [sinks: [{AlwaysFailSink, []}]]})

    for _ <- 1..1001, do: ingest!(config, "a", "{}")
    assert {:ok, _settled} = Pipeline.tick(config.instance)

    assert %{"total" => 1001, "entries" => entries} =
             JSON.decode!(call(config.instance, :get, "/v1/dlq?limit=5000").resp_body)

    assert length(entries) == 1000
  end

  test "a non-integer since or limit is 400 invalid_filter", %{inst: inst} do
    assert %{"error" => "invalid_filter", "field" => "since"} =
             JSON.decode!(call(inst, :get, "/v1/dlq?since=tuesday").resp_body)

    conn = call(inst, :get, "/v1/dlq?limit=many")
    assert conn.status == 400
    assert %{"error" => "invalid_filter", "field" => "limit"} = JSON.decode!(conn.resp_body)
  end

  # ── replay jobs ────────────────────────────────────────────────────────────

  defp poll_job(inst, id, fun, deadline) do
    {:ok, job} = Ankusa.Replay.get(inst, id)

    if fun.(job) do
      job
    else
      if System.monotonic_time(:millisecond) > deadline do
        flunk("replay job #{id} never reached the expected state: #{inspect(job)}")
      else
        Process.sleep(50)
        poll_job(inst, id, fun, deadline)
      end
    end
  end

  # The replayer loads its jobs from the store asynchronously at boot; a call
  # before that is a (correct) `:store_unavailable`. Tests that hit the API
  # right after boot wait for the load.
  defp wait_loaded(inst) do
    deadline = System.monotonic_time(:millisecond) + 3_000
    wait_loaded(inst, deadline)
  end

  defp wait_loaded(inst, deadline) do
    case Ankusa.Replay.list(inst) do
      {:ok, _jobs} ->
        :ok

      {:error, :store_unavailable} ->
        if System.monotonic_time(:millisecond) > deadline do
          flunk("replayer never loaded")
        else
          Process.sleep(25)
          wait_loaded(inst, deadline)
        end
    end
  end

  test "POST /v1/replays creates a dlq job that re-delivers the matching entry" do
    {:ok, agent} = Agent.start_link(fn -> false end)
    config = start_dlq_instance(%{"a" => [sinks: [{GatedSink, agent: agent, pid: self()}]]})

    target = ingest!(config, "a", ~s({"id":"evt_replay"}))
    _other = ingest!(config, "a", ~s({"id":"evt_other"}))
    assert {:ok, _settled} = Pipeline.tick(config.instance)

    # The sink recovers only after both hooks are dead-lettered.
    Agent.update(agent, fn _ -> true end)

    conn =
      call(
        config.instance,
        :post,
        "/v1/replays",
        JSON.encode!(%{"kind" => "dlq", "id" => target.id})
      )

    assert conn.status == 202
    replay = JSON.decode!(conn.resp_body)
    assert replay["kind"] == "dlq"
    assert replay["state"] == "running"
    assert replay["filter"] == %{"id" => target.id}
    assert replay["rate"] == 1000
    assert replay["max_lag_ms"] == 2000
    assert replay["finished_at"] == nil

    assert_receive {:delivered, id, body}, 5_000
    assert id == target.id
    assert body == target.body
    refute_received {:delivered, _, _}

    # The job flips to done when its range is exhausted, before the pipeline's
    # next housekeeping reports the delivery outcome; wait for both.
    deadline = System.monotonic_time(:millisecond) + 5_000

    job =
      poll_job(
        config.instance,
        replay["id"],
        fn job ->
          job.state == :done and job.delivered == 1
        end,
        deadline
      )

    assert job.moved == 1
    assert job.state == :done
  end

  test "the same create is idempotent: 202 then 200 with the same job" do
    {:ok, agent} = Agent.start_link(fn -> false end)
    config = start_dlq_instance(%{"a" => [sinks: [{GatedSink, agent: agent, pid: self()}]]})
    ingest!(config, "a", ~s({"id":"1"}))
    assert {:ok, _settled} = Pipeline.tick(config.instance)

    body = ~s({"kind":"dlq","source_id":"a","rate":500})

    first = call(config.instance, :post, "/v1/replays", body)
    assert first.status == 202
    job = JSON.decode!(first.resp_body)

    second = call(config.instance, :post, "/v1/replays", body)
    assert second.status == 200
    assert JSON.decode!(second.resp_body)["id"] == job["id"]
  end

  test "GET /v1/replays/:id, GET /v1/replays and PATCH state transitions" do
    config = start_dlq_instance(%{"a" => [sinks: [{AlwaysFailSink, []}]]})
    inst = config.instance

    created = JSON.decode!(call(inst, :post, "/v1/replays", ~s({"kind":"dlq"})).resp_body)
    id = created["id"]

    # Pause before it finishes.
    paused = call(inst, :patch, "/v1/replays/#{id}", ~s({"state":"paused"}))
    assert paused.status == 200
    assert JSON.decode!(paused.resp_body)["state"] == "paused"

    assert %{"id" => ^id, "state" => "paused"} =
             JSON.decode!(call(inst, :get, "/v1/replays/#{id}").resp_body)

    listed = JSON.decode!(call(inst, :get, "/v1/replays").resp_body)
    assert [%{"id" => ^id} | _] = listed["replays"]

    resumed = call(inst, :patch, "/v1/replays/#{id}", ~s({"state":"running"}))
    assert JSON.decode!(resumed.resp_body)["state"] == "running"

    cancelled = call(inst, :patch, "/v1/replays/#{id}", ~s({"state":"cancelled"}))
    assert cancelled.status == 200
    assert JSON.decode!(cancelled.resp_body)["state"] == "cancelled"

    # A finished job refuses further patches.
    again = call(inst, :patch, "/v1/replays/#{id}", ~s({"state":"running"}))
    assert again.status == 409
    assert JSON.decode!(again.resp_body) == %{"error" => "replay_finished"}
  end

  test "GET /v1/replays/:id is 404 replay_not_found for an unknown id" do
    config = test_config(roles: [:dispatch], admin: %{enabled: true, port: 0})
    put_config(config)
    start_supervised!({Ankusa.Instance, config})
    wait_loaded(config.instance)
    conn = call(config.instance, :get, "/v1/replays/nope")
    assert conn.status == 404
    assert JSON.decode!(conn.resp_body) == %{"error" => "replay_not_found"}
  end

  test "unknown keys and malformed bodies are 400 invalid_filter" do
    config = start_dlq_instance(%{"a" => [sinks: [{AlwaysFailSink, []}]]})
    inst = config.instance

    conn = call(inst, :post, "/v1/replays", ~s({"kind":"dlq","bogus":1}))
    assert conn.status == 400
    assert %{"error" => "invalid_filter", "field" => "bogus"} = JSON.decode!(conn.resp_body)

    conn = call(inst, :post, "/v1/replays", ~s({"kind":"nope"}))
    assert conn.status == 400
    assert %{"error" => "invalid_filter", "field" => "kind"} = JSON.decode!(conn.resp_body)

    conn = call(inst, :post, "/v1/replays", "not json")
    assert conn.status == 400
    assert %{"error" => "invalid_filter", "field" => "body"} = JSON.decode!(conn.resp_body)

    conn = call(inst, :post, "/v1/replays", "")
    assert conn.status == 400
    assert %{"error" => "invalid_filter", "field" => "kind"} = JSON.decode!(conn.resp_body)

    # A key from the other kind is refused, not dropped: a dlq spec carrying
    # an archive window must never become an unbounded DLQ replay.
    conn = call(inst, :post, "/v1/replays", ~s({"kind":"dlq","from":0,"to":1000}))
    assert conn.status == 400
    assert %{"error" => "invalid_filter", "field" => "from"} = JSON.decode!(conn.resp_body)

    conn =
      call(inst, :post, "/v1/replays", ~s({"kind":"archive","from":0,"to":1000,"since":5}))

    assert conn.status == 400
    assert %{"error" => "invalid_filter", "field" => "since"} = JSON.decode!(conn.resp_body)
  end

  test "an archive job on a node without a queue writer is 409 role_not_enabled/edge" do
    config = test_config(roles: [:dispatch], admin: %{enabled: true, port: 0})
    put_config(config)
    start_supervised!({Ankusa.Instance, config})
    wait_loaded(config.instance)
    conn = call(config.instance, :post, "/v1/replays", ~s({"kind":"archive","from":0,"to":1}))
    assert conn.status == 409
    assert JSON.decode!(conn.resp_body) == %{"error" => "role_not_enabled", "role" => "edge"}
  end

  test "17 concurrent jobs is 409 too_many_replays" do
    config = start_dlq_instance(%{"a" => [sinks: [{AlwaysFailSink, []}]]})
    inst = config.instance

    # Seed 16 paused jobs directly, so they count as active without racing the
    # replayer tick.
    now = System.system_time(:millisecond)

    jobs =
      for i <- 1..16 do
        %{
          id: "seed-#{i}",
          kind: :dlq,
          filter: %{id: "seed-#{i}"},
          rate: 1_000,
          max_lag_ms: 2_000,
          state: :paused,
          created_at: now + i,
          updated_at: now + i,
          finished_at: nil,
          cursor: nil,
          upto: now,
          moved: 0,
          scanned: 0,
          skipped: 0,
          delivered: 0,
          dead: 0,
          error: nil
        }
      end

    :ok =
      Ankusa.Store.write(
        inst,
        Enum.map(jobs, fn job ->
          {:put, :default, Ankusa.Store.Keys.replay_job(job.id), :erlang.term_to_binary(job)}
        end),
        sync: true
      )

    # A restarted replayer loads the seeded jobs.
    subtree = Ankusa.Instance.Isolated.subtree(inst, :dispatch)

    :ok = Supervisor.terminate_child(subtree, {Ankusa.Dispatch.Replayer, inst})
    {:ok, _} = Supervisor.restart_child(subtree, {Ankusa.Dispatch.Replayer, inst})

    deadline = System.monotonic_time(:millisecond) + 3_000

    until_loaded = fn until_loaded ->
      case Ankusa.Replay.list(inst) do
        {:ok, jobs} when length(jobs) == 16 ->
          :ok

        _other ->
          if System.monotonic_time(:millisecond) > deadline do
            flunk("seeded jobs were not loaded")
          else
            Process.sleep(50)
            until_loaded.(until_loaded)
          end
      end
    end

    until_loaded.(until_loaded)

    conn = call(inst, :post, "/v1/replays", ~s({"kind":"dlq"}))
    assert conn.status == 409
    assert JSON.decode!(conn.resp_body) == %{"error" => "too_many_replays"}
  end

  @tag :capture_log
  test "an unreachable store is 503 on dlq, replays and quarantine, and {} on /v1/wal" do
    config = start_dlq_instance(%{"a" => [sinks: [{AlwaysFailSink, []}]]})
    inst = config.instance
    :ok = Supervisor.terminate_child(Ankusa.via(inst, :instance), {Ankusa.Store, inst})

    conn = call(inst, :get, "/v1/dlq")
    assert conn.status == 503
    assert JSON.decode!(conn.resp_body) == %{"error" => "store_unavailable"}

    conn = call(inst, :post, "/v1/replays", ~s({"kind":"dlq"}))
    assert conn.status == 503
    assert JSON.decode!(conn.resp_body) == %{"error" => "store_unavailable"}

    conn = call(inst, :get, "/v1/quarantine")
    assert conn.status == 503
    assert JSON.decode!(conn.resp_body) == %{"error" => "store_unavailable"}

    conn = call(inst, :get, "/v1/wal")
    assert conn.status == 200
    assert %{"instance" => _, "wal" => %{}} = JSON.decode!(conn.resp_body)
  end

  @tag :capture_log
  test "replays while the dispatch domain is down is a 503, not a crashed request" do
    config = start_dlq_instance(%{"a" => [sinks: [{AlwaysFailSink, []}]]})
    inst = config.instance

    :ok =
      Supervisor.terminate_child(
        Ankusa.via(inst, :instance),
        {Ankusa.Instance.Isolated, :dispatch}
      )

    conn = call(inst, :post, "/v1/replays", ~s({"kind":"dlq"}))
    assert conn.status == 503
    assert JSON.decode!(conn.resp_body) == %{"error" => "store_unavailable"}
  end

  # ── quarantine ─────────────────────────────────────────────────────────────

  test "GET /v1/quarantine lists this node's recent entries without bodies" do
    sources = %{
      "strict" => [
        verifier: {Ankusa.Verifier.Hmac, scheme: :stripe, secret: "whsec_x"},
        on_verify_failure: :quarantine
      ]
    }

    config =
      test_config(
        roles: [:edge],
        admin: %{enabled: true, port: 0},
        source_store: {Ankusa.SourceStore.Static, sources: sources}
      )

    put_config(config)
    start_supervised!({Ankusa.Instance, config})

    assert {:quarantined, _reason} =
             Ankusa.Edge.Ingest.ingest(config.instance, request("strict", ~s({"id":"q1"})))

    conn = call(config.instance, :get, "/v1/quarantine")
    assert conn.status == 200

    assert %{"entries" => [entry]} = JSON.decode!(conn.resp_body)
    assert entry["source_id"] == "strict"
    assert entry["reason"] =~ "missing_signature"
    refute Map.has_key?(entry, "body")
  end

  test "DELETE /v1/quarantine purges the matching entries and reports their bytes" do
    sources =
      Map.new(["strict", "other"], fn id ->
        {id,
         [
           verifier: {Ankusa.Verifier.Hmac, scheme: :stripe, secret: "whsec_x"},
           on_verify_failure: :quarantine
         ]}
      end)

    config =
      test_config(
        roles: [:edge],
        admin: %{enabled: true, port: 0},
        source_store: {Ankusa.SourceStore.Static, sources: sources}
      )

    put_config(config)
    start_supervised!({Ankusa.Instance, config})

    for id <- ["strict", "strict", "other"] do
      assert {:quarantined, _reason} =
               Ankusa.Edge.Ingest.ingest(config.instance, request(id, ~s({"id":"q"})))
    end

    %{"entries" => listed} = JSON.decode!(call(config.instance, :get, "/v1/quarantine").resp_body)
    strict_bytes = for %{"source_id" => "strict", "size" => size} <- listed, do: size
    assert [_, _] = strict_bytes

    for {query, field} <- [
          {"until=-1", "until"},
          {"since=10&until=5", "until"},
          {"limit=x", "limit"}
        ] do
      conn = call(config.instance, :delete, "/v1/quarantine?" <> query)
      assert conn.status == 400
      assert JSON.decode!(conn.resp_body) == %{"error" => "invalid_filter", "field" => field}
    end

    conn = call(config.instance, :delete, "/v1/quarantine?source_id=strict")
    assert conn.status == 200
    assert JSON.decode!(conn.resp_body) == %{"deleted" => 2, "bytes" => Enum.sum(strict_bytes)}

    %{"entries" => [left]} = JSON.decode!(call(config.instance, :get, "/v1/quarantine").resp_body)
    assert left["source_id"] == "other"

    conn = call(config.instance, :delete, "/v1/quarantine?source_id=strict")
    assert JSON.decode!(conn.resp_body) == %{"deleted" => 0, "bytes" => 0}
  end

  # ── config redaction ───────────────────────────────────────────────────────

  test "GET /v1/config redacts a verifier secret and a URL password" do
    config =
      test_config(
        roles: [:edge],
        admin: %{enabled: true},
        source_store:
          {Ankusa.SourceStore.Static,
           sources: %{
             "stripe" => [
               verifier: {Ankusa.Verifier.Hmac, scheme: :stripe, secret: "whsec_leakhunter"},
               sinks: [{Ankusa.Sink.Http, url: "https://hooks:leakhunter@sink.internal/h"}]
             ]
           }}
      )

    put_config(config)

    conn = call(config.instance, :get, "/v1/config")
    assert conn.status == 200

    refute conn.resp_body =~ "leakhunter"
    assert conn.resp_body =~ "https://hooks:[REDACTED]@sink.internal/h"
    # sources, module pairs, and booleans all survive as themselves
    assert conn.resp_body =~ "stripe"
    assert conn.resp_body =~ ~s("module":"Ankusa.Verifier.Hmac")

    decoded = JSON.decode!(conn.resp_body)
    assert decoded["admin"]["enabled"] == true

    assert decoded["source_store"]["opts"]["sources"]["stripe"]["verifier"]["opts"]["secret"] ==
             "[REDACTED]"
  end

  test "GET /v1/config shows a {module, fun, args} callback without its args" do
    config =
      test_config(
        roles: [:dispatch],
        admin: %{enabled: true},
        storage: %{
          blob_store:
            {Ankusa.BlobStore.GCS,
             bucket: "hooks", token_provider: {Function, :identity, [{:ok, "ya29.leakhunter"}]}}
        }
      )

    put_config(config)

    conn = call(config.instance, :get, "/v1/config")

    refute conn.resp_body =~ "leakhunter"

    assert JSON.decode!(conn.resp_body)["storage"]["blob_store"]["opts"]["token_provider"] ==
             "Function.identity/1"
  end

  test "GET /v1/config keeps header names and redacts every header value" do
    sink =
      {Ankusa.Sink.Http,
       url: "https://sink.example/hook",
       headers: [{"authorization", "Bearer leakhunter"}, {"x-team", "payments"}]}

    config =
      test_config(
        roles: [:dispatch],
        admin: %{enabled: true},
        source_store: {Ankusa.SourceStore.Static, sources: %{"a" => [sinks: [sink]]}}
      )

    put_config(config)

    conn = call(config.instance, :get, "/v1/config")

    refute conn.resp_body =~ "leakhunter"

    assert [%{"opts" => %{"headers" => headers}}] =
             JSON.decode!(conn.resp_body)["source_store"]["opts"]["sources"]["a"]["sinks"]

    assert headers == %{"authorization" => "[REDACTED]", "x-team" => "[REDACTED]"}
  end

  test "GET /v1/config shows only allowlisted adapter options and hides the rest" do
    custom =
      {MyApp.CustomSink,
       credentials: "LEAK",
       region: "us-1",
       ssl: [keyfile: "/k/LEAK", password: ~c"LEAK"],
       sasl: {:plain, "u", "LEAK"}}

    config =
      test_config(
        roles: [:dispatch],
        admin: %{enabled: true},
        storage: %{
          blob_store:
            {Ankusa.BlobStore.Azure,
             account_name: "acct", container: "c", sas_token: "sv=1&sig=LEAK"}
        },
        source_store:
          {Ankusa.SourceStore.Static,
           sources: %{
             "a" => [
               sinks: [{Ankusa.Sink.Http, url: "https://h.example/p?token=LEAK&x=1"}, custom]
             ]
           }}
      )

    put_config(config)

    conn = call(config.instance, :get, "/v1/config")
    assert conn.status == 200

    refute conn.resp_body =~ "LEAK"
    assert conn.resp_body =~ ~s("acct")
    assert conn.resp_body =~ ~s("us-1")
    assert conn.resp_body =~ "https://h.example/p?token=[REDACTED]&x=[REDACTED]"
  end

  # ── AsyncAPI ───────────────────────────────────────────────────────────────

  test "GET /asyncapi.json serves the channels the sources publish to, without credentials" do
    config =
      test_config(
        roles: [:edge],
        admin: %{enabled: true},
        source_store:
          {Ankusa.SourceStore.Static,
           sources: %{
             "stripe" => [
               sinks: [
                 {Ankusa.Test.DescribedSink, address: "ankusa.hooks", password: "leakhunter"}
               ]
             ]
           }}
      )

    put_config(config)

    conn = call(config.instance, :get, "/asyncapi.json")

    assert conn.status == 200
    assert [content_type] = Plug.Conn.get_resp_header(conn, "content-type")
    assert content_type =~ "application/asyncapi+json"

    refute conn.resp_body =~ "leakhunter"

    document = JSON.decode!(conn.resp_body)
    assert document["asyncapi"] == "3.0.0"
    assert document["id"] == "urn:ankusa:instance:#{config.instance}"

    assert [%{"address" => "ankusa.hooks", "messages" => %{"stripe" => _}}] =
             Map.values(document["channels"])
  end

  test "Config.new/1 rejects an unknown admin key like any other section" do
    assert_raise ArgumentError, ~r/unknown Ankusa.Config key: admin.tokens/, fn ->
      Config.new(admin: %{enabled: true, tokens: ["nope"]})
    end
  end

  # ── tenant-scoped sources ──────────────────────────────────────────────────

  test "writing a source against a read-only store is 409", %{inst: inst} do
    # The module-level setup configures the default read-only (Static) store.
    conn =
      call(inst, :post, "/v1/tenants/acme/sources", JSON.encode!(spec(%{"name" => "billing"})))

    assert conn.status == 409
    assert %{"error" => "source_store_read_only"} = JSON.decode!(conn.resp_body)

    conn = call(inst, :delete, "/v1/tenants/acme/sources/billing")
    assert conn.status == 409
    assert %{"error" => "source_store_read_only"} = JSON.decode!(conn.resp_body)
  end

  describe "tenant-scoped sources with a writable store" do
    setup do
      config =
        test_config(
          roles: [:dispatch],
          admin: %{enabled: true},
          source_store: {Ankusa.SourceStore.Persistent, decoder: &decoder/2}
        )

      put_config(config)
      start_supervised!({Ankusa.Store, instance: config.instance, config: config})
      {:ok, pid} = Ankusa.SourceStore.Persistent.start_link(config)
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)

      %{inst: config.instance, config: config}
    end

    test "create, then list, then get one source", %{inst: inst} do
      conn =
        call(inst, :post, "/v1/tenants/acme/sources", JSON.encode!(spec(%{"name" => "billing"})))

      assert conn.status == 201
      created = JSON.decode!(conn.resp_body)

      assert created["tenant"] == "acme"
      assert created["name"] == "billing"
      assert created["source_id"] == "acme.billing"
      assert created["ingest_path"] == "/webhooks/acme.billing"

      assert created["verify"] == %{
               "type" => "hmac",
               "secret" => "[REDACTED]",
               "signature_header" => "X-Sig"
             }

      assert created["on_verify_failure"] == "reject"
      assert created["sinks"] == [%{"type" => "log"}]
      refute conn.resp_body =~ "s3cr3t"

      conn = call(inst, :get, "/v1/tenants/acme/sources")
      assert conn.status == 200
      assert %{"tenant" => "acme", "entries" => [listed]} = JSON.decode!(conn.resp_body)
      assert listed == created

      conn = call(inst, :get, "/v1/tenants/acme/sources/billing")
      assert conn.status == 200
      assert JSON.decode!(conn.resp_body) == created
    end

    test "list is sorted by name and scoped to its tenant", %{inst: inst} do
      for name <- ~w(zebra apple mango) do
        assert call(
                 inst,
                 :post,
                 "/v1/tenants/acme/sources",
                 JSON.encode!(spec(%{"name" => name}))
               ).status ==
                 201
      end

      assert call(
               inst,
               :post,
               "/v1/tenants/beta/sources",
               JSON.encode!(spec(%{"name" => "apple"}))
             ).status ==
               201

      assert %{"entries" => entries} =
               JSON.decode!(call(inst, :get, "/v1/tenants/acme/sources").resp_body)

      assert Enum.map(entries, & &1["name"]) == ~w(apple mango zebra)

      assert %{"entries" => beta} =
               JSON.decode!(call(inst, :get, "/v1/tenants/beta/sources").resp_body)

      assert Enum.map(beta, & &1["source_id"]) == ["beta.apple"]
    end

    test "verify defaults to type none when absent", %{inst: inst} do
      body = %{"name" => "plain", "sinks" => [%{"type" => "log"}]}

      conn = call(inst, :post, "/v1/tenants/acme/sources", JSON.encode!(body))
      assert conn.status == 201

      entry = JSON.decode!(conn.resp_body)
      assert entry["verify"] == %{"type" => "none"}
      assert entry["on_verify_failure"] == nil
    end

    test "PUT replaces the spec and a name in the body is ignored", %{inst: inst} do
      call(inst, :post, "/v1/tenants/acme/sources", JSON.encode!(spec(%{"name" => "billing"})))

      edit = %{"name" => "not-billing", "sinks" => [%{"type" => "log"}]}

      conn = call(inst, :put, "/v1/tenants/acme/sources/billing", JSON.encode!(edit))
      assert conn.status == 200

      entry = JSON.decode!(conn.resp_body)
      assert entry["name"] == "billing"
      assert entry["source_id"] == "acme.billing"
      assert entry["verify"] == %{"type" => "none"}
      assert entry["sinks"] == [%{"type" => "log"}]
    end

    test "redacts secrets at any depth, http headers, and URL passwords", %{inst: inst} do
      spec =
        spec(%{
          "name" => "hooks",
          "sinks" => [
            %{
              "type" => "http",
              "url" => "https://user:leakhunter@example.test/hook",
              "headers" => %{"authorization" => "Bearer leakhunter", "x-team" => "payments"}
            },
            %{"type" => "log", "password" => "leakhunter", "nested" => %{"token" => "leakhunter"}}
          ]
        })

      conn = call(inst, :post, "/v1/tenants/acme/sources", JSON.encode!(spec))
      assert conn.status == 201
      refute conn.resp_body =~ "leakhunter"

      entry = JSON.decode!(conn.resp_body)
      assert [http, log] = entry["sinks"]
      assert http["url"] == "https://user:[REDACTED]@example.test/hook"
      assert http["headers"] == %{"authorization" => "[REDACTED]", "x-team" => "[REDACTED]"}
      assert log["password"] == "[REDACTED]"
      assert log["nested"]["token"] == "[REDACTED]"
    end

    test "a bad tenant is 400 invalid_tenant on every route", %{inst: inst} do
      for {method, path, body} <- [
            {:get, "/v1/tenants/bad.tenant/sources", ""},
            {:post, "/v1/tenants/bad.tenant/sources", JSON.encode!(spec(%{"name" => "billing"}))},
            {:get, "/v1/tenants/bad.tenant/sources/billing", ""},
            {:put, "/v1/tenants/bad.tenant/sources/billing", JSON.encode!(spec())}
          ] do
        conn = call(inst, method, path, body)
        assert conn.status == 400, "#{method} #{path} returned #{conn.status}"
        assert %{"error" => "invalid_tenant"} = JSON.decode!(conn.resp_body)
      end
    end

    test "a bad name is 400 invalid_source with a message", %{inst: inst} do
      long = String.duplicate("a", 65)

      conn = call(inst, :get, "/v1/tenants/acme/sources/#{long}")
      assert conn.status == 400
      assert %{"error" => "invalid_source", "message" => message} = JSON.decode!(conn.resp_body)
      assert message =~ "name"

      conn = call(inst, :put, "/v1/tenants/acme/sources/#{long}", JSON.encode!(spec()))
      assert conn.status == 400
      assert %{"error" => "invalid_source"} = JSON.decode!(conn.resp_body)
    end

    test "a non-JSON or non-object body is 400 invalid_source", %{inst: inst} do
      for body <- ["not json", "[]", "", ~s("string")] do
        conn = call(inst, :post, "/v1/tenants/acme/sources", body)
        assert conn.status == 400, "body #{inspect(body)} returned #{conn.status}"
        assert %{"error" => "invalid_source", "message" => message} = JSON.decode!(conn.resp_body)
        assert message =~ "JSON object"
      end
    end

    test "a spec the decoder rejects is 400 invalid_source with the decoder's message", %{
      inst: inst
    } do
      conn =
        call(
          inst,
          :post,
          "/v1/tenants/acme/sources",
          JSON.encode!(%{"name" => "billing", "sinks" => []})
        )

      assert conn.status == 400
      assert %{"error" => "invalid_source", "message" => message} = JSON.decode!(conn.resp_body)
      assert message =~ "sinks"
    end

    test "getting or updating a missing source is 404", %{inst: inst} do
      conn = call(inst, :get, "/v1/tenants/acme/sources/nope")
      assert conn.status == 404
      assert %{"error" => "source_not_found"} = JSON.decode!(conn.resp_body)

      conn = call(inst, :put, "/v1/tenants/acme/sources/nope", JSON.encode!(spec()))
      assert conn.status == 404
      assert %{"error" => "source_not_found"} = JSON.decode!(conn.resp_body)
    end

    test "DELETE removes the source and it disappears from the list", %{inst: inst} do
      body = JSON.encode!(spec(%{"name" => "billing"}))
      assert call(inst, :post, "/v1/tenants/acme/sources", body).status == 201

      assert call(
               inst,
               :post,
               "/v1/tenants/acme/sources",
               JSON.encode!(spec(%{"name" => "other"}))
             ).status == 201

      conn = call(inst, :delete, "/v1/tenants/acme/sources/billing")
      assert conn.status == 204
      assert conn.resp_body in ["", nil]

      assert call(inst, :get, "/v1/tenants/acme/sources/billing").status == 404

      assert %{"entries" => entries} =
               JSON.decode!(call(inst, :get, "/v1/tenants/acme/sources").resp_body)

      assert Enum.map(entries, & &1["name"]) == ["other"]
    end

    test "DELETE of a missing source is 404", %{inst: inst} do
      conn = call(inst, :delete, "/v1/tenants/acme/sources/nope")
      assert conn.status == 404
      assert %{"error" => "source_not_found"} = JSON.decode!(conn.resp_body)
    end

    test "a seeded source is not deletable: 400 invalid_source", %{inst: _inst} do
      config =
        test_config(
          roles: [:dispatch],
          admin: %{enabled: true},
          source_store:
            {Ankusa.SourceStore.Persistent,
             [
               decoder: &decoder/2,
               sources: %{
                 "acme.billing" => [tenant_id: "acme", sinks: [{Ankusa.Sink.Log, []}]]
               }
             ]}
        )

      put_config(config)
      start_supervised!({Ankusa.Store, instance: config.instance, config: config})
      {:ok, pid} = Ankusa.SourceStore.Persistent.start_link(config)
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)

      conn = call(config.instance, :delete, "/v1/tenants/acme/sources/billing")
      assert conn.status == 400
      assert %{"error" => "invalid_source", "message" => message} = JSON.decode!(conn.resp_body)
      assert message =~ "read-only"
    end

    test "a bad tenant is 400 invalid_tenant on delete", %{inst: inst} do
      conn = call(inst, :delete, "/v1/tenants/bad.tenant/sources/billing")
      assert conn.status == 400
      assert %{"error" => "invalid_tenant"} = JSON.decode!(conn.resp_body)
    end

    test "a bad name is 400 invalid_source on delete", %{inst: inst} do
      conn = call(inst, :delete, "/v1/tenants/acme/sources/#{String.duplicate("a", 65)}")
      assert conn.status == 400
      assert %{"error" => "invalid_source", "message" => message} = JSON.decode!(conn.resp_body)
      assert message =~ "name"
    end

    test "creating an existing source is 409 source_exists", %{inst: inst} do
      body = JSON.encode!(spec(%{"name" => "billing"}))
      assert call(inst, :post, "/v1/tenants/acme/sources", body).status == 201

      conn = call(inst, :post, "/v1/tenants/acme/sources", body)
      assert conn.status == 409
      assert %{"error" => "source_exists"} = JSON.decode!(conn.resp_body)
    end

    test "a body over 64 KiB is 400 invalid_source", %{inst: inst} do
      oversized =
        JSON.encode!(spec(%{"name" => "billing", "padding" => String.duplicate("x", 70_000)}))

      conn = call(inst, :post, "/v1/tenants/acme/sources", oversized)
      assert conn.status == 400
      assert %{"error" => "invalid_source"} = JSON.decode!(conn.resp_body)
    end
  end

  describe "per-tenant rate limits" do
    setup do
      config =
        test_config(
          roles: [:edge],
          admin: %{enabled: true, port: 0},
          rate_limits: %{
            default: %{rate: 5, burst: 10},
            tenants: %{"acme" => %{rate: 1, burst: 2}}
          }
        )

      put_config(config)
      start_supervised!({Ankusa.Instance, config})
      %{inst: config.instance, config: config}
    end

    test "GET reports the config's limit, or the default", %{inst: inst} do
      assert JSON.decode!(call(inst, :get, "/v1/tenants/acme/rate-limit").resp_body) ==
               %{"tenant" => "acme", "rate" => 1, "burst" => 2, "source" => "config"}

      assert JSON.decode!(call(inst, :get, "/v1/tenants/globex/rate-limit").resp_body) ==
               %{"tenant" => "globex", "rate" => 5, "burst" => 10, "source" => "default"}
    end

    test "PUT takes effect at once, and DELETE hands the tenant back to config", %{inst: inst} do
      put = call(inst, :put, "/v1/tenants/acme/rate-limit", ~s({"rate":2.5,"burst":4}))

      assert put.status == 200

      assert JSON.decode!(put.resp_body) ==
               %{"tenant" => "acme", "rate" => 2.5, "burst" => 4, "source" => "override"}

      assert JSON.decode!(call(inst, :get, "/v1/tenants/acme/rate-limit").resp_body)["source"] ==
               "override"

      assert JSON.decode!(call(inst, :get, "/v1/rate-limits").resp_body) == %{
               "default" => %{"rate" => 5, "burst" => 10},
               "tenants" => [
                 %{"tenant" => "acme", "rate" => 2.5, "burst" => 4, "source" => "override"}
               ]
             }

      # 204 carries no body.
      assert call(inst, :delete, "/v1/tenants/acme/rate-limit").status == 204

      assert JSON.decode!(call(inst, :get, "/v1/tenants/acme/rate-limit").resp_body)["source"] ==
               "config"

      assert JSON.decode!(call(inst, :delete, "/v1/tenants/acme/rate-limit").resp_body) ==
               %{"error" => "rate_limit_not_found"}
    end

    @tag :capture_log
    test "an override the node cannot persist is 503 and changes nothing", %{inst: inst} do
      # The store under the limiter goes away: the write cannot land, so the
      # override is refused rather than applied in memory only.
      :ok = Supervisor.terminate_child(Ankusa.via(inst, :instance), {Ankusa.Store, inst})

      conn = call(inst, :put, "/v1/tenants/acme/rate-limit", ~s({"rate":100,"burst":100}))

      assert conn.status == 503
      assert JSON.decode!(conn.resp_body) == %{"error" => "store_unavailable"}

      assert JSON.decode!(call(inst, :get, "/v1/tenants/acme/rate-limit").resp_body)["source"] ==
               "config"
    end

    test "a bad limit is 400 invalid_rate_limit, named", %{inst: inst} do
      for {body, message} <- [
            {~s({"rate":0,"burst":1}), "rate must be a number greater than 0, got 0"},
            {~s({"rate":1}), "burst is required"},
            {~s({"rate":1,"burst":1.5}), "burst must be an integer of at least 1, got 1.5"},
            {~s({"rate":1,"burst":1,"per":"s"}), ~s(unknown field "per")},
            {"[1]", "body must be a JSON object"},
            {"not json", "body must be a JSON object"}
          ] do
        conn = call(inst, :put, "/v1/tenants/acme/rate-limit", body)

        assert conn.status == 400, "#{body} returned #{conn.status}"

        assert %{"error" => "invalid_rate_limit", "message" => ^message} =
                 JSON.decode!(conn.resp_body)
      end
    end

    test "a bad tenant is 400 invalid_tenant on every route", %{inst: inst} do
      for {method, body} <- [
            {:get, ""},
            {:put, ~s({"rate":1,"burst":1})},
            {:delete, ""}
          ] do
        conn = call(inst, method, "/v1/tenants/bad.tenant/rate-limit", body)
        assert conn.status == 400, "#{method} returned #{conn.status}"
        assert %{"error" => "invalid_tenant"} = JSON.decode!(conn.resp_body)
      end
    end
  end

  # ── source-test helpers ────────────────────────────────────────────────────

  defp spec(overrides \\ %{}) do
    Map.merge(
      %{
        "verify" => %{"type" => "hmac", "secret" => "s3cr3t", "signature_header" => "X-Sig"},
        "on_verify_failure" => "reject",
        "sinks" => [%{"type" => "log"}]
      },
      overrides
    )
  end

  # Stands in for `AnkusaServer.Config.source_from_map!/2`: the store only cares
  # that the decoder returns source options or raises.
  defp decoder(_source_id, spec) do
    case Map.get(spec, "sinks") do
      [_ | _] = sinks -> [sinks: Enum.map(sinks, &sink/1)]
      _ -> raise ArgumentError, "sinks must be a non-empty list"
    end
  end

  defp sink(%{"type" => "log"}), do: {Ankusa.Sink.Log, []}
  defp sink(%{"type" => "http"} = spec), do: {Ankusa.Sink.Http, [url: spec["url"] || "http://x"]}
  defp sink(other), do: raise(ArgumentError, "unknown sink: #{inspect(other)}")
end
