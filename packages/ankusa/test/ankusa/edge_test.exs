defmodule Ankusa.EdgeTest do
  use ExUnit.Case, async: false

  import Ankusa.TestHelpers
  alias Ankusa.Edge.{Ingest, Router}

  @secret "whsec_" <> Base.encode64("supersecret-key")

  # A hook is only stored when something is obliged to handle it, so sources
  # without sinks of their own get a log sink: stored hooks stay pending (no
  # dispatch role runs here), which is what these tests look at.
  defp start_edge(sources) do
    sources =
      Map.new(sources, fn {id, opts} ->
        {id, Keyword.put_new(opts, :sinks, [{Ankusa.Sink.Log, []}])}
      end)

    config =
      test_config(roles: [:edge], source_store: {Ankusa.SourceStore.Static, sources: sources})

    start_supervised!({Ankusa.Instance, config})
    config
  end

  defp route(config, req) do
    conn =
      Plug.Test.conn(:post, req.path, req.body)
      |> then(fn c ->
        Enum.reduce(req.headers, c, fn {k, v}, c -> Plug.Conn.put_req_header(c, k, v) end)
      end)

    Router.call(conn, Router.init(instance: config.instance))
  end

  test "accepts and durably commits a hook, returning 201 with id after commit" do
    config = start_edge(%{"demo" => [verifier: {Ankusa.Verifier.None, []}]})
    conn = route(config, request("demo", ~s({"hello":"world"})))

    assert conn.status == 201
    assert %{"status" => "accepted", "id" => id} = JSON.decode!(conn.resp_body)
    # Exactly these two keys: the ingest surface carries no node-local state.
    assert Map.keys(JSON.decode!(conn.resp_body)) == ["id", "status"]
    # durably readable straight after the ack
    assert {:ok, [env]} = Ankusa.Queue.hooks(config.instance, 0, 10)
    assert env.id == id
    assert env.body == ~s({"hello":"world"})
  end

  test "unknown source returns 404 and stores nothing" do
    config = start_edge(%{})
    conn = route(config, request("nope", "x"))
    assert conn.status == 404
    assert stored_ids(config.instance) == []
  end

  test "the same body posted twice is stored twice" do
    config = start_edge(%{"demo" => [verifier: {Ankusa.Verifier.None, []}]})

    body = ~s({"id":"evt_123"})
    first = route(config, request("demo", body))
    second = route(config, request("demo", body))

    assert first.status == 201
    assert second.status == 201
    assert %{"status" => "accepted", "id" => id1} = JSON.decode!(first.resp_body)
    assert %{"status" => "accepted", "id" => id2} = JSON.decode!(second.resp_body)
    assert id1 != id2
    assert length(stored_ids(config.instance)) == 2
  end

  test "valid Standard Webhooks signature is accepted; a bad one is rejected (401)" do
    sources = %{
      "swh" => [
        verifier: {Ankusa.Verifier.Hmac, scheme: :standard_webhooks, secret: @secret},
        on_verify_failure: :reject
      ]
    }

    config = start_edge(sources)
    body = ~s({"event":"ok"})
    headers = standard_webhooks_headers("msg_1", body, @secret)

    good = route(config, request("swh", body, headers))
    assert good.status == 201

    bad =
      route(
        config,
        request("swh", body, [
          {"webhook-id", "msg_2"},
          {"webhook-timestamp", "#{System.system_time(:second)}"},
          {"webhook-signature", "v1,deadbeef"}
        ])
      )

    assert bad.status == 401
    assert %{"error" => "verification_failed"} = JSON.decode!(bad.resp_body)

    # only the verified hook was stored
    assert length(stored_ids(config.instance)) == 1
  end

  test "quarantine policy durably holds a failed hook and returns 202" do
    sources = %{
      "q" => [
        verifier: {Ankusa.Verifier.Hmac, scheme: :standard_webhooks, secret: @secret},
        on_verify_failure: :quarantine
      ]
    }

    config = start_edge(sources)
    conn = route(config, request("q", "forged", [{"webhook-signature", "v1,nope"}]))

    assert conn.status == 202
    assert %{"status" => "quarantined"} = JSON.decode!(conn.resp_body)
    assert stored_ids(config.instance) == []
    assert {:ok, [entry]} = Ankusa.Edge.Quarantine.recent(config.instance, 10)
    assert entry.source_id == "q"
  end

  test "load shed: a full batcher queue returns 503 with Retry-After" do
    # a batcher whose queue is full replies :overload immediately
    config =
      test_config(
        roles: [:edge],
        source_store: {Ankusa.SourceStore.Static, sources: %{"demo" => []}},
        batcher: %{partitions: 1, max_batch: 100_000, max_delay_ms: 60_000, max_queue: 0}
      )

    start_supervised!({Ankusa.Instance, config})

    # max_queue: 0 sheds every request
    assert {:error, :overload} = Ingest.ingest(config.instance, request("demo", "x"))

    conn = route(config, request("demo", "x"))
    assert conn.status == 503
    assert Plug.Conn.get_resp_header(conn, "retry-after") == ["1"]
  end

  test "sheds with 503 once max_queue is reached while a commit is in flight" do
    # The queue writer is suspended, so the first commit blocks and the queue
    # fills behind it. Resuming it lets everything that was admitted commit.
    config =
      test_config(
        roles: [:edge],
        source_store:
          {Ankusa.SourceStore.Static, sources: %{"demo" => [sinks: [{Ankusa.Sink.Log, []}]]}},
        batcher: %{partitions: 1, max_batch: 2, max_queue: 4, max_delay_ms: 0}
      )

    start_supervised!({Ankusa.Instance, config})
    inst = config.instance

    writer = Ankusa.whereis(inst, :queue_writer)
    :ok = :sys.suspend(writer)

    tasks =
      Enum.map(1..20, fn _ -> Task.async(fn -> Ingest.ingest(inst, request("demo", "x")) end) end)

    # What is shed is answered at once, while the writer is still stuck; what
    # was admitted is still waiting on it.
    {done, pending} =
      tasks |> Task.yield_many(500) |> Enum.split_with(fn {_task, result} -> result != nil end)

    # The bound has to bite: with 20 concurrent callers and a queue that holds
    # 4, most of them never get in.
    assert Enum.count(done, &match?({_, {:ok, {:error, :overload}}}, &1)) >= 10

    :ok = :sys.resume(writer)

    results =
      Enum.map(done, fn {_task, {:ok, result}} -> result end) ++
        Enum.map(pending, fn {task, nil} -> Task.await(task, 30_000) end)

    overloads = Enum.count(results, &(&1 == {:error, :overload}))
    committed = for {:ok, env} <- results, do: env
    assert length(committed) + overloads == 20

    # ...and everything that *was* acked is durably stored.
    stored = inst |> stored_ids() |> MapSet.new()
    assert Enum.all?(committed, &MapSet.member?(stored, &1.id))
  end

  test "oversize payload is refused with 413" do
    config =
      test_config(
        roles: [:edge],
        max_body_bytes: 16,
        source_store: {Ankusa.SourceStore.Static, sources: %{"demo" => []}}
      )

    start_supervised!({Ankusa.Instance, config})
    conn = route(config, request("demo", String.duplicate("x", 100)))
    assert conn.status == 413
  end
end
