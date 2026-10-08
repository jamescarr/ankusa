defmodule Ankusa.EdgeTest do
  use ExUnit.Case, async: false

  import Ankusa.TestHelpers
  alias Ankusa.Edge.{Batcher, Ingest}
  alias Ankusa.Envelope

  @secret "whsec_" <> Base.encode64("supersecret-key")

  # A hook is only stored when something is obliged to handle it, so sources
  # without sinks of their own get a log sink: stored hooks stay pending (no
  # dispatch role runs here), which is what these tests look at.
  defp start_edge(sources, overrides \\ []) do
    sources =
      Map.new(sources, fn {id, opts} ->
        {id, Keyword.put_new(opts, :sinks, [{Ankusa.Sink.Log, []}])}
      end)

    config =
      test_config(
        [roles: [:edge], source_store: {Ankusa.SourceStore.Static, sources: sources}] ++
          overrides
      )

    start_supervised!({Ankusa.Instance, config})
    config
  end

  test "accepts and durably commits a hook, returning 201 with id after commit" do
    config = start_edge(%{"demo" => [verifier: {Ankusa.Verifier.None, []}]})
    conn = route_through_edge(config, request("demo", ~s({"hello":"world"})))

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
    conn = route_through_edge(config, request("nope", "x"))
    assert conn.status == 404
    assert stored_ids(config.instance) == []
  end

  test "the same body posted twice is stored twice" do
    config = start_edge(%{"demo" => [verifier: {Ankusa.Verifier.None, []}]})

    body = ~s({"id":"evt_123"})
    first = route_through_edge(config, request("demo", body))
    second = route_through_edge(config, request("demo", body))

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

    good = route_through_edge(config, request("swh", body, headers))
    assert good.status == 201

    bad =
      route_through_edge(
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
    conn = route_through_edge(config, request("q", "forged", [{"webhook-signature", "v1,nope"}]))

    assert conn.status == 202
    assert %{"status" => "quarantined"} = JSON.decode!(conn.resp_body)
    assert stored_ids(config.instance) == []
    assert {:ok, [entry]} = Ankusa.Edge.Quarantine.recent(config.instance, 10)
    assert entry.source_id == "q"
  end

  defp quarantining(ids) do
    Map.new(ids, fn id ->
      {id,
       [
         verifier: {Ankusa.Verifier.Hmac, scheme: :standard_webhooks, secret: @secret},
         on_verify_failure: :quarantine
       ]}
    end)
  end

  defp forged(config, source_id, body \\ "forged"),
    do: route_through_edge(config, request(source_id, body, [{"webhook-signature", "v1,nope"}]))

  test "each source has its own quarantine bucket; over it is 429 with Retry-After" do
    config = start_edge(quarantining(["q1", "q2"]), quarantine: %{burst: 2, rate: 1})

    assert forged(config, "q1").status == 202
    assert forged(config, "q1").status == 202

    limited = forged(config, "q1")
    assert limited.status == 429
    assert JSON.decode!(limited.resp_body) == %{"error" => "quarantine_rate_limited"}
    assert [retry_after] = Plug.Conn.get_resp_header(limited, "retry-after")
    assert String.to_integer(retry_after) >= 1

    # q1's flood never spent q2's tokens.
    assert forged(config, "q2").status == 202
    assert {:ok, entries} = Ankusa.Edge.Quarantine.recent(config.instance, 10)
    assert Enum.frequencies_by(entries, & &1.source_id) == %{"q1" => 2, "q2" => 1}
  end

  test "a full pen refuses with 503 and evicts nothing; a purge makes room again" do
    # One entry (a 2 KB body plus its envelope) fits under 3 KB; two never do.
    body = String.duplicate("x", 2_000)
    config = start_edge(quarantining(["q"]), quarantine: %{max_bytes: 3_000})

    assert forged(config, "q", body).status == 202
    assert {:ok, [held]} = Ankusa.Edge.Quarantine.recent(config.instance, 10)

    full = forged(config, "q", body)
    assert full.status == 503
    assert JSON.decode!(full.resp_body) == %{"error" => "quarantine_full"}
    assert Plug.Conn.get_resp_header(full, "retry-after") == ["60"]
    assert {:ok, [^held]} = Ankusa.Edge.Quarantine.recent(config.instance, 10)

    assert {:ok, %{deleted: 1, bytes: bytes}} =
             Ankusa.Edge.Quarantine.purge(config.instance, %{}, 10)

    assert bytes == held.size
    assert forged(config, "q", body).status == 202
  end

  test "the pen's byte count survives a restart" do
    body = String.duplicate("x", 2_000)
    config = start_edge(quarantining(["q"]), quarantine: %{max_bytes: 3_000})

    assert forged(config, "q", body).status == 202

    stop_supervised!({Ankusa.Instance, config.instance})
    start_supervised!({Ankusa.Instance, config})

    # The restarted pen counted the held entry back from the store.
    assert forged(config, "q", body).status == 503
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

    conn = route_through_edge(config, request("demo", "x"))
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

  # ── a commit is never abandoned, only unstarted work expires ───────────────

  defp start_stalled_edge(batcher \\ %{}) do
    config =
      test_config(
        roles: [:edge],
        source_store:
          {Ankusa.SourceStore.Static, sources: %{"demo" => [sinks: [{Ankusa.Sink.Log, []}]]}},
        batcher:
          Map.merge(%{partitions: 1, max_batch: 1, max_delay_ms: 0, max_queue: 10_000}, batcher)
      )

    start_supervised!({Ankusa.Instance, config})
    config.instance
  end

  defp entry(n, sinks \\ [{Ankusa.Sink.Log, []}]) do
    body = ~s({"n":#{n}})

    %{
      envelope: %Envelope{
        id: Ankusa.UUIDv7.generate(),
        source_id: "demo",
        tenant_id: "default",
        received_at: System.system_time(:millisecond),
        method: "POST",
        path: "/webhooks/demo",
        headers: [],
        body: body,
        size: byte_size(body)
      },
      sinks: sinks
    }
  end

  defp eventually(fun, tries \\ 200) do
    cond do
      fun.() ->
        :ok

      tries == 0 ->
        flunk("condition never became true")

      true ->
        Process.sleep(10)
        eventually(fun, tries - 1)
    end
  end

  defp commit_in_task(inst, entry, timeout) do
    Task.async(fn -> Batcher.commit(inst, 0, entry, timeout) end)
  end

  defp inflight?(batcher), do: :sys.get_state(batcher).inflight != nil

  @tag :capture_log
  test "a record still buffered at its deadline is answered 503 while the commit ahead of it is stuck" do
    inst = start_stalled_edge()
    batcher = Ankusa.whereis(inst, {:batcher, 0})
    writer = Ankusa.whereis(inst, :queue_writer)
    :ok = :sys.suspend(writer)

    a = entry(1)
    b = entry(2)
    task_a = commit_in_task(inst, a, 10_000)
    eventually(fn -> inflight?(batcher) end)
    task_b = commit_in_task(inst, b, 200)

    # B waited behind A and ran out its deadline; the writer is still stuck.
    assert {:ok, {:error, :store_unavailable}} = Task.yield(task_b, 1_000)

    :ok = :sys.resume(writer)
    assert {:committed, _} = Task.await(task_a)

    stored = stored_ids(inst)
    assert a.envelope.id in stored
    refute b.envelope.id in stored
  end

  @tag :capture_log
  test "an :infinity timeout waits behind a stuck commit and never expires; a finite one beside it does" do
    inst = start_stalled_edge()
    batcher = Ankusa.whereis(inst, {:batcher, 0})
    writer = Ankusa.whereis(inst, :queue_writer)
    :ok = :sys.suspend(writer)

    a = entry(1)
    b = entry(2)
    c = entry(3)
    task_a = commit_in_task(inst, a, :infinity)
    eventually(fn -> inflight?(batcher) end)
    task_b = commit_in_task(inst, b, :infinity)
    eventually(fn -> :sys.get_state(batcher).count == 1 end)
    task_c = commit_in_task(inst, c, 200)

    # C expires behind the stuck commit; B, with no deadline, keeps waiting.
    assert {:ok, {:error, :store_unavailable}} = Task.yield(task_c, 1_000)
    assert Task.yield(task_b, 0) == nil
    assert Ankusa.whereis(inst, {:batcher, 0}) == batcher

    :ok = :sys.resume(writer)
    assert {:committed, _} = Task.await(task_a)
    assert {:committed, _} = Task.await(task_b)

    stored = stored_ids(inst)
    assert a.envelope.id in stored
    assert b.envelope.id in stored
    refute c.envelope.id in stored
  end

  @tag :capture_log
  test "the writer refuses a batch whose deadline passed before it could start" do
    inst = start_stalled_edge()
    writer = Ankusa.whereis(inst, :queue_writer)
    :ok = :sys.suspend(writer)

    e = entry(1)
    task = commit_in_task(inst, e, 200)
    Process.sleep(400)
    :ok = :sys.resume(writer)

    assert {:error, :store_unavailable} = Task.await(task)
    refute e.envelope.id in stored_ids(inst)
  end

  @tag timeout: 30_000
  test "a commit stuck longer than the old 5 s writer timeout is waited out, not 503'd" do
    inst = start_stalled_edge()
    writer = Ankusa.whereis(inst, :queue_writer)
    :ok = :sys.suspend(writer)

    tasks =
      Enum.map(1..8, fn _ -> Task.async(fn -> Ingest.ingest(inst, request("demo", "x")) end) end)

    Process.sleep(5_500)
    :ok = :sys.resume(writer)

    results = Enum.map(tasks, &Task.await(&1, 10_000))
    assert Enum.all?(results, &match?({:ok, _env}, &1))

    stored = MapSet.new(stored_ids(inst))
    assert Enum.all?(results, fn {:ok, env} -> MapSet.member?(stored, env.id) end)
  end

  @tag :capture_log
  test "a commit task killed from outside fails its own batch only; the batcher and the buffer survive" do
    inst = start_stalled_edge()
    batcher = Ankusa.whereis(inst, {:batcher, 0})
    writer = Ankusa.whereis(inst, :queue_writer)
    :ok = :sys.suspend(writer)

    a = entry(1)
    b = entry(2)
    task_a = commit_in_task(inst, a, 10_000)
    eventually(fn -> inflight?(batcher) end)
    # A's call has reached the writer's mailbox, not just started its task: the
    # kill below must land while it waits there.
    eventually(fn -> Process.info(writer, :message_queue_len) == {:message_queue_len, 1} end)
    task_b = commit_in_task(inst, b, 10_000)
    eventually(fn -> :sys.get_state(batcher).count == 1 end)

    task_sup = :sys.get_state(batcher).task_sup
    [commit_task] = Task.Supervisor.children(task_sup)
    Process.exit(commit_task, :kill)

    assert {:error, :store_unavailable} = Task.await(task_a)
    assert Ankusa.whereis(inst, {:batcher, 0}) == batcher

    :ok = :sys.resume(writer)
    assert {:committed, _} = Task.await(task_b)

    # A was answered 503, so it must not be stored behind the caller's back: the
    # writer found its caller gone and committed nothing.
    stored = stored_ids(inst)
    refute a.envelope.id in stored
    assert b.envelope.id in stored
  end

  @tag :capture_log
  test "the batcher's status shows how many records it holds, never their sinks or bodies" do
    inst = start_stalled_edge()
    batcher = Ankusa.whereis(inst, {:batcher, 0})
    writer = Ankusa.whereis(inst, :queue_writer)
    :ok = :sys.suspend(writer)

    sinks = [{Ankusa.Sink.Log, [token: "s3cr3t-canary"]}]
    task_a = commit_in_task(inst, entry(1, sinks), 10_000)
    eventually(fn -> inflight?(batcher) end)
    task_b = commit_in_task(inst, entry(2, sinks), 10_000)
    eventually(fn -> :sys.get_state(batcher).count == 1 end)

    # The canary is in the state (one record in flight, one buffered): the
    # redaction is what keeps it out of the status.
    assert inspect(:sys.get_state(batcher), limit: :infinity) =~ "s3cr3t-canary"

    status = inspect(:sys.get_status(batcher), limit: :infinity, printable_limit: :infinity)
    refute status =~ "s3cr3t-canary"
    assert %{buffer: 1, inflight: 1} = status_state(batcher)

    :ok = :sys.resume(writer)
    assert {:committed, _} = Task.await(task_a)
    assert {:committed, _} = Task.await(task_b)
  end

  # The state `format_status/1` hands back, from `:sys.get_status/1`.
  defp status_state(pid) do
    {:status, _pid, _module, [_pdict, _sysstate, _parent, _dbg, misc]} = :sys.get_status(pid)
    [state] = for {:data, data} <- misc, {~c"State", state} <- data, do: state
    state
  end

  test "oversize payload is refused with 413" do
    config =
      test_config(
        roles: [:edge],
        max_body_bytes: 16,
        source_store: {Ankusa.SourceStore.Static, sources: %{"demo" => []}}
      )

    start_supervised!({Ankusa.Instance, config})
    conn = route_through_edge(config, request("demo", String.duplicate("x", 100)))
    assert conn.status == 413
  end

  # One request over a real socket: the request head, then `sent` bytes of a
  # body that claims to be `declared` long. The rest is never sent, so a reply
  # can only come from a server that did not wait for it.
  defp raw_post(port, path, declared, sent, extra_headers \\ []) do
    {:ok, sock} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])
    extra = Enum.map_join(extra_headers, &"#{&1}\r\n")

    :ok =
      :gen_tcp.send(
        sock,
        "POST #{path} HTTP/1.1\r\nHost: localhost\r\n#{extra}Content-Length: #{declared}\r\n\r\n#{sent}"
      )

    reply = :gen_tcp.recv(sock, 0, 2_000)
    :ok = :gen_tcp.close(sock)
    reply
  end

  test "an oversize Content-Length is refused before any of the body is read" do
    config =
      start_edge(%{"demo" => [verifier: {Ankusa.Verifier.None, []}]},
        max_body_bytes: 16,
        port: free_port()
      )

    assert {:ok, "HTTP/1.1 413" <> _} = raw_post(config.port, "/webhooks/demo", 100, "x")
    assert stored_ids(config.instance) == []
  end

  test "an unknown source is refused before the body is read" do
    config = start_edge(%{}, port: free_port())

    assert {:ok, "HTTP/1.1 404" <> _} = raw_post(config.port, "/webhooks/nope", 10, "")
    assert stored_ids(config.instance) == []
  end

  test "header bytes no sink can carry are refused before the body is read" do
    config = start_edge(%{"demo" => [verifier: {Ankusa.Verifier.None, []}]}, port: free_port())

    for {line, header} <- [
          {"x-note: caf\xC3\xA9", "x-note"},
          {"content-type: text/plain; x=\xFF", "content-type"}
        ] do
      assert {:ok, "HTTP/1.1 400" <> _ = reply} =
               raw_post(config.port, "/webhooks/demo", 1_000, "", [line])

      assert reply =~ JSON.encode!(%{error: "invalid_header", header: header})
      assert stored_ids(config.instance) == []
    end
  end

  test "Ingest.ingest/2 refuses a header with an undeliverable byte" do
    config = start_edge(%{"demo" => [verifier: {Ankusa.Verifier.None, []}]})
    req = request("demo", "x", [{"x-note", "caf\xC3\xA9"}])

    assert {:error, {:invalid_header, "x-note"}} = Ingest.ingest(config.instance, req)
    assert stored_ids(config.instance) == []
  end

  test "a pen that does not answer is a 503, not a crashed request" do
    config = start_edge(quarantining(["q"]))
    pen = Ankusa.whereis(config.instance, :quarantine)
    :ok = :sys.suspend(pen)
    on_exit(fn -> if Process.alive?(pen), do: :sys.resume(pen) end)

    conn = forged(config, "q")

    assert conn.status == 503
    assert JSON.decode!(conn.resp_body) == %{"error" => "store_unavailable"}
    assert Plug.Conn.get_resp_header(conn, "retry-after") == ["1"]
    :ok = :sys.resume(pen)
  end

  test "the envelope body is copied only when it is a sub-binary" do
    config = start_edge(%{"demo" => [verifier: {Ankusa.Verifier.None, []}]})

    # Over 64 bytes, or the VM copies the slice onto the heap itself and there is
    # no parent to pin.
    slice = :binary.part(String.duplicate("y", 2_000), 0, 100)
    assert :binary.referenced_byte_size(slice) == 2_000

    assert {:ok, env} = Ingest.ingest(config.instance, request("demo", slice))
    assert env.body == String.duplicate("y", 100)
    assert :binary.referenced_byte_size(env.body) == 100

    assert {:ok, env} = Ingest.ingest(config.instance, request("demo", "plain"))
    assert env.body == "plain"
  end
end
