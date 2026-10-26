defmodule Ankusa.EdgeTest do
  use ExUnit.Case, async: false

  import Ankusa.TestHelpers
  alias Ankusa.Edge.{Batcher, Ingest}
  alias Ankusa.{Envelope, Store}
  alias Ankusa.Store.Keys

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

  describe "stored-header policy" do
    @credentials [
      {"authorization", "Bearer sk_live_topsecret"},
      {"cookie", "sid=topsecret-session"},
      {"x-keep", "visible"}
    ]

    defp signed_source(extra \\ []) do
      %{
        "swh" =>
          [verifier: {Ankusa.Verifier.Hmac, scheme: :standard_webhooks, secret: @secret}] ++
            extra
      }
    end

    defp raw_hook_row(inst, %Envelope{seq: seq}) do
      assert {:ok, row} = Store.get(inst, :hooks, Keys.hook(seq))
      row
    end

    test "drops stored headers: a signed POST with authorization and cookie verifies, and neither is stored" do
      config = start_edge(signed_source())
      body = ~s({"event":"ok"})
      headers = standard_webhooks_headers("msg_1", body, @secret) ++ @credentials

      conn = route_through_edge(config, request("swh", body, headers))
      assert conn.status == 201

      assert {:ok, [env]} = Ankusa.Queue.hooks(config.instance, 0, 10)
      names = Enum.map(env.headers, &elem(&1, 0))
      refute "authorization" in names
      refute "cookie" in names
      assert Envelope.header(env, "x-keep") == "visible"
      assert Envelope.header(env, "webhook-id") == "msg_1"
      assert env.verification.status == :ok

      # not in the store's bytes either, names or values
      row = raw_hook_row(config.instance, env)
      refute row =~ "topsecret"
      refute row =~ "authorization"
      refute row =~ "cookie"

      # and never in a message to a sink, whatever `forward_headers` says
      ctx = %{instance: config.instance, attempt: 1, forward_headers: :default}
      assert {:ok, json} = Ankusa.Sink.Message.encode(env, ctx, 1_000)
      refute json =~ "topsecret"
      refute json =~ "authorization"
      assert JSON.decode!(json)["headers"]["x-keep"] == "visible"
    end

    test "the header is matched in any case, and the ingest span reports how many were dropped" do
      config = start_edge(signed_source())
      body = ~s({"event":"ok"})

      headers =
        standard_webhooks_headers("msg_1", body, @secret) ++
          [{"Authorization", "Bearer topsecret"}, {"X-API-Key", "topsecret"}, {"x-keep", "1"}]

      ref = make_ref()
      me = self()

      :telemetry.attach(
        "dropped-#{inspect(ref)}",
        [:ankusa, :ingest, :stop],
        fn _event, _measurements, meta, _ -> send(me, {:ingest, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("dropped-#{inspect(ref)}") end)

      assert {:ok, env} = Ingest.ingest(config.instance, request("swh", body, headers))
      assert_receive {:ingest, %{outcome: :committed, dropped_headers: 2}}

      assert Enum.map(env.headers, &elem(&1, 0)) |> Enum.sort() ==
               ["webhook-id", "webhook-signature", "webhook-timestamp", "x-keep"]
    end

    test "a hook that stores nothing drops nothing" do
      config = start_edge(signed_source())
      body = ~s({"event":"ok"})

      ref = make_ref()
      me = self()

      :telemetry.attach(
        "none-#{inspect(ref)}",
        [:ankusa, :ingest, :stop],
        fn _event, _measurements, meta, _ -> send(me, {:ingest, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("none-#{inspect(ref)}") end)

      bad = [{"webhook-id", "m"}, {"webhook-timestamp", "1"}, {"webhook-signature", "v1,no"}]

      assert {:rejected, _} =
               Ingest.ingest(config.instance, request("swh", body, bad ++ @credentials))

      assert_receive {:ingest, %{outcome: :rejected, dropped_headers: 0}}
    end

    test "a source override hashes a header, and the instance policy can be replaced" do
      sources =
        signed_source(store_headers: %{hash: ["Authorization"]}) |> Map.put("plain", [])

      config = start_edge(sources, store_headers: %{drop: ["x-keep"]})
      body = ~s({"event":"ok"})
      signed = standard_webhooks_headers("msg_1", body, @secret)

      # `swh` inherits the instance's `drop: ["x-keep"]` and hashes authorization
      assert {:ok, env} =
               Ingest.ingest(config.instance, request("swh", body, signed ++ @credentials))

      digest =
        "sha256:" <>
          Base.encode16(:crypto.hash(:sha256, "Bearer sk_live_topsecret"), case: :lower)

      assert Envelope.header(env, "authorization") == digest
      assert Envelope.header(env, "cookie") == "sid=topsecret-session"
      assert Envelope.header(env, "x-keep") == nil

      assert {:ok, [stored]} = Ankusa.Queue.hooks(config.instance, 0, 10)
      assert Envelope.header(stored, "authorization") == digest
      refute raw_hook_row(config.instance, stored) =~ "sk_live_topsecret"

      # a source with no override runs the instance policy as configured
      assert {:ok, plain} = Ingest.ingest(config.instance, request("plain", "x", @credentials))
      assert Envelope.header(plain, "authorization") == "Bearer sk_live_topsecret"
      assert Envelope.header(plain, "x-keep") == nil
    end

    test "the dedupe key is read before the header is dropped" do
      source = [dedupe: %{header: "x-api-key"}, verifier: {Ankusa.Verifier.None, []}]
      config = start_edge(%{"d" => source})

      req = request("d", "{}", [{"x-api-key", "evt-1"}])
      assert {:ok, first} = Ingest.ingest(config.instance, req)
      assert first.dedupe_key == "evt-1"
      assert Envelope.header(first, "x-api-key") == nil
      assert {:duplicate, %Envelope{id: id}} = Ingest.ingest(config.instance, req)
      assert id == first.id
    end

    test "a quarantined hook is held without the dropped headers" do
      config = start_edge(quarantining(["q"]))

      assert {:quarantined, _} =
               Ingest.ingest(config.instance, request("q", "forged", @credentials))

      # the held entry's body is the stored envelope
      assert {:ok, [%{id: id, received_at: received_at}]} =
               Ankusa.Edge.Quarantine.recent(config.instance, 10)

      assert {:ok, held} =
               Store.get(config.instance, :quarantine, Keys.quarantine_body(received_at, id))

      assert held =~ "x-keep"
      refute held =~ "topsecret"
      refute held =~ "authorization"
    end
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

  test "a hook the pen has no room for spends no token" do
    body = String.duplicate("x", 2_000)

    config =
      start_edge(quarantining(["q"]),
        quarantine: %{burst: 2, rate: 0.001, max_bytes: 3_000}
      )

    assert forged(config, "q", body).status == 202
    assert forged(config, "q", body).status == 503
    assert {:ok, %{deleted: 1}} = Ankusa.Edge.Quarantine.purge(config.instance, %{}, 10)

    # The refused hook gave its token back: the bucket still holds the second.
    assert forged(config, "q", body).status == 202
  end

  test "concurrent charges on one source's bucket never pass its burst" do
    config = start_edge(quarantining(["q"]), quarantine: %{burst: 50, rate: 0.001})

    results =
      1..200
      |> Task.async_stream(fn _ -> Ankusa.Edge.Quarantine.hit(config.instance, "q", nil) end,
        max_concurrency: 200
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &(&1 == :ok)) == 50
    assert Enum.all?(results -- List.duplicate(:ok, 50), &match?({:rate_limited, _}, &1))
  end

  test "a pen with 1 000 writes waiting refuses the next with 503, and counts nothing for it" do
    config =
      start_edge(quarantining(["q"]), quarantine: %{burst: 100_000, rate: 100_000})

    inst = config.instance
    assert {:ok, %{bytes: 0, pending: 0, entries: 0}} = Ankusa.Edge.Quarantine.held(inst)

    pen = Ankusa.whereis(inst, :quarantine)
    :ok = :sys.suspend(pen)
    on_exit(fn -> if Process.alive?(pen), do: :sys.resume(pen) end)

    waiting =
      for i <- 1..1_000 do
        Task.async(fn ->
          Ingest.ingest(inst, request("q", "forged-#{i}", [{"webhook-signature", "v1,nope"}]))
        end)
      end

    counters = :"ankusa_quarantine_counters_#{inst}"
    eventually(fn -> :ets.lookup_element(counters, :pending, 2) == 1_000 end)
    bytes_waiting = :ets.lookup_element(counters, :bytes, 2)

    refused = forged(config, "q")
    assert refused.status == 503
    assert JSON.decode!(refused.resp_body) == %{"error" => "overload"}
    assert Plug.Conn.get_resp_header(refused, "retry-after") == ["1"]
    assert :ets.lookup_element(counters, :bytes, 2) == bytes_waiting
    assert :ets.lookup_element(counters, :pending, 2) == 1_000

    :ok = :sys.resume(pen)
    assert Enum.all?(Task.await_many(waiting, 20_000), &match?({:quarantined, _}, &1))

    # Every waiting write landed, group-committed, and the count is exactly
    # their bytes.
    assert {:ok, %{bytes: bytes, entries: 1_000, pending: 0}} =
             Ankusa.Edge.Quarantine.held(inst)

    assert {:ok, entries} = Ankusa.Edge.Quarantine.recent(inst, 2_000)
    assert length(entries) == 1_000
    assert bytes == Enum.sum(Enum.map(entries, & &1.size))
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
    # One request, held at the stalled writer, fills the one place max_queue
    # allows, so the next request is shed at once.
    config =
      test_config(
        roles: [:edge],
        source_store:
          {Ankusa.SourceStore.Static, sources: %{"demo" => [sinks: [{Ankusa.Sink.Log, []}]]}},
        batcher: %{partitions: 1, max_queue: 1}
      )

    start_supervised!({Ankusa.Instance, config})
    writer = Ankusa.whereis(config.instance, :queue_writer)
    :ok = :sys.suspend(writer)
    held = Task.async(fn -> Ingest.ingest(config.instance, request("demo", "x")) end)
    eventually(fn -> Batcher.queued(config.instance, 0) == 1 end)

    assert {:error, :overload} = Ingest.ingest(config.instance, request("demo", "x"))

    conn = route_through_edge(config, request("demo", "x"))
    assert conn.status == 503
    assert Plug.Conn.get_resp_header(conn, "retry-after") == ["1"]

    :ok = :sys.resume(writer)
    assert {:ok, {:ok, _env}} = Task.yield(held, 10_000)
  end

  test "sheds with 503 once the bodies held pass max_queue_bytes, in flight included" do
    config =
      test_config(
        roles: [:edge],
        source_store:
          {Ankusa.SourceStore.Static, sources: %{"demo" => [sinks: [{Ankusa.Sink.Log, []}]]}},
        batcher: %{partitions: 1, max_queue_bytes: 10_000}
      )

    start_supervised!({Ankusa.Instance, config})
    inst = config.instance
    writer = Ankusa.whereis(inst, :queue_writer)
    :ok = :sys.suspend(writer)

    body = String.duplicate("x", 8_000)
    first = Task.async(fn -> Ingest.ingest(inst, request("demo", body)) end)

    # The first 8 KB body is the commit in flight (stuck on the writer)...
    eventually(fn -> Batcher.queued(inst, 0) == 1 end)

    # ...so a second one would hold 16 KB: refused, though one record is far
    # below `max_queue`.
    shed = route_through_edge(config, request("demo", body))
    assert shed.status == 503
    assert JSON.decode!(shed.resp_body) == %{"error" => "overload"}

    :ok = :sys.resume(writer)
    assert {:ok, _env} = Task.await(first, 30_000)

    # Once the commit is done the bytes are released.
    assert route_through_edge(config, request("demo", body)).status == 201
  end

  test "sheds with 503 once max_queue is reached while a commit is in flight" do
    # The queue writer is suspended, so the first commit blocks and the queue
    # fills behind it. Resuming it lets everything that was admitted commit.
    config =
      test_config(
        roles: [:edge],
        source_store:
          {Ankusa.SourceStore.Static, sources: %{"demo" => [sinks: [{Ankusa.Sink.Log, []}]]}},
        batcher: %{partitions: 1, max_queue: 4}
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
        # these tests stall the writer and watch what the calls waiting at it do
        batcher: Map.merge(%{partitions: 1, max_queue: 10_000}, batcher)
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

  @tag :capture_log
  test "a deadline that passes while the writer is stalled is refused afterwards; :infinity and a live one commit" do
    inst = start_stalled_edge()
    writer = Ankusa.whereis(inst, :queue_writer)
    :ok = :sys.suspend(writer)

    a = entry(1)
    b = entry(2)
    c = entry(3)
    task_a = commit_in_task(inst, a, :infinity)
    task_b = commit_in_task(inst, b, 10_000)
    task_c = commit_in_task(inst, c, 200)
    eventually(fn -> Process.info(writer, :message_queue_len) == {:message_queue_len, 3} end)
    assert Batcher.queued(inst, 0) == 3

    # C's deadline passes while the writer is stuck; A and B keep waiting.
    Process.sleep(400)
    assert Task.yield(task_c, 0) == nil

    :ok = :sys.resume(writer)
    assert {:error, :store_unavailable} = Task.await(task_c)
    assert {:committed, _} = Task.await(task_a)
    assert {:committed, _} = Task.await(task_b)
    eventually(fn -> Batcher.queued(inst, 0) == 0 end)

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
  test "a caller that dies while its call waits at the writer commits nothing" do
    inst = start_stalled_edge()
    writer = Ankusa.whereis(inst, :queue_writer)
    :ok = :sys.suspend(writer)

    a = entry(1)
    b = entry(2)
    task_a = commit_in_task(inst, a, 10_000)
    eventually(fn -> Process.info(writer, :message_queue_len) == {:message_queue_len, 1} end)
    task_b = commit_in_task(inst, b, 10_000)
    eventually(fn -> Process.info(writer, :message_queue_len) == {:message_queue_len, 2} end)

    Task.shutdown(task_a, :brutal_kill)

    :ok = :sys.resume(writer)
    assert {:committed, _} = Task.await(task_b)

    # A's caller was gone when the writer reached its call: nothing of it is stored.
    stored = stored_ids(inst)
    refute a.envelope.id in stored
    assert b.envelope.id in stored
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

  # The listener answers 408 on the way out or just closes: either way the
  # socket is gone within the timeout.
  defp assert_closed(sock) do
    case :gen_tcp.recv(sock, 0, 3_000) do
      {:ok, "HTTP/1.0 408" <> _} -> assert {:error, :closed} = :gen_tcp.recv(sock, 0, 3_000)
      {:error, :closed} -> :ok
      other -> flunk("expected the connection to be closed, got #{inspect(other)}")
    end
  end

  test "closes idle header read" do
    config =
      start_edge(%{"demo" => [verifier: {Ankusa.Verifier.None, []}]},
        port: free_port(),
        listener: %{read_timeout_ms: 200}
      )

    # A request head dribbled in and never finished.
    {:ok, slow} = :gen_tcp.connect({127, 0, 0, 1}, config.port, [:binary, active: false])
    :ok = :gen_tcp.send(slow, "POST /webhooks/demo HTTP/1.1\r\nHost: localhost\r\nX-Slow: ")
    assert_closed(slow)

    # A connection that never says anything at all.
    {:ok, silent} = :gen_tcp.connect({127, 0, 0, 1}, config.port, [:binary, active: false])
    assert_closed(silent)

    assert stored_ids(config.instance) == []
  end

  test "a body that has not arrived within request_deadline_ms is answered 408" do
    config =
      start_edge(%{"demo" => [verifier: {Ankusa.Verifier.None, []}]},
        port: free_port(),
        listener: %{request_deadline_ms: 300, body_read_timeout_ms: 10_000}
      )

    # 1 of 100 promised bytes: without the deadline the 10 s read timeout
    # would hold the connection past `raw_post`'s 2 s wait.
    assert {:ok, "HTTP/1.1 408" <> _} = raw_post(config.port, "/webhooks/demo", 100, "x")
    assert stored_ids(config.instance) == []
  end

  test "a keep-alive connection is closed after max_requests_per_connection" do
    config = start_edge(%{}, port: free_port(), listener: %{max_requests_per_connection: 2})
    {:ok, sock} = :gen_tcp.connect({127, 0, 0, 1}, config.port, [:binary, active: false])

    answered =
      Enum.count(1..5, fn _ ->
        with :ok <- :gen_tcp.send(sock, "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n"),
             {:ok, "HTTP/1.1 200" <> _} <- :gen_tcp.recv(sock, 0, 2_000) do
          true
        else
          _closed -> false
        end
      end)

    assert answered == 2
  end

  test "more headers than max_header_count are refused" do
    config =
      start_edge(%{"demo" => [verifier: {Ankusa.Verifier.None, []}]},
        port: free_port(),
        listener: %{max_header_count: 5}
      )

    headers = Enum.map(1..10, &"X-H#{&1}: v")
    assert {:ok, "HTTP/1.0 431" <> _} = raw_post(config.port, "/webhooks/demo", 1, "x", headers)
    assert stored_ids(config.instance) == []
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

  describe "GET /ready" do
    defp get_edge(config, path) do
      Ankusa.Edge.Router.call(
        Plug.Test.conn(:get, path),
        Ankusa.Edge.Router.init(instance: config.instance)
      )
    end

    test "is 200 while the store takes writes, 503 without it, 200 again once it is back" do
      config = start_edge(%{})
      name = to_string(config.instance)

      ready = get_edge(config, "/ready")
      assert ready.status == 200

      assert JSON.decode!(ready.resp_body) == %{
               "status" => "ready",
               "instance" => name,
               "store" => "ok"
             }

      sup = Ankusa.whereis(config.instance, :instance)
      :ok = Supervisor.terminate_child(sup, {Ankusa.Store, config.instance})

      down = get_edge(config, "/ready")
      assert down.status == 503
      assert Plug.Conn.get_resp_header(down, "retry-after") == ["1"]

      assert JSON.decode!(down.resp_body) == %{
               "status" => "unavailable",
               "instance" => name,
               "store" => "failing",
               "reasons" => %{"store" => "not_running"}
             }

      # Liveness is unaffected.
      assert get_edge(config, "/health").status == 200

      {:ok, _pid} = Supervisor.restart_child(sup, {Ankusa.Store, config.instance})
      assert get_edge(config, "/ready").status == 200
    end
  end

  defmodule DownStore do
    @behaviour Ankusa.SourceStore

    @impl true
    def fetch(_instance, _source_id), do: {:error, :unavailable}

    @impl true
    def list(_instance), do: []
  end

  test "a source store that cannot answer is a 503 the provider retries, not a 404" do
    config = test_config(roles: [:edge], source_store: {DownStore, []})
    start_supervised!({Ankusa.Instance, config})

    ref = make_ref()
    me = self()

    :telemetry.attach(
      "refused-#{inspect(ref)}",
      [:ankusa, :ingest, :refused],
      fn _event, _measurements, meta, _ -> send(me, {:refused, meta.reason}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach("refused-#{inspect(ref)}") end)

    conn = route_through_edge(config, request("demo", "{}"))

    assert conn.status == 503
    assert JSON.decode!(conn.resp_body) == %{"error" => "store_unavailable"}
    assert Plug.Conn.get_resp_header(conn, "retry-after") == ["1"]
    assert_receive {:refused, :store_unavailable}
    assert stored_ids(config.instance) == []
  end

  describe "tenant binding" do
    defp tenant_post(config, tenant, source_id, body \\ "{}", headers \\ []) do
      req = %{request(source_id, body, headers) | path: "/webhooks/#{tenant}/#{source_id}"}
      route_through_edge(config, req)
    end

    test "a tenant's source answers only under its own tenant; a shared one under any" do
      config =
        start_edge(
          %{
            "billing" => [verifier: {Ankusa.Verifier.None, []}, tenant_id: "acme"],
            "shared" => [verifier: {Ankusa.Verifier.None, []}, trust_url_tenant: true]
          },
          route_resolver: {Ankusa.RouteResolver.TenantPath, []},
          rate_limits: %{default: %{rate: 1_000, burst: 1_000}}
        )

      other = tenant_post(config, "globex", "billing")
      assert other.status == 404
      assert JSON.decode!(other.resp_body) == %{"error" => "unknown_source"}

      assert tenant_post(config, "acme", "billing").status == 201
      assert tenant_post(config, "acme", "shared").status == 201
      assert tenant_post(config, "globex", "shared").status == 201

      {:ok, envs} = Ankusa.Queue.hooks(config.instance, 0, 10)

      assert Enum.map(envs, &{&1.source_id, &1.tenant_id}) ==
               [{"billing", "acme"}, {"shared", "acme"}, {"shared", "globex"}]
    end

    test "flag-accepted forgeries spend the source's quarantine bucket, never the tenant's budget" do
      config =
        start_edge(
          %{
            "flag" => [
              verifier: {Ankusa.Verifier.Hmac, scheme: :standard_webhooks, secret: @secret},
              on_verify_failure: :accept_flag
            ]
          },
          quarantine: %{burst: 2, rate: 1},
          # One hook, then nothing for a very long time.
          rate_limits: %{default: %{rate: 0.001, burst: 1}}
        )

      assert forged(config, "flag").status == 201
      assert forged(config, "flag").status == 201

      limited = forged(config, "flag")
      assert limited.status == 429
      assert JSON.decode!(limited.resp_body) == %{"error" => "quarantine_rate_limited"}

      # The tenant's single token is still there for a genuine hook.
      body = ~s({"genuine":true})
      headers = standard_webhooks_headers("msg_genuine", body, @secret)
      assert route_through_edge(config, request("flag", body, headers)).status == 201
    end
  end

  describe "verifier breadth" do
    @tls_dir Path.expand("../fixtures/tls", __DIR__)

    defp cert_der(name) do
      [{:Certificate, der, _}] =
        @tls_dir |> Path.join(name) |> File.read!() |> :public_key.pem_decode()

      der
    end

    defp fingerprint(der), do: Base.encode16(:crypto.hash(:sha256, der), case: :lower)

    defp post(config, source_id, headers \\ [], path_suffix \\ "", extra \\ %{}) do
      req = %{
        request(source_id, ~s({"a":1}), headers)
        | path: "/webhooks/#{source_id}#{path_suffix}"
      }

      route_through_edge(config, Map.merge(req, extra))
    end

    defp stored_headers(config) do
      {:ok, hooks} = Ankusa.Queue.hooks(config.instance, 0, 10)
      Enum.flat_map(hooks, & &1.headers)
    end

    test "a token source: the right token is stored, a wrong or missing one is a 401, and the token is never stored" do
      config =
        start_edge(
          %{
            "bearer" => [verifier: {Ankusa.Verifier.Token, secret: "s3cret"}],
            "header" => [
              verifier:
                {Ankusa.Verifier.Token, secret: "s3cret", location: {:header, "x-hook-token"}}
            ],
            "query" => [
              verifier: {Ankusa.Verifier.Token, secret: "s3cret", location: {:query, "token"}}
            ]
          },
          # An instance policy that drops nothing: the token header is dropped anyway.
          store_headers: %{drop: []}
        )

      assert post(config, "bearer", [{"authorization", "Bearer s3cret"}, {"x-keep", "1"}]).status ==
               201

      assert post(config, "header", [{"x-hook-token", "s3cret"}, {"x-keep", "2"}]).status == 201
      assert post(config, "query", [{"x-keep", "3"}], "?token=s3cret").status == 201

      for {source, headers, suffix} <- [
            {"bearer", [{"authorization", "Bearer nope"}], ""},
            {"bearer", [], ""},
            {"header", [{"x-hook-token", "nope"}], ""},
            {"header", [{"authorization", "Bearer s3cret"}], ""},
            {"query", [], "?token=nope"},
            {"query", [{"x-hook-token", "s3cret"}], ""}
          ] do
        conn = post(config, source, headers, suffix)
        assert conn.status == 401, "#{source} #{inspect(headers)} #{suffix}"
        assert %{"error" => "verification_failed"} = JSON.decode!(conn.resp_body)
      end

      assert length(stored_ids(config.instance)) == 3

      stored = stored_headers(config)
      assert {"x-keep", "1"} in stored and {"x-keep", "2"} in stored and {"x-keep", "3"} in stored
      refute Enum.any?(stored, fn {name, _} -> name in ["authorization", "x-hook-token"] end)
      refute Enum.any?(stored, fn {_name, value} -> value =~ "s3cret" end)
    end

    test "an asymmetric source: a genuine Ed25519 signature is accepted; a wrong key, a stale timestamp and a tampered body are not" do
      {pub, priv} = :crypto.generate_key(:eddsa, :ed25519)
      {other_pub, _} = :crypto.generate_key(:eddsa, :ed25519)

      config =
        start_edge(%{
          "asym" => [
            verifier:
              {Ankusa.Verifier.Asymmetric,
               scheme: :standard_webhooks_asym, public_key: "whpk_" <> Base.encode64(pub)}
          ],
          "other" => [
            verifier:
              {Ankusa.Verifier.Asymmetric,
               scheme: :standard_webhooks_asym, public_key: "whpk_" <> Base.encode64(other_pub)}
          ]
        })

      body = ~s({"a":1})

      signed = fn ts, signed_body ->
        sig = :crypto.sign(:eddsa, :none, "msg_1.#{ts}.#{signed_body}", [priv, :ed25519])

        [
          {"webhook-id", "msg_1"},
          {"webhook-timestamp", to_string(ts)},
          {"webhook-signature", "v1a," <> Base.encode64(sig)}
        ]
      end

      now = System.system_time(:second)

      assert post(config, "asym", signed.(now, body)).status == 201
      assert post(config, "other", signed.(now, body)).status == 401
      assert post(config, "asym", signed.(now - 301, body)).status == 401
      assert post(config, "asym", signed.(now, ~s({"a":2}))).status == 401
      assert length(stored_ids(config.instance)) == 1
    end

    test "a client-certificate source rejects a request without the pinned certificate" do
      client = cert_der("client.pem")
      stranger = cert_der("stranger.pem")

      tls = [
        certfile: Path.join(@tls_dir, "server.pem"),
        keyfile: Path.join(@tls_dir, "server-key.pem"),
        cacertfile: Path.join(@tls_dir, "ca.pem"),
        verify: :verify_peer
      ]

      config =
        start_edge(
          %{
            "pinned" => [
              verifier: {Ankusa.Verifier.ClientCert, fingerprints: [fingerprint(client)]}
            ],
            "subject" => [
              verifier: {Ankusa.Verifier.ClientCert, subjects: ["cn:ankusa-test-client"]}
            ],
            "elsewhere" => [
              verifier: {Ankusa.Verifier.ClientCert, fingerprints: [String.duplicate("0", 64)]}
            ]
          },
          listener: %{tls: tls}
        )

      assert post(config, "pinned", [], "", %{peer_cert: client}).status == 201
      assert post(config, "subject", [], "", %{peer_cert: client}).status == 201

      # No certificate, another certificate, and a certificate pinned for a
      # different source: each a 401, nothing stored.
      assert post(config, "pinned").status == 401
      assert post(config, "pinned", [], "", %{peer_cert: stranger}).status == 401
      assert post(config, "subject", [], "", %{peer_cert: stranger}).status == 401
      assert post(config, "elsewhere", [], "", %{peer_cert: client}).status == 401

      # A header cannot stand in for the handshake.
      forged = [{"x-ssl-client-cert", Base.encode64(client)}, {"x-forwarded-client-cert", "x"}]
      assert post(config, "pinned", forged).status == 401

      assert length(stored_ids(config.instance)) == 2
    end
  end
end
