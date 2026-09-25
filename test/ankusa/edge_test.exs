defmodule Ankusa.EdgeTest do
  use ExUnit.Case, async: false

  import Ankusa.TestHelpers
  alias Ankusa.Edge.{Ingest, Router}
  alias Ankusa.WAL

  @secret "whsec_" <> Base.encode64("supersecret-key")

  defp start_edge(sources) do
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
    assert %{"status" => "accepted", "id" => id, "seq" => 1} = JSON.decode!(conn.resp_body)
    # durably readable straight after the ack
    assert [env] = WAL.read(config.instance, -1, 10)
    assert env.id == id
    assert env.body == ~s({"hello":"world"})
  end

  test "unknown source returns 404 and stores nothing" do
    config = start_edge(%{})
    conn = route(config, request("nope", "x"))
    assert conn.status == 404
    assert WAL.stats(config.instance).records == 0
  end

  test "idempotent: a repeated dedup key returns 200 duplicate and is stored once" do
    sources = %{
      "stripe" => [
        verifier: {Ankusa.Verifier.None, []},
        dedup: {Ankusa.DedupKey.Rules, json: ["id"]}
      ]
    }

    config = start_edge(sources)

    body = ~s({"id":"evt_123","type":"x"})
    first = route(config, request("stripe", body))
    second = route(config, request("stripe", body))

    assert first.status == 201
    assert second.status == 200
    assert %{"status" => "duplicate"} = JSON.decode!(second.resp_body)
    assert WAL.stats(config.instance).records == 1
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

    # only the verified hook made it to the WAL
    assert WAL.stats(config.instance).records == 1
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
    assert WAL.stats(config.instance).records == 0
    assert [entry] = Ankusa.Edge.Quarantine.recent(config.instance)
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
    # A WAL whose commit takes long enough that the queue fills behind it. Each
    # commit also makes progress (2 records leave the queue per 300 ms), so the
    # queue cannot simply fill once and stay full.
    defmodule SlowWAL do
      @behaviour Ankusa.WAL

      def child_spec(opts), do: Ankusa.WAL.DiskLog.child_spec(opts)

      def start_link(opts), do: Ankusa.WAL.DiskLog.start_link(opts)

      @impl Ankusa.WAL
      def append(server, records) do
        Process.sleep(300)
        Ankusa.WAL.DiskLog.append(server, records)
      end

      @impl Ankusa.WAL
      def read(server, after_seq, limit), do: Ankusa.WAL.DiskLog.read(server, after_seq, limit)

      @impl Ankusa.WAL
      def get_cursor(server, name), do: Ankusa.WAL.DiskLog.get_cursor(server, name)

      @impl Ankusa.WAL
      def put_cursor(server, name, seq, token),
        do: Ankusa.WAL.DiskLog.put_cursor(server, name, seq, token)

      @impl Ankusa.WAL
      def truncate_through(server, seq, token),
        do: Ankusa.WAL.DiskLog.truncate_through(server, seq, token)

      @impl Ankusa.WAL
      def stats(server), do: Ankusa.WAL.DiskLog.stats(server)

      @impl Ankusa.WAL
      def acquire_lease(server, name, holder, ttl_ms),
        do: Ankusa.WAL.DiskLog.acquire_lease(server, name, holder, ttl_ms)

      @impl Ankusa.WAL
      def renew_lease(server, lease), do: Ankusa.WAL.DiskLog.renew_lease(server, lease)

      @impl Ankusa.WAL
      def release_lease(server, lease), do: Ankusa.WAL.DiskLog.release_lease(server, lease)
    end

    config =
      test_config(
        roles: [:edge],
        wal: {SlowWAL, []},
        source_store: {Ankusa.SourceStore.Static, sources: %{"demo" => []}},
        batcher: %{partitions: 1, max_batch: 2, max_queue: 4, max_delay_ms: 0}
      )

    start_supervised!({Ankusa.Instance, config})
    inst = config.instance

    results =
      1..20
      |> Enum.map(fn _ -> Task.async(fn -> Ingest.ingest(inst, request("demo", "x")) end) end)
      |> Enum.map(&Task.await(&1, 30_000))

    overloads = Enum.count(results, &(&1 == {:error, :overload}))
    committed = for {:ok, env} <- results, do: env

    # The bound has to bite: with 20 concurrent callers and a queue that holds
    # 4, most of them never get in.
    assert overloads >= 10
    assert length(committed) + overloads == 20

    # ...and everything that *was* acked is durably in the WAL.
    in_wal = inst |> WAL.read(-1, 100) |> MapSet.new(& &1.id)
    assert Enum.all?(committed, &MapSet.member?(in_wal, &1.id))
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
