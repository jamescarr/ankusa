defmodule Hook.EdgeTest do
  use ExUnit.Case, async: false

  import Hook.TestHelpers
  alias Hook.Edge.{Ingest, Router}
  alias Hook.WAL

  @secret "whsec_" <> Base.encode64("supersecret-key")

  defp start_edge(sources) do
    config =
      test_config(roles: [:edge], source_store: {Hook.SourceStore.Static, sources: sources})

    start_supervised!({Hook.Instance, config})
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
    config = start_edge(%{"demo" => [verifier: {Hook.Verifier.None, []}]})
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
      "stripe" => [verifier: {Hook.Verifier.None, []}, dedup: {Hook.DedupKey.Rules, json: ["id"]}]
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
        verifier: {Hook.Verifier.StandardWebhooks, secret: @secret},
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
        verifier: {Hook.Verifier.StandardWebhooks, secret: @secret},
        on_verify_failure: :quarantine
      ]
    }

    config = start_edge(sources)
    conn = route(config, request("q", "forged", [{"webhook-signature", "v1,nope"}]))

    assert conn.status == 202
    assert %{"status" => "quarantined"} = JSON.decode!(conn.resp_body)
    assert WAL.stats(config.instance).records == 0
    assert [entry] = Hook.Edge.Quarantine.recent(config.instance)
    assert entry.source_id == "q"
  end

  test "load shed: a full batcher queue returns 503 with Retry-After" do
    # a batcher whose queue is full replies :overload immediately
    config =
      test_config(
        roles: [:edge],
        source_store: {Hook.SourceStore.Static, sources: %{"demo" => []}},
        batcher: %{partitions: 1, max_batch: 100_000, max_delay_ms: 60_000, max_queue: 0}
      )

    start_supervised!({Hook.Instance, config})

    # max_queue: 0 sheds every request
    assert {:error, :overload} = Ingest.ingest(config.instance, request("demo", "x"))

    conn = route(config, request("demo", "x"))
    assert conn.status == 503
    assert Plug.Conn.get_resp_header(conn, "retry-after") == ["1"]
  end

  test "oversize payload is refused with 413" do
    config =
      test_config(
        roles: [:edge],
        max_body_bytes: 16,
        source_store: {Hook.SourceStore.Static, sources: %{"demo" => []}}
      )

    start_supervised!({Hook.Instance, config})
    conn = route(config, request("demo", String.duplicate("x", 100)))
    assert conn.status == 413
  end
end
