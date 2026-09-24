defmodule Ankusa.Admin.RouterTest do
  @moduledoc """
  Exercises `Ankusa.Admin.Router`'s HTTP semantics directly via
  `Plug.Test`/`Router.call`, the same pattern `Ankusa.ClaimCheck.RouterTest`
  uses — no real socket needed to prove status-code mapping, role gating, and
  redaction are correct.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.{Config, Envelope, UUIDv7}
  alias Ankusa.Admin.Router
  alias Ankusa.Dispatch.DLQ

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

  defp envelope(source_id, body, overrides \\ %{}) do
    struct(
      %Envelope{
        id: UUIDv7.generate(),
        source_id: source_id,
        tenant_id: "default",
        received_at: 1_737_500_000_000,
        method: "POST",
        path: "/webhooks/#{source_id}",
        headers: [],
        content_type: "application/json",
        body: body,
        size: byte_size(body),
        seq: 1
      },
      overrides
    )
  end

  # ── no auth ────────────────────────────────────────────────────────────────

  test "every route answers with no authorization header at all", %{inst: inst} do
    for path <- ["/health", "/v1/config", "/v1/dlq", "/v1/dlq?source_id=x"] do
      conn = call(inst, :get, path)
      assert conn.status == 200, "#{path} returned #{conn.status}"
    end

    conn = call(inst, :post, "/v1/dlq/replay", "{}")
    assert conn.status == 200
  end

  test "an unrouted path is 404", %{inst: inst} do
    conn = call(inst, :get, "/nope")
    assert conn.status == 404
    assert %{"error" => "not_found"} = JSON.decode!(conn.resp_body)
  end

  test "GET /health reports the instance and its roles", %{inst: inst} do
    assert %{"status" => "ok", "instance" => inst_name, "roles" => ["dispatch"]} =
             JSON.decode!(call(inst, :get, "/health").resp_body)

    assert inst_name == to_string(inst)
  end

  # ── role gating ────────────────────────────────────────────────────────────

  test "a route needing a role this node does not run is 409" do
    config = test_config(roles: [:edge], admin: %{enabled: true})
    put_config(config)

    conn = call(config.instance, :get, "/v1/dlq")

    assert conn.status == 409
    assert %{"error" => "role_not_enabled", "role" => "dispatch"} = JSON.decode!(conn.resp_body)
  end

  # ── DLQ ────────────────────────────────────────────────────────────────────

  test "GET /v1/dlq filters by source and never returns bodies", %{inst: inst, config: config} do
    a = envelope("a", ~s({"secret":"top-secret-payload"}))
    DLQ.write(config, a, {:sink, Ankusa.Sink.Http, {:status, 503}})
    DLQ.write(config, envelope("b", ~s({"id":"b1"})), :timeout)

    conn = call(inst, :get, "/v1/dlq?source_id=a")
    assert conn.status == 200

    assert %{"total" => 1, "entries" => [entry]} = JSON.decode!(conn.resp_body)
    assert entry["id"] == a.id
    assert entry["source_id"] == "a"
    assert entry["seq"] == 1
    assert entry["reason"] =~ "503"
    refute Map.has_key?(entry, "body")
    refute conn.resp_body =~ "top-secret-payload"
  end

  test "GET /v1/dlq lists newest first and caps limit", %{inst: inst, config: config} do
    old = envelope("a", "{}", %{received_at: 1})
    new = envelope("a", "{}", %{received_at: 2})
    DLQ.write(config, old, :first)
    DLQ.write(config, new, :second)

    assert %{"total" => 2, "entries" => [only]} =
             JSON.decode!(call(inst, :get, "/v1/dlq?limit=1").resp_body)

    assert only["id"] == new.id

    assert %{"total" => 2, "entries" => [first, second]} =
             JSON.decode!(call(inst, :get, "/v1/dlq").resp_body)

    assert first["id"] == new.id
    assert second["id"] == old.id
  end

  test "a non-integer since or limit is 400 invalid_filter", %{inst: inst} do
    assert %{"error" => "invalid_filter", "field" => "since"} =
             JSON.decode!(call(inst, :get, "/v1/dlq?since=tuesday").resp_body)

    conn = call(inst, :get, "/v1/dlq?limit=many")
    assert conn.status == 400
    assert %{"error" => "invalid_filter", "field" => "limit"} = JSON.decode!(conn.resp_body)
  end

  test "POST /v1/dlq/replay re-delivers the matching entry through its sink" do
    {:ok, capture} = Agent.start_link(fn -> [] end)
    test_pid = self()

    Req.Test.stub(__MODULE__, fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      Agent.update(capture, &[body | &1])
      send(test_pid, {:sink_called, conn.request_path})
      Plug.Conn.send_resp(conn, 200, "ok")
    end)

    config =
      test_config(
        roles: [:dispatch],
        admin: %{enabled: true},
        source_store:
          {Ankusa.SourceStore.Static,
           sources: %{
             "a" => [
               sinks: [
                 {Ankusa.Sink.Http,
                  url: "http://sink.test/hook", req_options: [plug: {Req.Test, __MODULE__}]}
               ]
             ]
           }}
      )

    put_config(config)

    target = envelope("a", ~s({"id":"evt_replay"}))
    DLQ.write(config, target, {:sink, Ankusa.Sink.Http, {:status, 503}})
    DLQ.write(config, envelope("b", ~s({"id":"evt_other"})), :timeout)

    conn = call(config.instance, :post, "/v1/dlq/replay", JSON.encode!(%{"id" => target.id}))

    assert conn.status == 200
    assert %{"replayed" => 1} = JSON.decode!(conn.resp_body)
    assert Agent.get(capture, & &1) == [target.body]
  end

  test "an empty replay body replays everything", %{inst: inst, config: config} do
    DLQ.write(config, envelope("a", ~s({"id":"1"})), :timeout)
    DLQ.write(config, envelope("b", ~s({"id":"2"})), :timeout)

    conn = call(inst, :post, "/v1/dlq/replay", "")
    assert conn.status == 200
    assert %{"replayed" => 2} = JSON.decode!(conn.resp_body)

    conn = call(inst, :post, "/v1/dlq/replay", "not json")
    assert conn.status == 400
    assert %{"error" => "invalid_filter", "field" => "body"} = JSON.decode!(conn.resp_body)
  end

  # ── quarantine ─────────────────────────────────────────────────────────────

  test "GET /v1/quarantine lists this node's recent entries without bodies" do
    sources = %{
      "strict" => [
        verifier: {Ankusa.Verifier.Stripe, secret: "whsec_x"},
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

  # ── config redaction ───────────────────────────────────────────────────────

  test "GET /v1/config redacts a verifier secret and a URL password" do
    config =
      test_config(
        roles: [:edge],
        admin: %{enabled: true},
        wal: {Ankusa.WAL.Postgres, url: "postgres://ankusa:leakhunter@db:5432/ankusa"},
        source_store:
          {Ankusa.SourceStore.Static,
           sources: %{
             "stripe" => [verifier: {Ankusa.Verifier.Stripe, secret: "whsec_leakhunter"}]
           }}
      )

    put_config(config)

    conn = call(config.instance, :get, "/v1/config")
    assert conn.status == 200

    refute conn.resp_body =~ "leakhunter"
    assert conn.resp_body =~ "postgres://ankusa:[REDACTED]@db:5432/ankusa"
    # sources, module pairs, and booleans all survive as themselves
    assert conn.resp_body =~ "stripe"
    assert conn.resp_body =~ ~s("module":"Ankusa.Verifier.Stripe")

    decoded = JSON.decode!(conn.resp_body)
    assert decoded["admin"]["enabled"] == true

    assert decoded["source_store"]["opts"]["sources"]["stripe"]["verifier"]["opts"]["secret"] ==
             "[REDACTED]"
  end

  test "Config.new/1 rejects an unknown admin key like any other section" do
    assert_raise ArgumentError, ~r/unknown Ankusa.Config key: admin.tokens/, fn ->
      Config.new(admin: %{enabled: true, tokens: ["nope"]})
    end
  end
end
