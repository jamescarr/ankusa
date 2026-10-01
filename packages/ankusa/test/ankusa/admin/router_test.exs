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

  test "GET /health reports the instance, its roles, and the ankusa version", %{inst: inst} do
    assert %{"status" => "ok", "instance" => inst_name, "roles" => ["dispatch"], "version" => vsn} =
             JSON.decode!(call(inst, :get, "/health").resp_body)

    assert inst_name == to_string(inst)
    assert is_binary(vsn) and vsn != ""
    assert vsn == to_string(Application.spec(:ankusa, :vsn))
  end

  # ── WAL ────────────────────────────────────────────────────────────────────

  test "GET /v1/wal reports this node's log, 409 when this node has none" do
    config = test_config(roles: [:edge], admin: %{enabled: true})
    put_config(config)
    start_supervised!({Ankusa.Instance, config})

    assert %{"instance" => inst_name, "wal" => wal} =
             JSON.decode!(call(config.instance, :get, "/v1/wal").resp_body)

    assert inst_name == to_string(config.instance)
    assert %{"records" => _, "next_seq" => _, "cursors" => _} = wal

    none = test_config(roles: [:edge], admin: %{enabled: true}, wal: :none)
    put_config(none)

    conn = call(none.instance, :get, "/v1/wal")
    assert conn.status == 409
    assert %{"error" => "wal_disabled"} = JSON.decode!(conn.resp_body)
  end

  # ── role gating ────────────────────────────────────────────────────────────

  test "a route needing a role this node does not run is 409" do
    config = test_config(roles: [:edge], admin: %{enabled: true})
    put_config(config)

    conn = call(config.instance, :get, "/v1/dlq")

    assert conn.status == 409
    assert %{"error" => "role_not_enabled", "role" => "dispatch"} = JSON.decode!(conn.resp_body)
  end

  test "the rate-limit routes are edge-only, like the quarantine list", %{inst: inst} do
    conn = call(inst, :get, "/v1/rate-limits")

    assert conn.status == 409
    assert %{"error" => "role_not_enabled", "role" => "edge"} = JSON.decode!(conn.resp_body)
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

  test "GET /v1/dlq returns the newest write first, up to limit", %{inst: inst, config: config} do
    old = envelope("a", "{}")
    new = envelope("a", "{}")
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

  test "GET /v1/dlq clamps limit to 1000", %{inst: inst, config: config} do
    for _ <- 1..1001, do: DLQ.write(config, envelope("a", "{}"), :timeout)

    assert %{"total" => 1001, "entries" => entries} =
             JSON.decode!(call(inst, :get, "/v1/dlq?limit=5000").resp_body)

    assert length(entries) == 1000
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

    Req.Test.stub(__MODULE__, fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      Agent.update(capture, &[body | &1])
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

    test "an override the node cannot persist is 503 and changes nothing", %{
      inst: inst,
      config: config
    } do
      # A directory where `rate_limits.json` belongs: the write cannot land, so
      # the override is refused rather than applied in memory only.
      File.mkdir_p!(Ankusa.Config.path(config, "rate_limits.json"))

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
