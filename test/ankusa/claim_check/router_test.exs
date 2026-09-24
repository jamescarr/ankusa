defmodule Ankusa.ClaimCheck.RouterTest do
  @moduledoc """
  Exercises `Ankusa.ClaimCheck.Router`'s HTTP semantics directly via
  `Plug.Test`/`Router.call`, the same pattern `Ankusa.EdgeTest` uses for
  `Ankusa.Edge.Router` — no real socket needed to prove request handling,
  auth, and status-code mapping are correct. The real-wire cross-mode proof
  (Direct check-in redeemed over an actual HTTP hop via `Remote`, and back)
  lives in `Ankusa.ClaimCheck.CrossModeTest`.
  """

  use ExUnit.Case, async: false

  alias Ankusa.{Config, UUIDv7}
  alias Ankusa.ClaimCheck.Router

  setup do
    inst = :"ccr#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "ankusa_#{inst}")
    on_exit(fn -> File.rm_rf(dir) end)

    config =
      Config.new(
        instance: inst,
        data_dir: dir,
        roles: [:claim_check],
        claim_check: %{api_tokens: %{"secret" => :all, "scoped" => ["acme"]}}
      )

    Ankusa.put_config(config)
    %{inst: inst, config: config}
  end

  defp call(inst, method, path, body \\ "", headers \\ []) do
    conn =
      Plug.Test.conn(method, path, body)
      |> then(fn c ->
        Enum.reduce(headers, c, fn {k, v}, c -> Plug.Conn.put_req_header(c, k, v) end)
      end)

    Router.call(conn, Router.init(instance: inst))
  end

  defp auth(token), do: [{"authorization", "Bearer #{token}"}]

  test "GET /health is unauthenticated", %{inst: inst} do
    conn = call(inst, :get, "/health")
    assert conn.status == 200
    assert %{"status" => "ok"} = JSON.decode!(conn.resp_body)
  end

  test "PUT then GET round-trips the exact bytes with a full-scope token", %{inst: inst} do
    id = UUIDv7.generate()
    body = :crypto.strong_rand_bytes(1024)

    put_conn =
      call(
        inst,
        :put,
        "/v1/claims/acme/#{id}",
        body,
        auth("secret") ++ [{"content-type", "application/json"}]
      )

    assert put_conn.status == 201
    assert %{"ticket" => ticket} = JSON.decode!(put_conn.resp_body)
    assert ticket["tenant_id"] == "acme"
    assert ticket["id"] == id
    assert ticket["size"] == byte_size(body)
    assert ticket["content_type"] == "application/json"

    get_conn = call(inst, :get, "/v1/claims/acme/#{id}", "", auth("secret"))
    assert get_conn.status == 200
    assert get_conn.resp_body == body
    assert Plug.Conn.get_resp_header(get_conn, "content-type") == ["application/octet-stream"]
  end

  test "PUT without an authorization header is 401", %{inst: inst} do
    conn = call(inst, :put, "/v1/claims/acme/#{UUIDv7.generate()}", "x")
    assert conn.status == 401
    assert %{"error" => "unauthorized"} = JSON.decode!(conn.resp_body)
  end

  test "PUT with an unknown token is 401", %{inst: inst} do
    conn = call(inst, :put, "/v1/claims/acme/#{UUIDv7.generate()}", "x", auth("not-a-real-token"))
    assert conn.status == 401
  end

  test "a token scoped to another tenant gets 403 for this tenant", %{inst: inst} do
    conn = call(inst, :put, "/v1/claims/globex/#{UUIDv7.generate()}", "x", auth("scoped"))
    assert conn.status == 403
    assert %{"error" => "forbidden_tenant"} = JSON.decode!(conn.resp_body)
  end

  test "GET on a claim that was never checked in is 404", %{inst: inst} do
    conn = call(inst, :get, "/v1/claims/acme/#{UUIDv7.generate()}", "", auth("secret"))
    assert conn.status == 404
    assert %{"error" => "not_found"} = JSON.decode!(conn.resp_body)
  end

  test "a non-UUIDv7 id in the path is 400 invalid_id", %{inst: inst} do
    conn = call(inst, :put, "/v1/claims/acme/not-a-uuid", "x", auth("secret"))
    assert conn.status == 400
    assert %{"error" => "invalid_id"} = JSON.decode!(conn.resp_body)
  end

  test "a body over claim_check.max_bytes is 413", %{inst: inst, config: config} do
    Ankusa.put_config(%{config | claim_check: %{config.claim_check | max_bytes: 10}})

    conn =
      call(
        inst,
        :put,
        "/v1/claims/acme/#{UUIDv7.generate()}",
        String.duplicate("x", 11),
        auth("secret")
      )

    assert conn.status == 413
  end

  test "a mismatched x-ankusa-sha256 header is 422 integrity_mismatch", %{inst: inst} do
    headers = auth("secret") ++ [{"x-ankusa-sha256", String.duplicate("0", 64)}]
    conn = call(inst, :put, "/v1/claims/acme/#{UUIDv7.generate()}", "actual body", headers)
    assert conn.status == 422
    assert %{"error" => "integrity_mismatch"} = JSON.decode!(conn.resp_body)
  end

  test "an unrouted path is 404", %{inst: inst} do
    conn = call(inst, :get, "/nope")
    assert conn.status == 404
  end

  test "with no api_tokens configured the gateway is open: no header needed", %{
    inst: inst,
    config: config
  } do
    Ankusa.put_config(%{config | claim_check: %{config.claim_check | api_tokens: %{}}})

    id = UUIDv7.generate()
    body = :crypto.strong_rand_bytes(64)

    put_conn = call(inst, :put, "/v1/claims/acme/#{id}", body)
    assert put_conn.status == 201

    get_conn = call(inst, :get, "/v1/claims/acme/#{id}")
    assert get_conn.status == 200
    assert get_conn.resp_body == body
  end
end
