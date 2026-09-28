defmodule Ankusa.ClaimCheck.RouterTest do
  @moduledoc """
  Exercises `Ankusa.ClaimCheck.Router`'s HTTP semantics directly via
  `Plug.Test`/`Router.call`, the same pattern `Ankusa.EdgeTest` uses for
  `Ankusa.Edge.Router` — no real socket needed to prove request handling and
  status-code mapping.
  """

  use ExUnit.Case, async: false

  alias Ankusa.{ClaimCheck, Config, UUIDv7}
  alias Ankusa.ClaimCheck.{Ref, Router}

  setup do
    inst = :"ccr#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "ankusa_#{inst}")
    on_exit(fn -> File.rm_rf(dir) end)

    config = Config.new(instance: inst, data_dir: dir, roles: [:claim_check])
    Ankusa.put_config(config)
    %{inst: inst, config: config}
  end

  defp call(inst, method, path, body \\ "") do
    Plug.Test.conn(method, path, body) |> Router.call(Router.init(instance: inst))
  end

  defp check_in(inst, body) do
    id = UUIDv7.generate()

    {:ok, claims} =
      ClaimCheck.check_in(inst, "acme", [%{id: "other", body: "x"}, %{id: id, body: body}])

    claims[id].ref
  end

  defp error(conn), do: JSON.decode!(conn.resp_body)["error"]

  test "GET /health", %{inst: inst} do
    conn = call(inst, :get, "/health")
    assert conn.status == 200
    assert %{"status" => "ok"} = JSON.decode!(conn.resp_body)
  end

  test "GET on a ref's path returns exactly its bytes, cacheable forever", %{inst: inst} do
    body = :crypto.strong_rand_bytes(4096)
    ref = check_in(inst, body)

    conn = call(inst, :get, Ref.path(ref))

    assert conn.status == 200
    assert conn.resp_body == body
    assert Plug.Conn.get_resp_header(conn, "content-type") == ["application/octet-stream"]

    assert Plug.Conn.get_resp_header(conn, "cache-control") == [
             "public, max-age=31536000, immutable"
           ]
  end

  test "a malformed tenant or claim id is 400", %{inst: inst} do
    claim_id = Ref.claim_id(Ref.new_pack_id(), 0)

    assert error(call(inst, :get, "/v1/claims/ac%25me/#{claim_id}")) == "invalid_tenant"

    for bad <- [UUIDv7.generate(), String.downcase(claim_id), "7ZZZZZZZZZZZZZZZZZZZZZZZZZ"] do
      conn = call(inst, :get, "/v1/claims/acme/#{bad}")
      assert {conn.status, error(conn)} == {400, "invalid_id"}, "id #{bad}"
    end
  end

  test "a pack that was never written is 404", %{inst: inst} do
    conn = call(inst, :get, "/v1/claims/acme/#{Ref.claim_id(Ref.new_pack_id(), 0)}")
    assert {conn.status, error(conn)} == {404, "not_found"}
  end

  test "a claim id past the end of a real pack is 404", %{inst: inst} do
    ref = check_in(inst, "small")
    {:ok, pack_id, 1} = Ref.locate(ref.claim_id)

    for index <- [2, 0xFFFF] do
      conn = call(inst, :get, "/v1/claims/acme/#{Ref.claim_id(pack_id, index)}")
      assert {conn.status, error(conn)} == {404, "not_found"}, "index #{index}"
    end
  end

  test "the path is exactly two segments", %{inst: inst} do
    ref = check_in(inst, "small")

    for path <- [Ref.path(ref) <> "/0/5", "/v1/claims/#{ref.claim_id}"] do
      assert call(inst, :get, path).status == 404, path
    end
  end

  test "there is no write route: PUT is 404", %{inst: inst} do
    conn = call(inst, :put, "/v1/claims/acme/#{Ref.claim_id(Ref.new_pack_id(), 0)}", "body")
    assert conn.status == 404
  end

  test "a store that can't be reached is 503 with Retry-After", %{inst: inst, config: config} do
    Ankusa.put_config(%{
      config
      | storage: %{config.storage | blob_store: {__MODULE__.DownStore, []}}
    })

    conn = call(inst, :get, "/v1/claims/acme/#{Ref.claim_id(Ref.new_pack_id(), 0)}")

    assert {conn.status, error(conn)} == {503, "store_unavailable"}
    assert Plug.Conn.get_resp_header(conn, "retry-after") == ["1"]
  end

  defmodule DownStore do
    @behaviour Ankusa.BlobStore
    @impl true
    def put(_, _, _, _), do: {:error, :econnrefused}
    @impl true
    def get(_, _, _), do: {:error, :econnrefused}
    @impl true
    def get_range(_, _, _, _, _), do: {:error, :econnrefused}
    @impl true
    def delete(_, _, _), do: :ok
    @impl true
    def list(_, _, _), do: []
  end
end
