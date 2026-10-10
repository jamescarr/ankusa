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
    dir = Ankusa.TestHelpers.unique_data_dir(inst)
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

  test "GET on a ref's path returns exactly its bytes, privately cacheable forever", %{inst: inst} do
    body = :crypto.strong_rand_bytes(4096)
    ref = check_in(inst, body)

    conn = call(inst, :get, Ref.path(ref))

    assert conn.status == 200
    assert conn.resp_body == body
    assert Plug.Conn.get_resp_header(conn, "content-type") == ["application/octet-stream"]

    assert Plug.Conn.get_resp_header(conn, "cache-control") == [
             "private, max-age=31536000, immutable"
           ]
  end

  test "HEAD answers the size without the body", %{inst: inst} do
    body = :crypto.strong_rand_bytes(4096)
    ref = check_in(inst, body)

    conn = call(inst, :head, Ref.path(ref))

    assert conn.status == 200
    assert conn.resp_body == ""
    assert Plug.Conn.get_resp_header(conn, "content-length") == ["4096"]
  end

  defp get_range(inst, path, range) do
    Plug.Test.conn(:get, path)
    |> Plug.Conn.put_req_header("range", range)
    |> Router.call(Router.init(instance: inst))
  end

  defp header(conn, name), do: Plug.Conn.get_resp_header(conn, name)

  describe "Range" do
    setup %{inst: inst} do
      body = :crypto.strong_rand_bytes(4096)
      %{body: body, path: Ref.path(check_in(inst, body))}
    end

    test "bytes=a-b is 206 with exactly those bytes", %{inst: inst, body: body, path: path} do
      conn = get_range(inst, path, "bytes=10-13")

      assert conn.status == 206
      assert conn.resp_body == binary_part(body, 10, 4)
      assert header(conn, "content-range") == ["bytes 10-13/4096"]
      assert header(conn, "accept-ranges") == ["bytes"]
      assert header(conn, "cache-control") == ["private, max-age=31536000, immutable"]
    end

    test "an open or overlong end runs to the claim's last byte", %{
      inst: inst,
      body: body,
      path: path
    } do
      for range <- ["bytes=4000-", "bytes=4000-99999"] do
        conn = get_range(inst, path, range)

        assert conn.status == 206
        assert conn.resp_body == binary_part(body, 4000, 96)
        assert header(conn, "content-range") == ["bytes 4000-4095/4096"]
      end
    end

    test "bytes=-n is the last n bytes, all of it when n exceeds the claim", %{
      inst: inst,
      body: body,
      path: path
    } do
      conn = get_range(inst, path, "bytes=-100")
      assert conn.status == 206
      assert conn.resp_body == binary_part(body, 3996, 100)
      assert header(conn, "content-range") == ["bytes 3996-4095/4096"]

      conn = get_range(inst, path, "bytes=-5000")
      assert conn.status == 206
      assert conn.resp_body == body
      assert header(conn, "content-range") == ["bytes 0-4095/4096"]
    end

    test "a range past the end, or bytes=-0, is 416 with the claim's length", %{
      inst: inst,
      path: path
    } do
      for range <- ["bytes=5000-", "bytes=4096-4100", "bytes=-0"] do
        conn = get_range(inst, path, range)

        assert conn.status == 416, "#{range} returned #{conn.status}"
        assert conn.resp_body == ""
        assert header(conn, "content-range") == ["bytes */4096"]
        assert header(conn, "accept-ranges") == ["bytes"]
      end
    end

    test "several ranges, another unit, or a malformed spec is ignored: 200 whole", %{
      inst: inst,
      body: body,
      path: path
    } do
      for range <- ["bytes=0-1,3-4", "items=0-1", "bytes=5-3", "bytes=-", "bytes=a-b", "bytes"] do
        conn = get_range(inst, path, range)

        assert conn.status == 200, "#{range} returned #{conn.status}"
        assert conn.resp_body == body
        assert header(conn, "content-range") == []
        assert header(conn, "accept-ranges") == ["bytes"]
      end
    end

    test "an If-Range makes the request whole: claims carry no validator", %{
      inst: inst,
      body: body,
      path: path
    } do
      conn =
        Plug.Test.conn(:get, path)
        |> Plug.Conn.put_req_header("range", "bytes=10-13")
        |> Plug.Conn.put_req_header("if-range", ~s("abc"))
        |> Router.call(Router.init(instance: inst))

      assert conn.status == 200
      assert conn.resp_body == body
    end

    test "HEAD ignores Range and answers the whole length", %{inst: inst, path: path} do
      conn =
        Plug.Test.conn(:head, path)
        |> Plug.Conn.put_req_header("range", "bytes=10-13")
        |> Router.call(Router.init(instance: inst))

      assert conn.status == 200
      assert conn.resp_body == ""
      assert header(conn, "content-length") == ["4096"]
      assert header(conn, "accept-ranges") == ["bytes"]
      assert header(conn, "content-range") == []
    end

    test "a range of a missing claim is still 404", %{inst: inst} do
      path = "/v1/claims/acme/#{Ref.claim_id(Ref.new_pack_id(), 0)}"
      conn = get_range(inst, path, "bytes=0-1")

      assert conn.status == 404
      assert error(conn) == "not_found"
    end
  end

  test "a store that refuses the credential is 503 store_forbidden, its body kept out", %{
    inst: inst,
    config: config
  } do
    Ankusa.put_config(%{
      config
      | storage: %{config.storage | blob_store: {__MODULE__.ForbiddenStore, []}}
    })

    conn = call(inst, :get, "/v1/claims/acme/#{Ref.claim_id(Ref.new_pack_id(), 0)}")

    assert {conn.status, JSON.decode!(conn.resp_body)} == {503, %{"error" => "store_forbidden"}}
    assert Plug.Conn.get_resp_header(conn, "retry-after") == ["60"]
    refute conn.resp_body =~ "AccessDenied"
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

    assert {conn.status, JSON.decode!(conn.resp_body)} == {503, %{"error" => "store_unavailable"}}
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
    def list(_, _, _), do: {:ok, []}
  end

  defmodule ForbiddenStore do
    @behaviour Ankusa.BlobStore
    @impl true
    def put(_, _, _, _), do: {:error, {:status, 403, "<Error><Code>AccessDenied</Code></Error>"}}
    @impl true
    def get(_, _, _), do: {:error, {:status, 403, "<Error><Code>AccessDenied</Code></Error>"}}
    @impl true
    def get_range(_, _, _, _, _),
      do: {:error, {:status, 403, "<Error><Code>AccessDenied</Code></Error>"}}

    @impl true
    def delete(_, _, _), do: :ok
    @impl true
    def list(_, _, _), do: {:ok, []}
  end
end
