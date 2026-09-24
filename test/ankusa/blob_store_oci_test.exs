defmodule Ankusa.BlobStore.OCITest do
  @moduledoc """
  Request plumbing for `Ankusa.BlobStore.OCI`, captured with `Req.Test`:
  URL construction (RFC 3986 path encoding, namespace/bucket segments),
  `get_range` byte windows, `list` JSON parsing, and the `:not_found`
  mapping. Signing itself is covered separately in
  `test/ankusa/blob_store_oci_signing_test.exs`.
  """

  use ExUnit.Case, async: true

  alias Ankusa.BlobStore.OCI
  alias Ankusa.Test.OCIKey, as: Key

  @opts [
    region: "us-ashburn-1",
    namespace: "ns",
    bucket: "b",
    tenancy_ocid: "ocid1.tenancy.oc1..t",
    user_ocid: "ocid1.user.oc1..u",
    key_fingerprint: "aa:bb:cc",
    private_key: nil,
    endpoint: "http://objectstorage.test"
  ]

  @list_json ~s({"objects":[{"name":"seg/b"},{"name":"seg/a"}],"prefixes":[],"nextStartWith":null})

  setup do
    {:ok, capture} = Agent.start_link(fn -> [] end)

    Req.Test.stub(__MODULE__, fn conn ->
      {:ok, _body, conn} = Plug.Conn.read_body(conn)

      Agent.update(capture, fn seen ->
        [{conn.method, conn.request_path, conn.query_string, conn.req_headers} | seen]
      end)

      cond do
        String.contains?(conn.request_path, "missing") ->
          Plug.Conn.send_resp(conn, 404, ~s({"code":"ObjectNotFound"}))

        String.contains?(conn.request_path, "boom") ->
          Plug.Conn.send_resp(conn, 500, "kaboom")

        String.contains?(conn.query_string, "prefix=seg") ->
          Plug.Conn.send_resp(conn, 200, @list_json)

        true ->
          Plug.Conn.send_resp(conn, 200, "ok")
      end
    end)

    opts = Keyword.put(@opts, :private_key, Key.private_key_pem())
    %{capture: capture, opts: opts ++ [req_options: [plug: {Req.Test, __MODULE__}]]}
  end

  test "put addresses the object under /n/{namespace}/b/{bucket}/o/", %{
    capture: capture,
    opts: opts
  } do
    body = :crypto.strong_rand_bytes(16)
    assert :ok = OCI.put(:i, "seg/a b+c.seg", body, opts)

    assert [{"PUT", path, _query, _headers}] = Agent.get(capture, & &1)
    # space → %20, plus → %2B, `/` kept as a separator
    assert path == "/n/ns/b/b/o/seg/a%20b%2Bc.seg"
  end

  test "get_range asks for exactly the byte window", %{capture: capture, opts: opts} do
    assert {:ok, "ok"} = OCI.get_range(:i, "seg/x.seg", 10, 4, opts)

    assert [{"GET", path, _query, headers}] = Agent.get(capture, & &1)
    assert path == "/n/ns/b/b/o/seg/x.seg"
    assert Map.new(headers)["range"] == "bytes=10-13"
  end

  test "list sends prefix and sorts the object names", %{
    capture: capture,
    opts: opts
  } do
    assert ["seg/a", "seg/b"] = OCI.list(:i, "seg/", opts)

    assert [{"GET", path, query, _headers}] = Agent.get(capture, & &1)
    assert path == "/n/ns/b/b/o"
    # `prefix` value is RFC 3986-encoded (`/` → %2F)
    assert query == "prefix=seg%2F"
  end

  test "a 200 that isn't ListObjects JSON reads as no keys", %{opts: opts} do
    Req.Test.stub(__MODULE__, fn conn ->
      Plug.Conn.send_resp(conn, 200, "not json at all")
    end)

    assert [] = OCI.list(:i, "seg/", opts)
  end

  test "a 404 is :not_found; any other non-2xx keeps its status", %{opts: opts} do
    assert {:error, :not_found} = OCI.get(:i, "missing/x", opts)
    assert {:error, {:status, 500, "kaboom"}} = OCI.get(:i, "boom", opts)
  end

  test "delete returns :ok", %{opts: opts} do
    assert :ok = OCI.delete(:i, "seg/x.seg", opts)
  end

  test "a missing :private_key raises at request time", %{opts: opts} do
    opts = Keyword.delete(opts, :private_key)
    assert_raise ArgumentError, ~r/private_key/, fn -> OCI.get(:i, "seg/x", opts) end
  end
end
