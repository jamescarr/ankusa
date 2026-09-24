defmodule Ankusa.BlobStore.AzureTest do
  @moduledoc """
  Request plumbing for `Ankusa.BlobStore.Azure`, captured with `Req.Test`:
  URL construction (account-in-path for Azurite, RFC 3986 key encoding),
  Put Blob headers, SAS appending vs. bearer `:token_provider`, `get_range`
  byte windows, `list` XML parsing, and the `:not_found` mapping. The
  round-trip against a real Azurite container lives in
  `test/ankusa/blob_store_azure_integration_test.exs`.
  """

  use ExUnit.Case, async: true

  alias Ankusa.BlobStore.Azure

  @opts [
    account_name: "acct",
    container: "cont",
    endpoint: "http://localhost:10000/acct"
  ]

  @list_xml """
  <?xml version="1.0" encoding="utf-8"?>
  <EnumerationResults ContainerName="cont">
    <Blobs>
      <Blob><Name>seg/b</Name></Blob>
      <Blob><Name>seg/a</Name></Blob>
      <Blob><Name>seg/café</Name></Blob>
    </Blobs>
  </EnumerationResults>
  """

  def token_provider, do: {:ok, "test-bearer"}

  setup do
    {:ok, capture} = Agent.start_link(fn -> [] end)

    Req.Test.stub(__MODULE__, fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      Agent.update(capture, fn seen ->
        [{conn.method, conn.request_path, conn.query_string, conn.req_headers, body} | seen]
      end)

      cond do
        String.contains?(conn.request_path, "missing") ->
          Plug.Conn.send_resp(conn, 404, "NotFound")

        String.contains?(conn.request_path, "boom") ->
          Plug.Conn.send_resp(conn, 500, "kaboom")

        String.contains?(conn.query_string, "comp=list") ->
          Plug.Conn.send_resp(conn, 200, @list_xml)

        true ->
          Plug.Conn.send_resp(conn, 200, "ok")
      end
    end)

    %{capture: capture, opts: @opts ++ [req_options: [plug: {Req.Test, __MODULE__}]]}
  end

  test "put uses Put Blob headers and appends the SAS token verbatim", %{
    capture: capture,
    opts: opts
  } do
    body = :crypto.strong_rand_bytes(32)
    sas = "sv=2024-11-04&sr=c&sp=rwdl&se=2099-01-01T00%3A00%3A00Z&sig=abc%2Fdef%3D"

    assert :ok = Azure.put(:i, "seg/a b.seg", body, opts ++ [sas_token: sas])

    assert [{"PUT", path, query, headers, sent}] = Agent.get(capture, & &1)
    # endpoint is Azurite's IP-style URL: the account name is already in the
    # endpoint path, so the blob path is /{account}/{container}/{key}.
    assert path == "/acct/cont/seg/a%20b.seg"
    assert query == sas
    assert sent == body

    h = Map.new(headers)
    assert h["x-ms-blob-type"] == "BlockBlob"
    assert h["content-type"] == "application/octet-stream"
    # a SAS already encodes its API version — no x-ms-version alongside it
    refute Map.has_key?(h, "x-ms-version")
  end

  test "a bearer :token_provider sets authorization and x-ms-version", %{
    capture: capture,
    opts: opts
  } do
    assert {:ok, "ok"} =
             Azure.get(
               :i,
               "seg/x.seg",
               opts ++ [token_provider: {__MODULE__, :token_provider, []}]
             )

    assert [{"GET", _path, _query, headers, _body}] = Agent.get(capture, & &1)
    h = Map.new(headers)
    assert h["authorization"] == "Bearer test-bearer"
    assert h["x-ms-version"] == "2024-11-04"
  end

  test "get_range asks for exactly the byte window", %{capture: capture, opts: opts} do
    assert {:ok, "ok"} = Azure.get_range(:i, "seg/x.seg", 10, 4, opts)

    assert [{"GET", path, _query, headers, _body}] = Agent.get(capture, & &1)
    assert path == "/acct/cont/seg/x.seg"
    assert Map.new(headers)["range"] == "bytes=10-13"
  end

  test "list sends restype=container&comp=list and sorts the blob names", %{
    capture: capture,
    opts: opts
  } do
    assert ["seg/a", "seg/b", "seg/café"] = Azure.list(:i, "seg/", opts)

    assert [{"GET", path, query, _headers, _body}] = Agent.get(capture, & &1)
    assert path == "/acct/cont"
    assert query == "restype=container&comp=list&prefix=seg%2F"
  end

  test "a 200 that isn't ListBlobs XML reads as no keys instead of crashing", %{opts: opts} do
    Req.Test.stub(__MODULE__, fn conn ->
      Plug.Conn.send_resp(conn, 200, "not xml at all")
    end)

    assert [] = Azure.list(:i, "seg/", opts)
  end

  test "a 404 is :not_found; any other non-2xx keeps its status", %{opts: opts} do
    assert {:error, :not_found} = Azure.get(:i, "missing/x", opts)
    assert {:error, {:status, 500, "kaboom"}} = Azure.get(:i, "boom", opts)
  end

  test "delete returns :ok", %{opts: opts} do
    assert :ok = Azure.delete(:i, "seg/x.seg", opts)
  end
end
