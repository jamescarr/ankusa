defmodule Ankusa.BlobStore.MultipartTest do
  @moduledoc """
  A 16 MiB pack on each HTTP object store, against a stub that behaves like a
  1 MiB/s link: it refuses (a transport timeout) any single request that would
  take longer than the legacy 10 s `timeout_ms`, so only an upload cut into
  parts or chunks gets through. The link's time is scaled down 32× so the test
  does not sit through 16 real seconds; the rule it enforces is the same one.
  """

  use ExUnit.Case, async: true

  alias Ankusa.BlobStore.{Azure, Checksum, GCS, OCI, S3}
  alias Ankusa.Test.BlobStoreAdapters, as: Adapters

  @mib 1_048_576
  @size 16 * @mib
  # 1 MiB/s for 10 s: the most one request can carry before it times out.
  @single_request_limit 10 * @mib

  setup do
    {:ok, body: :crypto.strong_rand_bytes(@size)}
  end

  # Time on the wire, scaled 32×.
  defp wire(bytes), do: Process.sleep(div(bytes * 1000, 32 * @mib))

  defp single(conn, body, test) do
    send(test, {:single, byte_size(body)})

    if byte_size(body) > @single_request_limit do
      Req.Test.transport_error(conn, :timeout)
    else
      wire(byte_size(body))
      Plug.Conn.send_resp(conn, 200, "")
    end
  end

  defp serve(adapter) do
    test = self()

    Req.Test.stub(__MODULE__, fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn, length: 64 * @mib)
      handle(adapter, conn, body, test)
    end)
  end

  # ── S3: create, parts, complete ─────────────────────────────────────────────

  defp handle(S3, conn, body, test) do
    query = Adapters.query(conn)
    headers = Adapters.headers(conn)

    cond do
      conn.method == "POST" and Map.has_key?(query, "uploads") ->
        send(test, :created)

        Plug.Conn.send_resp(
          conn,
          200,
          "<InitiateMultipartUploadResult><UploadId>up-1</UploadId></InitiateMultipartUploadResult>"
        )

      conn.method == "PUT" and Map.has_key?(query, "partNumber") ->
        assert headers["content-md5"] == Checksum.md5_base64(body)
        assert query["uploadId"] == "up-1"
        n = String.to_integer(query["partNumber"])
        send(test, {:part, n, byte_size(body)})
        wire(byte_size(body))

        conn
        |> Plug.Conn.put_resp_header("etag", ~s("etag-#{n}"))
        |> Plug.Conn.send_resp(200, "")

      conn.method == "POST" and query["uploadId"] == "up-1" ->
        send(test, {:completed, body, headers})
        Plug.Conn.send_resp(conn, 200, "<CompleteMultipartUploadResult/>")

      conn.method == "DELETE" and query["uploadId"] == "up-1" ->
        send(test, :aborted)
        Plug.Conn.send_resp(conn, 204, "")

      conn.method == "PUT" ->
        single(conn, body, test)
    end
  end

  # ── GCS: a resumable session, then chunks ───────────────────────────────────

  defp handle(GCS, conn, body, test) do
    query = Adapters.query(conn)
    headers = Adapters.headers(conn)

    cond do
      conn.method == "POST" and query["uploadType"] == "resumable" ->
        assert String.to_integer(headers["x-upload-content-length"]) >= 8 * @mib
        send(test, :created)

        conn
        |> Plug.Conn.put_resp_header("location", "http://#{conn.host}/upload/session/1")
        |> Plug.Conn.send_resp(200, "")

      conn.method == "PUT" and conn.request_path == "/upload/session/1" ->
        ["bytes " <> range] = [headers["content-range"]]
        [span, total] = String.split(range, "/")
        [first, last] = span |> String.split("-") |> Enum.map(&String.to_integer/1)
        send(test, {:part, first, byte_size(body)})
        wire(byte_size(body))

        if last + 1 == String.to_integer(total) do
          send(test, :completed)
          Plug.Conn.send_resp(conn, 200, "{}")
        else
          Plug.Conn.send_resp(conn, 308, "")
        end

      conn.method == "POST" ->
        single(conn, body, test)
    end
  end

  # ── Azure: blocks, then a block list ────────────────────────────────────────

  defp handle(Azure, conn, body, test) do
    query = Adapters.query(conn)
    headers = Adapters.headers(conn)

    cond do
      conn.method == "PUT" and query["comp"] == "block" ->
        assert headers["content-md5"] == Checksum.md5_base64(body)
        send(test, {:part, query["blockid"], byte_size(body)})
        wire(byte_size(body))
        Plug.Conn.send_resp(conn, 201, "")

      conn.method == "PUT" and query["comp"] == "blocklist" ->
        send(test, {:completed, body, headers})
        Plug.Conn.send_resp(conn, 201, "")

      conn.method == "PUT" ->
        single(conn, body, test)
    end
  end

  # ── OCI: create, parts, commit ──────────────────────────────────────────────

  defp handle(OCI, conn, body, test) do
    query = Adapters.query(conn)
    headers = Adapters.headers(conn)

    cond do
      conn.method == "POST" and String.ends_with?(conn.request_path, "/b/b/u") ->
        assert JSON.decode!(body) == %{"object" => "claims/big"}
        send(test, :created)
        Plug.Conn.send_resp(conn, 200, JSON.encode!(%{"uploadId" => "up-1"}))

      conn.method == "PUT" and Map.has_key?(query, "uploadPartNum") ->
        assert headers["content-md5"] == Checksum.md5_base64(body)
        n = String.to_integer(query["uploadPartNum"])
        send(test, {:part, n, byte_size(body)})
        wire(byte_size(body))

        conn
        |> Plug.Conn.put_resp_header("etag", "etag-#{n}")
        |> Plug.Conn.send_resp(200, "")

      conn.method == "POST" and query["uploadId"] == "up-1" ->
        send(test, {:completed, body, headers})
        Plug.Conn.send_resp(conn, 200, "")

      conn.method == "PUT" ->
        single(conn, body, test)
    end
  end

  # Did the upload finish (`:completed`, or `{:completed, body, headers}`)?
  defp completed? do
    receive do
      :completed -> true
      {:completed, _body, _headers} -> true
    after
      0 -> false
    end
  end

  defp parts(acc \\ []) do
    receive do
      {:part, id, size} -> parts([{id, size} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  for adapter <- Adapters.all() do
    test "#{inspect(adapter)}: a 16 MiB pack goes up in parts under the 1 MiB/s link", %{
      body: body
    } do
      serve(unquote(adapter))
      {mod, opts} = Adapters.opts(unquote(adapter), "mp", __MODULE__)

      assert :ok = mod.put(:i, "claims/big", body, opts)

      # Two parts of 8 MiB, and never the one request the link would time out.
      assert [{_, size_1}, {_, size_2}] = parts()
      assert size_1 == 8 * @mib and size_2 == 8 * @mib
      assert completed?()
      refute_received {:single, _}
    end

    test "#{inspect(adapter)}: below the threshold it is one request", %{body: _body} do
      serve(unquote(adapter))
      {mod, opts} = Adapters.opts(unquote(adapter), "mp-small", __MODULE__)

      assert :ok = mod.put(:i, "claims/small", :crypto.strong_rand_bytes(@mib), opts)
      assert_received {:single, @mib}
      refute_received :created
    end

    test "#{inspect(adapter)}: multipart_threshold_bytes and multipart_part_bytes set the cut", %{
      body: body
    } do
      serve(unquote(adapter))

      {mod, opts} =
        Adapters.opts(unquote(adapter), "mp-part", __MODULE__,
          multipart_threshold_bytes: 4 * @mib,
          multipart_part_bytes: 5 * @mib
        )

      assert :ok = mod.put(:i, "claims/big", binary_part(body, 0, 12 * @mib), opts)
      five = 5 * @mib
      two = 2 * @mib
      assert [{_, ^five}, {_, ^five}, {_, ^two}] = parts()
    end
  end

  test "S3: a part that fails aborts the upload, so no orphan parts are kept", %{body: body} do
    test = self()

    Req.Test.stub(__MODULE__, fn conn ->
      {:ok, request_body, conn} = Plug.Conn.read_body(conn, length: 64 * @mib)

      if Adapters.query(conn)["partNumber"] == "2" do
        Plug.Conn.send_resp(conn, 500, "boom")
      else
        handle(S3, conn, request_body, test)
      end
    end)

    {mod, opts} = Adapters.opts(S3, "mp-abort", __MODULE__, retries: 0)

    assert {:error, {:status, 500, "boom"}} = mod.put(:i, "claims/big", body, opts)
    assert_received :aborted
    refute_received {:completed, _, _}
  end

  test "S3: a conditional multipart upload that loses the race is :already_exists, and aborted",
       %{body: body} do
    test = self()

    Req.Test.stub(__MODULE__, fn conn ->
      {:ok, request_body, conn} = Plug.Conn.read_body(conn, length: 64 * @mib)
      query = Adapters.query(conn)

      cond do
        conn.method == "POST" and query["uploadId"] == "up-1" ->
          assert Adapters.headers(conn)["if-none-match"] == "*"
          Plug.Conn.send_resp(conn, 412, "PreconditionFailed")

        conn.method == "GET" ->
          Plug.Conn.send_resp(conn, 200, "somebody else's pack")

        true ->
          handle(S3, conn, request_body, test)
      end
    end)

    {mod, opts} = Adapters.opts(S3, "mp-cond", __MODULE__)

    assert {:error, :already_exists} = mod.put_if_absent(:i, "claims/big", body, opts)
    assert_received :aborted
  end

  test "the S3 complete request lists every part with the ETag it was given", %{body: body} do
    serve(S3)
    {mod, opts} = Adapters.opts(S3, "mp-complete", __MODULE__)

    assert :ok = mod.put(:i, "claims/big", body, opts)
    assert_received {:completed, xml, _headers}

    assert xml =~ "<PartNumber>1</PartNumber><ETag>\"etag-1\"</ETag>"
    assert xml =~ "<PartNumber>2</PartNumber><ETag>\"etag-2\"</ETag>"
  end

  test "Azure commits its blocks in order, with the whole blob's md5", %{body: body} do
    serve(Azure)
    {mod, opts} = Adapters.opts(Azure, "mp-blocks", __MODULE__)

    assert :ok = mod.put(:i, "claims/big", body, opts)
    assert_received {:completed, xml, headers}

    ids = Regex.scan(~r{<Latest>([^<]+)</Latest>}, xml, capture: :all_but_first)
    assert [[id_1], [id_2]] = ids
    assert Base.decode64!(id_1) == "00000000"
    assert Base.decode64!(id_2) == "00000001"
    assert headers["x-ms-blob-content-md5"] == Checksum.md5_base64(body)
  end
end
