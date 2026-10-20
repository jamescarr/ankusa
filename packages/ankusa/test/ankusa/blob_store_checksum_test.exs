defmodule Ankusa.BlobStore.ChecksumTest do
  @moduledoc """
  The integrity headers each HTTP object store sends on a `PUT`, and the check of
  the digest it sends back on a `GET`: a corrupted `GET` is `{:error, :integrity}`,
  a response with no digest (or a composite one) is not checked, a range read is
  not checked.
  """

  use ExUnit.Case, async: true

  alias Ankusa.BlobStore.{Azure, Checksum, GCS, OCI, S3}
  alias Ankusa.Test.BlobStoreAdapters, as: Adapters

  # {PUT request header, GET response header, value of the GET header for `body`}
  defp put_digest(S3, body), do: {"x-amz-checksum-sha256", Checksum.sha256_base64(body)}
  defp put_digest(GCS, body), do: {"x-goog-hash", "md5=" <> Checksum.md5_base64(body)}
  defp put_digest(Azure, body), do: {"content-md5", Checksum.md5_base64(body)}
  defp put_digest(OCI, body), do: {"content-md5", Checksum.md5_base64(body)}

  defp get_digest(S3, digest), do: {"x-amz-checksum-sha256", digest}
  defp get_digest(GCS, digest), do: {"x-goog-hash", "crc32c=AAAAAA==,md5=" <> digest}
  defp get_digest(Azure, digest), do: {"content-md5", digest}
  defp get_digest(OCI, digest), do: {"opc-content-md5", digest}

  defp serve_get(body, response_headers) do
    Req.Test.stub(__MODULE__, fn conn ->
      conn =
        Enum.reduce(response_headers, conn, fn {k, v}, conn ->
          Plug.Conn.put_resp_header(conn, k, v)
        end)

      Plug.Conn.send_resp(conn, 200, body)
    end)
  end

  for adapter <- Adapters.all() do
    describe inspect(adapter) do
      test "a put carries the digest of its bytes" do
        test = self()

        Req.Test.stub(__MODULE__, fn conn ->
          {:ok, body, conn} = Plug.Conn.read_body(conn)
          send(test, {:put, Adapters.headers(conn), body})
          Plug.Conn.send_resp(conn, 200, "")
        end)

        {mod, opts} = Adapters.opts(unquote(adapter), "sum-put", __MODULE__)
        assert :ok = mod.put(:i, "seg/a", ["ab", ["cd"]], opts)

        assert_received {:put, headers, "abcd"}
        {name, digest} = put_digest(unquote(adapter), "abcd")
        assert headers[name] == digest
      end

      test "a get whose bytes match the digest is returned" do
        digest = Checksum.md5_base64("abcd")
        digest = if unquote(adapter) == S3, do: Checksum.sha256_base64("abcd"), else: digest
        serve_get("abcd", [get_digest(unquote(adapter), digest)])

        {mod, opts} = Adapters.opts(unquote(adapter), "sum-ok", __MODULE__)
        assert {:ok, "abcd"} = mod.get(:i, "seg/a", opts)
      end

      test "a get whose bytes do not match the digest is {:error, :integrity}" do
        wrong = Checksum.md5_base64("not abcd")
        wrong = if unquote(adapter) == S3, do: Checksum.sha256_base64("not abcd"), else: wrong
        serve_get("abcd", [get_digest(unquote(adapter), wrong)])

        {mod, opts} = Adapters.opts(unquote(adapter), "sum-bad", __MODULE__)
        assert {:error, :integrity} = mod.get(:i, "seg/a", opts)
      end

      test "a get with no digest is not checked, and neither is a range read" do
        serve_get("abcd", [])
        {mod, opts} = Adapters.opts(unquote(adapter), "sum-none", __MODULE__)
        assert {:ok, "abcd"} = mod.get(:i, "seg/a", opts)

        wrong = Checksum.md5_base64("nope")
        wrong = if unquote(adapter) == S3, do: Checksum.sha256_base64("nope"), else: wrong
        serve_get("ab", [get_digest(unquote(adapter), wrong)])
        assert {:ok, "ab"} = mod.get_range(:i, "seg/a", 0, 2, opts)
      end
    end
  end

  test "an S3 get asks for the checksum" do
    test = self()

    Req.Test.stub(__MODULE__, fn conn ->
      send(test, {:headers, Adapters.headers(conn)})
      Plug.Conn.send_resp(conn, 200, "abcd")
    end)

    {mod, opts} = Adapters.opts(S3, "sum-mode", __MODULE__)
    assert {:ok, "abcd"} = mod.get(:i, "seg/a", opts)
    assert_received {:headers, %{"x-amz-checksum-mode" => "ENABLED"}}
  end

  test "an S3 composite checksum (<digest>-<parts>) is not a digest of the bytes, and is skipped" do
    serve_get("abcd", [{"x-amz-checksum-sha256", "AAAAAAAA-3"}])
    {mod, opts} = Adapters.opts(S3, "sum-composite", __MODULE__)
    assert {:ok, "abcd"} = mod.get(:i, "seg/a", opts)
  end

  test "Checksum.goog_md5/1 reads the md5 out of one or several x-goog-hash headers" do
    assert Checksum.goog_md5(%{"x-goog-hash" => ["crc32c=AAAAAA==", "md5=Zm9v=="]}) == "Zm9v=="
    assert Checksum.goog_md5(%{"x-goog-hash" => ["crc32c=AAAAAA==,md5=Zm9v"]}) == "Zm9v"
    assert Checksum.goog_md5(%{"x-goog-hash" => ["crc32c=AAAAAA=="]}) == nil
    assert Checksum.goog_md5(%{}) == nil
  end
end
