defmodule Ankusa.BlobStore.S3SigningTest do
  @moduledoc """
  Signing is the part of the S3 adapter that a permissive emulator cannot test.

  `floci` does not validate SigV4 at all — a bogus `authorization` header, no
  header, and a wrong-secret signature all return `200` (checked by hand) — so
  the `:integration` suite proves the request *plumbing*, never the signature.

  These tests close that gap from both ends:

    1. `aws_signature`, driven with this adapter's calling convention,
       reproduces the signatures **AWS publishes in its own SigV4 examples** —
       including S3's "sign the path exactly as sent" rule
       (`uri_encode_path: false`), which is the one S3-specific subtlety and the
       easiest thing to get silently wrong.
    2. `Req.Test` captures what the adapter actually puts on the wire, so the
       inputs those signatures are computed over (host, path, method, body
       digest, signed-header list) are asserted rather than assumed.
  """

  use ExUnit.Case, async: true

  alias Ankusa.BlobStore.S3

  # AWS's published example credentials and date, from
  # https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
  @access_key "AKIAIOSFODNN7EXAMPLE"
  @secret "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  @datetime {{2013, 5, 24}, {0, 0, 0}}

  @get_signature "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
  @put_signature "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"

  # The adapter's calling convention: region "us-east-1", service "s3",
  # `uri_encode_path: false`, and a host header derived from the URL.
  defp sign(url, method, headers, body, opts) do
    :aws_signature.sign_v4(
      @access_key,
      @secret,
      "us-east-1",
      "s3",
      @datetime,
      method,
      url,
      headers,
      body,
      opts
    )
  end

  defp authorization(headers), do: headers |> Map.new() |> Map.fetch!("Authorization")

  # "20260524T000000Z" -> {{2026, 5, 24}, {0, 0, 0}}
  defp parse_amz_date(
         <<y::binary-4, mo::binary-2, d::binary-2, "T", h::binary-2, mi::binary-2, s::binary-2,
           "Z">>
       ) do
    {{to_int(y), to_int(mo), to_int(d)}, {to_int(h), to_int(mi), to_int(s)}}
  end

  defp to_int(bin), do: String.to_integer(bin)

  describe "AWS's published SigV4 examples" do
    test "GET object" do
      headers = [{"Host", "examplebucket.s3.amazonaws.com"}, {"Range", "bytes=0-9"}]

      assert authorization(
               sign("https://examplebucket.s3.amazonaws.com/test.txt", "GET", headers, "",
                 uri_encode_path: false
               )
             ) ==
               "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request," <>
                 "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date," <>
                 "Signature=#{@get_signature}"
    end

    test "PUT object: a pre-encoded path, and the digest covers the exact body bytes" do
      headers = [
        {"Host", "examplebucket.s3.amazonaws.com"},
        {"Date", "Fri, 24 May 2013 00:00:00 GMT"},
        {"X-Amz-Storage-Class", "REDUCED_REDUNDANCY"}
      ]

      signed =
        sign(
          "https://examplebucket.s3.amazonaws.com/test%24file.text",
          "PUT",
          headers,
          "Welcome to Amazon S3.",
          uri_encode_path: false
        )

      by_name = Map.new(signed)

      assert by_name["Authorization"] =~ "Signature=#{@put_signature}"

      assert by_name["X-Amz-Content-SHA256"] ==
               "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"
    end

    test "S3 signs the path as sent: the library's double-encoding default does not match AWS" do
      headers = [
        {"Host", "examplebucket.s3.amazonaws.com"},
        {"Date", "Fri, 24 May 2013 00:00:00 GMT"},
        {"X-Amz-Storage-Class", "REDUCED_REDUNDANCY"}
      ]

      url = "https://examplebucket.s3.amazonaws.com/test%24file.text"
      body = "Welcome to Amazon S3."

      as_sent = authorization(sign(url, "PUT", headers, body, uri_encode_path: false))
      double_encoded = authorization(sign(url, "PUT", headers, body, uri_encode_path: true))

      assert as_sent =~ "Signature=#{@put_signature}"
      refute double_encoded == as_sent
    end
  end

  describe "the request the adapter actually sends" do
    @opts [
      bucket: "b",
      region: "us-east-1",
      endpoint: "http://localhost:4566",
      access_key_id: "test",
      secret_access_key: "test"
    ]

    @list_xml """
    <?xml version="1.0" encoding="UTF-8"?>
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Name>b</Name><Prefix>claims/a b/</Prefix>
      <KeyCount>2</KeyCount><IsTruncated>false</IsTruncated>
      <Contents><Key>claims/a b/2</Key><Size>3</Size></Contents>
      <Contents><Key>claims/a b/1</Key><Size>3</Size></Contents>
    </ListBucketResult>
    """

    setup do
      {:ok, capture} = Agent.start_link(fn -> [] end)

      Req.Test.stub(__MODULE__, fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)

        Agent.update(capture, fn seen ->
          [{conn.method, conn.request_path, conn.query_string, conn.req_headers, body} | seen]
        end)

        cond do
          String.contains?(conn.request_path, "missing") ->
            Plug.Conn.send_resp(conn, 404, "NoSuchKey")

          String.contains?(conn.request_path, "boom") ->
            Plug.Conn.send_resp(conn, 500, "kaboom")

          String.contains?(conn.query_string, "list-type") ->
            Plug.Conn.send_resp(conn, 200, @list_xml)

          true ->
            Plug.Conn.send_resp(conn, 200, "ok")
        end
      end)

      %{capture: capture, opts: @opts ++ [req_options: [plug: {Req.Test, __MODULE__}]]}
    end

    test "put: one round of path escaping, digest over the bytes sent, only our headers signed",
         %{capture: capture, opts: opts} do
      body = :crypto.strong_rand_bytes(64)

      assert :ok = S3.put(:i, "seg/a b+c.bin", body, opts)

      assert [{method, path, query, headers, sent}] = Agent.get(capture, & &1)

      assert method == "PUT"
      # `%20` for the space, `%2B` for the plus, `/` kept as a separator — one
      # round, because the signer is told not to encode the path again.
      assert path == "/b/seg/a%20b%2Bc.bin"
      assert query in ["", nil]
      assert sent == body

      h = Map.new(headers)

      assert h["host"] == "localhost:4566"

      assert h["x-amz-content-sha256"] ==
               Base.encode16(:crypto.hash(:sha256, body), case: :lower)

      assert h["x-amz-date"] =~ ~r/^\d{8}T\d{6}Z$/

      assert h["authorization"] =~
               ~r|^AWS4-HMAC-SHA256 Credential=test/\d{8}/us-east-1/s3/aws4_request,|

      # exactly the headers we handed the signer — a stray one would change the
      # signature AWS checks
      assert h["authorization"] =~ "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
    end

    test "get_range asks for exactly the byte window", %{capture: capture, opts: opts} do
      assert {:ok, "ok"} = S3.get_range(:i, "seg/x.seg", 10, 4, opts)

      assert [{method, path, _query, headers, _body}] = Agent.get(capture, & &1)
      assert method == "GET"
      assert path == "/b/seg/x.seg"
      assert Map.new(headers)["range"] == "bytes=10-13"
    end

    test "list sends ListObjectsV2 with RFC 3986 query encoding, and sorts the keys", %{
      capture: capture,
      opts: opts
    } do
      assert ["claims/a b/1", "claims/a b/2"] = S3.list(:i, "claims/a b/", opts)

      assert [{method, path, query, _headers, _body}] = Agent.get(capture, & &1)
      assert method == "GET"
      assert path == "/b"
      # space as %20 (not `+`), and `/` escaped inside the value
      assert query == "list-type=2&prefix=claims%2Fa%20b%2F"
    end

    test "a 200 that isn't ListObjectsV2 XML reads as no keys instead of crashing", %{opts: opts} do
      Req.Test.stub(__MODULE__, fn conn ->
        Plug.Conn.send_resp(conn, 200, "not xml at all")
      end)

      # the claim-check sweeper calls this: a bad body must not take it down
      assert [] = S3.list(:i, "claims/", opts)
    end

    test "the adapter signs S3's way: the path exactly as sent", %{capture: capture, opts: opts} do
      # `%` and a space in the key: re-encoding the path would change the signature
      assert :ok = S3.put(:i, "seg/a%b c.bin", "x", opts)

      assert [{"PUT", path, _query, headers, body}] = Agent.get(capture, & &1)
      h = Map.new(headers)

      # Reconstruct the adapter's signing call from the request it sent, then
      # check which option reproduces the signature it actually used. This is
      # what pins `uri_encode_path: false` — the S3-specific rule — to this
      # adapter rather than just to the library.
      resign = fn signing_opts ->
        :aws_signature.sign_v4(
          "test",
          "test",
          "us-east-1",
          "s3",
          parse_amz_date(h["x-amz-date"]),
          "PUT",
          "http://localhost:4566" <> path,
          [{"host", h["host"]}],
          body,
          signing_opts
        )
      end

      sent = h["authorization"]
      assert authorization(resign.(uri_encode_path: false)) == sent
      refute authorization(resign.(uri_encode_path: true)) == sent
    end

    test "a 404 is :not_found; any other non-2xx keeps its status", %{opts: opts} do
      assert {:error, :not_found} = S3.get(:i, "missing/x", opts)
      assert {:error, {:status, 500, "kaboom"}} = S3.get(:i, "boom", opts)
    end
  end
end
