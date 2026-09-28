defmodule Ankusa.BlobStore.OCISigningTest do
  @moduledoc """
  Signing is the part of the OCI adapter that no emulator can test — OCI
  publishes no Object Storage emulator, and the RSA-SHA256 signature is the
  only thing standing between the adapter and a 401 on someone else's tenancy.

  Two independent pins:

    1. OCI's own published test key and signing string, from the "Request
       Signatures" page. The RSA-SHA256 signature of that string, computed
       with OpenSSL (outside the BEAM), is asserted against
       `:public_key.sign/3` — so the cryptographic step is not "the same code
       agreeing with itself".
    2. `Req.Test` captures what the adapter actually puts on the wire, and the
       signing string is *reconstructed* from the captured `date`, method,
       path, query, and body headers, then re-signed with the test key. If the
       adapter signed anything other than the request it actually sent, the
       reconstructed signature will not match.
  """

  use ExUnit.Case, async: true

  alias Ankusa.BlobStore.OCI
  alias Ankusa.Test.OCIKey, as: Key

  defp sign(string), do: :public_key.sign(string, :sha256, Key.private_key()) |> Base.encode64()

  defp auth_params(authorization) do
    Regex.scan(~r/(\w+)="([^"]*)"/, authorization)
    |> Map.new(fn [_, k, v] -> {k, v} end)
  end

  test "RSA-SHA256 matches OCI's published reference (signed with OpenSSL)" do
    assert sign(Key.signing_string()) == Key.reference_signature()
  end

  describe "the request the adapter actually sends" do
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

    @list_json ~s({"objects":[{"name":"seg/b"},{"name":"seg/a"},{"name":"seg/caf\u00e9"}],"prefixes":[],"nextStartWith":null})

    setup do
      {:ok, capture} = Agent.start_link(fn -> [] end)

      Req.Test.stub(__MODULE__, fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)

        Agent.update(capture, fn seen ->
          [{conn.method, conn.request_path, conn.query_string, conn.req_headers, body} | seen]
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

    test "GET signs exactly (request-target), date, and host", %{capture: capture, opts: opts} do
      assert ["seg/a", "seg/b", "seg/café"] = OCI.list(:i, "seg/", opts)

      assert [{"GET", path, query, headers, _body}] = Agent.get(capture, & &1)
      assert path == "/n/ns/b/b/o"
      assert query =~ "prefix=seg%2F"

      h = Map.new(headers)
      assert h["date"] =~ ~r/^\w{3}, \d{2} \w{3} \d{4} \d{2}:\d{2}:\d{2} GMT$/

      p = auth_params(h["authorization"])

      assert p["version"] == "1"
      assert p["algorithm"] == "rsa-sha256"
      assert p["keyId"] == "ocid1.tenancy.oc1..t/ocid1.user.oc1..u/aa:bb:cc"
      assert p["headers"] == "date (request-target) host"

      # Reconstruct the signing string from the request as captured.
      request_target = "get " <> path <> "?" <> query

      signing_string =
        "date: #{h["date"]}\n(request-target): #{request_target}\nhost: objectstorage.test"

      assert sign(signing_string) == p["signature"]
    end

    test "PUT signs the body headers too, and digests the exact body bytes", %{
      capture: capture,
      opts: opts
    } do
      body = :crypto.strong_rand_bytes(64)
      assert :ok = OCI.put(:i, "seg/a b.seg", body, opts)

      assert [{"PUT", path, _query, headers, sent}] = Agent.get(capture, & &1)
      assert path == "/n/ns/b/b/o/seg/a%20b.seg"
      assert sent == body

      h = Map.new(headers)

      assert h["content-type"] == "application/octet-stream"
      assert h["x-content-sha256"] == Base.encode64(:crypto.hash(:sha256, body))

      p = auth_params(h["authorization"])

      assert p["headers"] ==
               "date (request-target) host content-length content-type x-content-sha256"

      signing_string =
        "date: #{h["date"]}\n" <>
          "(request-target): put #{path}\n" <>
          "host: objectstorage.test\n" <>
          "content-length: #{byte_size(body)}\n" <>
          "content-type: application/octet-stream\n" <>
          "x-content-sha256: #{h["x-content-sha256"]}"

      assert sign(signing_string) == p["signature"]
    end
  end
end
