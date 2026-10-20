defmodule Ankusa.BlobStore.CredentialsTest do
  @moduledoc """
  The credential providers behind the object stores, and the adapters' refresh on
  a `401`: `Ankusa.BlobStore.GCS.MetadataServer` and
  `Ankusa.BlobStore.OCI.InstancePrincipal` against stubbed metadata and auth
  endpoints, and `Ankusa.BlobStore.Azure.ManagedIdentity` through
  `Ankusa.BlobStore.Azure`. An expired token is dropped on the `401`, and the
  retry carries a new one: the caller sees the object, not the `401`.
  """

  use ExUnit.Case, async: false

  alias Ankusa.BlobStore.{Azure, GCS, OCI, TokenCache}
  alias Ankusa.BlobStore.Azure.ManagedIdentity
  alias Ankusa.BlobStore.GCS.MetadataServer
  alias Ankusa.BlobStore.OCI.InstancePrincipal
  alias Ankusa.Test.BlobStoreAdapters, as: Adapters

  # A token provider that keeps nothing to drop.
  defmodule StaticToken do
    @moduledoc false
    def token, do: {:ok, "static"}
  end

  setup do
    TokenCache.clear()
    Ankusa.BlobStore.Breaker.reset()
    :ok
  end

  defp counter, do: elem(Agent.start_link(fn -> 0 end), 1)
  defp bump(counter), do: Agent.get_and_update(counter, &{&1 + 1, &1 + 1})

  defp json(conn, status \\ 200, map), do: Plug.Conn.send_resp(conn, status, JSON.encode!(map))

  # ── GCS ─────────────────────────────────────────────────────────────────────

  describe "GCS.MetadataServer" do
    defp metadata(expires_in) do
      calls = counter()
      test = self()

      Req.Test.stub(MetadataServer, fn conn ->
        n = bump(calls)
        send(test, {:metadata, conn.request_path, Adapters.headers(conn)["metadata-flavor"]})
        json(conn, %{"access_token" => "tok-#{n}", "expires_in" => expires_in})
      end)

      [
        endpoint: "http://metadata.cred#{expires_in}.test/token",
        req_options: [plug: {Req.Test, MetadataServer}]
      ]
    end

    test "the token is fetched with Metadata-Flavor, cached, and dropped by invalidate/1" do
      opts = metadata(3600)

      assert {:ok, "tok-1"} = MetadataServer.token(opts)
      assert {:ok, "tok-1"} = MetadataServer.token(opts)
      assert_received {:metadata, "/token", "Google"}
      refute_received {:metadata, _, _}

      MetadataServer.invalidate(opts)
      assert {:ok, "tok-2"} = MetadataServer.token(opts)
    end

    test "a token inside its last minute is fetched again" do
      opts = metadata(30)

      assert {:ok, "tok-1"} = MetadataServer.token(opts)
      assert {:ok, "tok-2"} = MetadataServer.token(opts)
    end

    test "a metadata server that does not answer is :error" do
      Req.Test.stub(MetadataServer, &Plug.Conn.send_resp(&1, 500, "down"))

      assert :error =
               MetadataServer.token(
                 endpoint: "http://metadata.down.test/token",
                 req_options: [plug: {Req.Test, MetadataServer}]
               )
    end

    test "the adapter retries a 401 once with a fresh token" do
      meta = metadata(3600)
      test = self()

      Req.Test.stub(GCS, fn conn ->
        auth = Adapters.headers(conn)["authorization"]
        send(test, {:auth, auth})

        if auth == "Bearer tok-1",
          do: Plug.Conn.send_resp(conn, 401, "expired"),
          else: Plug.Conn.send_resp(conn, 200, "object bytes")
      end)

      opts = [
        bucket: "b",
        endpoint: "http://gcs.cred-refresh.test",
        token_provider: {MetadataServer, :token, [meta]},
        req_options: [plug: {Req.Test, GCS}]
      ]

      assert {:ok, "object bytes"} = GCS.get(:i, "seg/a", opts)
      assert_received {:auth, "Bearer tok-1"}
      assert_received {:auth, "Bearer tok-2"}
      refute_received {:auth, _}
    end

    test "a 401 stays a 401 when the provider keeps no token to drop" do
      test = self()

      Req.Test.stub(GCS, fn conn ->
        send(test, :request)
        Plug.Conn.send_resp(conn, 401, "nope")
      end)

      opts = [
        bucket: "b",
        endpoint: "http://gcs.cred-static.test",
        token_provider: {StaticToken, :token, []},
        req_options: [plug: {Req.Test, GCS}]
      ]

      assert {:error, {:status, 401, "nope"}} = GCS.get(:i, "seg/a", opts)
      assert_received :request
      refute_received :request
    end
  end

  # ── Azure ───────────────────────────────────────────────────────────────────

  test "Azure: a 401 drops the managed-identity token, and the retry carries a new one" do
    calls = counter()
    test = self()

    Req.Test.stub(ManagedIdentity, fn conn ->
      n = bump(calls)
      expires_on = Integer.to_string(System.system_time(:second) + 3600)
      json(conn, %{"access_token" => "az-#{n}", "expires_on" => expires_on})
    end)

    Req.Test.stub(Azure, fn conn ->
      auth = Adapters.headers(conn)["authorization"]
      send(test, {:auth, auth})

      if auth == "Bearer az-1",
        do: Plug.Conn.send_resp(conn, 401, "expired"),
        else: Plug.Conn.send_resp(conn, 200, "blob bytes")
    end)

    identity = [
      endpoint: "http://imds.cred-azure.test/token",
      client_id: "cred-test",
      req_options: [plug: {Req.Test, ManagedIdentity}]
    ]

    opts = [
      account_name: "acct",
      container: "c",
      endpoint: "http://azure.cred-refresh.test/acct",
      token_provider: {ManagedIdentity, :token, [identity]},
      req_options: [plug: {Req.Test, Azure}]
    ]

    ManagedIdentity.invalidate(identity)
    assert {:ok, "blob bytes"} = Azure.get(:i, "seg/a", opts)
    assert_received {:auth, "Bearer az-1"}
    assert_received {:auth, "Bearer az-2"}
  end

  # ── OCI ─────────────────────────────────────────────────────────────────────

  describe "OCI.InstancePrincipal" do
    @tenancy "ocid1.tenancy.oc1..tenant"

    setup do
      %{server_config: cfg} =
        :public_key.pkix_test_data(%{
          server_chain: %{root: [key: {:rsa, 2048, 65_537}], peer: [key: {:rsa, 2048, 65_537}]},
          client_chain: %{root: [key: {:rsa, 2048, 65_537}], peer: [key: {:rsa, 2048, 65_537}]}
        })

      {type, key_der} = cfg[:key]
      leaf_key = :public_key.pem_encode([{type, key_der, :not_encrypted}])

      %{
        leaf_cert: cfg[:cert],
        leaf_key: leaf_key,
        leaf_public:
          leaf_key |> :public_key.pem_decode() |> hd() |> :public_key.pem_entry_decode(),
        intermediate: :public_key.pem_encode([{:Certificate, hd(cfg[:cacerts]), :not_encrypted}])
      }
    end

    # IMDS and the auth service, as one stub: federations are counted, each
    # token is `jwt-<n>` with an `exp`, and `check` sees every `/v1/x509` request.
    defp instance_principal(ctx, check \\ fn _conn, _body -> :ok end) do
      federations = counter()
      test = self()

      Req.Test.stub(InstancePrincipal, fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)

        case conn.request_path do
          "/opc/v2/identity/cert.pem" ->
            assert Adapters.headers(conn)["authorization"] == "Bearer Oracle"

            Plug.Conn.send_resp(
              conn,
              200,
              :public_key.pem_encode([{:Certificate, ctx.leaf_cert, :not_encrypted}])
            )

          "/opc/v2/identity/key.pem" ->
            Plug.Conn.send_resp(conn, 200, ctx.leaf_key)

          "/opc/v2/identity/intermediate.pem" ->
            Plug.Conn.send_resp(conn, 200, ctx.intermediate)

          "/v1/x509" ->
            n = bump(federations)
            check.(conn, body)
            send(test, {:federated, n, conn, body})
            payload = %{"exp" => System.system_time(:second) + 3600, "n" => n}
            jwt = "e30." <> Base.url_encode64(JSON.encode!(payload), padding: false) <> ".sig"
            json(conn, %{"token" => jwt})
        end
      end)

      [
        region: "us-ashburn-1",
        tenancy_ocid: @tenancy,
        imds_endpoint: "http://imds.cred-oci.test/opc/v2",
        auth_endpoint: "http://auth.cred-oci.test",
        req_options: [plug: {Req.Test, InstancePrincipal}]
      ]
    end

    test "federates: the leaf signs the request, the session key comes back", ctx do
      opts = instance_principal(ctx)

      assert {:ok, %{key_id: "ST$" <> token, private_key: session_pem}} =
               InstancePrincipal.credentials(opts)

      assert_received {:federated, 1, conn, body}
      headers = Adapters.headers(conn)

      # The request: leaf cert, intermediates, the session public key.
      assert %{"certificate" => cert, "publicKey" => public, "intermediateCertificates" => [_]} =
               JSON.decode!(body)

      assert Base.decode64!(cert) == ctx.leaf_cert

      # Signed with the leaf key, under the tenancy and the cert's sha256 fingerprint.
      fingerprint =
        :sha256
        |> :crypto.hash(ctx.leaf_cert)
        |> Base.encode16()
        |> String.graphemes()
        |> Enum.chunk_every(2)
        |> Enum.map_join(":", &Enum.join/1)

      authorization = headers["authorization"]
      assert authorization =~ ~s(keyId="#{@tenancy}/fed-x509-sha256/#{fingerprint}")

      assert authorization =~
               ~s|headers="date (request-target) content-length content-type x-content-sha256"|

      signing_string =
        Enum.join(
          [
            "date: #{headers["date"]}",
            "(request-target): post /v1/x509",
            "content-length: #{byte_size(body)}",
            "content-type: application/json",
            "x-content-sha256: #{headers["x-content-sha256"]}"
          ],
          "\n"
        )

      [_, signature] = Regex.run(~r/signature="([^"]+)"/, authorization)

      assert :public_key.verify(
               signing_string,
               :sha256,
               Base.decode64!(signature),
               ctx.leaf_public
             )

      assert headers["x-content-sha256"] == Base.encode64(:crypto.hash(:sha256, body))

      # The session key pair is the one the token was asked for.
      assert token =~ "."

      session_key =
        session_pem |> :public_key.pem_decode() |> hd() |> :public_key.pem_entry_decode()

      session_public =
        {:SubjectPublicKeyInfo, Base.decode64!(public), :not_encrypted}
        |> :public_key.pem_entry_decode()

      proof = :public_key.sign("proof", :sha256, session_key)
      assert :public_key.verify("proof", :sha256, proof, session_public)
    end

    test "the session is cached until invalidated", ctx do
      opts = instance_principal(ctx)

      assert {:ok, %{key_id: first}} = InstancePrincipal.credentials(opts)
      assert {:ok, %{key_id: ^first}} = InstancePrincipal.credentials(opts)
      assert_received {:federated, 1, _, _}
      refute_received {:federated, 2, _, _}

      InstancePrincipal.invalidate(opts)
      assert {:ok, %{key_id: second}} = InstancePrincipal.credentials(opts)
      assert second != first
      assert_received {:federated, 2, _, _}
    end

    test "an IMDS that does not answer is :error", ctx do
      opts = instance_principal(ctx)
      Req.Test.stub(InstancePrincipal, &Plug.Conn.send_resp(&1, 404, ""))

      assert :error = InstancePrincipal.credentials(opts)
    end

    test "the adapter retries a 401 with a session federated afresh", ctx do
      ip = instance_principal(ctx)
      test = self()
      {:ok, first} = Agent.start_link(fn -> nil end)

      # IMDS, auth and the object API behind one stub, told apart by path. The
      # first session the object API sees is the expired one.
      Req.Test.stub(InstancePrincipal, fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)

        if String.starts_with?(conn.request_path, "/opc/v2/") or conn.request_path == "/v1/x509" do
          serve_ip(conn, body, ctx)
        else
          [_, key_id] = Regex.run(~r/keyId="([^"]+)"/, Adapters.headers(conn)["authorization"])
          send(test, {:object_key_id, key_id})
          expired = Agent.get_and_update(first, fn seen -> {seen || key_id, seen || key_id} end)

          if key_id == expired,
            do: Plug.Conn.send_resp(conn, 401, "expired"),
            else: Plug.Conn.send_resp(conn, 200, "object bytes")
        end
      end)

      opts = [
        region: "us-ashburn-1",
        namespace: "ns",
        bucket: "b",
        endpoint: "http://oci.cred-refresh.test",
        key_provider: {InstancePrincipal, :credentials, [ip]},
        req_options: [plug: {Req.Test, InstancePrincipal}]
      ]

      assert {:ok, "object bytes"} = OCI.get(:i, "seg/a", opts)
      assert_received {:object_key_id, "ST$" <> _ = stale}
      assert_received {:object_key_id, "ST$" <> _ = fresh}
      assert stale != fresh
    end
  end

  # The IMDS and auth endpoints of `instance_principal/2`, for a stub that also
  # serves the object API.
  defp serve_ip(conn, body, ctx) do
    case conn.request_path do
      "/opc/v2/identity/cert.pem" ->
        Plug.Conn.send_resp(
          conn,
          200,
          :public_key.pem_encode([{:Certificate, ctx.leaf_cert, :not_encrypted}])
        )

      "/opc/v2/identity/key.pem" ->
        Plug.Conn.send_resp(conn, 200, ctx.leaf_key)

      "/opc/v2/identity/intermediate.pem" ->
        Plug.Conn.send_resp(conn, 200, ctx.intermediate)

      "/v1/x509" ->
        n = System.unique_integer([:positive])

        payload = %{
          "exp" => System.system_time(:second) + 3600,
          "n" => n,
          "len" => byte_size(body)
        }

        jwt = "e30." <> Base.url_encode64(JSON.encode!(payload), padding: false) <> ".sig"
        json(conn, %{"token" => jwt})
    end
  end
end
