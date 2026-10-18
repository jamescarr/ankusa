defmodule Ankusa.Admin.AuthTest do
  @moduledoc """
  The credential half of `Ankusa.Admin.Auth`: how a bearer token, a JWT and a
  client certificate become an `Ankusa.Principal` (or nothing), driven through
  the plug the three routers mount. Which route needs which scope is the
  routers' tests.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Admin.Auth
  alias Ankusa.Admin.Auth.JWT.Keys
  alias Ankusa.Admin.Auth.{MTLS, Token}
  alias Ankusa.Principal

  @issuer "https://idp.test/"
  @audience "ankusa-admin"
  @tls_dir Path.expand("../../fixtures/tls", __DIR__)

  # ── helpers ────────────────────────────────────────────────────────────────

  # A conn as a router's plug pipeline sees it: the instance already assigned.
  defp conn(instance, headers \\ []) do
    base =
      Plug.Test.conn(:get, "/v1/config") |> Plug.Conn.assign(:ankusa_opts, instance: instance)

    Enum.reduce(headers, base, fn {k, v}, c -> Plug.Conn.put_req_header(c, k, v) end)
  end

  defp run(instance, headers \\ []) do
    instance |> conn(headers) |> Auth.call(:admin)
  end

  defp bearer(token), do: [{"authorization", "Bearer " <> token}]

  defp start_instance_config(auth) do
    config = test_config(admin: %{enabled: true, auth: auth})
    put_config(config)
    config
  end

  defp token_entry(id, token, scopes), do: %{id: id, sha256: Token.hash(token), scopes: scopes}

  # ── Principal ──────────────────────────────────────────────────────────────

  describe "Ankusa.Principal" do
    test "parses scopes, tenant scopes and unknown entries apart" do
      assert {["dlq:read", "sources:write"], ["acme", "globex"], ["openid"]} =
               Principal.parse_scopes([
                 "dlq:read",
                 "tenant:acme",
                 "openid",
                 "sources:write",
                 "tenant:globex"
               ])

      assert {[], :all, []} = Principal.parse_scopes(["tenant:*", "tenant:acme"])
    end

    test "new/3 keeps this system's scopes, merges the tenants claim and honours *" do
      p = Principal.new("alice", ["openid", "dlq:read", "tenant:acme"], ["globex"])
      assert p.id == "alice"
      assert p.scopes == ["dlq:read"]
      assert p.tenants == ["acme", "globex"]
      assert Principal.tenant?(p, "acme")
      refute Principal.tenant?(p, "initech")
      refute Principal.all_tenants?(p)

      assert Principal.new("bob", ["dlq:read"], ["*"]).tenants == :all
      assert Principal.new("bob", ["dlq:read", "tenant:*"], ["acme"]).tenants == :all
    end

    test "the operator holds every scope and every tenant" do
      p = Principal.operator()
      assert Enum.all?(Principal.scopes(), &Principal.scope?(p, &1))
      assert Principal.all_tenants?(p) and Principal.tenant?(p, "anyone")
    end
  end

  # ── bearer extraction ──────────────────────────────────────────────────────

  describe "the Authorization header" do
    test "accepts Bearer in any case and nothing else" do
      assert {:ok, "abc"} = Auth.bearer(conn(:x, [{"authorization", "Bearer abc"}]))
      assert {:ok, "abc"} = Auth.bearer(conn(:x, [{"authorization", "bearer abc"}]))
      assert :error = Auth.bearer(conn(:x))
      assert :error = Auth.bearer(conn(:x, [{"authorization", "Basic abc"}]))
      assert :error = Auth.bearer(conn(:x, [{"authorization", "Bearer "}]))
      assert :error = Auth.bearer(conn(:x, [{"authorization", "Bearer a b"}]))

      assert :error =
               Auth.bearer(
                 conn(:x, [{"authorization", "Bearer " <> String.duplicate("a", 9_000)}])
               )
    end

    test "two Authorization headers are refused" do
      conn =
        conn(:x)
        |> Map.update!(
          :req_headers,
          &(&1 ++ [{"authorization", "Bearer a"}, {"authorization", "Bearer b"}])
        )

      assert :error = Auth.bearer(conn)
    end
  end

  # ── mode: :none ────────────────────────────────────────────────────────────

  test "mode none hands every request the operator, and /health and /ready are exempt anyway" do
    config = start_instance_config(%{mode: :none})
    conn = run(config.instance)
    refute conn.halted
    assert conn.assigns.principal == Principal.operator()
  end

  test "an exempt path gets a principal with no scopes, with no credential, in any mode" do
    config =
      start_instance_config(%{mode: :token, tokens: [token_entry("ci", "t", ["dlq:read"])]})

    for path <- ["/health", "/ready"] do
      conn =
        Plug.Test.conn(:get, path)
        |> Plug.Conn.assign(:ankusa_opts, instance: config.instance)
        |> Auth.call(:admin)

      refute conn.halted
      assert %Principal{id: "anonymous", scopes: []} = conn.assigns.principal
    end
  end

  # ── mode: :token ───────────────────────────────────────────────────────────

  describe "mode token" do
    setup do
      tokens = [
        token_entry("ci", "ci-token-aaaa", ["dlq:read", "tenant:acme"]),
        token_entry("ops", "ops-token-bbbb", ["config:read", "tenant:*"])
      ]

      config = start_instance_config(%{mode: :token, tokens: tokens})
      %{inst: config.instance}
    end

    test "a known token is its entry's principal", %{inst: inst} do
      conn = run(inst, bearer("ci-token-aaaa"))
      refute conn.halted

      assert %Principal{id: "ci", scopes: ["dlq:read"], tenants: ["acme"]} =
               conn.assigns.principal

      assert %Principal{id: "ops", tenants: :all} =
               run(inst, bearer("ops-token-bbbb")).assigns.principal
    end

    test "401 with a challenge for no credential, a wrong token, or another scheme", %{inst: inst} do
      for headers <- [
            [],
            bearer("nope"),
            bearer("ci-token-aaa"),
            [{"authorization", "Basic Y2k6Y2k="}]
          ] do
        conn = run(inst, headers)
        assert conn.halted
        assert conn.status == 401
        assert %{"error" => "unauthenticated"} = JSON.decode!(conn.resp_body)
        assert Plug.Conn.get_resp_header(conn, "www-authenticate") == [~s(Bearer realm="ankusa")]
      end
    end

    test "the hash is the lowercase hex sha256 of the token" do
      assert Token.hash("abc") ==
               "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    end
  end

  # ── mode: :mtls ────────────────────────────────────────────────────────────

  describe "mtls principal" do
    defp pem_der(name) do
      [{:Certificate, der, _}] =
        @tls_dir |> Path.join(name) |> File.read!() |> :public_key.pem_decode()

      der
    end

    defp mtls_conn(der) do
      Plug.Test.conn(:get, "/v1/config")
      |> Plug.Test.put_peer_data(%{address: {127, 0, 0, 1}, port: 1, ssl_cert: der})
    end

    @opts %{
      instance: :unused,
      section: :admin,
      config: %{
        mode: :mtls,
        mtls: %{subject_map: %{"cn:ankusa-test-client" => ["dlq:read", "tenant:acme"]}}
      }
    }

    test "the certificate's common name is mapped to a principal" do
      assert {:ok,
              %Principal{id: "cn:ankusa-test-client", scopes: ["dlq:read"], tenants: ["acme"]}} =
               MTLS.authenticate(mtls_conn(pem_der("client.pem")), @opts)
    end

    test "a valid certificate nobody mapped, or no certificate, is unauthenticated" do
      assert {:error, :unauthenticated} =
               MTLS.authenticate(mtls_conn(pem_der("stranger.pem")), @opts)

      assert {:error, :unauthenticated} = MTLS.authenticate(mtls_conn(nil), @opts)
      assert {:error, :unauthenticated} = MTLS.authenticate(mtls_conn("not a certificate"), @opts)
    end

    test "identities lists the common name of a certificate" do
      assert MTLS.identities(pem_der("client.pem")) == ["cn:ankusa-test-client"]
      assert MTLS.identities(pem_der("server.pem")) |> Enum.member?("cn:localhost")
      assert MTLS.identities("garbage") == []
    end

    test "the subject alternative names come before the common name" do
      %{client_config: client} =
        :public_key.pkix_test_data(%{
          server_chain: %{root: [], peer: []},
          client_chain: %{
            root: [],
            peer: [
              extensions: [
                {:Extension, {2, 5, 29, 17}, false,
                 [
                   {:dNSName, ~c"ops.example.com"},
                   {:rfc822Name, ~c"ops@example.com"},
                   {:uniformResourceIdentifier, ~c"spiffe://prod/ops"},
                   {:iPAddress, <<10, 0, 0, 7>>}
                 ]}
              ]
            ]
          }
        })

      der = Keyword.fetch!(client, :cert)

      assert [
               "dns:ops.example.com",
               "email:ops@example.com",
               "uri:spiffe://prod/ops",
               "ip:10.0.0.7" | _common_names
             ] = MTLS.identities(der)

      opts = put_in(@opts.config.mtls.subject_map, %{"uri:spiffe://prod/ops" => ["metrics:read"]})

      assert {:ok, %Principal{id: "uri:spiffe://prod/ops", scopes: ["metrics:read"]}} =
               MTLS.authenticate(mtls_conn(der), opts)
    end
  end

  # ── mode: :jwt ─────────────────────────────────────────────────────────────

  describe "mode jwt" do
    defp keypair(kid \\ "k1") do
      jwk = JOSE.JWK.generate_key({:ec, "P-256"})
      {_, public} = jwk |> JOSE.JWK.to_public() |> JOSE.JWK.to_map()
      {jwk, Map.put(public, "kid", kid)}
    end

    defp claims(overrides \\ %{}) do
      now = System.os_time(:second)

      Map.merge(
        %{
          "iss" => @issuer,
          "aud" => @audience,
          "sub" => "alice@example.com",
          "exp" => now + 300,
          "iat" => now,
          "scope" => "openid dlq:read config:read tenant:acme",
          "ankusa_tenants" => ["globex"]
        },
        overrides
      )
    end

    defp sign(jwk, claims, header \\ %{}) do
      header = Map.merge(%{"alg" => "ES256", "kid" => "k1"}, header)
      {_, compact} = jwk |> JOSE.JWT.sign(header, claims) |> JOSE.JWS.compact()
      compact
    end

    defp b64(term), do: term |> JSON.encode!() |> Base.url_encode64(padding: false)

    defp jwt_instance(extra \\ %{}, keys) do
      jwt =
        Map.merge(
          %{issuer: @issuer, audience: @audience, keys: keys, leeway_s: 0},
          extra
        )

      config = start_instance_config(%{mode: :jwt, jwt: jwt})
      start_supervised!({Keys, instance: config.instance})
      config.instance
    end

    test "a valid token is its subject's principal, scopes and tenants from the claims" do
      {jwk, public} = keypair()
      inst = jwt_instance([public])

      conn = run(inst, bearer(sign(jwk, claims())))
      refute conn.halted

      assert %Principal{
               id: "alice@example.com",
               scopes: ["dlq:read", "config:read"],
               tenants: ["acme", "globex"]
             } = conn.assigns.principal
    end

    test "scope may be a list, and ankusa_tenants * is every tenant" do
      {jwk, public} = keypair()
      inst = jwt_instance([public])

      token = sign(jwk, claims(%{"scope" => ["dlq:read"], "ankusa_tenants" => ["*"]}))

      assert %Principal{scopes: ["dlq:read"], tenants: :all} =
               run(inst, bearer(token)).assigns.principal
    end

    test "the claim names are configurable" do
      {jwk, public} = keypair()
      inst = jwt_instance(%{scope_claim: "scp", tenants_claim: "orgs"}, [public])

      token =
        sign(jwk, claims(%{"scp" => ["sources:read"], "orgs" => ["acme"], "scope" => "dlq:read"}))

      assert %Principal{scopes: ["sources:read"], tenants: ["acme"]} =
               run(inst, bearer(token)).assigns.principal
    end

    test "401 for a token that is wrong in any one way" do
      {jwk, public} = keypair()
      {other, _} = keypair()
      inst = jwt_instance([public])
      now = System.os_time(:second)

      bad = %{
        "wrong issuer" => sign(jwk, claims(%{"iss" => "https://evil.test/"})),
        "wrong audience" => sign(jwk, claims(%{"aud" => "someone-else"})),
        "no audience" => sign(jwk, Map.delete(claims(), "aud")),
        "expired" => sign(jwk, claims(%{"exp" => now - 1})),
        "no exp" => sign(jwk, Map.delete(claims(), "exp")),
        "not yet valid" => sign(jwk, claims(%{"nbf" => now + 60})),
        "no sub" => sign(jwk, Map.delete(claims(), "sub")),
        "signed by another key" => sign(other, claims()),
        "unknown kid" => sign(jwk, claims(), %{"kid" => "k9"}),
        "garbage" => "not.a.jwt",
        "empty" => "a"
      }

      for {why, token} <- bad do
        conn = run(inst, bearer(token))
        assert conn.status == 401, "#{why} was accepted"
        assert conn.halted
      end

      # The good one beside them, so the list above is not 401 for a stray reason.
      refute run(inst, bearer(sign(jwk, claims()))).halted
    end

    test "a tampered payload fails the signature" do
      {jwk, public} = keypair()
      inst = jwt_instance([public])

      [header, _payload, signature] = jwk |> sign(claims()) |> String.split(".")
      forged = b64(claims(%{"scope" => "config:write tenant:*"}))

      assert run(inst, bearer(Enum.join([header, forged, signature], "."))).status == 401
    end

    test "alg none and a symmetric alg are never accepted" do
      {jwk, public} = keypair()
      inst = jwt_instance([public])

      none = Enum.join([b64(%{"alg" => "none", "kid" => "k1"}), b64(claims()), ""], ".")
      assert run(inst, bearer(none)).status == 401

      # An HS256 token whose secret is the public key's own bytes: the classic
      # confusion between the algorithm family a key is for.
      {_, pem} = jwk |> JOSE.JWK.to_public() |> JOSE.JWK.to_pem()

      {_, hs} =
        pem
        |> JOSE.JWK.from_oct()
        |> JOSE.JWT.sign(%{"alg" => "HS256", "kid" => "k1"}, claims())
        |> JOSE.JWS.compact()

      assert run(inst, bearer(hs)).status == 401
    end

    test "leeway forgives a little clock skew in either direction" do
      {jwk, public} = keypair()
      inst = jwt_instance(%{leeway_s: 30}, [public])
      now = System.os_time(:second)

      refute run(inst, bearer(sign(jwk, claims(%{"exp" => now - 10})))).halted
      refute run(inst, bearer(sign(jwk, claims(%{"nbf" => now + 10})))).halted
      assert run(inst, bearer(sign(jwk, claims(%{"exp" => now - 60})))).halted
      assert run(inst, bearer(sign(jwk, claims(%{"nbf" => now + 60})))).halted
    end

    test "audience may be a list on either side" do
      {jwk, public} = keypair()
      inst = jwt_instance(%{audience: ["a", @audience]}, [public])

      refute run(inst, bearer(sign(jwk, claims(%{"aud" => [@audience, "x"]})))).halted
      refute run(inst, bearer(sign(jwk, claims(%{"aud" => "a"})))).halted
      assert run(inst, bearer(sign(jwk, claims(%{"aud" => ["x", "y"]})))).halted
    end

    test "a PEM key with no kid verifies a token with any kid, or none" do
      {jwk, _public} = keypair()
      {_, pem} = jwk |> JOSE.JWK.to_public() |> JOSE.JWK.to_pem()
      inst = jwt_instance([pem])

      refute run(inst, bearer(sign(jwk, claims()))).halted

      {_, no_kid} =
        jwk |> JOSE.JWT.sign(%{"alg" => "ES256"}, claims()) |> JOSE.JWS.compact()

      refute run(inst, bearer(no_kid)).halted
    end

    test "no key loaded means no token is accepted" do
      {jwk, _public} = keypair()
      # A jwks_url nothing answers on: the keys never arrive.
      inst = jwt_instance(%{keys: [], jwks_url: "http://127.0.0.1:#{free_port()}/jwks"}, [])
      assert run(inst, bearer(sign(jwk, claims()))).status == 401
    end

    test "a private or shared key in the configuration is refused" do
      {jwk, _public} = keypair()
      {_, private} = JOSE.JWK.to_map(jwk)
      {_, oct} = "secret" |> JOSE.JWK.from_oct() |> JOSE.JWK.to_map()

      for key <- [private, oct, "not a key"] do
        assert_raise ArgumentError, ~r/must be a public RSA, EC or OKP key/, fn ->
          Auth.public_key!(key, "admin.auth.jwt.keys[0]")
        end
      end
    end
  end

  # ── the JWKS URL ───────────────────────────────────────────────────────────

  describe "jwks_url" do
    defmodule JWKS do
      @moduledoc false
      @behaviour Plug

      def init(opts), do: opts

      def call(conn, agent: agent) do
        {status, body} = Agent.get(agent, & &1)
        Plug.Conn.send_resp(conn, status, body)
      end
    end

    defp jwks_body(publics), do: JSON.encode!(%{"keys" => publics})

    defp serve_jwks(initial) do
      agent = start_supervised!({Agent, fn -> initial end})
      port = free_port()
      start_supervised!({Bandit, plug: {JWKS, agent: agent}, port: port, ip: {127, 0, 0, 1}})
      {agent, "http://127.0.0.1:#{port}/jwks.json"}
    end

    defp jwks_instance(url, extra \\ %{}) do
      jwt =
        Map.merge(
          %{issuer: @issuer, audience: @audience, jwks_url: url, min_refresh_ms: 0},
          extra
        )

      config = start_instance_config(%{mode: :jwt, jwt: jwt})
      start_supervised!({Keys, instance: config.instance})
      config.instance
    end

    defp eventually(fun, tries \\ 100) do
      cond do
        fun.() -> :ok
        tries == 0 -> flunk("condition never held")
        true -> Process.sleep(50) && eventually(fun, tries - 1)
      end
    end

    test "the keys are fetched after start, without blocking it" do
      {jwk, public} = keypair()
      {_agent, url} = serve_jwks({200, jwks_body([public])})
      inst = jwks_instance(url)
      token = sign(jwk, claims())

      eventually(fn -> not run(inst, bearer(token)).halted end)
    end

    test "an unknown kid triggers a refresh, so a rotated key works on the next request" do
      {old, old_public} = keypair("old")
      {new, new_public} = keypair("new")
      {agent, url} = serve_jwks({200, jwks_body([old_public])})
      inst = jwks_instance(url)

      eventually(fn -> not run(inst, bearer(sign(old, claims(), %{"kid" => "old"}))).halted end)

      Agent.update(agent, fn _ -> {200, jwks_body([old_public, new_public])} end)
      rotated = sign(new, claims(), %{"kid" => "new"})

      # The first request names a kid we do not have: refused, and a refresh is asked for.
      assert run(inst, bearer(rotated)).status == 401
      eventually(fn -> not run(inst, bearer(rotated)).halted end)
    end

    test "a failing JWKS endpoint keeps the keys already loaded" do
      {jwk, public} = keypair()
      {agent, url} = serve_jwks({200, jwks_body([public])})
      inst = jwks_instance(url)
      token = sign(jwk, claims())

      eventually(fn -> not run(inst, bearer(token)).halted end)

      Agent.update(agent, fn _ -> {500, "boom"} end)

      assert {:error, {:status, 500}} =
               ExUnit.CaptureLog.with_log(fn -> Keys.reload(inst, :admin) end) |> elem(0)

      refute run(inst, bearer(token)).halted

      Agent.update(agent, fn _ -> {200, "not json"} end)

      assert {:error, :invalid_jwks} =
               ExUnit.CaptureLog.with_log(fn -> Keys.reload(inst, :admin) end) |> elem(0)

      refute run(inst, bearer(token)).halted
    end

    test "only public signing keys of a JWKS are kept" do
      {jwk, public} = keypair()
      {_, private} = JOSE.JWK.to_map(jwk)
      {_, oct} = "secret" |> JOSE.JWK.from_oct() |> JOSE.JWK.to_map()
      enc = Map.put(public, "use", "enc")

      {_agent, url} = serve_jwks({200, jwks_body([private, oct, enc])})
      inst = jwks_instance(url)

      assert {:error, :no_usable_keys} =
               ExUnit.CaptureLog.with_log(fn -> Keys.reload(inst, :admin) end) |> elem(0)

      assert Keys.candidates(inst, :admin, nil) == []
    end
  end
end
