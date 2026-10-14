defmodule Ankusa.Auth.OIDCTest do
  @moduledoc """
  `Ankusa.Auth.OIDC` against a fake identity provider (Bandit on a port the OS
  picks) serving discovery and a JWKS, with tokens signed by real keys.
  """

  use ExUnit.Case, async: false

  alias Ankusa.Auth.OIDC

  # The identity provider: its keys, what it answers, and how often it was asked.
  defmodule IdP do
    @behaviour Plug

    import Plug.Conn

    def start(opts \\ []) do
      port = Keyword.get(opts, :port) || free_port()

      {:ok, state} =
        Agent.start_link(fn ->
          %{
            keys: [],
            mode: :ok,
            content_type: "application/json",
            hits: %{discovery: 0, jwks: 0},
            issuer_in_discovery: nil,
            base: "http://127.0.0.1:#{port}"
          }
        end)

      {:ok, _server} =
        Bandit.start_link(
          plug: {__MODULE__, state},
          scheme: :http,
          ip: :loopback,
          port: port,
          startup_log: false
        )

      %{state: state, base: "http://127.0.0.1:#{port}", port: port}
    end

    def free_port do
      {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false])
      {:ok, port} = :inet.port(socket)
      :ok = :gen_tcp.close(socket)
      port
    end

    def put(%{state: state}, key, value), do: Agent.update(state, &Map.put(&1, key, value))
    def hits(%{state: state}, what), do: Agent.get(state, & &1.hits[what])

    @impl true
    def init(state), do: state

    @impl true
    def call(conn, state) do
      snapshot = Agent.get(state, & &1)

      case {snapshot.mode, conn.request_path} do
        {:down, _} ->
          send_resp(conn, 503, "down")

        {_, "/.well-known/openid-configuration"} ->
          Agent.update(state, &update_in(&1.hits.discovery, fn n -> n + 1 end))

          body = %{
            "issuer" => snapshot.issuer_in_discovery || snapshot.base,
            "jwks_uri" => snapshot.base <> "/jwks"
          }

          conn |> put_resp_content_type("application/json") |> send_resp(200, JSON.encode!(body))

        {_, "/jwks"} ->
          Agent.update(state, &update_in(&1.hits.jwks, fn n -> n + 1 end))

          conn
          |> put_resp_header("content-type", snapshot.content_type)
          |> send_resp(200, JSON.encode!(%{"keys" => snapshot.keys}))

        _ ->
          send_resp(conn, 404, "")
      end
    end
  end

  @audience "ankusa"

  setup_all do
    %{
      k1: key("k1", {:rsa, 2048}),
      k2: key("k2", {:rsa, 2048}),
      attacker: key("k1", {:rsa, 2048}),
      ec: key("ec1", {:ec, "P-256"})
    }
  end

  defp key(kid, spec) do
    jwk = JOSE.JWK.generate_key(spec)
    {_, public} = jwk |> JOSE.JWK.to_public() |> JOSE.JWK.to_map()
    %{kid: kid, jwk: jwk, public: Map.merge(public, %{"kid" => kid, "use" => "sig"})}
  end

  defp setup_verifier(idp, extra \\ []) do
    instance = :"oidc#{System.unique_integer([:positive])}"

    opts =
      Keyword.merge(
        [
          issuer: idp.base,
          audience: @audience,
          min_refresh_interval_ms: 100,
          http_timeout_ms: 1_000
        ],
        extra
      )

    Ankusa.put_config(
      Ankusa.Config.new(
        instance: instance,
        data_dir: Path.join(System.tmp_dir!(), "ankusa_#{instance}"),
        auth: %{enabled: true, verifier: {OIDC, opts}}
      )
    )

    start_supervised!({OIDC, instance: instance}, id: instance)
    {instance, opts}
  end

  defp idp_serving(keys) do
    idp = IdP.start()
    IdP.put(idp, :keys, Enum.map(keys, & &1.public))
    idp
  end

  defp claims(idp, overrides \\ %{}) do
    now = System.system_time(:second)

    Map.merge(
      %{
        "iss" => idp.base,
        "aud" => @audience,
        "sub" => "u-1",
        "exp" => now + 300,
        "preferred_username" => "jane@corp.com",
        "groups" => ["Ops"]
      },
      overrides
    )
  end

  defp sign(key, claims, header \\ %{}) do
    jws = Map.merge(%{"alg" => "RS256", "kid" => key.kid}, header)
    {_, token} = key.jwk |> JOSE.JWT.sign(jws, claims) |> JOSE.JWS.compact()
    token
  end

  defp b64(map), do: map |> JSON.encode!() |> Base.url_encode64(padding: false)

  describe "a valid token" do
    test "verifies and yields its claims", %{k1: k1} do
      idp = idp_serving([k1])
      {instance, opts} = setup_verifier(idp)
      claims = claims(idp)

      assert {:ok, ^claims} = OIDC.verify(instance, sign(k1, claims), opts)
    end

    test "an ES256 token, when the algorithm is allowed", %{ec: ec} do
      idp = idp_serving([ec])
      {instance, opts} = setup_verifier(idp)

      token = sign(ec, claims(idp), %{"alg" => "ES256"})
      assert {:ok, %{"sub" => "u-1"}} = OIDC.verify(instance, token, opts)
    end

    test "a key set served as application/jwk-set+json", %{k1: k1} do
      idp = idp_serving([k1])
      IdP.put(idp, :content_type, "application/jwk-set+json")
      {instance, opts} = setup_verifier(idp)

      assert {:ok, _} = OIDC.verify(instance, sign(k1, claims(idp)), opts)
    end

    test "a token with no kid is tried against every key", %{k1: k1, k2: k2} do
      idp = idp_serving([k1, k2])
      {instance, opts} = setup_verifier(idp)

      token = sign(k2, claims(idp), %{"kid" => nil})
      {:ok, header} = token |> JOSE.JWS.peek_protected() |> JSON.decode()
      refute header["kid"]
      assert {:ok, _} = OIDC.verify(instance, token, opts)
    end

    test "an explicit jwks_uri skips discovery", %{k1: k1} do
      idp = idp_serving([k1])
      {instance, opts} = setup_verifier(idp, jwks_uri: idp.base <> "/jwks")

      assert {:ok, _} = OIDC.verify(instance, sign(k1, claims(idp)), opts)
      assert IdP.hits(idp, :discovery) == 0
    end
  end

  describe "claims" do
    setup %{k1: k1} do
      idp = idp_serving([k1])
      {instance, opts} = setup_verifier(idp)
      %{idp: idp, instance: instance, opts: opts, k1: k1}
    end

    defp verify(ctx, claims), do: OIDC.verify(ctx.instance, sign(ctx.k1, claims), ctx.opts)

    test "the issuer must match exactly", ctx do
      assert verify(ctx, claims(ctx.idp, %{"iss" => ctx.idp.base <> "/"})) ==
               {:error, :invalid_token}

      assert verify(ctx, claims(ctx.idp, %{"iss" => "https://evil.example"})) ==
               {:error, :invalid_token}

      assert verify(ctx, Map.delete(claims(ctx.idp), "iss")) == {:error, :invalid_token}
    end

    test "the audience: a string must equal it, a list must contain it", ctx do
      assert {:ok, _} = verify(ctx, claims(ctx.idp, %{"aud" => ["other", @audience]}))
      assert verify(ctx, claims(ctx.idp, %{"aud" => "other"})) == {:error, :invalid_token}
      assert verify(ctx, claims(ctx.idp, %{"aud" => ["a", "b"]})) == {:error, :invalid_token}
      assert verify(ctx, claims(ctx.idp, %{"aud" => []})) == {:error, :invalid_token}
      assert verify(ctx, Map.delete(claims(ctx.idp), "aud")) == {:error, :invalid_token}
      assert verify(ctx, claims(ctx.idp, %{"aud" => 42})) == {:error, :invalid_token}
    end

    test "exp is required, and may be past by the leeway only", ctx do
      now = System.system_time(:second)

      assert verify(ctx, Map.delete(claims(ctx.idp), "exp")) == {:error, :invalid_token}
      assert verify(ctx, claims(ctx.idp, %{"exp" => "soon"})) == {:error, :invalid_token}
      assert verify(ctx, claims(ctx.idp, %{"exp" => now - 61})) == {:error, :invalid_token}
      assert verify(ctx, claims(ctx.idp, %{"exp" => now - 3_600})) == {:error, :invalid_token}
      assert {:ok, _} = verify(ctx, claims(ctx.idp, %{"exp" => now - 30}))
    end

    test "nbf in the future beyond the leeway is refused", ctx do
      now = System.system_time(:second)

      assert verify(ctx, claims(ctx.idp, %{"nbf" => now + 3_600})) == {:error, :invalid_token}
      assert verify(ctx, claims(ctx.idp, %{"nbf" => now + 61})) == {:error, :invalid_token}
      assert verify(ctx, claims(ctx.idp, %{"nbf" => "later"})) == {:error, :invalid_token}
      assert {:ok, _} = verify(ctx, claims(ctx.idp, %{"nbf" => now + 30}))
      assert {:ok, _} = verify(ctx, claims(ctx.idp, %{"nbf" => now - 10}))
    end

    test "the leeway is configurable", %{k1: k1} do
      idp = idp_serving([k1])
      now = System.system_time(:second)

      {strict, strict_opts} = setup_verifier(idp, leeway_seconds: 0)
      {lenient, lenient_opts} = setup_verifier(idp, leeway_seconds: 600)

      five_seconds_late = sign(k1, claims(idp, %{"exp" => now - 5}))
      assert OIDC.verify(strict, five_seconds_late, strict_opts) == {:error, :invalid_token}
      assert {:ok, _} = OIDC.verify(lenient, five_seconds_late, lenient_opts)

      five_minutes_late = sign(k1, claims(idp, %{"exp" => now - 300}))
      assert {:ok, _} = OIDC.verify(lenient, five_minutes_late, lenient_opts)
    end
  end

  describe "tokens that must never verify" do
    setup %{k1: k1, attacker: attacker} do
      idp = idp_serving([k1])
      {instance, opts} = setup_verifier(idp)
      %{idp: idp, instance: instance, opts: opts, k1: k1, attacker: attacker}
    end

    test "alg none", ctx do
      unsigned =
        b64(%{"alg" => "none", "typ" => "JWT", "kid" => "k1"}) <>
          "." <> b64(claims(ctx.idp)) <> "."

      assert OIDC.verify(ctx.instance, unsigned, ctx.opts) == {:error, :invalid_token}

      # and with the empty signature spelled out as a third segment
      assert OIDC.verify(ctx.instance, unsigned <> "x", ctx.opts) == {:error, :invalid_token}
    end

    test "an HS256 token signed with the public key's own bytes", ctx do
      {_, pem} = ctx.k1.jwk |> JOSE.JWK.to_public() |> JOSE.JWK.to_pem()

      for secret <- [pem, JSON.encode!(ctx.k1.public)] do
        hmac = JOSE.JWK.from_oct(secret)

        {_, token} =
          hmac
          |> JOSE.JWT.sign(%{"alg" => "HS256", "kid" => "k1"}, claims(ctx.idp))
          |> JOSE.JWS.compact()

        assert OIDC.verify(ctx.instance, token, ctx.opts) == {:error, :invalid_token}
      end
    end

    test "an algorithm the configuration does not allow", ctx do
      token = sign(ctx.k1, claims(ctx.idp), %{"alg" => "RS384"})
      assert OIDC.verify(ctx.instance, token, ctx.opts) == {:error, :invalid_token}

      assert OIDC.verify(
               ctx.instance,
               sign(ctx.k1, claims(ctx.idp), %{"alg" => "PS256"}),
               ctx.opts
             ) == {:error, :invalid_token}
    end

    test "a token signed by someone else under the provider's kid", ctx do
      assert OIDC.verify(ctx.instance, sign(ctx.attacker, claims(ctx.idp)), ctx.opts) ==
               {:error, :invalid_token}
    end

    test "a payload changed after signing", ctx do
      [header, _payload, signature] = ctx.k1 |> sign(claims(ctx.idp)) |> String.split(".")
      forged = b64(claims(ctx.idp, %{"sub" => "admin", "groups" => ["Admins"]}))

      assert OIDC.verify(ctx.instance, Enum.join([header, forged, signature], "."), ctx.opts) ==
               {:error, :invalid_token}
    end

    test "garbage is invalid, never a crash", ctx do
      for token <- [
            "",
            "a",
            "a.b",
            "a.b.c",
            "....",
            "💥",
            <<0, 255, 254>>,
            String.duplicate("a", 100_000),
            b64(%{}) <> "." <> b64(%{}) <> ".",
            "eyJhbGciOiJSUzI1NiIsImtpZCI6MX0.e30.c2ln"
          ] do
        assert OIDC.verify(ctx.instance, token, ctx.opts) == {:error, :invalid_token}
      end

      assert OIDC.verify(ctx.instance, nil, ctx.opts) == {:error, :invalid_token}
      assert OIDC.verify(ctx.instance, 12_345, ctx.opts) == {:error, :invalid_token}
    end

    test "a kid that is not a string", ctx do
      token = sign(ctx.k1, claims(ctx.idp), %{"kid" => 7})
      assert OIDC.verify(ctx.instance, token, ctx.opts) == {:error, :invalid_token}
    end

    test "a key the provider marks for encryption is never used to verify", %{k1: k1, k2: k2} do
      idp = IdP.start()
      IdP.put(idp, :keys, [k1.public, Map.put(k2.public, "use", "enc")])
      {instance, opts} = setup_verifier(idp)

      assert {:ok, _} = OIDC.verify(instance, sign(k1, claims(idp)), opts)
      assert OIDC.verify(instance, sign(k2, claims(idp)), opts) == {:error, :invalid_token}
    end

    test "a symmetric key in the JWKS is not a verification key", %{k1: k1} do
      idp = IdP.start()
      oct = %{"kty" => "oct", "kid" => "hmac", "k" => Base.url_encode64("secret", padding: false)}
      IdP.put(idp, :keys, [k1.public, oct])
      {instance, opts} = setup_verifier(idp, algorithms: ["RS256", "ES256"])

      {_, token} =
        oct
        |> JOSE.JWK.from_map()
        |> JOSE.JWT.sign(%{"alg" => "HS256", "kid" => "hmac"}, claims(idp))
        |> JOSE.JWS.compact()

      assert OIDC.verify(instance, token, opts) == {:error, :invalid_token}
    end
  end

  describe "key rotation" do
    test "a new kid is found by one refresh, and unknown kids cannot make it hammer the provider",
         %{k1: k1, k2: k2} do
      idp = idp_serving([k1])
      {instance, opts} = setup_verifier(idp)

      assert {:ok, _} = OIDC.verify(instance, sign(k1, claims(idp)), opts)
      assert IdP.hits(idp, :jwks) == 1

      # the provider rotates; a token under the new key arrives after the cooldown
      IdP.put(idp, :keys, [k2.public])
      Process.sleep(150)

      assert {:ok, _} = OIDC.verify(instance, sign(k2, claims(idp)), opts)
      assert IdP.hits(idp, :jwks) == 2

      # a kid nobody has: refused, and inside the cooldown, not even asked about
      stranger = key("k3", {:rsa, 2048})
      assert OIDC.verify(instance, sign(stranger, claims(idp)), opts) == {:error, :invalid_token}
      assert OIDC.verify(instance, sign(stranger, claims(idp)), opts) == {:error, :invalid_token}
      assert IdP.hits(idp, :jwks) == 2

      # after the cooldown, the next unknown kid refreshes once
      Process.sleep(150)
      assert OIDC.verify(instance, sign(stranger, claims(idp)), opts) == {:error, :invalid_token}
      assert IdP.hits(idp, :jwks) == 3
    end

    test "a retired key stops verifying once the keys are refreshed", %{k1: k1, k2: k2} do
      idp = idp_serving([k1])
      {instance, opts} = setup_verifier(idp)
      assert {:ok, _} = OIDC.verify(instance, sign(k1, claims(idp)), opts)

      IdP.put(idp, :keys, [k2.public])
      Process.sleep(150)
      # the refresh a new kid triggers replaces the key set
      assert {:ok, _} = OIDC.verify(instance, sign(k2, claims(idp)), opts)
      assert OIDC.verify(instance, sign(k1, claims(idp)), opts) == {:error, :invalid_token}
    end

    test "a failing refresh keeps the keys it has", %{k1: k1} do
      idp = idp_serving([k1])
      {instance, opts} = setup_verifier(idp)
      assert {:ok, _} = OIDC.verify(instance, sign(k1, claims(idp)), opts)

      IdP.put(idp, :mode, :down)
      Process.sleep(150)

      # an unknown kid forces a fetch, which fails
      stranger = key("k9", {:rsa, 2048})
      assert OIDC.verify(instance, sign(stranger, claims(idp)), opts) == {:error, :invalid_token}
      assert {:ok, _} = OIDC.verify(instance, sign(k1, claims(idp)), opts)
    end
  end

  describe "the provider is not there" do
    test "unavailable until it is, then verifies", %{k1: k1} do
      port = IdP.free_port()
      base = "http://127.0.0.1:#{port}"
      idp_stub = %{base: base}
      {instance, opts} = setup_verifier(idp_stub)

      token = sign(k1, claims(idp_stub))
      assert OIDC.verify(instance, token, opts) == {:error, :unavailable}
      assert OIDC.verify(instance, token, opts) == {:error, :unavailable}

      idp = IdP.start(port: port)
      IdP.put(idp, :keys, [k1.public])
      Process.sleep(150)

      assert {:ok, %{"sub" => "u-1"}} = OIDC.verify(instance, token, opts)
    end

    test "a provider that answers errors is unavailable, not a verdict on the token", %{k1: k1} do
      idp = idp_serving([k1])
      IdP.put(idp, :mode, :down)
      {instance, opts} = setup_verifier(idp)

      assert OIDC.verify(instance, sign(k1, claims(idp)), opts) == {:error, :unavailable}
    end

    test "discovery that names another issuer is refused", %{k1: k1} do
      idp = idp_serving([k1])
      IdP.put(idp, :issuer_in_discovery, "https://evil.example")
      {instance, opts} = setup_verifier(idp)

      assert OIDC.verify(instance, sign(k1, claims(idp)), opts) == {:error, :unavailable}
    end

    test "no verifier process at all is unavailable", %{k1: k1} do
      assert OIDC.verify(:nobody_home, sign(k1, %{"exp" => 1}), issuer: "x", audience: "y") ==
               {:error, :unavailable}
    end
  end

  describe "validate_opts!/1" do
    test "accepts a minimal and a full configuration" do
      assert :ok = OIDC.validate_opts!(issuer: "https://idp", audience: "a")

      assert :ok =
               OIDC.validate_opts!(
                 issuer: "https://idp",
                 audience: "a",
                 algorithms: ["RS256", "RS384", "RS512", "ES256", "ES384", "PS256"],
                 leeway_seconds: 0,
                 jwks_uri: "https://idp/keys",
                 refresh_interval_ms: 1,
                 min_refresh_interval_ms: 1,
                 http_timeout_ms: 1
               )
    end

    test "issuer and audience are required strings" do
      for opts <- [
            [],
            [issuer: "i"],
            [audience: "a"],
            [issuer: "", audience: "a"],
            [issuer: "i", audience: :a],
            [issuer: nil, audience: "a"]
          ] do
        assert_raise ArgumentError, ~r/auth.oidc.(issuer|audience) is required/, fn ->
          OIDC.validate_opts!(opts)
        end
      end
    end

    test "none and the HS family are refused, whatever else is listed" do
      for algorithms <- [["none"], ["HS256"], ["RS256", "HS512"], ["rs256"], ["EdDSA"], []] do
        assert_raise ArgumentError, ~r/auth.oidc.algorithms/, fn ->
          OIDC.validate_opts!(issuer: "i", audience: "a", algorithms: algorithms)
        end
      end

      assert_raise ArgumentError, ~r/never none, never HS\*/, fn ->
        OIDC.validate_opts!(issuer: "i", audience: "a", algorithms: ["HS256"])
      end
    end

    test "numbers are sane and unknown options are refused" do
      for {key, value} <- [
            leeway_seconds: -1,
            leeway_seconds: 1.5,
            http_timeout_ms: 0,
            refresh_interval_ms: "1h",
            min_refresh_interval_ms: 0
          ] do
        assert_raise ArgumentError, ~r/auth.oidc.#{key}/, fn ->
          OIDC.validate_opts!([{:issuer, "i"}, {:audience, "a"}, {key, value}])
        end
      end

      assert_raise ArgumentError, ~r/unknown auth.oidc option/, fn ->
        OIDC.validate_opts!(issuer: "i", audience: "a", audiences: ["x"])
      end
    end

    test "the instance runs it at boot" do
      config =
        Ankusa.Config.new(
          auth: %{
            enabled: true,
            verifier: {OIDC, issuer: "i", audience: "a", algorithms: ["HS256"]}
          }
        )

      assert_raise ArgumentError, ~r/auth.oidc.algorithms/, fn ->
        Ankusa.Auth.validate_config!(config)
      end
    end
  end
end
