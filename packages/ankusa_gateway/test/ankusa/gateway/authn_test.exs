defmodule Ankusa.Gateway.AuthnTest do
  @moduledoc """
  The authentication chain (`Ankusa.Gateway.Authn` and the `token`, `jwt` and
  `mtls` authenticators), driven through the plug as the router mounts it.
  """

  use ExUnit.Case, async: false

  alias Ankusa.Admin.Auth.Token
  alias Ankusa.Gateway.Authn
  alias Ankusa.Gateway.Config, as: GatewayConfig
  alias Ankusa.Principal

  @tls_dir Path.expand("../../../../ankusa/test/fixtures/tls", __DIR__)
  @first "https://first.test/"
  @second "https://second.test/"

  setup do
    inst = :"authn#{System.unique_integer([:positive])}"
    Ankusa.put_config(Ankusa.Config.new(instance: inst, roles: []))
    %{inst: inst}
  end

  # A gateway config with `authenticators`, stored, and its keys server (when
  # a `jwt` authenticator needs one) started.
  defp chain(inst, authenticators, opts \\ []) do
    config =
      GatewayConfig.new([instance: inst, auth: %{authenticators: authenticators}] ++ opts)

    Ankusa.Gateway.put_config(config)
    Enum.each(Ankusa.Gateway.Supervisor.keys_children(config), &start_supervised!/1)
    config
  end

  defp run(inst, headers \\ [], conn \\ Plug.Test.conn(:get, "/v1/claims/acme/x")) do
    conn
    |> Plug.Conn.assign(:ankusa_opts, instance: inst)
    |> then(&Enum.reduce(headers, &1, fn {k, v}, c -> Plug.Conn.put_req_header(c, k, v) end))
    |> Authn.call([])
  end

  defp bearer(token), do: [{"authorization", "Bearer " <> token}]

  defp token(id, scopes, extra \\ %{}),
    do: Map.merge(%{id: id, sha256: Token.hash(id), scopes: scopes}, extra)

  defp keypair(kid) do
    jwk = JOSE.JWK.generate_key({:ec, "P-256"})
    {_, public} = jwk |> JOSE.JWK.to_public() |> JOSE.JWK.to_map()
    {jwk, Map.put(public, "kid", kid)}
  end

  defp sign(jwk, kid, claims) do
    now = System.os_time(:second)

    claims =
      Map.merge(
        %{"aud" => "gw", "sub" => "alice", "exp" => now + 300, "scope" => "claims:read"},
        claims
      )

    {_, compact} =
      jwk |> JOSE.JWT.sign(%{"alg" => "ES256", "kid" => kid}, claims) |> JOSE.JWS.compact()

    compact
  end

  defp jwt(issuer, public, extra \\ %{}),
    do:
      Map.merge(%{type: :jwt, issuer: issuer, audience: "gw", keys: [public], leeway_s: 0}, extra)

  defp pem_der(name) do
    [{:Certificate, der, _}] =
      @tls_dir |> Path.join(name) |> File.read!() |> :public_key.pem_decode()

    der
  end

  defp tls do
    %{
      certfile: Path.join(@tls_dir, "server.pem"),
      keyfile: Path.join(@tls_dir, "server-key.pem"),
      cacertfile: Path.join(@tls_dir, "ca.pem"),
      verify: :verify_peer
    }
  end

  test "the chain is tried in order and a skip falls through to the next", %{inst: inst} do
    chain(inst, [
      %{type: :token, name: "first", tokens: [token("one", ["claims:read"])]},
      %{type: :token, name: "second", tokens: [token("two", ["claims:read"])]}
    ])

    conn = run(inst, bearer("two"))
    refute conn.halted
    assert %Principal{id: "two"} = conn.assigns.principal
    assert conn.assigns.principal_via == :token

    conn = run(inst, bearer("three"))
    assert {conn.status, conn.halted} == {401, true}
    assert Plug.Conn.get_resp_header(conn, "www-authenticate") == [~s(Bearer realm="ankusa")]
    assert run(inst).status == 401
  end

  test "each issuer is verified by its own jwt authenticator, on one keys server", %{inst: inst} do
    {jwk1, pub1} = keypair("k1")
    {jwk2, pub2} = keypair("k2")

    chain(inst, [
      jwt(@first, pub1),
      jwt(@second, pub2, %{subject_prefix: "second|", groups_claim: "roles"})
    ])

    claims = %{"iss" => @second, "roles" => ["eng", "ops", 7], "team" => "core"}
    conn = run(inst, bearer(sign(jwk2, "k2", claims)))

    refute conn.halted
    assert %Principal{id: "second|alice", groups: ["eng", "ops"]} = conn.assigns.principal
    assert conn.assigns.principal.claims["team"] == "core"
    assert conn.assigns.principal.scopes == ["claims:read"]
    assert conn.assigns.principal_via == :jwt

    assert %Principal{id: "alice", groups: []} =
             run(inst, bearer(sign(jwk1, "k1", %{"iss" => @first}))).assigns.principal

    # Space-delimited groups are split.
    conn = run(inst, bearer(sign(jwk2, "k2", %{"iss" => @second, "roles" => "a b"})))
    assert conn.assigns.principal.groups == ["a", "b"]
  end

  test "a token of a known issuer that does not verify is 401, and no later authenticator sees it",
       %{inst: inst} do
    {_jwk1, pub1} = keypair("k1")
    {forger, _} = keypair("k1")

    defmodule Spy do
      @behaviour Ankusa.Gateway.Authenticator
      def init!(_config, _key), do: %{}
      def credential(_conn, _ctx), do: {:ok, "anything"}

      def authenticate(_conn, _ctx) do
        send(self(), :spy_asked)
        {:ok, Ankusa.Principal.operator(), nil}
      end
    end

    chain(inst, [jwt(@first, pub1), %{type: Spy}])

    conn = run(inst, bearer(sign(forger, "k1", %{"iss" => @first})))
    assert conn.status == 401
    refute_received :spy_asked

    # A token of an issuer nobody configured is not the jwt authenticator's: it skips.
    conn = run(inst, bearer(sign(forger, "k1", %{"iss" => "https://elsewhere/"})))
    refute conn.halted
    assert conn.assigns.principal_via == Spy
    assert_received :spy_asked
  end

  test "a bearer no token entry matches falls through to a jwt authenticator", %{inst: inst} do
    {jwk, pub} = keypair("k1")
    chain(inst, [%{type: :token, tokens: [token("static", ["claims:read"])]}, jwt(@first, pub)])

    conn = run(inst, bearer(sign(jwk, "k1", %{"iss" => @first})))
    assert %Principal{id: "alice"} = conn.assigns.principal
    assert conn.assigns.principal_via == :jwt

    assert %Principal{id: "static"} = run(inst, bearer("static")).assigns.principal
  end

  test "a jwt authenticator reads another header when told to", %{inst: inst} do
    {jwk, pub} = keypair("k1")
    chain(inst, [jwt(@first, pub, %{header: "x-access-token"})])

    token = sign(jwk, "k1", %{"iss" => @first})
    assert run(inst, [{"x-access-token", token}]).assigns.principal.id == "alice"
    assert run(inst, bearer(token)).status == 401
  end

  test "token entries carry their groups and claims", %{inst: inst} do
    chain(inst, [
      %{
        type: :token,
        tokens: [token("w", ["claims:read"], %{groups: ["workers"], claims: %{"team" => "x"}})]
      }
    ])

    assert %Principal{groups: ["workers"], claims: %{"team" => "x"}} =
             run(inst, bearer("w")).assigns.principal
  end

  test "an empty chain off loopback is refused unless allow_insecure", %{inst: inst} do
    assert_raise ArgumentError, ~r/gateway.auth.authenticators is empty but gateway.ip/, fn ->
      GatewayConfig.new(instance: inst, ip: "0.0.0.0")
    end

    assert %GatewayConfig{} =
             GatewayConfig.new(instance: inst, ip: "0.0.0.0", auth: %{allow_insecure: true})

    assert %GatewayConfig{} =
             GatewayConfig.new([instance: inst, ip: "0.0.0.0"], check_exposure: false)

    # On loopback the empty chain is the operator.
    chain(inst, [])
    conn = run(inst)
    assert conn.assigns.principal == Principal.operator()
    assert conn.assigns.principal_via == :anonymous
  end

  test "mtls without a mapped identity skips to a bearer token; a mapped one carries its groups",
       %{inst: inst} do
    chain(
      inst,
      [
        %{
          type: :mtls,
          subject_map: %{
            "cn:ankusa-test-client" => %{scopes: ["claims:read", "tenant:acme"], groups: ["ops"]}
          }
        },
        %{type: :token, tokens: [token("t", ["claims:read"])]}
      ],
      tls: tls()
    )

    peer = fn der ->
      Plug.Test.put_peer_data(Plug.Test.conn(:get, "/v1/claims/acme/x"), %{
        address: {127, 0, 0, 1},
        port: 1,
        ssl_cert: der
      })
    end

    conn = run(inst, [], peer.(pem_der("client.pem")))

    assert %Principal{id: "cn:ankusa-test-client", groups: ["ops"], tenants: ["acme"]} =
             conn.assigns.principal

    assert conn.assigns.principal_via == :mtls

    conn = run(inst, bearer("t"), peer.(pem_der("stranger.pem")))
    assert %Principal{id: "t"} = conn.assigns.principal
    assert conn.assigns.principal_via == :token

    assert run(inst, [], peer.(pem_der("stranger.pem"))).status == 401
  end

  test "an mtls authenticator needs a verify_peer listener", %{inst: inst} do
    assert_raise ArgumentError, ~r/mtls needs gateway.tls with verify: :verify_peer/, fn ->
      GatewayConfig.new(
        instance: inst,
        auth: %{authenticators: [%{type: :mtls, subject_map: %{"cn:x" => ["claims:read"]}}]}
      )
    end
  end

  test "the same subject with different claims gets a different subject_key", %{inst: inst} do
    {jwk, pub} = keypair("k1")
    chain(inst, [jwt(@first, pub)])

    a = run(inst, bearer(sign(jwk, "k1", %{"iss" => @first, "team" => "a"})))
    b = run(inst, bearer(sign(jwk, "k1", %{"iss" => @first, "team" => "b"})))

    assert a.assigns.principal.id == b.assigns.principal.id
    assert byte_size(a.assigns.subject_key) == 16
    assert a.assigns.subject_key != b.assigns.subject_key

    assert a.assigns.subject_key ==
             Authn.subject_key(:jwt, a.assigns.principal)
  end

  test "probes are exempt", %{inst: inst} do
    chain(inst, [%{type: :token, tokens: [token("t", ["claims:read"])]}])

    for path <- ["/health", "/ready"] do
      conn = run(inst, [], Plug.Test.conn(:get, path))
      refute conn.halted
      refute Map.has_key?(conn.assigns, :principal)
    end
  end
end
