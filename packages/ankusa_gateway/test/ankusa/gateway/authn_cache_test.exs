defmodule Ankusa.Gateway.AuthnCacheTest do
  @moduledoc """
  The authentication cache of `Ankusa.Gateway.Authn`: hits, the bound by a
  credential's own expiry, negative caching only where an authenticator asks
  for it, the size cap, and `ttl_ms: 0`.
  """

  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Ankusa.Admin.Auth.Token
  alias Ankusa.Gateway.{Authn, Cache}
  alias Ankusa.Gateway.Config, as: GatewayConfig
  alias Ankusa.Principal

  # A bearer authenticator that counts its calls and refuses "bad", and asks
  # for its refusals to be cached, as one that calls out would.
  defmodule Remote do
    @moduledoc false
    @behaviour Ankusa.Gateway.Authenticator

    @impl true
    def init!(config, _key), do: config

    @impl true
    def credential(conn, _ctx) do
      case Plug.Conn.get_req_header(conn, "authorization") do
        ["Bearer " <> token] -> {:ok, token}
        _ -> :none
      end
    end

    @impl true
    def authenticate(conn, %{config: %{test: test}} = ctx) do
      {:ok, token} = credential(conn, ctx)
      send(test, {:authenticated, token})

      if token == "bad",
        do: {:error, :unauthenticated},
        else: {:ok, Principal.new(token, ["claims:read"]), nil}
    end

    @impl true
    def cache_negative?, do: true
  end

  setup do
    inst = :"authncache#{System.unique_integer([:positive])}"
    Ankusa.put_config(Ankusa.Config.new(instance: inst, roles: []))
    test = self()
    handler = "authn-cache-#{inst}"

    :telemetry.attach(
      handler,
      [:ankusa, :gateway, :authn],
      fn
        _event, _measurements, %{instance: ^inst} = meta, _ -> send(test, {:authn, meta})
        _event, _measurements, _meta, _ -> :ok
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    %{inst: inst}
  end

  defp chain(inst, authenticators, cache \\ %{}) do
    config =
      GatewayConfig.new(instance: inst, auth: %{authenticators: authenticators, cache: cache})

    Ankusa.Gateway.put_config(config)

    start_supervised!(
      Cache.child_spec_for(inst, :authn,
        max_entries: 1_000,
        max_bytes: 1_048_576,
        max_ttl_ms: 60_000
      )
    )

    Enum.each(Ankusa.Gateway.Supervisor.keys_children(config), &start_supervised!/1)
    config
  end

  defp run(inst, token) do
    Plug.Test.conn(:get, "/v1/claims/acme/x")
    |> Plug.Conn.assign(:ankusa_opts, instance: inst)
    |> Plug.Conn.put_req_header("authorization", "Bearer " <> token)
    |> Authn.call([])
  end

  defp static(id),
    do: %{type: :token, tokens: [%{id: id, sha256: Token.hash(id), scopes: ["claims:read"]}]}

  defp keypair do
    jwk = JOSE.JWK.generate_key({:ec, "P-256"})
    {_, public} = jwk |> JOSE.JWK.to_public() |> JOSE.JWK.to_map()
    {jwk, Map.put(public, "kid", "k")}
  end

  defp jwt_authenticator(public),
    do: %{type: :jwt, issuer: "https://i.test/", audience: "gw", keys: [public], leeway_s: 0}

  defp sign(jwk, claims) do
    claims = Map.merge(%{"aud" => "gw", "sub" => "alice", "scope" => "claims:read"}, claims)

    {_, compact} =
      jwk |> JOSE.JWT.sign(%{"alg" => "ES256", "kid" => "k"}, claims) |> JOSE.JWS.compact()

    compact
  end

  test "a credential seen before is a hit: same principal, same subject_key", %{inst: inst} do
    chain(inst, [static("tok")])

    first = run(inst, "tok")
    assert_received {:authn, %{result: :ok, cached: false, authenticator: "token-0"}}
    second = run(inst, "tok")
    assert_received {:authn, %{result: :ok, cached: true, authenticator: "token-0"}}

    assert second.assigns.principal == first.assigns.principal
    assert second.assigns.subject_key == first.assigns.subject_key
    assert second.assigns.principal_via == :token
  end

  test "only the credential's hash is the key", %{inst: inst} do
    chain(inst, [static("tok")])
    run(inst, "tok")

    assert {:ok, {:ok, %Principal{}, :token, _key, nil}} =
             Cache.fetch(inst, :authn, {0, :crypto.hash(:sha256, "tok")})
  end

  test "an entry is never used past the credential's own expiry", %{inst: inst} do
    {jwk, public} = keypair()
    chain(inst, [jwt_authenticator(public)])

    # Two seconds: a second boundary between signing and the first request
    # must not leave the token expired, or its cache entry no time to live.
    token = sign(jwk, %{"iss" => "https://i.test/", "exp" => System.os_time(:second) + 2})
    assert run(inst, token).assigns.principal.id == "alice"
    assert run(inst, token).assigns.principal.id == "alice"
    assert_received {:authn, %{cached: false}}
    assert_received {:authn, %{cached: true}}

    Process.sleep(3_100)
    conn = run(inst, token)
    assert conn.status == 401
    assert_received {:authn, %{result: :unauthenticated, cached: false}}
  end

  test "a hit needs no signature check: it holds with the keys server gone", %{inst: inst} do
    {jwk, public} = keypair()
    chain(inst, [jwt_authenticator(public)])
    token = sign(jwk, %{"iss" => "https://i.test/", "exp" => System.os_time(:second) + 300})

    assert run(inst, token).assigns.principal.id == "alice"
    # The keys table goes with its owner: a miss could verify nothing now.
    stop_supervised!({Ankusa.Admin.Auth.JWT.Keys, inst, :gateway_auth_keys})
    refute :ets.whereis(:"ankusa_gateway_auth_keys_#{inst}") |> is_reference()
    assert run(inst, token).assigns.principal.id == "alice"
    assert_received {:authn, %{cached: false, result: :ok}}
    assert_received {:authn, %{cached: true, result: :ok}}
  end

  test "a JWT that does not verify is never cached", %{inst: inst} do
    {_jwk, public} = keypair()
    {other, _} = keypair()
    chain(inst, [jwt_authenticator(public)])
    forged = sign(other, %{"iss" => "https://i.test/", "exp" => System.os_time(:second) + 300})

    assert run(inst, forged).status == 401
    assert run(inst, forged).status == 401
    assert_received {:authn, %{result: :unauthenticated, cached: false}}
    assert_received {:authn, %{result: :unauthenticated, cached: false}}
  end

  test "a refusal is cached only by an authenticator that asks for it", %{inst: inst} do
    chain(inst, [%{type: Remote, test: self()}])

    assert run(inst, "bad").status == 401
    assert run(inst, "bad").status == 401
    assert_received {:authenticated, "bad"}
    refute_received {:authenticated, "bad"}

    assert run(inst, "good").assigns.principal.id == "good"
    assert run(inst, "good").assigns.principal.id == "good"
    assert_received {:authenticated, "good"}
    refute_received {:authenticated, "good"}
  end

  test "a static token's refusal is not cached", %{inst: inst} do
    chain(inst, [static("tok")])

    run(inst, "wrong")
    run(inst, "wrong")
    assert_received {:authn, %{result: :unauthenticated, cached: false}}
    assert_received {:authn, %{result: :unauthenticated, cached: false}}
  end

  test "a credential over 16 KiB is passed over", %{inst: inst} do
    chain(inst, [%{type: Remote, test: self()}])

    conn = run(inst, String.duplicate("a", 16_385))
    assert conn.status == 401
    refute_received {:authenticated, _}
    assert_received {:authn, %{authenticator: "none", result: :unauthenticated}}
  end

  test "ttl_ms: 0 turns the cache off", %{inst: inst} do
    chain(inst, [%{type: Remote, test: self()}], %{ttl_ms: 0})

    run(inst, "good")
    run(inst, "good")
    assert_received {:authenticated, "good"}
    assert_received {:authenticated, "good"}
  end
end
