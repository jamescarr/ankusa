defmodule Ankusa.Gateway.IntrospectionTest do
  @moduledoc """
  `Ankusa.Gateway.Authn.Introspection`: RFC 7662 introspection through its
  `Ankusa.Gateway.Caller` — the exact request, the authentication cache and
  negative caching, coalescing under concurrency, and the responses that mean
  "unavailable" rather than "refused".
  """

  use ExUnit.Case, async: false

  alias Ankusa.Gateway.Authn
  alias Ankusa.Gateway.Cache
  alias Ankusa.Gateway.Config, as: GatewayConfig
  alias Ankusa.Principal

  @identity "https://idp.test/oauth2/v1/introspect"
  @token "opaque-access-token"

  setup do
    inst = :"intro#{System.unique_integer([:positive])}"
    Ankusa.put_config(Ankusa.Config.new(instance: inst, roles: []))
    %{inst: inst}
  end

  defp entry(extra \\ %{}) do
    Map.merge(
      %{
        type: :introspection,
        url: @identity,
        client_id: "cid",
        client_secret: "sec",
        req_options: [plug: {Req.Test, __MODULE__}]
      },
      extra
    )
  end

  defp chain(inst, entry) do
    config =
      GatewayConfig.new(instance: inst, auth: %{authenticators: [entry]})

    Ankusa.Gateway.put_config(config)

    start_supervised!(
      Cache.child_spec_for(inst, :authn,
        max_entries: 1_000,
        max_bytes: 1_048_576,
        max_ttl_ms: 60_000
      )
    )

    Enum.each(Ankusa.Gateway.Supervisor.authn_children(config), &start_supervised!/1)
    config
  end

  defp run(inst, headers \\ []) do
    Plug.Test.conn(:get, "/v1/claims/acme/x")
    |> Plug.Conn.assign(:ankusa_opts, instance: inst)
    |> then(&Enum.reduce(headers, &1, fn {k, v}, c -> Plug.Conn.put_req_header(c, k, v) end))
    |> Authn.call([])
  end

  defp bearer(token), do: [{"authorization", "Bearer " <> token}]

  defp counter, do: :counters.new(1, [:write_concurrency])

  defp calls(count), do: :counters.get(count, 1)

  defp active(claims \\ %{}) do
    fn conn ->
      Req.Test.json(
        conn,
        Map.merge(
          %{
            "active" => true,
            "sub" => "alice",
            "scope" => "claims:read tenant:acme",
            "exp" => System.os_time(:second) + 300
          },
          claims
        )
      )
    end
  end

  test "an active token authenticates from the response, once; the next request makes no call",
       %{inst: inst} do
    count = counter()

    Req.Test.stub(__MODULE__, fn conn ->
      :counters.add(count, 1, 1)
      active().(conn)
    end)

    chain(inst, entry())

    conn = run(inst, bearer(@token))
    refute conn.halted

    assert %Principal{id: "alice", scopes: ["claims:read"], tenants: ["acme"]} =
             conn.assigns.principal

    assert conn.assigns.principal.claims["active"] == true
    assert conn.assigns.principal_via == :introspection
    assert calls(count) == 1

    # The credential's answer is cached: no second call, same principal.
    refute run(inst, bearer(@token)).halted
    assert calls(count) == 1
  end

  test "an inactive token is 401, and its refusal is remembered", %{inst: inst} do
    count = counter()

    Req.Test.stub(__MODULE__, fn conn ->
      :counters.add(count, 1, 1)
      Req.Test.json(conn, %{"active" => false})
    end)

    chain(inst, entry())

    conn = run(inst, bearer(@token))
    assert conn.status == 401
    assert Plug.Conn.get_resp_header(conn, "www-authenticate") == [~s(Bearer realm="ankusa")]

    assert run(inst, bearer(@token)).status == 401
    assert calls(count) == 1
  end

  test "no bearer token is a skip, not a refusal", %{inst: inst} do
    chain(inst, entry())
    assert run(inst).status == 401
  end

  test "an identity provider that does not answer is 503 auth_unavailable", %{inst: inst} do
    Req.Test.stub(__MODULE__, fn conn -> Plug.Conn.send_resp(conn, 500, "boom") end)
    chain(inst, entry())

    conn = run(inst, bearer(@token))
    assert conn.status == 503
    assert JSON.decode!(conn.resp_body) == %{"error" => "auth_unavailable"}
    assert [_retry_after] = Plug.Conn.get_resp_header(conn, "retry-after")
  end

  test "an answer over 64 KiB is unavailable, not a refusal", %{inst: inst} do
    Req.Test.stub(__MODULE__, fn conn ->
      Plug.Conn.send_resp(conn, 200, String.duplicate("x", 70_000))
    end)

    chain(inst, entry())
    assert run(inst, bearer(@token)).status == 503
  end

  test "100 concurrent requests with one token make one call", %{inst: inst} do
    count = counter()

    Req.Test.stub(__MODULE__, fn conn ->
      :counters.add(count, 1, 1)
      # Long enough that the 100 callers are all waiting on the one run.
      Process.sleep(50)
      active().(conn)
    end)

    chain(inst, entry())

    conns =
      1..100
      |> Enum.map(fn _ -> Task.async(fn -> run(inst, bearer(@token)) end) end)
      |> Enum.map(&Task.await(&1, 5_000))

    assert Enum.all?(conns, &(not &1.halted and &1.assigns.principal.id == "alice"))
    assert calls(count) == 1
  end

  test "the request is a Basic-authenticated form post, and the token stays one parameter",
       %{inst: inst} do
    test = self()

    Req.Test.stub(__MODULE__, fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      send(test, {:request, conn, body})
      active().(conn)
    end)

    chain(inst, entry())

    # A token that would add a parameter if it were concatenated into the body.
    token = "abc&token_type_hint=x&token=evil"
    run(inst, bearer(token))

    assert_receive {:request, conn, body}

    assert Plug.Conn.get_req_header(conn, "authorization") == [
             "Basic " <> Base.encode64("cid:sec")
           ]

    assert Plug.Conn.get_req_header(conn, "content-type") == [
             "application/x-www-form-urlencoded"
           ]

    assert URI.decode_query(body) == %{
             "token" => token,
             "token_type_hint" => "access_token"
           }
  end

  test "the response's iss and aud are checked when configured", %{inst: inst} do
    Req.Test.stub(__MODULE__, active(%{"iss" => "https://other.test", "aud" => "elsewhere"}))

    chain(inst, entry(%{issuer: "https://idp.test", audience: ["api://one", "api://two"]}))
    assert run(inst, bearer(@token)).status == 401

    Req.Test.stub(__MODULE__, active(%{"iss" => "https://idp.test", "aud" => "api://two"}))
    refute run(inst, bearer("another-" <> @token)).halted
  end

  test "an answer without exp is good for a minute", %{inst: inst} do
    Req.Test.stub(__MODULE__, active(%{"exp" => nil}))
    chain(inst, entry())

    before = System.os_time(:second)
    refute run(inst, bearer(@token)).halted

    key = {0, :crypto.hash(:sha256, @token)}

    assert {:ok, {:ok, %Principal{id: "alice"}, :introspection, _subject_key, expires_at}} =
             Cache.fetch(inst, :authn, key)

    assert expires_at in (before + 59)..(before + 61)
  end

  test "at most one introspection entry, and nothing that reads the bearer after it" do
    token_entry = %{
      type: :token,
      tokens: [%{id: "w", sha256: String.duplicate("a", 64), scopes: []}]
    }

    # The other order is the working one: the static token is tried first.
    assert %GatewayConfig{} = config([token_entry, entry()])

    assert_raise ArgumentError,
                 ~r/gateway\.auth\.authenticators\[1\] reads the bearer token after the introspection authenticator at \[0\]/,
                 fn -> config([entry(), token_entry]) end

    assert_raise ArgumentError,
                 "gateway.auth.authenticators has 2 introspection entries; at most one",
                 fn -> config([entry(), entry(%{name: "idp-two"})]) end
  end

  defp config(authenticators) do
    GatewayConfig.new(auth: %{authenticators: authenticators})
  end

  test "the url must be https, or http to loopback" do
    assert_raise ArgumentError, ~r/\.url must be an https URL/, fn ->
      GatewayConfig.new(auth: %{authenticators: [entry(%{url: "http://idp.test/introspect"})]})
    end

    assert %GatewayConfig{} =
             GatewayConfig.new(
               auth: %{authenticators: [entry(%{url: "http://127.0.0.1:9000/introspect"})]}
             )
  end
end
