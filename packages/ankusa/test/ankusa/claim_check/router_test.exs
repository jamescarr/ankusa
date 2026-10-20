defmodule Ankusa.ClaimCheck.RouterTest do
  @moduledoc """
  Exercises `Ankusa.ClaimCheck.Router`'s HTTP semantics directly via
  `Plug.Test`/`Router.call`, the same pattern `Ankusa.EdgeTest` uses for
  `Ankusa.Edge.Router` — no real socket needed to prove request handling and
  status-code mapping.
  """

  use ExUnit.Case, async: false

  alias Ankusa.{ClaimCheck, Config, UUIDv7}
  alias Ankusa.ClaimCheck.{Cache, Reader, Ref, Router, Stores}
  alias Ankusa.Test.CountingBlobStore

  setup do
    inst = :"ccr#{System.unique_integer([:positive])}"
    dir = Ankusa.TestHelpers.unique_data_dir(inst)
    on_exit(fn -> File.rm_rf(dir) end)

    config = Config.new(instance: inst, data_dir: dir, roles: [:claim_check])
    Ankusa.put_config(config)
    %{inst: inst, config: config}
  end

  defp call(inst, method, path, body \\ "") do
    Plug.Test.conn(method, path, body) |> Router.call(Router.init(instance: inst))
  end

  defp check_in(inst, body) do
    id = UUIDv7.generate()

    {:ok, claims} =
      ClaimCheck.check_in(inst, "acme", [%{id: "other", body: "x"}, %{id: id, body: body}])

    claims[id].ref
  end

  defp error(conn), do: JSON.decode!(conn.resp_body)["error"]

  # What a `:claim_check` node runs beside the listener, under the test's
  # config with `claim_check` merged in.
  defp start_reader(inst, claim_check) do
    config = Ankusa.config(inst)
    Ankusa.put_config(%{config | claim_check: Map.merge(config.claim_check, claim_check)})
    start_supervised!({Stores, instance: inst})

    if Ankusa.config(inst).claim_check.read_cache_bytes > 0,
      do: start_supervised!({Cache, instance: inst})

    start_supervised!({Reader, instance: inst})
  end

  test "GET /health", %{inst: inst} do
    conn = call(inst, :get, "/health")
    assert conn.status == 200
    assert %{"status" => "ok"} = JSON.decode!(conn.resp_body)
  end

  test "GET on a ref's path returns exactly its bytes, privately cacheable forever", %{inst: inst} do
    body = :crypto.strong_rand_bytes(4096)
    ref = check_in(inst, body)

    conn = call(inst, :get, Ref.path(ref))

    assert conn.status == 200
    assert conn.resp_body == body
    assert Plug.Conn.get_resp_header(conn, "content-type") == ["application/octet-stream"]

    assert Plug.Conn.get_resp_header(conn, "cache-control") == [
             "private, max-age=31536000, immutable"
           ]
  end

  test "HEAD answers the size without the body", %{inst: inst} do
    body = :crypto.strong_rand_bytes(4096)
    ref = check_in(inst, body)

    conn = call(inst, :head, Ref.path(ref))

    assert conn.status == 200
    assert conn.resp_body == ""
    assert Plug.Conn.get_resp_header(conn, "content-length") == ["4096"]
  end

  defp get_range(inst, path, range) do
    Plug.Test.conn(:get, path)
    |> Plug.Conn.put_req_header("range", range)
    |> Router.call(Router.init(instance: inst))
  end

  defp header(conn, name), do: Plug.Conn.get_resp_header(conn, name)

  describe "Range" do
    setup %{inst: inst} do
      body = :crypto.strong_rand_bytes(4096)
      %{body: body, path: Ref.path(check_in(inst, body))}
    end

    test "bytes=a-b is 206 with exactly those bytes", %{inst: inst, body: body, path: path} do
      conn = get_range(inst, path, "bytes=10-13")

      assert conn.status == 206
      assert conn.resp_body == binary_part(body, 10, 4)
      assert header(conn, "content-range") == ["bytes 10-13/4096"]
      assert header(conn, "accept-ranges") == ["bytes"]
      assert header(conn, "cache-control") == ["private, max-age=31536000, immutable"]
    end

    test "an open or overlong end runs to the claim's last byte", %{
      inst: inst,
      body: body,
      path: path
    } do
      for range <- ["bytes=4000-", "bytes=4000-99999"] do
        conn = get_range(inst, path, range)

        assert conn.status == 206
        assert conn.resp_body == binary_part(body, 4000, 96)
        assert header(conn, "content-range") == ["bytes 4000-4095/4096"]
      end
    end

    test "bytes=-n is the last n bytes, all of it when n exceeds the claim", %{
      inst: inst,
      body: body,
      path: path
    } do
      conn = get_range(inst, path, "bytes=-100")
      assert conn.status == 206
      assert conn.resp_body == binary_part(body, 3996, 100)
      assert header(conn, "content-range") == ["bytes 3996-4095/4096"]

      conn = get_range(inst, path, "bytes=-5000")
      assert conn.status == 206
      assert conn.resp_body == body
      assert header(conn, "content-range") == ["bytes 0-4095/4096"]
    end

    test "a range past the end, or bytes=-0, is 416 with the claim's length", %{
      inst: inst,
      path: path
    } do
      for range <- ["bytes=5000-", "bytes=4096-4100", "bytes=-0"] do
        conn = get_range(inst, path, range)

        assert conn.status == 416, "#{range} returned #{conn.status}"
        assert conn.resp_body == ""
        assert header(conn, "content-range") == ["bytes */4096"]
        assert header(conn, "accept-ranges") == ["bytes"]
      end
    end

    test "several ranges, another unit, or a malformed spec is ignored: 200 whole", %{
      inst: inst,
      body: body,
      path: path
    } do
      for range <- ["bytes=0-1,3-4", "items=0-1", "bytes=5-3", "bytes=-", "bytes=a-b", "bytes"] do
        conn = get_range(inst, path, range)

        assert conn.status == 200, "#{range} returned #{conn.status}"
        assert conn.resp_body == body
        assert header(conn, "content-range") == []
        assert header(conn, "accept-ranges") == ["bytes"]
      end
    end

    test "an If-Range makes the request whole: claims carry no validator", %{
      inst: inst,
      body: body,
      path: path
    } do
      conn =
        Plug.Test.conn(:get, path)
        |> Plug.Conn.put_req_header("range", "bytes=10-13")
        |> Plug.Conn.put_req_header("if-range", ~s("abc"))
        |> Router.call(Router.init(instance: inst))

      assert conn.status == 200
      assert conn.resp_body == body
    end

    test "HEAD ignores Range and answers the whole length", %{inst: inst, path: path} do
      conn =
        Plug.Test.conn(:head, path)
        |> Plug.Conn.put_req_header("range", "bytes=10-13")
        |> Router.call(Router.init(instance: inst))

      assert conn.status == 200
      assert conn.resp_body == ""
      assert header(conn, "content-length") == ["4096"]
      assert header(conn, "accept-ranges") == ["bytes"]
      assert header(conn, "content-range") == []
    end

    test "a range of a missing claim is still 404", %{inst: inst} do
      path = "/v1/claims/acme/#{Ref.claim_id(Ref.new_pack_id(), 0)}"
      conn = get_range(inst, path, "bytes=0-1")

      assert conn.status == 404
      assert error(conn) == "not_found"
    end
  end

  test "a store that refuses the credential is 503 store_forbidden, its body kept out", %{
    inst: inst,
    config: config
  } do
    Ankusa.put_config(%{
      config
      | storage: %{config.storage | blob_store: {__MODULE__.ForbiddenStore, []}}
    })

    conn = call(inst, :get, "/v1/claims/acme/#{Ref.claim_id(Ref.new_pack_id(), 0)}")

    assert {conn.status, JSON.decode!(conn.resp_body)} == {503, %{"error" => "store_forbidden"}}
    assert Plug.Conn.get_resp_header(conn, "retry-after") == ["60"]
    refute conn.resp_body =~ "AccessDenied"
  end

  test "a malformed tenant or claim id is 400", %{inst: inst} do
    claim_id = Ref.claim_id(Ref.new_pack_id(), 0)

    assert error(call(inst, :get, "/v1/claims/ac%25me/#{claim_id}")) == "invalid_tenant"

    for bad <- [UUIDv7.generate(), String.downcase(claim_id), "7ZZZZZZZZZZZZZZZZZZZZZZZZZ"] do
      conn = call(inst, :get, "/v1/claims/acme/#{bad}")
      assert {conn.status, error(conn)} == {400, "invalid_id"}, "id #{bad}"
    end
  end

  test "a pack that was never written is 404", %{inst: inst} do
    conn = call(inst, :get, "/v1/claims/acme/#{Ref.claim_id(Ref.new_pack_id(), 0)}")
    assert {conn.status, error(conn)} == {404, "not_found"}
  end

  test "a claim id past the end of a real pack is 404", %{inst: inst} do
    ref = check_in(inst, "small")
    {:ok, pack_id, 1} = Ref.locate(ref.claim_id)

    for index <- [2, 0xFFFF] do
      conn = call(inst, :get, "/v1/claims/acme/#{Ref.claim_id(pack_id, index)}")
      assert {conn.status, error(conn)} == {404, "not_found"}, "index #{index}"
    end
  end

  test "the path is exactly two segments", %{inst: inst} do
    ref = check_in(inst, "small")

    for path <- [Ref.path(ref) <> "/0/5", "/v1/claims/#{ref.claim_id}"] do
      assert call(inst, :get, path).status == 404, path
    end
  end

  test "there is no write route: PUT is 404", %{inst: inst} do
    conn = call(inst, :put, "/v1/claims/acme/#{Ref.claim_id(Ref.new_pack_id(), 0)}", "body")
    assert conn.status == 404
  end

  @tag :capture_log
  test "a store that can't be reached is 503 with a jittered Retry-After", %{
    inst: inst,
    config: config
  } do
    Ankusa.put_config(%{
      config
      | storage: %{config.storage | blob_store: {__MODULE__.DownStore, []}}
    })

    conn = call(inst, :get, "/v1/claims/acme/#{Ref.claim_id(Ref.new_pack_id(), 0)}")

    assert {conn.status, JSON.decode!(conn.resp_body)} == {503, %{"error" => "store_unavailable"}}
    assert [retry_after] = Plug.Conn.get_resp_header(conn, "retry-after")
    assert retry_after in ["1", "2", "3"]
  end

  @tag :capture_log
  test "every claim request emits its method, status and duration", %{inst: inst, config: config} do
    test = self()
    handler = "router-test-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:ankusa, :claim_check, :gateway],
      fn _event, measurements, meta, _ ->
        if meta.instance == inst, do: send(test, {:gateway, measurements, meta})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    path = Ref.path(check_in(inst, "found"))
    call(inst, :get, path)
    assert_received {:gateway, %{duration: d}, %{method: "GET", status: 200}} when d > 0

    call(inst, :head, path)
    assert_received {:gateway, _measurements, %{method: "HEAD", status: 200}}

    get_range(inst, path, "bytes=99-")
    assert_received {:gateway, _measurements, %{method: "GET", status: 416}}

    call(inst, :get, "/v1/claims/acme/#{Ref.claim_id(Ref.new_pack_id(), 0)}")
    assert_received {:gateway, _measurements, %{method: "GET", status: 404}}

    Ankusa.put_config(%{
      config
      | storage: %{config.storage | blob_store: {__MODULE__.DownStore, []}}
    })

    call(inst, :get, "/v1/claims/acme/#{Ref.claim_id(Ref.new_pack_id(), 0)}")
    assert_received {:gateway, _measurements, %{method: "GET", status: 503}}
  end

  test "a store slower than read_deadline_ms is 503 at the deadline, not when it answers", %{
    inst: inst
  } do
    root = Path.join(System.tmp_dir!(), "ankusa_claims_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf(root) end)

    start_reader(inst, %{
      read_deadline_ms: 100,
      blob_stores: [{CountingBlobStore, root: root, range_delay_ms: 400}]
    })

    {elapsed_us, conn} =
      :timer.tc(fn ->
        call(inst, :get, "/v1/claims/acme/#{Ref.claim_id(Ref.new_pack_id(), 0)}")
      end)

    assert {conn.status, error(conn)} == {503, "store_unavailable"}
    assert div(elapsed_us, 1000) < 250
  end

  defmodule DownStore do
    @behaviour Ankusa.BlobStore
    @impl true
    def put(_, _, _, _), do: {:error, :econnrefused}
    @impl true
    def put_if_absent(_, _, _, _), do: {:error, :econnrefused}
    @impl true
    def get(_, _, _), do: {:error, :econnrefused}
    @impl true
    def get_range(_, _, _, _, _), do: {:error, :econnrefused}
    @impl true
    def delete(_, _, _), do: :ok
    @impl true
    def list(_, _, _), do: {:ok, []}
  end

  defmodule ForbiddenStore do
    @behaviour Ankusa.BlobStore
    @impl true
    def put(_, _, _, _), do: {:error, {:status, 403, "<Error><Code>AccessDenied</Code></Error>"}}
    @impl true
    def put_if_absent(_, _, _, _),
      do: {:error, {:status, 403, "<Error><Code>AccessDenied</Code></Error>"}}

    @impl true
    def get(_, _, _), do: {:error, {:status, 403, "<Error><Code>AccessDenied</Code></Error>"}}
    @impl true
    def get_range(_, _, _, _, _),
      do: {:error, {:status, 403, "<Error><Code>AccessDenied</Code></Error>"}}

    @impl true
    def delete(_, _, _), do: :ok
    @impl true
    def list(_, _, _), do: {:ok, []}
  end

  describe "authentication and tenant scope" do
    alias Ankusa.Admin.Auth.Token

    defp token(id, scopes), do: %{id: id, sha256: Token.hash(id), scopes: scopes}

    defp authed_call(inst, method, path, token) do
      conn = Plug.Test.conn(method, path, "")

      conn =
        if token,
          do: Plug.Conn.put_req_header(conn, "authorization", "Bearer " <> token),
          else: conn

      Router.call(conn, Router.init(instance: inst))
    end

    setup do
      inst = :"ccra#{System.unique_integer([:positive])}"
      dir = Ankusa.TestHelpers.unique_data_dir(inst)
      on_exit(fn -> File.rm_rf(dir) end)

      tokens = [
        token("acme-reader", ["claims:read", "tenant:acme"]),
        token("any-reader", ["claims:read", "tenant:*"]),
        token("acme-writer", ["sources:write", "tenant:acme"])
      ]

      config =
        Config.new(
          instance: inst,
          data_dir: dir,
          roles: [:claim_check],
          claim_check: %{auth: %{mode: :token, tokens: tokens}}
        )

      Ankusa.put_config(config)

      {:ok, acme} = ClaimCheck.check_in(inst, "acme", [%{id: "a", body: "acme bytes"}])
      {:ok, globex} = ClaimCheck.check_in(inst, "globex", [%{id: "g", body: "globex bytes"}])

      %{inst: inst, acme: Ref.path(acme["a"].ref), globex: Ref.path(globex["g"].ref)}
    end

    test "401 with no credential or a wrong one, for GET and HEAD alike", %{
      inst: inst,
      acme: acme
    } do
      for method <- [:get, :head], token <- [nil, "guess"] do
        conn = authed_call(inst, method, acme, token)
        assert conn.status == 401
        assert Plug.Conn.get_resp_header(conn, "www-authenticate") == [~s(Bearer realm="ankusa")]
      end

      # Nothing about the claim — not even that it is missing — to a stranger.
      assert authed_call(inst, :get, "/v1/claims/acme/nonexistent", nil).status == 401
    end

    test "/health needs no credential", %{inst: inst} do
      assert authed_call(inst, :get, "/health", nil).status == 200
    end

    test "tenant scope: a tenant's token reads its own claims and 403s on another tenant's",
         %{inst: inst, acme: acme, globex: globex} do
      ok = authed_call(inst, :get, acme, "acme-reader")
      assert {ok.status, ok.resp_body} == {200, "acme bytes"}
      assert authed_call(inst, :head, acme, "acme-reader").status == 200

      denied = authed_call(inst, :get, globex, "acme-reader")
      assert denied.status == 403

      assert %{"error" => "forbidden", "scope" => "tenant:globex"} =
               JSON.decode!(denied.resp_body)

      refute denied.resp_body =~ "globex bytes"

      assert authed_call(inst, :head, globex, "acme-reader").status == 403
    end

    test "tenant:* reads every tenant's claims", %{inst: inst, acme: acme, globex: globex} do
      assert authed_call(inst, :get, acme, "any-reader").resp_body == "acme bytes"
      assert authed_call(inst, :get, globex, "any-reader").resp_body == "globex bytes"
    end

    test "403 for a token without claims:read, even for its own tenant", %{inst: inst, acme: acme} do
      conn = authed_call(inst, :get, acme, "acme-writer")
      assert conn.status == 403
      assert %{"scope" => "claims:read"} = JSON.decode!(conn.resp_body)
    end

    test "a missing claim is 404 to an authorized caller only", %{inst: inst} do
      path = "/v1/claims/acme/#{Ref.claim_id(Ref.new_pack_id(), 0)}"
      assert authed_call(inst, :get, path, "acme-reader").status == 404
      assert authed_call(inst, :get, path, "guess").status == 401
    end
  end

  describe "signed refs" do
    @key "router-test-signing-key-0123456789"
    @previous "router-test-previous-key-9876543210"

    defp signed_call(inst, method, path, bearer \\ "acme-reader") do
      conn =
        method
        |> Plug.Test.conn(path, "")
        |> Plug.Conn.put_req_header("authorization", "Bearer " <> bearer)

      Router.call(conn, Router.init(instance: inst))
    end

    setup do
      inst = :"ccrs#{System.unique_integer([:positive])}"
      dir = Ankusa.TestHelpers.unique_data_dir(inst)
      on_exit(fn -> File.rm_rf(dir) end)

      tokens = [
        token("acme-reader", ["claims:read", "tenant:acme"]),
        token("any-reader", ["claims:read", "tenant:*"])
      ]

      config =
        Config.new(
          instance: inst,
          data_dir: dir,
          roles: [:claim_check],
          claim_check: %{
            ref_signing_keys: [@key, @previous],
            auth: %{mode: :token, tokens: tokens}
          }
        )

      Ankusa.put_config(config)

      {:ok, acme} = ClaimCheck.check_in(inst, "acme", [%{id: "a", body: "acme bytes"}])
      {:ok, globex} = ClaimCheck.check_in(inst, "globex", [%{id: "g", body: "globex bytes"}])

      %{inst: inst, config: config, acme: acme["a"].ref, globex: globex["g"].ref}
    end

    defp resign(ref, key, expires_at),
      do: Ref.sign(%Ref{tenant_id: ref.tenant_id, claim_id: ref.claim_id}, key, expires_at)

    test "a gateway with keys mints v2 refs, and their path redeems", %{inst: inst, acme: acme} do
      assert %Ref{signature: signature, expires_at: exp} = acme
      assert is_binary(signature) and exp > System.system_time(:second)
      assert Ref.path(acme) =~ "?exp=#{exp}&sig=#{signature}"

      for method <- [:get, :head] do
        assert signed_call(inst, method, Ref.path(acme)).status == 200
      end

      assert signed_call(inst, :get, Ref.path(acme)).resp_body == "acme bytes"
    end

    test "a ref signed with the previous key is still served", %{inst: inst, acme: acme} do
      ref = resign(acme, @previous, System.system_time(:second) + 60)
      assert signed_call(inst, :get, Ref.path(ref)).status == 200
    end

    test "an expired signed ref is 410, for GET and HEAD, and reads nothing", %{
      inst: inst,
      acme: acme
    } do
      ref = resign(acme, @key, System.system_time(:second) - 1)

      conn = signed_call(inst, :get, Ref.path(ref))
      assert {conn.status, JSON.decode!(conn.resp_body)} == {410, %{"error" => "ref_expired"}}

      # a HEAD answer has the status and no body
      assert signed_call(inst, :head, Ref.path(ref)).status == 410
    end

    test "a tampered signature, a swapped claim or expiry, or a foreign key is 400", %{
      inst: inst,
      acme: acme
    } do
      exp = System.system_time(:second) + 60
      good = resign(acme, @key, exp)
      <<first::binary-size(1), rest::binary>> = good.signature
      flipped = if(first == "A", do: "B", else: "A") <> rest
      other_claim = Ref.claim_id(elem(Ref.locate(acme.claim_id), 1), 7)

      for path <- [
            "/v1/claims/acme/#{acme.claim_id}?exp=#{exp}&sig=#{flipped}",
            # the signature is for another claim, and for another expiry
            "/v1/claims/acme/#{other_claim}?exp=#{exp}&sig=#{good.signature}",
            "/v1/claims/acme/#{acme.claim_id}?exp=#{exp + 1}&sig=#{good.signature}",
            Ref.path(resign(acme, "some-other-signing-key-0123456789", exp))
          ] do
        conn = signed_call(inst, :get, path)
        assert {conn.status, error(conn)} == {400, "bad_signature"}, path
      end
    end

    test "an expired ref with a forged signature is 400, not 410", %{inst: inst, acme: acme} do
      forged = resign(acme, "some-other-signing-key-0123456789", System.system_time(:second) - 5)
      conn = signed_call(inst, :get, Ref.path(forged))
      assert {conn.status, error(conn)} == {400, "bad_signature"}
    end

    test "exp without sig, sig without exp, and values out of grammar are 400", %{
      inst: inst,
      acme: acme
    } do
      base = "/v1/claims/acme/#{acme.claim_id}"

      for query <- [
            "?exp=#{acme.expires_at}",
            "?sig=#{acme.signature}",
            "?exp=abc&sig=#{acme.signature}",
            "?exp=#{acme.expires_at}&sig=short",
            "?exp[]=1&sig=#{acme.signature}"
          ] do
        conn = signed_call(inst, :get, base <> query)
        assert {conn.status, error(conn)} == {400, "bad_signature"}, query
      end
    end

    test "an unsigned v1 path is 400 unless accept_unsigned_refs is on", %{
      inst: inst,
      config: config,
      acme: acme
    } do
      path = "/v1/claims/acme/#{acme.claim_id}"
      conn = signed_call(inst, :get, path)
      assert {conn.status, error(conn)} == {400, "unsigned_ref"}

      Ankusa.put_config(%{
        config
        | claim_check: %{config.claim_check | accept_unsigned_refs: true}
      })

      assert signed_call(inst, :get, path).status == 200
    end

    test "authorization comes first: another tenant's credential is 403, whatever the ref", %{
      inst: inst,
      globex: globex
    } do
      assert signed_call(inst, :get, Ref.path(globex)).status == 403

      expired = resign(globex, @key, System.system_time(:second) - 1)
      conn = signed_call(inst, :get, Ref.path(expired))
      assert {conn.status, error(conn)} == {403, "forbidden"}

      conn = signed_call(inst, :get, Ref.path(globex), "any-reader")
      assert {conn.status, conn.resp_body} == {200, "globex bytes"}
    end

    test "every refusal after authorization is on the gateway event", %{inst: inst, acme: acme} do
      test = self()
      handler = "router-signed-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:ankusa, :claim_check, :gateway],
        fn _event, _measurements, meta, _ ->
          if meta.instance == inst, do: send(test, {:gateway, meta.status})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      signed_call(inst, :get, Ref.path(resign(acme, @key, System.system_time(:second) - 1)))
      assert_received {:gateway, 410}
      signed_call(inst, :get, "/v1/claims/acme/#{acme.claim_id}")
      assert_received {:gateway, 400}
    end
  end

  describe "claim_check.authorize" do
    defmodule Gate do
      def allow(%{principal: principal, tenant_id: tenant, claim_id: claim_id, conn: conn}, only) do
        send(self(), {:asked, principal.id, tenant, claim_id, conn.method})
        if tenant == only, do: :ok, else: {:error, :forbidden}
      end
    end

    setup %{config: config, inst: inst} do
      {:ok, acme} = ClaimCheck.check_in(inst, "acme", [%{id: "a", body: "acme bytes"}])
      {:ok, globex} = ClaimCheck.check_in(inst, "globex", [%{id: "g", body: "globex bytes"}])

      Ankusa.put_config(%{
        config
        | claim_check: %{config.claim_check | authorize: {Gate, :allow, ["acme"]}}
      })

      %{acme: Ref.path(acme["a"].ref), globex: Ref.path(globex["g"].ref)}
    end

    test "is asked before anything is read, with the principal, tenant, claim and conn", %{
      inst: inst,
      acme: acme
    } do
      conn = call(inst, :get, acme)
      assert {conn.status, conn.resp_body} == {200, "acme bytes"}
      assert_received {:asked, "anonymous", "acme", claim_id, "GET"}
      assert acme == "/v1/claims/acme/#{claim_id}"
    end

    test "a refusal is a 403 that names no scope, and the claim stays unread", %{
      inst: inst,
      globex: globex
    } do
      for method <- [:get, :head] do
        conn = call(inst, method, globex)
        assert conn.status == 403
        refute conn.resp_body =~ "globex bytes"
      end

      assert JSON.decode!(call(inst, :get, globex).resp_body) == %{"error" => "forbidden"}
    end

    test "replaces the built-in scope check: a token without claims:read is up to the hook", %{
      config: config,
      inst: inst,
      acme: acme
    } do
      tokens = [
        %{id: "writer", sha256: Ankusa.Admin.Auth.Token.hash("writer"), scopes: ["sources:write"]}
      ]

      Ankusa.put_config(%{
        config
        | claim_check: %{
            config.claim_check
            | authorize: {Gate, :allow, ["acme"]},
              auth: %{config.claim_check.auth | mode: :token, tokens: tokens}
          }
      })

      conn =
        :get
        |> Plug.Test.conn(acme, "")
        |> Plug.Conn.put_req_header("authorization", "Bearer writer")
        |> Router.call(Router.init(instance: inst))

      assert conn.status == 200
      assert_received {:asked, "writer", "acme", _claim_id, "GET"}
    end
  end

  describe "claim_check.max_concurrent_reads" do
    setup %{inst: inst} do
      start_reader(inst, %{max_concurrent_reads: 1})
      %{path: Ref.path(check_in(inst, "found"))}
    end

    defp hold_slot(inst) do
      parent = self()

      pid =
        spawn(fn ->
          :ok = Reader.admit(inst, 1)
          send(parent, :admitted)
          Process.sleep(:infinity)
        end)

      on_exit(fn -> Process.exit(pid, :kill) end)
      assert_receive :admitted
      pid
    end

    test "a request past the limit is 503 overloaded with a jittered Retry-After", %{
      inst: inst,
      path: path
    } do
      assert call(inst, :get, path).status == 200
      hold_slot(inst)

      for method <- [:get, :head] do
        conn = call(inst, method, path)
        assert conn.status == 503
        assert Plug.Conn.get_resp_header(conn, "retry-after") in [["1"], ["2"], ["3"]]
      end

      assert JSON.decode!(call(inst, :get, path).resp_body) == %{"error" => "overloaded"}
    end

    test "a finished request gives its slot back", %{inst: inst, path: path} do
      for _ <- 1..5, do: assert(call(inst, :get, path).status == 200)
      assert :ets.info(:"ankusa_claim_gate_#{inst}", :size) == 0
    end

    test "a request process killed mid-request leaks no slot: the reader's tick drops it", %{
      inst: inst,
      path: path
    } do
      holder = hold_slot(inst)
      assert call(inst, :get, path).status == 503

      Process.exit(holder, :kill)
      reader = Ankusa.whereis(inst, :claim_reader)
      send(reader, :tick)
      :sys.get_state(reader)

      assert call(inst, :get, path).status == 200
    end

    test "the gate refuses a bad ref before it takes a slot", %{inst: inst, path: path} do
      conn = call(inst, :get, path <> "?exp=1")
      assert conn.status == 400
      assert :ets.info(:"ankusa_claim_gate_#{inst}", :size) == 0
    end

    test "0 means no limit", %{inst: inst, path: path} do
      config = Ankusa.config(inst)
      Ankusa.put_config(%{config | claim_check: %{config.claim_check | max_concurrent_reads: 0}})
      hold_slot(inst)
      assert call(inst, :get, path).status == 200
    end
  end

  describe "integrity" do
    setup %{inst: inst} do
      root = Path.join(System.tmp_dir!(), "ankusa_claims_#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm_rf(root) end)

      start_reader(inst, %{
        blob_stores: [
          {CountingBlobStore, root: root, notify_range: {self(), 0}, range_delay_ms: 100}
        ]
      })

      %{root: root}
    end

    defp sha(bin), do: Base.encode16(:crypto.hash(:sha256, bin), case: :lower)

    # The store reads since the last call: one `0` per ranged read.
    defp ranges(acc \\ []) do
      receive do
        {:blob_range, tag} -> ranges([tag | acc])
      after
        0 -> Enum.reverse(acc)
      end
    end

    # The one pack object `check_in/2` wrote under `root`.
    defp pack_file(root) do
      [file] = root |> Path.join("**/*") |> Path.wildcard() |> Enum.filter(&File.regular?/1)
      file
    end

    defp flip_byte_of(file, body) do
      data = File.read!(file)
      {pos, _length} = :binary.match(data, body)
      at = pos + div(byte_size(body), 2)
      <<head::binary-size(^at), byte, tail::binary>> = data
      File.write!(file, [head, Bitwise.bxor(byte, 0xFF), tail])
    end

    @tag :capture_log
    test "integrity mismatch: a flipped body byte is 502, never sent; its pack's other claims are 200",
         %{inst: inst, root: root} do
      body = :crypto.strong_rand_bytes(2048)
      ref = check_in(inst, body)
      flip_byte_of(pack_file(root), body)

      conn = call(inst, :get, Ref.path(ref))
      assert {conn.status, error(conn)} == {502, "integrity_mismatch"}
      assert header(conn, "x-ankusa-sha256") == []

      {:ok, pack_id, 1} = Ref.locate(ref.claim_id)
      conn = call(inst, :get, "/v1/claims/acme/#{Ref.claim_id(pack_id, 0)}")
      assert {conn.status, conn.resp_body} == {200, "x"}
      assert header(conn, "x-ankusa-sha256") == [sha("x")]
    end

    @tag :capture_log
    test "integrity mismatch: a manifest that does not parse fails every claim of its pack", %{
      inst: inst,
      root: root
    } do
      ref = check_in(inst, "some bytes")
      file = pack_file(root)
      File.write!(file, :binary.replace(File.read!(file), ~s("claims"), ~s("claimz")))

      {:ok, pack_id, 1} = Ref.locate(ref.claim_id)

      for path <- [Ref.path(ref), "/v1/claims/acme/#{Ref.claim_id(pack_id, 0)}"] do
        conn = call(inst, :get, path)
        assert {conn.status, error(conn)} == {502, "integrity_mismatch"}
      end
    end

    @tag :capture_log
    test "integrity_check: :off serves the store's bytes unchecked, with no digest", %{
      inst: inst,
      root: root
    } do
      body = :crypto.strong_rand_bytes(2048)
      ref = check_in(inst, body)
      flip_byte_of(pack_file(root), body)

      config = Ankusa.config(inst)
      Ankusa.put_config(%{config | claim_check: %{config.claim_check | integrity_check: :off}})

      conn = call(inst, :get, Ref.path(ref))
      assert conn.status == 200
      assert byte_size(conn.resp_body) == 2048 and conn.resp_body != body
      assert header(conn, "x-ankusa-sha256") == []
      # No manifest was read: the index and the bytes were.
      assert ranges() == [0, 0]
    end

    test "a good claim carries its sha256: 200 and 206 do, HEAD and 416 do not", %{inst: inst} do
      body = :crypto.strong_rand_bytes(4096)
      path = Ref.path(check_in(inst, body))

      conn = call(inst, :get, path)
      assert {conn.status, conn.resp_body} == {200, body}
      assert header(conn, "x-ankusa-sha256") == [sha(body)]

      conn = get_range(inst, path, "bytes=0-9")
      assert {conn.status, conn.resp_body} == {206, binary_part(body, 0, 10)}
      assert header(conn, "x-ankusa-sha256") == [sha(body)]

      conn = get_range(inst, path, "bytes=99999-")
      assert conn.status == 416
      assert header(conn, "x-ankusa-sha256") == []

      conn = call(inst, :head, path)
      assert conn.status == 200
      assert header(conn, "x-ankusa-sha256") == []
    end

    test "single flight: 100 concurrent HEADs of one claim cause one store read", %{inst: inst} do
      path = Ref.path(check_in(inst, "shared"))

      conns =
        1..100
        |> Enum.map(fn _ -> Task.async(fn -> call(inst, :head, path) end) end)
        |> Task.await_many(10_000)

      assert Enum.all?(conns, &(&1.status == 200))
      assert ranges() == [0]
    end

    test "single flight: 100 concurrent GETs read the index, the manifest and the claim once each",
         %{inst: inst} do
      body = :crypto.strong_rand_bytes(1024)
      ref = check_in(inst, body)
      path = Ref.path(ref)

      conns =
        1..100
        |> Enum.map(fn _ -> Task.async(fn -> call(inst, :get, path) end) end)
        |> Task.await_many(10_000)

      assert Enum.all?(conns, &(&1.status == 200 and &1.resp_body == body))
      assert Enum.all?(conns, &(header(&1, "x-ankusa-sha256") == [sha(body)]))
      assert ranges() == [0, 0, 0]

      # The pack's digests are cached now: another claim of it costs its bytes only.
      {:ok, pack_id, 1} = Ref.locate(ref.claim_id)
      assert call(inst, :get, "/v1/claims/acme/#{Ref.claim_id(pack_id, 0)}").status == 200
      assert ranges() == [0]
    end

    test "digest: a pack of more than 1 024 claims is checked from its last row", %{inst: inst} do
      items = for i <- 1..1100, do: %{id: UUIDv7.generate(), body: "body #{i}"}
      {:ok, claims} = ClaimCheck.check_in(inst, "acme", items)

      for item <- [hd(items), List.last(items)] do
        %{ref: ref, sha256: expected} = claims[item.id]
        assert {:ok, location} = ClaimCheck.locate(inst, "acme", ref.claim_id)
        assert {:ok, ^expected} = ClaimCheck.digest(inst, location)
        assert call(inst, :get, Ref.path(ref)).status == 200
      end
    end
  end
end
