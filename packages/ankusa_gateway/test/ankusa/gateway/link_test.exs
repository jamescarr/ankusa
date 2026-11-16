defmodule Ankusa.Gateway.LinkTest do
  @moduledoc """
  Share links v1 (`Ankusa.Gateway.Sharing.Link`) and the `link` authenticator
  (`Ankusa.Gateway.Authn.Link`) through `Ankusa.Gateway.Authn`: the canonical
  string and its HMAC, rotation, the grammar, the encoded path, and the routes
  a link is and is not a credential for.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Admin.Auth.Token
  alias Ankusa.Gateway.ACL
  alias Ankusa.Gateway.Authn
  alias Ankusa.Gateway.Sharing.Link
  alias Ankusa.Gateway.Spaces

  @moduletag :capture_log

  @key "teams/eng/q3.pdf"
  @new_key String.duplicate("n", 32)
  @old_key String.duplicate("o", 32)

  setup do
    config = test_config(roles: [])
    start_supervised!({Ankusa.Instance, config})
    inst = config.instance

    gateway =
      Ankusa.Gateway.Config.new(
        instance: inst,
        port: 0,
        auth: %{
          authenticators: [
            %{type: :token, tokens: [%{id: "tok", sha256: Token.hash("tok"), scopes: []}]}
          ]
        },
        sharing: %{enabled: true, signing_keys: [@new_key, @old_key]}
      )

    start_supervised!({Ankusa.Gateway, gateway})
    %{inst: inst}
  end

  defp now, do: System.os_time(:second)

  defp link(space, key, exp), do: link([@new_key, @old_key], space, key, exp)

  defp link(keys, space, key, exp) do
    id = ACL.grant_id({:object, key})
    sig = Link.sign(keys, space, id, exp)
    {id, sig, Link.path(space, key, id, exp, sig)}
  end

  defp authn(inst, method, path) do
    Plug.Test.conn(method, path)
    |> Plug.Conn.assign(:ankusa_opts, instance: inst)
    |> Authn.call([])
  end

  defp capture_authn do
    id = {__MODULE__, make_ref()}

    :telemetry.attach(
      id,
      [:ankusa, :gateway, :authn],
      fn _event, _measurements, meta, pid -> send(pid, {:authn, meta}) end,
      self()
    )

    on_exit(fn -> :telemetry.detach(id) end)
    id
  end

  describe "the canonical string" do
    test "names the format, the space, the grant and the expiry" do
      id = ACL.grant_id({:object, @key})

      assert Link.canonical("docs", id, 1_800_000_000) ==
               "urn:ankusa:link:v1:docs:#{id}:1800000000"
    end

    test "a signature is the unpadded base64url HMAC-SHA256 under the first key" do
      id = ACL.grant_id({:object, @key})
      sig = Link.sign([@new_key, @old_key], "docs", id, 1_800_000_000)

      assert sig =~ ~r/\A[A-Za-z0-9_-]{43}\z/
      refute String.ends_with?(sig, "=")

      expected =
        :hmac
        |> :crypto.mac(:sha256, @new_key, Link.canonical("docs", id, 1_800_000_000))
        |> Base.url_encode64(padding: false)

      assert sig == expected
    end
  end

  describe "verification and rotation" do
    test "any listed key verifies a signature, so an old key can be kept beside a new one" do
      id = ACL.grant_id({:object, @key})
      exp = 1_800_000_000
      signed_with_old = Link.sign([@old_key], "docs", id, exp)

      assert Link.valid?([@new_key, @old_key], "docs", id, exp, signed_with_old)
      assert Link.valid?([@old_key, @new_key], "docs", id, exp, signed_with_old)
      refute Link.valid?([@new_key], "docs", id, exp, signed_with_old)
    end

    test "another space, grant, expiry or a changed signature does not verify" do
      id = ACL.grant_id({:object, @key})
      exp = 1_800_000_000
      sig = Link.sign([@new_key], "docs", id, exp)

      assert Link.valid?([@new_key], "docs", id, exp, sig)
      refute Link.valid?([@new_key], "other", id, exp, sig)
      refute Link.valid?([@new_key], "docs", ACL.grant_id({:object, "a"}), exp, sig)
      refute Link.valid?([@new_key], "docs", id, exp + 1, sig)
      refute Link.valid?([@new_key], "docs", id, exp, String.duplicate("A", 43))
    end
  end

  describe "the grammar" do
    test "exp is a positive decimal and sig 43 url-safe characters" do
      sig = String.duplicate("A", 43)

      assert Link.grammar?("1800000000", sig)
      assert Link.grammar?("1", sig)
      assert Link.grammar?("999999999999", sig)

      for exp <- ["0", "0123", "", "1a", "9999999999999", "-1", " 1"] do
        refute Link.grammar?(exp, sig), inspect(exp)
      end

      assert Link.grammar?("1", String.duplicate("-", 43))
      assert Link.grammar?("1", String.duplicate("_", 43))

      for bad <- [
            String.duplicate("A", 42),
            String.duplicate("A", 44),
            "!" <> String.duplicate("A", 42)
          ] do
        refute Link.grammar?("1", bad), inspect(bad)
      end

      refute Link.grammar?(nil, sig)
      refute Link.grammar?("1", nil)
    end
  end

  describe "path/5" do
    test "percent-encodes each key segment and reads back with key_from_segments/1" do
      key = "my file/x?y=1/100%"
      id = ACL.grant_id({:object, key})
      exp = now() + 600
      sig = Link.sign([@new_key], "docs", id, exp)

      path = Link.path("docs", key, id, exp, sig)
      uri = URI.parse(path)

      assert uri.path == "/v1/objects/docs/my%20file/x%3Fy%3D1/100%25"

      segments = uri.path |> String.split("/") |> Enum.drop(4)
      assert Spaces.key_from_segments(segments) == {:ok, key}

      assert URI.decode_query(uri.query) == %{
               "link" => id,
               "exp" => Integer.to_string(exp),
               "sig" => sig
             }
    end
  end

  describe "the link authenticator" do
    test "a valid link authenticates as link:<grant id>, via :link", %{inst: inst} do
      {id, _sig, path} = link("docs", @key, now() + 600)

      conn = authn(inst, :get, path)

      refute conn.halted
      assert conn.status == nil
      assert conn.assigns.principal_via == :link
      assert conn.assigns.principal.id == "link:" <> id
      assert conn.assigns.principal.scopes == []
      assert conn.assigns.principal.tenants == []
      assert conn.assigns.principal.groups == []
      assert byte_size(conn.assigns.subject_key) == 16
    end

    test "a HEAD of the object reads the link too", %{inst: inst} do
      {id, _sig, path} = link("docs", @key, now() + 600)

      conn = authn(inst, :head, path)
      refute conn.halted
      assert conn.assigns.principal.id == "link:" <> id
    end

    test "an expired link is 410 link_expired", %{inst: inst} do
      {_id, _sig, path} = link("docs", @key, now() - 5)

      conn = authn(inst, :get, path)

      assert conn.halted
      assert conn.status == 410
      assert JSON.decode!(conn.resp_body) == %{"error" => "link_expired"}
    end

    test "a tampered signature is 401", %{inst: inst} do
      {id, _sig, _path} = link("docs", @key, now() + 600)
      exp = now() + 600

      path = Link.path("docs", @key, id, exp, String.duplicate("A", 43))
      conn = authn(inst, :get, path)

      assert conn.status == 401
      assert JSON.decode!(conn.resp_body) == %{"error" => "unauthenticated"}

      assert Plug.Conn.get_resp_header(conn, "www-authenticate") == [~s(Bearer realm="ankusa")]
    end

    test "a link minted for one key does not authenticate another key", %{inst: inst} do
      {id, sig, _path} = link("docs", @key, now() + 600)
      exp = now() + 600

      other = "teams/eng/other.pdf"
      assert ACL.hash(other) != ACL.hash(@key)

      conn = authn(inst, :get, Link.path("docs", other, id, exp, sig))

      assert conn.status == 401
      assert JSON.decode!(conn.resp_body) == %{"error" => "unauthenticated"}
    end

    test "an unknown link id is 401", %{inst: inst} do
      exp = now() + 600
      id = ACL.grant_id({:object, "some/other/key"})
      sig = Link.sign([@new_key], "docs", id, exp)

      conn = authn(inst, :get, Link.path("docs", @key, id, exp, sig))
      assert conn.status == 401
    end

    test "an extra query parameter authenticates from the same cache entry", %{inst: inst} do
      capture_authn()
      {_id, _sig, path} = link("docs", @key, now() + 600)

      first = authn(inst, :get, path)
      refute first.halted
      assert_receive {:authn, %{authenticator: "link", result: :ok, cached: false}}

      second = authn(inst, :get, path <> "&utm_source=mail")
      refute second.halted
      assert second.assigns.principal.id == first.assigns.principal.id
      assert second.assigns.subject_key == first.assigns.subject_key
      assert_receive {:authn, %{authenticator: "link", result: :ok, cached: true}}
    end

    test "a link is no credential for a write, a delete, a listing or a sharing route",
         %{inst: inst} do
      {_id, _sig, path} = link("docs", @key, now() + 600)
      query = path |> URI.parse() |> Map.fetch!(:query)

      for {method, target} <- [
            {:put, "/v1/objects/docs/#{@key}?#{query}"},
            {:delete, "/v1/objects/docs/#{@key}?#{query}"},
            {:get, "/v1/objects/docs?#{query}"},
            {:post, "/v1/spaces/docs/links?#{query}"}
          ] do
        conn = authn(inst, method, target)

        assert conn.status == 401, inspect({method, target})
        assert JSON.decode!(conn.resp_body) == %{"error" => "unauthenticated"}
      end
    end

    test "a request without a credential is 401, links or not", %{inst: inst} do
      conn = authn(inst, :get, "/v1/objects/docs/" <> @key)

      assert conn.status == 401
      assert Plug.Conn.get_resp_header(conn, "www-authenticate") == [~s(Bearer realm="ankusa")]
    end
  end
end
