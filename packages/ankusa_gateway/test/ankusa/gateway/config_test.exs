defmodule Ankusa.Gateway.ConfigTest do
  @moduledoc "`Ankusa.Gateway.Config.new/2`: defaults, ranges, unknown keys, and `redact/1`."

  use ExUnit.Case, async: true

  alias Ankusa.Admin.Auth.Token
  alias Ankusa.BlobStore.LocalFS
  alias Ankusa.Gateway.Config

  @tls_dir Path.expand("../../../../ankusa/test/fixtures/tls", __DIR__)

  test "defaults" do
    config = Config.new()

    assert %Config{
             instance: :default,
             port: 4001,
             ip: "127.0.0.1",
             tls: nil,
             max_concurrent_requests: 1024,
             auth: %{allow_insecure: false, authenticators: []},
             claims: %{enabled: true, policy: "claims", integrity_check: :manifest}
           } = config

    assert %{"scopes" => %{module: Ankusa.Gateway.Authz.Scopes}} = config.authz.authorizers

    assert config.authz.policies == %{
             "claims" => %{mode: :first, steps: [%{use: "scopes", on_unavailable: :fail}]}
           }
  end

  test "every key's range and type error names the key" do
    for {opts, message} <- [
          {[port: 70_000], "gateway.port must be an integer from 0 to 65535, got 70000"},
          {[port: "4001"], ~s(gateway.port must be an integer from 0 to 65535, got "4001")},
          {[ip: "10"], ~r/gateway\.ip must be an IP address/},
          {[max_concurrent_requests: -1],
           "gateway.max_concurrent_requests must be a non-negative integer, got -1"},
          {[instance: "x"], ~s(gateway.instance must be an atom, got "x")},
          {[auth: %{allow_insecure: "yes"}],
           ~s(gateway.auth.allow_insecure must be true or false, got "yes")},
          {[auth: %{authenticators: :none}],
           "gateway.auth.authenticators must be a list, got :none"},
          {[claims: %{enabled: 1}], "gateway.claims.enabled must be true or false, got 1"},
          {[claims: %{integrity_check: :sha1}],
           "gateway.claims.integrity_check must be :off or :manifest, got :sha1"},
          {[claims: %{policy: "nope"}],
           ~s(gateway.claims.policy must name a policy of gateway.authz.policies, got "nope")},
          {[tls: %{keyfile: "/no/such.pem"}], ~r/gateway\.tls\.certfile is required/}
        ] do
      assert_raise ArgumentError, message, fn -> Config.new(opts) end
    end
  end

  test "an unknown key is refused naming it" do
    for {opts, key} <- [
          {[nope: 1], "gateway.nope"},
          {[auth: %{mode: :token}], "gateway.auth.mode"},
          {[claims: %{port: 1}], "gateway.claims.port"},
          {[authz: %{rules: []}], "gateway.authz.rules"},
          {[auth: %{authenticators: [%{type: :token, tokens: [tok("a")], extra: 1}]}],
           "gateway.auth.authenticators[0].extra"},
          {[auth: %{authenticators: [%{type: :token, tokens: [Map.put(tok("a"), :x, 1)]}]}],
           "gateway.auth.authenticators[0].tokens[0].x"}
        ] do
      assert_raise ArgumentError, "unknown Ankusa.Gateway.Config key: #{key}", fn ->
        Config.new(opts)
      end
    end
  end

  defp tok(id, scopes \\ ["claims:read"]), do: %{id: id, sha256: Token.hash(id), scopes: scopes}

  describe "authenticators" do
    test "are normalized with a default name, and names are unique" do
      config =
        Config.new(
          auth: %{
            authenticators: [
              %{type: :token, tokens: [tok("a")]},
              %{type: :token, name: "ci", tokens: [tok("b")]}
            ]
          }
        )

      assert [
               %{type: :token, name: "token-0", module: Ankusa.Gateway.Authn.Token},
               %{type: :token, name: "ci"}
             ] = config.auth.authenticators

      assert_raise ArgumentError, "gateway.auth.authenticators: two entries are named x", fn ->
        Config.new(
          auth: %{
            authenticators: [
              %{type: :token, name: "x", tokens: [tok("a")]},
              %{type: :token, name: "x", tokens: [tok("b")]}
            ]
          }
        )
      end

      assert_raise ArgumentError, ~r/gateway.auth.authenticators\[0\].name must be 1 to 32/, fn ->
        Config.new(
          auth: %{authenticators: [%{type: :token, name: "Bad Name", tokens: [tok("a")]}]}
        )
      end
    end

    test "at most 8, at most one mtls, and a known type" do
      nine = for i <- 1..9, do: %{type: :token, tokens: [tok("t#{i}")]}

      assert_raise ArgumentError, ~r/has 9 entries; at most 8/, fn ->
        Config.new(auth: %{authenticators: nine})
      end

      tls = %{
        certfile: Path.join(@tls_dir, "server.pem"),
        keyfile: Path.join(@tls_dir, "server-key.pem"),
        cacertfile: Path.join(@tls_dir, "ca.pem"),
        verify: :verify_peer
      }

      mtls = %{type: :mtls, subject_map: %{"cn:a" => ["claims:read"]}}

      assert_raise ArgumentError, ~r/has 2 mtls entries; at most one/, fn ->
        Config.new(tls: tls, auth: %{authenticators: [mtls, mtls]})
      end

      assert_raise ArgumentError, ~r/gateway.auth.authenticators\[0\].type must be one of/, fn ->
        Config.new(auth: %{authenticators: [%{type: :basic}]})
      end
    end

    test "token entries: duplicate ids, unknown scopes, a bad hash" do
      assert_raise ArgumentError, "gateway.auth.authenticators[0].tokens: duplicate id a", fn ->
        Config.new(auth: %{authenticators: [%{type: :token, tokens: [tok("a"), tok("a")]}]})
      end

      assert_raise ArgumentError,
                   ~r/tokens\[0\].scopes has unknown scopes \["claims:wrte"\]/,
                   fn ->
                     Config.new(
                       auth: %{
                         authenticators: [%{type: :token, tokens: [tok("a", ["claims:wrte"])]}]
                       }
                     )
                   end

      assert_raise ArgumentError, ~r/tokens\[0\].sha256 must be the 64-character hex/, fn ->
        Config.new(
          auth: %{authenticators: [%{type: :token, tokens: [%{id: "a", sha256: "abc"}]}]}
        )
      end

      # No scope at all is fine: an authorizer other than `scopes` may decide.
      assert %Config{} =
               Config.new(auth: %{authenticators: [%{type: :token, tokens: [tok("a", [])]}]})
    end

    test "jwt: core's rules, and the gateway's own keys" do
      base = %{type: :jwt, issuer: "https://idp/", audience: "gw", keys: []}

      assert_raise ArgumentError, "gateway.auth.authenticators[0] needs jwks_url or keys", fn ->
        Config.new(auth: %{authenticators: [base]})
      end

      assert_raise ArgumentError, "gateway.auth.authenticators[0].audience must be set", fn ->
        Config.new(auth: %{authenticators: [%{base | audience: nil, keys: []}]})
      end

      assert_raise ArgumentError, ~r/jwks_url must be https/, fn ->
        Config.new(auth: %{authenticators: [Map.put(base, :jwks_url, "http://idp.example/jwks")]})
      end

      jwks = Map.put(base, :jwks_url, "https://idp/jwks")

      assert_raise ArgumentError, ~r/subject_prefix must be at most 64 bytes/, fn ->
        Config.new(auth: %{authenticators: [Map.put(jwks, :subject_prefix, "has space")]})
      end

      assert_raise ArgumentError, ~r/header must be a lowercase header name/, fn ->
        Config.new(auth: %{authenticators: [Map.put(jwks, :header, "X-Token")]})
      end

      [%{config: jwt}] = Config.new(auth: %{authenticators: [jwks]}).auth.authenticators

      assert %{
               groups_claim: "groups",
               header: "authorization",
               subject_prefix: "",
               scope_claim: "scope",
               refresh_ms: 300_000
             } = jwt
    end

    test "mtls: the subject map's shapes" do
      tls = %{
        certfile: Path.join(@tls_dir, "server.pem"),
        keyfile: Path.join(@tls_dir, "server-key.pem"),
        cacertfile: Path.join(@tls_dir, "ca.pem"),
        verify: :verify_peer
      }

      [%{config: %{subject_map: map}}] =
        Config.new(
          tls: tls,
          auth: %{
            authenticators: [
              %{
                type: :mtls,
                subject_map: %{
                  "cn:a" => ["claims:read"],
                  "cn:b" => %{scopes: ["claims:read"], groups: ["ops"]}
                }
              }
            ]
          }
        ).auth.authenticators

      assert map == %{
               "cn:a" => %{scopes: ["claims:read"], groups: []},
               "cn:b" => %{scopes: ["claims:read"], groups: ["ops"]}
             }

      assert_raise ArgumentError,
                   "gateway.auth.authenticators[0].subject_map[cn:a] must be a list of scopes or a map with scopes and groups",
                   fn ->
                     Config.new(
                       tls: tls,
                       auth: %{authenticators: [%{type: :mtls, subject_map: %{"cn:a" => "all"}}]}
                     )
                   end
    end
  end

  describe "authorizers and policies" do
    defmodule Allow do
      @behaviour Ankusa.Gateway.Authorizer
      def init!(opts, _key), do: opts
      def kind(_), do: :inline
      def decide(_, _, _), do: {:ok, :allow, %{}}
    end

    test "scopes is reserved, steps name defined authorizers, modes and on_unavailable are checked" do
      assert_raise ArgumentError,
                   ~r/gateway.authz.authorizers.scopes: the name scopes is reserved/,
                   fn ->
                     Config.new(authz: %{authorizers: %{"scopes" => {Allow, %{}}}})
                   end

      assert_raise ArgumentError,
                   ~r/must name a module implementing Ankusa.Gateway.Authorizer/,
                   fn ->
                     Config.new(authz: %{authorizers: %{"x" => {Enum, %{}}}})
                   end

      assert_raise ArgumentError, ~r/gateway.authz.policies.p.steps\[0\].use names nope/, fn ->
        Config.new(authz: %{policies: %{"p" => %{steps: ["nope"]}}})
      end

      assert_raise ArgumentError,
                   ~r/gateway.authz.policies.p.mode must be :first, :all or :any/,
                   fn ->
                     Config.new(authz: %{policies: %{"p" => %{mode: :most, steps: ["scopes"]}}})
                   end

      assert_raise ArgumentError, ~r/steps must be a non-empty list/, fn ->
        Config.new(authz: %{policies: %{"p" => %{steps: []}}})
      end

      assert_raise ArgumentError, ~r/on_unavailable must be :fail, :abstain or :deny/, fn ->
        Config.new(
          authz: %{policies: %{"p" => %{steps: [%{use: "scopes", on_unavailable: :retry}]}}}
        )
      end

      config =
        Config.new(
          authz: %{
            authorizers: %{"allow" => {Allow, %{x: 1}}},
            policies: %{
              "claims" => %{mode: :any, steps: [%{use: "allow", on_unavailable: :deny}, "scopes"]}
            }
          }
        )

      assert config.authz.authorizers["allow"] ==
               %{module: Allow, state: %{x: 1, instance: :default}}

      assert config.authz.policies["claims"] == %{
               mode: :any,
               steps: [
                 %{use: "allow", on_unavailable: :deny},
                 %{use: "scopes", on_unavailable: :fail}
               ]
             }
    end
  end

  test "redact/1 shows token ids and names, hides the hashes and static keys" do
    {_, public} =
      {:ec, "P-256"} |> JOSE.JWK.generate_key() |> JOSE.JWK.to_public() |> JOSE.JWK.to_map()

    config =
      Config.new(
        auth: %{
          authenticators: [
            %{type: :token, name: "ci", tokens: [tok("worker")]},
            %{type: :jwt, issuer: "https://idp/", audience: "gw", keys: [public]}
          ]
        }
      )

    shown = Config.redact(config)
    json = JSON.encode!(shown)

    assert [%{"name" => "ci", "type" => "token", "config" => %{"tokens" => [token]}}, jwt] =
             shown["auth"]["authenticators"]

    assert token["id"] == "worker"
    assert token["sha256"] == "[REDACTED]"
    refute json =~ Token.hash("worker")
    assert jwt["config"]["issuer"] == "https://idp/"
    refute json =~ public["x"]
    assert shown["port"] == 4001
    assert shown["claims"]["policy"] == "claims"
  end

  describe "spaces and objects" do
    @tag :tmp_dir
    test "a space loads with its defaults, and its store opts carry the objects keys", %{
      tmp_dir: dir
    } do
      config =
        Config.new(
          spaces: %{"docs" => %{store: {LocalFS, root: dir}, policy: "claims"}},
          objects: %{timeout_ms: 2_000}
        )

      space = config.spaces["docs"]

      assert %{
               name: "docs",
               prefix: "",
               max_object_bytes: 67_108_864,
               read_first_bytes: 1_048_576,
               read_only: false
             } = space

      assert Config.store_opts(config, space) == [
               root: dir,
               op_deadline_ms: 30_000,
               timeout_ms: 2_000,
               retries: 1
             ]
    end

    @tag :tmp_dir
    test "two spaces on one store must not overlap; prefixes obey the key rules", %{tmp_dir: dir} do
      store = {LocalFS, root: dir}
      space = fn prefix -> %{store: store, prefix: prefix, policy: "claims"} end

      assert %{} = Config.new(spaces: %{"a" => space.("a/"), "b" => space.("b/")}).spaces

      for {a, b} <- [{"", "a/"}, {"a/", "a/"}, {"a/", "a/b/"}] do
        assert_raise ArgumentError, ~r/share a store and their prefixes overlap/, fn ->
          Config.new(spaces: %{"one" => space.(a), "two" => space.(b)})
        end
      end

      # Two stores apart: overlapping prefixes are fine.
      other = %{store: {LocalFS, root: Path.join(dir, "other")}, prefix: "", policy: "claims"}
      assert %{} = Config.new(spaces: %{"one" => space.(""), "two" => other}).spaces

      for {prefix, message} <- [
            {"docs/.ankusa/", ~r/not a valid key prefix/},
            {"docs", ~r/must be empty or end with \//},
            {"a//", ~r/not a valid key prefix/},
            {"../", ~r/not a valid key prefix/}
          ] do
        assert_raise ArgumentError, message, fn ->
          Config.new(spaces: %{"docs" => space.(prefix)})
        end
      end
    end

    @tag :tmp_dir
    test "every error names its key", %{tmp_dir: dir} do
      store = {LocalFS, root: dir}

      for {spaces, objects, message} <- [
            {%{"claims" => %{store: store, policy: "claims"}}, %{}, "gateway.spaces.claims"},
            {%{"Docs" => %{store: store, policy: "claims"}}, %{}, "gateway.spaces: a space name"},
            {%{"docs" => %{store: store, policy: "nope"}}, %{}, "gateway.spaces.docs.policy"},
            {%{"docs" => %{store: store}}, %{}, "gateway.spaces.docs.policy"},
            {%{"docs" => %{store: {LocalFS, []}, policy: "claims"}}, %{}, "needs :root"},
            {%{"docs" => %{store: {String, []}, policy: "claims"}}, %{},
             "does not implement the object operations"},
            {%{"docs" => %{store: store, policy: "claims", colour: "red"}}, %{},
             "unknown Ankusa.Gateway.Config key: gateway.spaces.docs.colour"},
            {%{"docs" => %{store: store, policy: "claims", read_first_bytes: 10}}, %{},
             "gateway.spaces.docs.read_first_bytes"},
            {%{"docs" => %{store: store, policy: "claims"}}, %{max_inflight_bytes: 1_000},
             "gateway.objects.max_inflight_bytes (1000) must be at least"},
            {%{}, %{retries: 11}, "gateway.objects.retries"}
          ] do
        error =
          assert_raise ArgumentError, fn -> Config.new(spaces: spaces, objects: objects) end

        assert error.message =~ message
      end
    end
  end
end
