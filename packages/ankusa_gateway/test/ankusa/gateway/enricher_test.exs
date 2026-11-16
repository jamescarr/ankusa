defmodule Ankusa.Gateway.EnricherTest do
  @moduledoc """
  `gateway.auth.enrichers`: the enriched principal is what the plug fingerprints
  and caches, the enricher's own result cache, `on_unavailable: :skip`, both
  enrichers (`okta_groups`, `http`) against `Req.Test` stubs, and groups
  reaching a `rules` decision end to end through `Ankusa.Gateway.Router`.
  """

  use ExUnit.Case, async: false

  alias Ankusa.Admin.Auth.Token
  alias Ankusa.ClaimCheck.{Reader, Stores}
  alias Ankusa.Gateway.{Authn, Cache}
  alias Ankusa.Gateway.Config, as: GatewayConfig
  alias Ankusa.Gateway.Router
  alias Ankusa.Principal

  @claim_path "/v1/claims/acme/01M39VMD8RA3C5HR4RBV67Y002"

  setup do
    inst = :"enr#{System.unique_integer([:positive])}"
    dir = Ankusa.TestHelpers.unique_data_dir(inst)
    on_exit(fn -> File.rm_rf(dir) end)
    Ankusa.put_config(Ankusa.Config.new(instance: inst, roles: [], data_dir: dir))
    %{inst: inst}
  end

  # A bearer authenticator that counts and answers `Principal.new("alice")` for
  # any token: two tokens, one subject.
  defmodule AnyToken do
    @moduledoc false
    @behaviour Ankusa.Gateway.Authenticator

    @impl true
    def init!(_config, _key), do: %{}

    @impl true
    def credential(conn, _ctx) do
      case Plug.Conn.get_req_header(conn, "authorization") do
        ["Bearer " <> token] -> {:ok, token}
        _other -> :none
      end
    end

    @impl true
    def authenticate(_conn, _ctx) do
      principal = %Principal{
        id: "alice",
        scopes: ["claims:read"],
        tenants: :all,
        claims: %{"uid" => "u1"}
      }

      {:ok, principal, nil}
    end
  end

  defp token(id, claims \\ %{}) do
    %{
      id: id,
      sha256: Token.hash(id),
      scopes: ["claims:read", "tenant:*"],
      claims: claims
    }
  end

  defp okta_groups(extra \\ %{}) do
    Map.merge(
      %{
        type: :okta_groups,
        domain: "acme.okta.com",
        api_token: "00-token",
        req_options: [plug: {Req.Test, __MODULE__}]
      },
      extra
    )
  end

  defp http_enricher(extra) do
    Map.merge(
      %{
        type: :http,
        url: "https://dir.test/users/{{ claims.uid }}",
        groups_path: "data.groups",
        req_options: [plug: {Req.Test, __MODULE__}]
      },
      extra
    )
  end

  defp config(inst, authenticators, opts) do
    auth =
      [authenticators: authenticators, enrichers: Keyword.get(opts, :enrichers, [])]
      |> put_opt(:cache, Keyword.get(opts, :cache))

    GatewayConfig.new([instance: inst, auth: auth] ++ Keyword.take(opts, [:authz]))
  end

  defp put_opt(list, _key, nil), do: list
  defp put_opt(list, key, value), do: Keyword.put(list, key, value)

  # Store `config` and start what the plug needs: the cache and every
  # authenticator's and enricher's children.
  defp boot(inst, config) do
    Ankusa.Gateway.put_config(config)

    start_supervised!(
      Cache.child_spec_for(inst, :authn,
        max_entries: 10_000,
        max_bytes: 1_048_576,
        max_ttl_ms: 86_400_000
      )
    )

    Enum.each(Ankusa.Gateway.Supervisor.authn_children(config), &start_supervised!/1)
    config
  end

  defp run(inst, headers) do
    Plug.Test.conn(:get, @claim_path)
    |> Plug.Conn.assign(:ankusa_opts, instance: inst)
    |> then(&Enum.reduce(headers, &1, fn {k, v}, c -> Plug.Conn.put_req_header(c, k, v) end))
    |> Authn.call([])
  end

  defp get(inst, token) do
    Plug.Test.conn(:get, @claim_path)
    |> Plug.Conn.put_req_header("authorization", "Bearer " <> token)
    |> Router.call(Router.init(instance: inst))
  end

  defp bearer(token), do: [{"authorization", "Bearer " <> token}]

  defp counter, do: :counters.new(1, [:write_concurrency])

  defp calls(count), do: :counters.get(count, 1)

  test "okta_groups follows the link header's next page, encoding the user id", %{inst: inst} do
    test = self()

    Req.Test.stub(__MODULE__, fn conn ->
      send(test, {:page, conn.request_path, conn.query_string})
      send(test, {:authorization, Plug.Conn.get_req_header(conn, "authorization")})

      case conn.query_string do
        "limit=200" ->
          conn
          |> Plug.Conn.put_resp_header(
            "link",
            ~s(<https://acme.okta.com/api/v1/users/u%2F1/groups?limit=200&after=abc>; rel="next")
          )
          |> Req.Test.json([%{"profile" => %{"name" => "engineering"}}])

        "limit=200&after=abc" ->
          Req.Test.json(conn, [
            %{"profile" => %{"name" => "ops"}},
            %{"profile" => %{"name" => "engineering"}}
          ])
      end
    end)

    boot(
      inst,
      config(inst, [%{type: :token, tokens: [token("worker", %{"uid" => "u/1"})]}],
        enrichers: [okta_groups()]
      )
    )

    conn = run(inst, bearer("worker"))
    refute conn.halted

    assert conn.assigns.principal.groups == ["engineering", "ops"]

    assert_receive {:page, "/api/v1/users/u%2F1/groups", "limit=200"}
    assert_receive {:page, "/api/v1/users/u%2F1/groups", "limit=200&after=abc"}
    assert_receive {:authorization, ["SSWS 00-token"]}
  end

  test "okta_groups does not follow a next page on another host", %{inst: inst} do
    count = counter()

    Req.Test.stub(__MODULE__, fn conn ->
      :counters.add(count, 1, 1)

      conn
      |> Plug.Conn.put_resp_header("link", ~s(<https://evil.test/groups?after=abc>; rel="next"))
      |> Req.Test.json([%{"profile" => %{"name" => "engineering"}}])
    end)

    boot(
      inst,
      config(inst, [%{type: :token, tokens: [token("worker", %{"uid" => "u1"})]}],
        enrichers: [okta_groups()]
      )
    )

    assert run(inst, bearer("worker")).assigns.principal.groups == ["engineering"]
    assert calls(count) == 1
  end

  test "an enricher's groups reach a rules decision, through the router", %{inst: inst} do
    Req.Test.stub(__MODULE__, fn conn ->
      case conn.request_path do
        "/api/v1/users/u1/groups" -> Req.Test.json(conn, [%{"profile" => %{"name" => "readers"}}])
        _other -> Req.Test.json(conn, [%{"profile" => %{"name" => "others"}}])
      end
    end)

    config =
      config(
        inst,
        [
          %{
            type: :token,
            tokens: [
              token("alice", %{"uid" => "u1"}),
              token("bob", %{"uid" => "u2"})
            ]
          }
        ],
        enrichers: [okta_groups()],
        authz: %{
          authorizers: %{
            "team" =>
              {Ankusa.Gateway.Authz.Rules,
               %{
                 rules: [
                   %{
                     effect: :allow,
                     actions: [:read],
                     spaces: ["claims"],
                     keys: ["{tenant}/**"],
                     when: %{groups_any: ["readers"]}
                   }
                 ]
               }}
          },
          policies: %{"claims" => %{mode: :first, steps: ["team"]}}
        }
      )

    boot(inst, config)
    start_supervised!({Stores, instance: inst})
    start_supervised!({Reader, instance: inst})

    # Authorized by the group the enricher added; the claim itself does not exist.
    assert get(inst, "alice").status == 404

    # The same rule, a principal whose group is not readers.
    assert get(inst, "bob").status == 403
  end

  test "on_unavailable: fail is 503 auth_unavailable, and nothing is cached", %{inst: inst} do
    count = counter()

    Req.Test.stub(__MODULE__, fn conn ->
      :counters.add(count, 1, 1)
      Plug.Conn.send_resp(conn, 500, "down")
    end)

    boot(
      inst,
      config(inst, [%{type: :token, tokens: [token("worker", %{"uid" => "u1"})]}],
        enrichers: [okta_groups()]
      )
    )

    conn = run(inst, bearer("worker"))
    assert conn.status == 503
    assert JSON.decode!(conn.resp_body) == %{"error" => "auth_unavailable"}

    assert run(inst, bearer("worker")).status == 503
    assert calls(count) == 2
  end

  test "on_unavailable: skip carries the unenriched principal, and calls again after the TTL",
       %{inst: inst} do
    count = counter()

    Req.Test.stub(__MODULE__, fn conn ->
      :counters.add(count, 1, 1)
      Plug.Conn.send_resp(conn, 429, "slow down")
    end)

    boot(
      inst,
      config(inst, [%{type: :token, tokens: [token("worker", %{"uid" => "u1"})]}],
        enrichers: [okta_groups(%{on_unavailable: :skip})],
        cache: %{negative_ttl_ms: 200}
      )
    )

    conn = run(inst, bearer("worker"))
    refute conn.halted
    assert conn.assigns.principal.id == "worker"
    assert conn.assigns.principal.groups == []
    assert calls(count) == 1

    # Cached for the negative TTL only: no second call, then a new one.
    refute run(inst, bearer("worker")).halted
    assert calls(count) == 1

    Process.sleep(300)
    refute run(inst, bearer("worker")).halted
    assert calls(count) == 2
  end

  test "a second token for the same subject reuses the enricher's result", %{inst: inst} do
    count = counter()

    Req.Test.stub(__MODULE__, fn conn ->
      :counters.add(count, 1, 1)
      Req.Test.json(conn, [%{"profile" => %{"name" => "readers"}}])
    end)

    boot(inst, config(inst, [%{type: AnyToken}], enrichers: [okta_groups()]))

    assert run(inst, bearer("token-a")).assigns.principal.groups == ["readers"]
    assert run(inst, bearer("token-b")).assigns.principal.groups == ["readers"]
    assert calls(count) == 1
  end

  test "the http enricher adds groups and merges claims without overriding the token's",
       %{inst: inst} do
    test = self()

    Req.Test.stub(__MODULE__, fn conn ->
      send(test, {:url, conn.request_path})
      send(test, {:headers, Plug.Conn.get_req_header(conn, "authorization")})

      Req.Test.json(conn, %{
        "data" => %{
          "groups" => ["eng", "ops"],
          "profile" => %{"team" => "directory", "region" => "eu"}
        }
      })
    end)

    boot(
      inst,
      config(
        inst,
        [
          %{
            type: :token,
            tokens: [token("worker", %{"uid" => "a/b c", "team" => "from-token"})]
          }
        ],
        enrichers: [
          http_enricher(%{
            url: "https://dir.test/users/{{ claims.uid }}",
            headers: %{"authorization" => "Bearer dir-token"},
            claims_path: "data.profile"
          })
        ]
      )
    )

    conn = run(inst, bearer("worker"))
    refute conn.halted

    assert conn.assigns.principal.groups == ["eng", "ops"]

    assert conn.assigns.principal.claims == %{
             "uid" => "a/b c",
             "team" => "from-token",
             "region" => "eu"
           }

    assert_receive {:url, "/users/a%2Fb%20c"}
    assert_receive {:headers, ["Bearer dir-token"]}
  end

  test "an http enricher's origin cannot be templated, at boot" do
    assert_raise ArgumentError,
                 ~r/gateway\.auth\.enrichers\[0\]\.url: the scheme, host and port of a URL cannot be templated/,
                 fn ->
                   GatewayConfig.new(
                     auth: %{
                       authenticators: [
                         %{type: :token, tokens: [token("worker", %{"uid" => "u1"})]}
                       ],
                       enrichers: [
                         %{
                           type: :http,
                           url: "https://{{ claims.host }}/groups",
                           groups_path: "groups"
                         }
                       ]
                     }
                   )
                 end
  end

  test "the common options and the chain are checked at boot" do
    authenticators = [%{type: :token, name: "workers", tokens: [token("worker")]}]

    assert_raise ArgumentError,
                 "gateway.auth.enrichers[0].only names ops, which is not an authenticator of the chain (defined: workers)",
                 fn ->
                   GatewayConfig.new(
                     auth: %{
                       authenticators: authenticators,
                       enrichers: [okta_groups(%{only: ["ops"]})]
                     }
                   )
                 end

    assert_raise ArgumentError,
                 "gateway.auth.enrichers[0].on_unavailable must be :fail or :skip, got :abstain",
                 fn ->
                   GatewayConfig.new(
                     auth: %{
                       authenticators: authenticators,
                       enrichers: [okta_groups(%{on_unavailable: :abstain})]
                     }
                   )
                 end

    assert_raise ArgumentError, "gateway.auth.enrichers has 5 entries; at most 4", fn ->
      GatewayConfig.new(
        auth: %{
          authenticators: authenticators,
          enrichers: List.duplicate(okta_groups(), 5)
        }
      )
    end

    assert_raise ArgumentError, ~r/gateway\.auth\.enrichers\[0\]\.type must be one of/, fn ->
      GatewayConfig.new(auth: %{authenticators: authenticators, enrichers: [%{type: :nope}]})
    end
  end

  test "the http enricher adds nothing when a path does not name what it reads", %{inst: inst} do
    Req.Test.stub(__MODULE__, fn conn ->
      Req.Test.json(conn, %{"data" => %{"groups" => %{"not" => "a list"}, "profile" => "nope"}})
    end)

    boot(
      inst,
      config(inst, [%{type: :token, tokens: [token("worker", %{"uid" => "u1"})]}],
        enrichers: [http_enricher(%{claims_path: "data.profile"})]
      )
    )

    conn = run(inst, bearer("worker"))
    refute conn.halted
    assert conn.assigns.principal.groups == []
    assert conn.assigns.principal.claims == %{"uid" => "u1"}
  end

  test "the http enricher answers unavailable for a body or a status it cannot use" do
    for reply <- [
          fn conn -> Plug.Conn.send_resp(conn, 500, "down") end,
          fn conn -> Plug.Conn.send_resp(conn, 200, "not json") end,
          fn conn -> Req.Test.json(conn, ["a list, not an object"]) end
        ] do
      inst = :"enr#{System.unique_integer([:positive])}"

      boot(
        inst,
        config(inst, [%{type: :token, tokens: [token("worker", %{"uid" => "u1"})]}],
          enrichers: [http_enricher(%{})]
        )
      )

      Req.Test.stub(__MODULE__, reply)
      assert run(inst, bearer("worker")).status == 503
    end
  end

  test "an http enricher's headers and paths are checked at boot" do
    authenticators = [%{type: :token, tokens: [token("worker")]}]

    for {enricher, message} <- [
          {http_enricher(%{groups_path: "data..groups"}), "must be dotted segments"},
          {http_enricher(%{groups_path: 5}), "must be a dotted path string"},
          {http_enricher(%{url: "http://dir.test/users"}), "must be an https URL"},
          {http_enricher(%{headers: Map.new(1..17, &{"h#{&1}", "v"})}),
           "at most 16 header names"},
          {http_enricher(%{headers: [{"a", "b"}]}), "must be a map of at most 16 header names"},
          {http_enricher(%{groups_path: nil}), "needs groups_path or claims_path"}
        ] do
      assert_raise ArgumentError, ~r/#{message}/, fn ->
        GatewayConfig.new(auth: %{authenticators: authenticators, enrichers: [enricher]})
      end
    end
  end

  test "an enricher runs only after the authenticators it names", %{inst: inst} do
    count = counter()

    Req.Test.stub(__MODULE__, fn conn ->
      :counters.add(count, 1, 1)
      Req.Test.json(conn, [%{"profile" => %{"name" => "readers"}}])
    end)

    boot(
      inst,
      config(
        inst,
        [
          %{type: :token, name: "workers", tokens: [token("worker", %{"uid" => "u1"})]},
          %{type: :token, name: "people", tokens: [token("person", %{"uid" => "u2"})]}
        ],
        enrichers: [okta_groups(%{only: ["people"]})]
      )
    )

    assert run(inst, bearer("worker")).assigns.principal.groups == []
    assert calls(count) == 0

    assert run(inst, bearer("person")).assigns.principal.groups == ["readers"]
    assert calls(count) == 1
  end
end
