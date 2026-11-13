defmodule Ankusa.Gateway.MetricsTest do
  @moduledoc """
  `/metrics` after real requests: every series family `Ankusa.Gateway.Metrics`
  defines for authentication, authorization, the remote callers and the
  caches is in the scrape.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Admin.Auth.Token
  alias Ankusa.ClaimCheck.Ref
  alias Ankusa.Gateway.Authz
  alias Ankusa.Gateway.Authz.Request

  @moduletag :capture_log

  defmodule Remote do
    @moduledoc false
    @behaviour Ankusa.Gateway.Authorizer

    @impl true
    def init!(opts, key),
      do: Map.put(opts, :remote, Ankusa.Gateway.Authz.Remote.options!(opts, key))

    @impl true
    def kind(_state), do: :remote

    @impl true
    def decide(_request, %{answer: :allow}, _ctx), do: {:ok, :allow, %{}}
    def decide(_request, %{answer: :down}, _ctx), do: {:error, {:unavailable, :down}}
  end

  defp get(port, path, headers \\ []),
    do: Req.get!("http://127.0.0.1:#{port}#{path}", retry: false, headers: headers)

  test "one authenticated, authorized claim read shows every family" do
    metrics_port = free_port()
    port = free_port()

    config =
      test_config(
        roles: [],
        metrics: %{enabled: true, port: metrics_port, extra: [Ankusa.Gateway.Metrics]}
      )

    start_supervised!({Ankusa.Instance, config})
    inst = config.instance

    gateway =
      Ankusa.Gateway.Config.new(
        instance: inst,
        port: port,
        auth: %{
          authenticators: [
            %{
              type: :token,
              tokens: [%{id: "w", sha256: Token.hash("tok"), scopes: ["claims:read", "tenant:*"]}]
            }
          ]
        },
        authz: %{
          authorizers: %{
            "pdp" => {Remote, %{answer: :allow}},
            "flaky" => {Remote, %{answer: :down, breaker_failures: 1}}
          },
          policies: %{
            "claims" => %{mode: :all, steps: ["pdp", "scopes"]},
            "flaky" => %{mode: :first, steps: ["flaky"]}
          }
        }
      )

    start_supervised!({Ankusa.Gateway, gateway})

    {:ok, claims} = Ankusa.ClaimCheck.check_in(inst, "acme", [%{id: "a", body: "hello"}])

    assert %{status: 200, body: "hello"} =
             get(port, Ref.path(claims["a"].ref), [{"authorization", "Bearer tok"}])

    # One breaker transition, from the flaky authorizer.
    principal = Ankusa.Principal.new("w", [])

    request = %Request{
      principal: principal,
      via: :token,
      subject_key: Ankusa.Gateway.Authn.subject_key(:token, principal),
      action: :read,
      resource: %{kind: :object, space: "docs", key: "k", tenant: nil}
    }

    assert {:error, {:unavailable, :down}} = Authz.decide(inst, "flaky", request)

    # The gauges, now rather than at their next tick.
    caller = Ankusa.whereis(inst, {:gateway_caller, {:authz, "pdp"}})
    send(caller, :tick)
    :sys.get_state(caller)
    Ankusa.Gateway.Gauges.sample(inst)

    scrape = get(metrics_port, "/metrics").body

    for family <- [
          ~s(ankusa_gateway_authn_total{authenticator="token-0",cached="false",instance="#{inst}",result="ok"} 1),
          ~s(ankusa_gateway_authz_decisions_total{action="read",cached="false",decision="allow",instance="#{inst}",policy="claims"} 1),
          "ankusa_gateway_authz_duration_seconds_bucket{",
          ~s(ankusa_gateway_calls_total{caller="authz:pdp",instance="#{inst}",result="ok"} 1),
          "ankusa_gateway_call_duration_seconds_bucket{",
          ~s(ankusa_gateway_caller_limit{caller="authz:pdp",instance="#{inst}"} 64),
          ~s(ankusa_gateway_caller_inflight{caller="authz:pdp",instance="#{inst}"} 0),
          ~s(ankusa_gateway_caller_queued{caller="authz:pdp",instance="#{inst}"} 0),
          ~s(ankusa_gateway_caller_breaker_transitions_total{caller="authz:flaky",instance="#{inst}",state="open"} 1),
          ~s(ankusa_gateway_cache_entries{cache="authz",instance="#{inst}"} 1),
          ~s(ankusa_gateway_cache_bytes{cache="authn",instance="#{inst}"}),
          ~s(ankusa_gateway_objects_inflight_bytes{instance="#{inst}"} 0)
        ] do
      assert scrape =~ family
    end
  end
end
