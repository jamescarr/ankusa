defmodule Ankusa.Gateway.AuthzTest do
  @moduledoc """
  The policy engine (`Ankusa.Gateway.Authz.decide/4`): the three modes over
  stub authorizers, `on_unavailable`, and an authorizer that breaks.
  """

  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Ankusa.Gateway.Authz
  alias Ankusa.Gateway.Authz.Request
  alias Ankusa.Gateway.Config, as: GatewayConfig

  defmodule Stub do
    @behaviour Ankusa.Gateway.Authorizer

    @impl true
    def init!(%{answer: _} = opts, _key), do: opts

    @impl true
    def kind(_state), do: :inline

    @impl true
    def decide(_request, %{answer: :raise}, _ctx), do: raise("boom")
    def decide(_request, %{answer: :garbage}, _ctx), do: :yes
    def decide(_request, %{answer: :unavailable}, _ctx), do: {:error, {:unavailable, :down}}

    def decide(_request, %{answer: answer} = state, _ctx),
      do: {:ok, answer, Map.get(state, :meta, %{})}
  end

  @stubs %{
    "allow" => {Stub, %{answer: :allow}},
    "deny" => {Stub, %{answer: :deny, meta: %{scope: "x:y"}}},
    "abstain" => {Stub, %{answer: :abstain}},
    "down" => {Stub, %{answer: :unavailable}},
    "boom" => {Stub, %{answer: :raise}},
    "garbage" => {Stub, %{answer: :garbage}}
  }

  setup do
    %{inst: :"authz#{System.unique_integer([:positive])}"}
  end

  defp decide(inst, mode, steps) do
    Ankusa.Gateway.put_config(
      GatewayConfig.new(
        instance: inst,
        authz: %{authorizers: @stubs, policies: %{"p" => %{mode: mode, steps: steps}}}
      )
    )

    request = %Request{
      principal: Ankusa.Principal.operator(),
      via: :anonymous,
      subject_key: <<0::128>>,
      action: :read,
      resource: %{kind: :object, space: "docs", key: "a", tenant: nil}
    }

    Authz.decide(inst, "p", request)
  end

  defp verdict({:allow, _meta}), do: :allow
  defp verdict({:deny, _meta}), do: :deny
  defp verdict({:error, {:unavailable, _}}), do: :unavailable

  test "the truth table of the three modes", %{inst: inst} do
    table = [
      # steps, first, all, any
      {["allow"], :allow, :allow, :allow},
      {["deny"], :deny, :deny, :deny},
      {["abstain"], :deny, :deny, :deny},
      {["abstain", "abstain"], :deny, :deny, :deny},
      {["allow", "deny"], :allow, :deny, :allow},
      {["deny", "allow"], :deny, :deny, :allow},
      {["abstain", "allow"], :allow, :allow, :allow},
      {["abstain", "deny"], :deny, :deny, :deny},
      {["allow", "abstain"], :allow, :allow, :allow},
      {["allow", "allow"], :allow, :allow, :allow}
    ]

    for {steps, first, all, any} <- table do
      assert verdict(decide(inst, :first, steps)) == first, "first #{inspect(steps)}"
      assert verdict(decide(inst, :all, steps)) == all, "all #{inspect(steps)}"
      assert verdict(decide(inst, :any, steps)) == any, "any #{inspect(steps)}"
    end
  end

  test "the deciding step's meta is the decision's, with its name", %{inst: inst} do
    assert {:deny, %{scope: "x:y", authorizer: "deny"}} =
             decide(inst, :first, ["abstain", "deny"])

    assert {:allow, %{authorizer: "allow"}} = decide(inst, :all, ["allow", "abstain"])
    assert {:deny, %{authorizer: nil}} = decide(inst, :any, ["abstain"])
    assert {:deny, %{authorizer: "deny", scope: "x:y"}} = decide(inst, :any, ["deny", "abstain"])
  end

  test "on_unavailable: fail ends the decision, abstain and deny stand in", %{inst: inst} do
    assert {:error, {:unavailable, :down}} = decide(inst, :first, ["down", "allow"])
    assert {:error, {:unavailable, :down}} = decide(inst, :any, [%{use: "down"}, "allow"])

    assert {:allow, _} =
             decide(inst, :first, [%{use: "down", on_unavailable: :abstain}, "allow"])

    assert {:deny, %{authorizer: "down"}} =
             decide(inst, :first, [%{use: "down", on_unavailable: :deny}, "allow"])

    assert {:allow, _} = decide(inst, :any, [%{use: "down", on_unavailable: :deny}, "allow"])

    # An earlier decision is not undone by a later outage.
    assert {:allow, _} = decide(inst, :first, ["allow", "down"])
    assert {:error, {:unavailable, :down}} = decide(inst, :all, ["allow", "down"])
  end

  test "an authorizer that raises or answers garbage is unavailable", %{inst: inst} do
    log =
      capture_log(fn ->
        assert {:error, {:unavailable, {:authorizer, "boom"}}} = decide(inst, :first, ["boom"])

        assert {:error, {:unavailable, {:authorizer, "garbage"}}} =
                 decide(inst, :first, ["garbage"])
      end)

    assert log =~ "gateway authorizer boom failed"
    assert log =~ "gateway authorizer garbage failed"

    capture_log(fn ->
      assert {:allow, _} =
               decide(inst, :first, [%{use: "boom", on_unavailable: :abstain}, "allow"])
    end)
  end

  test "the scopes authorizer: claims:read and the tenant, abstain for anything else" do
    alias Ankusa.Gateway.Authz.Scopes
    claim = %{kind: :claim, space: "claims", key: "acme/x", tenant: "acme"}

    request = fn principal, action, resource ->
      %Request{
        principal: principal,
        via: :token,
        subject_key: <<>>,
        action: action,
        resource: resource
      }
    end

    reader = Ankusa.Principal.new("r", ["claims:read", "tenant:acme"])
    writer = Ankusa.Principal.new("w", ["sources:write", "tenant:acme"])
    other = Ankusa.Principal.new("o", ["claims:read", "tenant:globex"])

    assert {:ok, :allow, _} = Scopes.decide(request.(reader, :read, claim), %{}, %{})

    assert {:ok, :deny, %{scope: "claims:read"}} =
             Scopes.decide(request.(writer, :read, claim), %{}, %{})

    assert {:ok, :deny, %{scope: "tenant:acme"}} =
             Scopes.decide(request.(other, :read, claim), %{}, %{})

    object = %{kind: :object, space: "docs", key: "a", tenant: nil}
    assert {:ok, :abstain, _} = Scopes.decide(request.(reader, :read, object), %{}, %{})
    assert {:ok, :abstain, _} = Scopes.decide(request.(reader, :write, claim), %{}, %{})
  end
end
