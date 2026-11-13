defmodule Ankusa.Gateway.Authz.Scopes do
  @moduledoc """
  The built-in authorizer `"scopes"`, the claims API's rule since before the
  gateway left core: a claim read needs `claims:read` and the claim's tenant
  (`tenant:<tenant_id>` or `tenant:*`). The deny names the scope the principal
  lacks. Anything but a claim read abstains.

  It is always defined, and the default `"claims"` policy is `[scopes]`.
  """

  @behaviour Ankusa.Gateway.Authorizer

  alias Ankusa.Gateway.Authz.Request
  alias Ankusa.Principal

  @impl true
  def init!(_opts, _key), do: %{}

  @impl true
  def kind(_state), do: :inline

  @impl true
  def decide(%Request{action: :read, resource: %{kind: :claim, tenant: tenant}} = request, _, _) do
    principal = request.principal

    cond do
      not Principal.scope?(principal, "claims:read") -> {:ok, :deny, %{scope: "claims:read"}}
      not Principal.tenant?(principal, tenant) -> {:ok, :deny, %{scope: "tenant:#{tenant}"}}
      true -> {:ok, :allow, %{}}
    end
  end

  def decide(%Request{}, _state, _ctx), do: {:ok, :abstain, %{}}
end
