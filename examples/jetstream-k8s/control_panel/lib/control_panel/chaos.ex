defmodule ControlPanel.Chaos do
  @moduledoc """
  The levers: kill pods, cut a connection, break the gateway.

  * `kill_pod/1`, `kill_random/1`: delete a pod with `gracePeriodSeconds=0`,
    which is a SIGKILL, not a polite shutdown, so it behaves like a crash.
  * `deny/1`, `allow/1`: an Istio `AuthorizationPolicy` with action `DENY` on
    the server workload, matching the client's ServiceAccount principal. The
    sidecars speak mTLS to each other, so that principal is the caller's
    identity. A policy takes effect for new connections at once and for
    existing ones when Envoy drains them, a few seconds later.
  * `fault/1`: set or clear `fault` on the gateway `VirtualService`'s ingest
    route (`/spec/http/0/fault`), so a fraction of `POST /webhooks/demo`
    requests are answered `503` by Envoy, or delayed.

  Every function returns `:ok` or `{:error, reason}`.
  """

  alias ControlPanel.{Cluster, K8s}

  @nats %{"app.kubernetes.io/name" => "nats"}
  @claim_check %{"app.kubernetes.io/name" => "claim-check"}

  # lever id => {the server's pod labels, the client's ServiceAccount}
  @levers %{
    "ankusa-nats" => {@nats, "ankusa"},
    "consumer-nats" => {@nats, "consumer"},
    "consumer-claim-check" => {@claim_check, "consumer"}
  }

  @kill_groups ~w(ankusa consumer nats claim-check)

  @type lever :: String.t()

  @spec levers() :: [lever()]
  def levers, do: @levers |> Map.keys() |> Enum.sort()

  ## Pods

  @spec kill_pod(String.t()) :: :ok | {:error, term()}
  def kill_pod(name) do
    "/api/v1/namespaces/#{K8s.namespace()}/pods/#{name}"
    |> K8s.delete(gracePeriodSeconds: 0)
    |> ok_or_error()
  end

  @spec kill_random(String.t()) :: :ok | {:error, term()}
  def kill_random(group) when group in @kill_groups do
    candidates =
      for pod <- Cluster.snapshot().pods,
          pod.group == group,
          not pod.terminating?,
          pod.phase == "Running",
          do: pod.name

    case candidates do
      [] -> {:error, :none}
      names -> names |> Enum.random() |> kill_pod()
    end
  end

  ## Connections

  @spec deny(lever()) :: :ok | {:error, term()}
  def deny(lever) do
    with {:ok, {selector, service_account}} <- Map.fetch(@levers, lever) do
      ns = K8s.namespace()

      policy = %{
        apiVersion: "security.istio.io/v1",
        kind: "AuthorizationPolicy",
        metadata: %{name: policy_name(lever)},
        spec: %{
          selector: %{matchLabels: selector},
          action: "DENY",
          rules: [
            %{from: [%{source: %{principals: ["cluster.local/ns/#{ns}/sa/#{service_account}"]}}]}
          ]
        }
      }

      "/apis/security.istio.io/v1/namespaces/#{ns}/authorizationpolicies"
      |> K8s.post(policy)
      |> ok_or_error(already_exists: 409)
    else
      :error -> {:error, :unknown_lever}
    end
  end

  @spec allow(lever()) :: :ok | {:error, term()}
  def allow(lever) when is_map_key(@levers, lever) do
    "/apis/security.istio.io/v1/namespaces/#{K8s.namespace()}/authorizationpolicies/#{policy_name(lever)}"
    |> K8s.delete()
    |> ok_or_error(not_found: 404)
  end

  def allow(_lever), do: {:error, :unknown_lever}

  defp policy_name(lever), do: "chaos-deny-" <> lever

  ## Gateway faults

  @spec fault(:none | {:abort, number()} | {:delay, number(), pos_integer()}) ::
          :ok | {:error, term()}
  def fault(:none) do
    # `remove` of a member that is not there is a 422: nothing to clear.
    virtual_service()
    |> K8s.json_patch([%{op: "remove", path: "/spec/http/0/fault"}])
    |> ok_or_error(nothing_to_remove: 422)
  end

  def fault({:abort, pct}) do
    set_fault(%{abort: %{httpStatus: 503, percentage: %{value: pct}}})
  end

  def fault({:delay, pct, ms}) do
    set_fault(%{delay: %{fixedDelay: "#{ms}ms", percentage: %{value: pct}}})
  end

  # JSON Patch `add` on an existing member replaces it.
  defp set_fault(fault) do
    virtual_service()
    |> K8s.json_patch([%{op: "add", path: "/spec/http/0/fault", value: fault}])
    |> ok_or_error()
  end

  defp virtual_service,
    do: "/apis/networking.istio.io/v1/namespaces/#{K8s.namespace()}/virtualservices/demo-routes"

  ## Results

  # `tolerated` names HTTP statuses that mean "already in the state you asked for".
  defp ok_or_error(result, tolerated \\ [])
  defp ok_or_error({:ok, _body}, _tolerated), do: :ok

  defp ok_or_error({:error, {status, _body}} = error, tolerated) do
    if status in Keyword.values(tolerated), do: :ok, else: error
  end

  defp ok_or_error({:error, _reason} = error, _tolerated), do: error
end
