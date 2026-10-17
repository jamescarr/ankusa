defmodule ControlPanel.Chaos do
  @moduledoc """
  The levers: kill pods, cut a connection, break the gateway, replay the
  durable fleet's DLQ.

  * `kill_pod/1`, `kill_random/1`, `kill_group/1`: delete a pod (or every pod
    of a group) with `gracePeriodSeconds=0`, which is a SIGKILL, not a polite
    shutdown, so it behaves like a crash.
  * `deny/1`, `allow/1`: an Istio `AuthorizationPolicy` with action `DENY` on
    the server workload, matching the client's ServiceAccount principal. The
    sidecars speak mTLS to each other, so that principal is the caller's
    identity. A policy takes effect for new connections at once and for
    existing ones when Envoy drains them, a few seconds later.
  * `fault/1`: set or clear `fault` on both ingest routes of the gateway
    `VirtualService` (`/spec/http/0/fault` for `POST /webhooks/demo`,
    `/spec/http/1/fault` for `POST /webhooks/durable`), so a fraction of
    requests are answered `503` by Envoy, or delayed.
  * `replay_dlq/0`: start a DLQ replay on every ready durable pod through its
    admin API (`POST :4002/v1/replays`). Each pod has its own store, so each
    replays its own dead rows.

  Every function returns `:ok` (`replay_dlq/0`: `{:ok, pods}`) or
  `{:error, reason}`.
  """

  alias ControlPanel.{Cluster, K8s}

  @nats %{"app.kubernetes.io/name" => "nats"}
  @claim_check %{"app.kubernetes.io/name" => "claim-check"}

  # lever id => {the server's pod labels, the client's ServiceAccount}
  @levers %{
    "ankusa-nats" => {@nats, "ankusa"},
    "durable-nats" => {@nats, "ankusa-durable"},
    "consumer-nats" => {@nats, "consumer"},
    "consumer-claim-check" => {@claim_check, "consumer"}
  }

  @kill_groups ~w(ankusa ankusa-durable consumer nats claim-check)
  @durable "ankusa-durable"

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
    case running(group) do
      [] -> {:error, :none}
      names -> names |> Enum.random() |> kill_pod()
    end
  end

  @doc "Kill every running pod of `group` at once: the first error, if any, after trying them all."
  @spec kill_group(String.t()) :: :ok | {:error, term()}
  def kill_group(group) when group in @kill_groups do
    case running(group) do
      [] ->
        {:error, :none}

      names ->
        names |> Enum.map(&kill_pod/1) |> Enum.find(:ok, &(&1 != :ok))
    end
  end

  defp running(group) do
    for pod <- Cluster.snapshot().pods,
        pod.group == group,
        not pod.terminating?,
        pod.phase == "Running",
        do: pod.name
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
    patch_routes(fn path -> [%{op: "remove", path: path}] end, nothing_to_remove: 422)
  end

  def fault({:abort, pct}) do
    set_fault(%{abort: %{httpStatus: 503, percentage: %{value: pct}}})
  end

  def fault({:delay, pct, ms}) do
    set_fault(%{delay: %{fixedDelay: "#{ms}ms", percentage: %{value: pct}}})
  end

  # JSON Patch `add` on an existing member replaces it.
  defp set_fault(fault),
    do: patch_routes(fn path -> [%{op: "add", path: path, value: fault}] end)

  # One patch per ingest route, so a 422 on one (nothing to remove) does not
  # cancel the other: a JSON Patch fails as a whole.
  defp patch_routes(ops, tolerated \\ []) do
    results =
      for path <- ~w(/spec/http/0/fault /spec/http/1/fault) do
        virtual_service() |> K8s.json_patch(ops.(path)) |> ok_or_error(tolerated)
      end

    Enum.find(results, :ok, &(&1 != :ok))
  end

  defp virtual_service,
    do: "/apis/networking.istio.io/v1/namespaces/#{K8s.namespace()}/virtualservices/demo-routes"

  ## The durable fleet's DLQ

  @doc """
  Start a replay of source `durable`'s dead rows on every ready durable pod.
  A pod that already runs one answers `200` with it, which counts as started.
  """
  @spec replay_dlq() :: {:ok, pos_integer()} | {:error, term()}
  def replay_dlq do
    case for(pod <- Cluster.snapshot().pods, pod.group == @durable, pod.ready?, pod.ip, do: pod) do
      [] ->
        {:error, :none}

      pods ->
        failures =
          pods
          |> Task.async_stream(&start_replay/1,
            timeout: 6_000,
            on_timeout: :kill_task,
            zip_input_on_exit: true
          )
          |> Enum.flat_map(fn
            {:ok, :ok} -> []
            {:ok, {:error, failure}} -> [failure]
            {:exit, {pod, reason}} -> [{pod.name, reason}]
          end)

        if failures == [], do: {:ok, length(pods)}, else: {:error, {:replay, Map.new(failures)}}
    end
  end

  defp start_replay(%{name: name, ip: ip}) do
    case Req.post("http://#{ip}:4002/v1/replays",
           body: JSON.encode!(%{kind: "dlq", source_id: "durable"}),
           headers: [{"content-type", "application/json"}],
           retry: false,
           receive_timeout: 5_000
         ) do
      {:ok, %Req.Response{status: status}} when status in 200..299 -> :ok
      {:ok, %Req.Response{status: status}} -> {:error, {name, status}}
      {:error, exception} -> {:error, {name, exception}}
    end
  end

  ## Results

  # `tolerated` names HTTP statuses that mean "already in the state you asked for".
  defp ok_or_error(result, tolerated \\ [])
  defp ok_or_error({:ok, _body}, _tolerated), do: :ok

  defp ok_or_error({:error, {status, _body}} = error, tolerated) do
    if status in Keyword.values(tolerated), do: :ok, else: error
  end

  defp ok_or_error({:error, _reason} = error, _tolerated), do: error
end
