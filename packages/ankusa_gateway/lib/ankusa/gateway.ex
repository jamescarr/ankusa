defmodule Ankusa.Gateway do
  @moduledoc """
  The gateway: an HTTP listener that serves claim-check refs
  (`GET /v1/claims/:tenant_id/:claim_id`) and authenticates and authorizes
  every request with a configurable chain (ADR 0005).

  It runs beside a core instance, never without one: the instance owns the
  claim stores, the blob store pools and the listener limits the gateway reads.
  Start both, in this order, under one `:one_for_one` supervisor:

      children = [
        {Ankusa.Instance, Ankusa.Config.new(instance: :default, roles: [])},
        {Ankusa.Gateway, Ankusa.Gateway.Config.new(instance: :default, port: 4001)}
      ]

  `roles: []` is a gateway-only node; a node that also takes or dispatches
  hooks keeps its roles. The gateway's own failure domain is `:gateway`
  (`Ankusa.Instance.Isolated`): a crash in it never restarts the instance.

  ## Request pipeline

  Every route but `GET /health` and `GET /ready`:

    1. **Authenticate** (`Ankusa.Gateway.Authn`): the ordered authenticators of
       `gateway.auth.authenticators`. `401` when none accepts the request,
       `503 auth_unavailable` when one could not decide.
    2. **Authorize** (`Ankusa.Gateway.Authz`): the resource's policy, a list
       of authorizer steps. `403` on deny, `503 authz_unavailable` when a step
       could not decide and fails closed.
    3. **Admit**: at most `gateway.max_concurrent_requests` requests in flight
       on this node, else `503 overloaded`.
    4. **Serve**.

  ## Telemetry

    * `[:ankusa, :gateway, :request]` — `%{duration}`, metadata
      `%{instance, api, method, status}`: one per authorized request answered
      (`api` is `:claims`). `Ankusa.Gateway.Metrics` exports it as
      `ankusa_gateway_requests_total{api,status}` and
      `ankusa_gateway_request_duration_seconds{api}`; add the module to core's
      `metrics.extra` to see them on the node's `/metrics`.
  """

  alias Ankusa.Gateway.Config

  @doc """
  The child spec of the gateway `config` describes. Building it stores the
  config (`config/1` reads it back by instance name), so build it only for a
  gateway you are about to start.
  """
  @spec child_spec(Config.t()) :: Supervisor.child_spec()
  def child_spec(%Config{instance: instance} = config) do
    :persistent_term.put(key(instance), config)

    %{
      id: {__MODULE__, instance},
      start: {Ankusa.Gateway.Supervisor, :start_link, [instance]},
      type: :supervisor
    }
  end

  @doc "The config of `instance`'s gateway. Raises when none was stored."
  @spec config(atom()) :: Config.t()
  def config(instance), do: :persistent_term.get(key(instance))

  @doc "The config of `instance`'s gateway, or `:error` when none was stored."
  @spec fetch_config(atom()) :: {:ok, Config.t()} | :error
  def fetch_config(instance) do
    case :persistent_term.get(key(instance), nil) do
      nil -> :error
      config -> {:ok, config}
    end
  end

  @doc false
  # Store `config` without a child spec: tests that call the router directly.
  @spec put_config(Config.t()) :: :ok
  def put_config(%Config{instance: instance} = config),
    do: :persistent_term.put(key(instance), config)

  defp key(instance), do: {__MODULE__, :config, instance}
end
