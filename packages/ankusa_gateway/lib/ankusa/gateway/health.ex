defmodule Ankusa.Gateway.Health do
  @moduledoc """
  The gateway's `GET /ready`: the node's own readiness (`Ankusa.Health.ready/1`:
  draining, and the store, sinks and disk checks of a node that has them; a
  gateway-only node has none) with `gateway: "ok"` added while the gateway
  runs, and `authorizers: %{name => "ok" | "not_ready"}` for every authorizer
  that exports `ready?/3`. A `not_ready` one makes the answer `503`: only an
  authorizer that cannot decide anything until it has loaded (a `table`
  snapshot before its first load) ever says so, and only for a bounded time.
  An authorizer whose backend is down is not a readiness matter: its requests
  fail closed, per policy step.
  """

  @spec ready(atom()) :: {:ok, map()} | {:error, map()}
  def ready(instance) do
    authorizers = authorizers(instance)
    not_ready? = Enum.any?(authorizers, fn {_name, state} -> state == "not_ready" end)

    body_of = fn body ->
      body
      |> Map.put(:gateway, "ok")
      |> then(&if authorizers == %{}, do: &1, else: Map.put(&1, :authorizers, authorizers))
    end

    case Ankusa.Health.ready(instance) do
      {:ok, body} when not_ready? -> {:error, %{body_of.(body) | status: "unavailable"}}
      {:ok, body} -> {:ok, body_of.(body)}
      {:error, body} -> {:error, body_of.(body)}
    end
  end

  defp authorizers(instance) do
    case Ankusa.Gateway.fetch_config(instance) do
      {:ok, gateway} ->
        for {name, %{module: module, state: state}} <- gateway.authz.authorizers,
            function_exported?(module, :ready?, 3),
            into: %{},
            do: {name, if(module.ready?(instance, name, state), do: "ok", else: "not_ready")}

      :error ->
        %{}
    end
  end
end
