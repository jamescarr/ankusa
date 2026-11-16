defmodule Ankusa.Gateway.Health do
  @moduledoc """
  The gateway's `GET /ready`: the node's own readiness (`Ankusa.Health.ready/1`:
  draining, and the store, sinks and disk checks of a node that has them; a
  gateway-only node has none) with `gateway: "ok"` added while the gateway
  runs.
  """

  @spec ready(atom()) :: {:ok, map()} | {:error, map()}
  def ready(instance) do
    case Ankusa.Health.ready(instance) do
      {:ok, body} -> {:ok, Map.put(body, :gateway, "ok")}
      {:error, body} -> {:error, Map.put(body, :gateway, "ok")}
    end
  end
end
