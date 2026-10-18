defmodule Ankusa.Health do
  @moduledoc """
  Readiness, as `GET /ready` on the ingress and admin listeners answers it.

  `GET /health` is liveness: the listener answers, nothing more. `/ready` asks
  whether this node can ack a hook right now: on a node that runs the local
  store (`Ankusa.Instance.store?/1`), whether the store takes a synced write
  and no write has failed in the last 5 s (`Ankusa.Store.ready/1`; an ingest
  batch refused on a full disk while the probe's few bytes still fit). A full
  disk, a store that is closed or reopening, or a store process that does not
  answer turns the node unready; it turns ready again on its own once writes
  succeed. A node without a store has
  nothing to check and is ready while it runs.

  The body never carries a raw error term: `store` is `"ok"`, `"none"`,
  `"store_unavailable"` or `"write_failed"`.
  """

  alias Ankusa.Instance
  alias Ankusa.Store

  @type body :: %{status: String.t(), instance: String.t(), store: String.t()}

  @spec ready(atom()) :: {:ok, body()} | {:error, body()}
  def ready(instance) do
    name = to_string(instance)

    if Instance.store?(Ankusa.config(instance)) do
      case Store.ready(instance) do
        :ok ->
          {:ok, %{status: "ready", instance: name, store: "ok"}}

        {:error, reason} ->
          {:error, %{status: "unavailable", instance: name, store: label(reason)}}
      end
    else
      {:ok, %{status: "ready", instance: name, store: "none"}}
    end
  end

  defp label(:store_unavailable), do: "store_unavailable"
  defp label(_reason), do: "write_failed"
end
