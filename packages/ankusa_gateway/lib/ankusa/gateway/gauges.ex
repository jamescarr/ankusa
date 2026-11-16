defmodule Ankusa.Gateway.Gauges do
  @moduledoc """
  Samples the gateway's state every 5 seconds for `/metrics`:
  `[:ankusa, :gateway, :cache_state]` `%{entries, bytes}` `%{instance, cache}`
  for each of its caches (`Ankusa.Gateway.Cache`), and
  `[:ankusa, :gateway, :objects_state]` `%{inflight_bytes}` `%{instance}`, the
  objects API's byte budget, after releasing what requests that died still
  held (`Ankusa.Gateway.Objects.Budget.sweep/1`).
  """

  use GenServer

  alias Ankusa.Gateway.Cache
  alias Ankusa.Gateway.Objects.Budget
  alias Ankusa.Telemetry

  @interval_ms 5_000
  @caches [:authn, :authz, :acl]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, instance, name: Ankusa.via(instance, :gateway_gauges))
  end

  def child_spec(opts),
    do: %{
      id: {__MODULE__, Keyword.fetch!(opts, :instance)},
      start: {__MODULE__, :start_link, [opts]}
    }

  @impl true
  def init(instance) do
    Process.send_after(self(), :tick, @interval_ms)
    {:ok, instance}
  end

  @impl true
  def handle_info(:tick, instance) do
    sample(instance)
    Process.send_after(self(), :tick, @interval_ms)
    {:noreply, instance}
  end

  def handle_info(_message, instance), do: {:noreply, instance}

  @doc false
  # One sample, now (tests).
  @spec sample(atom()) :: :ok
  def sample(instance) do
    for cache <- @caches do
      Telemetry.emit([:gateway, :cache_state], Cache.stats(instance, cache), %{
        instance: instance,
        cache: cache
      })
    end

    Telemetry.emit([:gateway, :objects_state], %{inflight_bytes: Budget.sweep(instance)}, %{
      instance: instance
    })

    :ok
  end
end
