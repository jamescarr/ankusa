defmodule Ankusa.Health.Check.Sinks do
  @moduledoc """
  `sinks`, under `wal: :none` (the direct ack path) only: no sink failed its
  last publish within `listener.ready_sink_error_window_ms`. Reason `publish_failed`.

  In that mode the broker is the durable store and the provider is the retry, so
  a node whose sinks refuse every publish answers every hook `503`; unready tells
  the load balancer to send them elsewhere. A sink that publishes again clears
  its own entry (`Ankusa.Health.SinkErrors`), so the node is ready the moment the
  broker is back. Under `wal: :disk` and `:dual` the queue retries a failing
  sink, and the hook is acked regardless: there is nothing to report here.
  """

  @behaviour Ankusa.Health.Check

  alias Ankusa.Health.SinkErrors

  @impl true
  def name, do: :sinks

  @impl true
  def applies?(config), do: config.wal == :none

  @impl true
  def gates?(_config), do: true

  @impl true
  def run(config) do
    case SinkErrors.failing(config.instance, config.listener.ready_sink_error_window_ms) do
      0 -> :ok
      _failing -> {:failing, :publish_failed}
    end
  end
end
