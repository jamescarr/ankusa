defmodule Ankusa.Health.Check.Intake do
  @moduledoc """
  `intake`, under `wal: :dual` only: the intake log answers. The reason is
  `down` (the breaker is open and the log is not answering: the load balancer
  drains the node) or `recovering` (boot replay is running).
  """

  @behaviour Ankusa.Health.Check

  @impl true
  def name, do: :intake

  @impl true
  def applies?(config), do: config.wal == :dual and Ankusa.Instance.store?(config)

  @impl true
  def gates?(_config), do: true

  @impl true
  def run(config) do
    case Ankusa.Intake.state(config.instance) do
      :up -> :ok
      state -> {:failing, state}
    end
  end
end
