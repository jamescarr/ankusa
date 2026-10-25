defmodule Ankusa.Health.Check.Drain do
  @moduledoc """
  `drain`, only while the node is being stopped (`Ankusa.Health.drain/1`, which
  `Ankusa.Instance.Drain` calls first when the VM is told to stop): the state is
  `draining`, reason `shutting_down`, and `/ready` answers `503` with
  `status: "draining"`. A node that is not stopping has no `drain` key.
  """

  @behaviour Ankusa.Health.Check

  @impl true
  def name, do: :drain

  @impl true
  def applies?(config), do: Ankusa.Health.draining?(config.instance)

  @impl true
  def gates?(_config), do: true

  @impl true
  def run(_config), do: {:draining, :shutting_down}
end
