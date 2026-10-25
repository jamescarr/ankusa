defmodule Ankusa.Health.Check.Store do
  @moduledoc """
  `store`: on a node that runs the local store, whether it takes a synced write
  and no write failed in the last 5 s (`Ankusa.Store.ready/1`). The reason is
  the store's failure class (`Ankusa.Store.failure_class/1`: `no_space`,
  `io_error`, `read_only`, `corruption`, `busy`, `other`, plus `timeout` and
  `not_running` when it does not answer).
  """

  @behaviour Ankusa.Health.Check

  @impl true
  def name, do: :store

  @impl true
  def applies?(config), do: Ankusa.Instance.store?(config)

  @impl true
  def gates?(_config), do: true

  @impl true
  def run(config) do
    case Ankusa.Store.ready(config.instance) do
      :ok -> :ok
      {:error, {_kind, class}} -> {:failing, class}
    end
  end
end
