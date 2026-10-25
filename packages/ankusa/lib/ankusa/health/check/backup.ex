defmodule Ankusa.Health.Check.Backup do
  @moduledoc """
  `backup`, with `backup.enabled` and `backup.max_age_ms`: the last successful
  backup is no older than `max_age_ms`. Failing (reason `stale`) when it is
  older, or when the uploader is not running. Gates only with
  `backup.require_fresh`; otherwise it is a label on a node that stays ready.
  """

  @behaviour Ankusa.Health.Check

  alias Ankusa.Store.Backup

  @impl true
  def name, do: :backup

  @impl true
  def applies?(%{backup: %{enabled: true, max_age_ms: max_age}} = config)
      when is_integer(max_age),
      do: Ankusa.Instance.store?(config)

  def applies?(_config), do: false

  @impl true
  def gates?(config), do: config.backup.require_fresh

  @impl true
  def run(config) do
    case Backup.age_ms(config.instance) do
      age when is_integer(age) and age <= config.backup.max_age_ms -> :ok
      _stale_or_unknown -> {:failing, :stale}
    end
  end
end
