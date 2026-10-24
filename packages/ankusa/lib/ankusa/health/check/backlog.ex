defmodule Ankusa.Health.Check.Backlog do
  @moduledoc """
  `backlog`, with `queue.max_oldest_due_ms` set: the oldest due delivery row has
  waited no longer than that (the figure `ankusa_queue_oldest_due_age_seconds`
  reports, from the same read). Over it the reason is `over_threshold`.

  A label, never a reason to be unready: a node with a backlog still takes
  hooks, and taking it out of the load balancer would only move the load. A
  store that does not answer is `unavailable` (the `store` check is what
  gates).
  """

  @behaviour Ankusa.Health.Check

  alias Ankusa.Queue.Deliveries

  @impl true
  def name, do: :backlog

  @impl true
  def applies?(config),
    do: config.queue.max_oldest_due_ms != nil and Ankusa.Instance.store?(config)

  @impl true
  def gates?(_config), do: false

  @impl true
  def run(config) do
    case Deliveries.next_due_at(config.instance, %{}, tenants: 1) do
      {:ok, nil} ->
        :ok

      {:ok, due_at} ->
        if System.system_time(:millisecond) - due_at > config.queue.max_oldest_due_ms,
          do: {:failing, :over_threshold},
          else: :ok

      {:error, _reason} ->
        {:failing, :unavailable}
    end
  end
end
