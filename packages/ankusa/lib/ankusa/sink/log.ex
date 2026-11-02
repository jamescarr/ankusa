defmodule Ankusa.Sink.Log do
  @moduledoc "A sink that logs each delivered hook. Useful as a default/no-op."

  @behaviour Ankusa.Sink

  require Logger

  @impl true
  def deliver(env, ctx, _opts) do
    Logger.info(
      "hook delivered id=#{env.id}#{event_type(env.event_type)} " <>
        "source=#{ctx.source_id} attempt=#{ctx.attempt}"
    )

    :ok
  end

  defp event_type(event_type) when is_binary(event_type) and event_type != "",
    do: " event_type=#{event_type}"

  defp event_type(_event_type), do: ""

  # A log line keeps nothing: a node that crashed right after saying `:ok`
  # would have dropped the hook. `wal.type: none` must not ack on this.
  @impl true
  def durable?(_opts), do: false
end
