defmodule Ankusa.Sink.Log do
  @moduledoc "A sink that logs each delivered hook. Useful as a default/no-op."

  @behaviour Ankusa.Sink

  require Logger

  @impl true
  def deliver(env, ctx, _opts) do
    Logger.info("hook delivered id=#{env.id} source=#{ctx.source_id} attempt=#{ctx.attempt}")

    :ok
  end

  # A log line has no ordering guarantee to preserve (interleaved lines are
  # fine), so it imposes none on dispatch either.
  @impl true
  def ordering_key(_env, _opts), do: nil

  # A log line keeps nothing: a node that crashed right after saying `:ok`
  # would have dropped the hook. `wal.type: none` must not ack on this.
  @impl true
  def durable?(_opts), do: false
end
