defmodule Ankusa.Sink.Log do
  @moduledoc "A sink that logs each delivered hook. Useful as a default/no-op."

  @behaviour Ankusa.Sink

  require Logger

  @impl true
  def deliver(env, ctx, _opts) do
    Logger.info("hook delivered id=#{env.id} source=#{ctx.source_id} attempt=#{ctx.attempt}")

    :ok
  end
end
