defmodule Ankusa.Dispatch.DLQ do
  @moduledoc """
  Durable, append-only dead-letter log.

  One record per dispatch give-up: `%{envelope: env, reason: reason, at: unix_ms}`.
  Framing, torn-tail tolerance, and the atom policy live in
  `Ankusa.DurableLog`.
  """

  alias Ankusa.{Config, DurableLog}

  @spec write(Config.t(), Ankusa.Envelope.t(), term()) :: :ok
  def write(config, env, reason) do
    DurableLog.append(path(config), %{
      envelope: env,
      reason: reason,
      at: System.system_time(:millisecond)
    })
  end

  @spec entries(Config.t()) :: [map()]
  def entries(config) do
    # An envelope carries module atoms (`verification.provider`), which `:safe`
    # would reject on a node that hasn't interned them yet — see
    # `Ankusa.DurableLog` on atom safety.
    DurableLog.read(path(config), safe: false)
  end

  defp path(config), do: Config.path(config, "dlq/dlq.log")
end
