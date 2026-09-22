defmodule Ankusa.Dispatch.DLQ do
  @moduledoc """
  Durable, append-only dead-letter log.

  Each record is a length-prefixed `:erlang.term_to_binary/1` of
  `%{envelope: env, reason: reason, at: unix_ms}`. Reads are tolerant of a torn
  trailing record (a partial append) and simply drop it.
  """

  alias Ankusa.Config

  @spec write(Config.t(), Ankusa.Envelope.t(), term()) :: :ok
  def write(config, env, reason) do
    path = path(config)
    File.mkdir_p!(Path.dirname(path))

    record =
      :erlang.term_to_binary(%{
        envelope: env,
        reason: reason,
        at: System.system_time(:millisecond)
      })

    frame = <<byte_size(record)::32, record::binary>>
    File.write!(path, frame, [:append, :binary])
    :ok
  end

  @spec entries(Config.t()) :: [map()]
  def entries(config) do
    case File.read(path(config)) do
      {:ok, bin} -> parse(bin, [])
      {:error, _} -> []
    end
  end

  defp path(config), do: Config.path(config, "dlq/dlq.log")

  defp parse(<<len::32, rest::binary>>, acc) when byte_size(rest) >= len do
    <<record::binary-size(^len), tail::binary>> = rest
    parse(tail, [:erlang.binary_to_term(record) | acc])
  end

  # torn / incomplete trailing record — drop it
  defp parse(_leftover, acc), do: Enum.reverse(acc)
end
