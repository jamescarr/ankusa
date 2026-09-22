defmodule Hook.Storage.Index do
  @moduledoc """
  Durable, append-only index mapping an event id to its byte range inside a
  compacted segment.

  Lives at `Config.path(config, "segments/index.log")`. Each row is written as
  `<<len::32, term::binary>>` where `term` is `:erlang.term_to_binary/1` of the
  row map. `all/1` replays the file and drops a torn trailing record (a write
  that never finished), mirroring the WAL's crash-safety discipline.
  """

  alias Hook.Config

  @type row :: %{
          event_id: String.t(),
          source_id: String.t(),
          tenant_id: String.t(),
          received_at: integer(),
          seq: non_neg_integer(),
          segment_key: String.t(),
          offset: non_neg_integer(),
          length: pos_integer()
        }

  @spec append(Config.t(), [row()]) :: :ok
  def append(config, rows) do
    path = path(config)
    File.mkdir_p!(Path.dirname(path))

    iodata =
      Enum.map(rows, fn row ->
        bin = :erlang.term_to_binary(row)
        [<<byte_size(bin)::32>>, bin]
      end)

    File.write!(path, iodata, [:append])
    :ok
  end

  @spec all(Config.t()) :: [row()]
  def all(config) do
    case File.read(path(config)) do
      {:ok, bin} -> parse(bin, [])
      {:error, _} -> []
    end
  end

  @spec lookup(Config.t(), String.t()) :: {:ok, row()} | :error
  def lookup(config, event_id) do
    case Enum.find(all(config), fn row -> row.event_id == event_id end) do
      nil -> :error
      row -> {:ok, row}
    end
  end

  defp parse(<<len::32, rest::binary>>, acc) do
    case rest do
      <<bin::binary-size(^len), tail::binary>> ->
        parse(tail, [:erlang.binary_to_term(bin, [:safe]) | acc])

      # torn trailing record: length prefix promises more than is present
      _ ->
        Enum.reverse(acc)
    end
  end

  defp parse(_leftover, acc), do: Enum.reverse(acc)

  defp path(config), do: Config.path(config, "segments/index.log")
end
