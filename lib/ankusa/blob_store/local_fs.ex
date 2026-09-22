defmodule Ankusa.BlobStore.LocalFS do
  @moduledoc """
  Default `Ankusa.BlobStore`: immutable segments on the local filesystem.

  Segments live under `Config.path(config, "segments")`. Writes are atomic
  (temp file + rename) so a reader never sees a half-written segment. Reads use
  `:file.pread/3` for a single-record range `GET` without slurping the whole
  segment into memory.
  """

  @behaviour Ankusa.BlobStore

  alias Ankusa.Config

  @impl true
  def put(instance, key, data, _opts) do
    path = abs(instance, key)
    File.mkdir_p!(Path.dirname(path))
    tmp = path <> ".tmp." <> Integer.to_string(System.unique_integer([:positive]))
    File.write!(tmp, data)
    File.rename!(tmp, path)
    :ok
  end

  @impl true
  def get(instance, key, _opts) do
    File.read(abs(instance, key))
  end

  @impl true
  def get_range(instance, key, offset, length, _opts) do
    case :file.open(abs(instance, key), [:read, :raw, :binary]) do
      {:ok, fd} ->
        result = :file.pread(fd, offset, length)
        :file.close(fd)

        case result do
          {:ok, bin} -> {:ok, bin}
          :eof -> {:error, :eof}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def delete(instance, key, _opts) do
    _ = File.rm(abs(instance, key))
    :ok
  end

  @impl true
  def list(instance, prefix, _opts) do
    root = root(instance)

    Path.join(root, "**")
    |> Path.wildcard()
    |> Enum.filter(&File.regular?/1)
    |> Enum.map(&Path.relative_to(&1, root))
    |> Enum.filter(&String.starts_with?(&1, prefix))
    |> Enum.sort()
  end

  defp abs(instance, key), do: Path.join(root(instance), key)

  defp root(instance), do: Config.path(Ankusa.config(instance), "segments")
end
