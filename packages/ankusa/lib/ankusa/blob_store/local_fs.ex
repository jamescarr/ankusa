defmodule Ankusa.BlobStore.LocalFS do
  @moduledoc """
  Default `Ankusa.BlobStore`: immutable objects on the local filesystem.

  Objects live under `opts[:root]` (an absolute path), by default
  `Config.path(config, "segments")`. A dedicated claim store
  (`claim_check.blob_store: {LocalFS, root: "/shared/claims"}`) points it
  elsewhere — at a directory every node and the gateway mount, say.

  Writes are atomic (temp file + rename) and durable (file and directory
  fsyncs), so a reader never sees a half-written object and a committed one
  survives power loss. Reads use `:file.pread/3` for a single-record range
  `GET` without slurping the whole object into memory.
  """

  @behaviour Ankusa.BlobStore

  alias Ankusa.Config

  @impl true
  def put(instance, key, data, opts) do
    root = root(instance, opts)
    path = Path.join(root, key)

    with :ok <- Ankusa.Fsync.mkdir_p(Path.dirname(path), root) do
      Ankusa.Fsync.write_file(path, data)
    end
  end

  @impl true
  def get(instance, key, opts) do
    case File.read(Path.join(root(instance, opts), key)) do
      {:ok, bin} -> {:ok, bin}
      {:error, :enoent} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def get_range(instance, key, offset, length, opts) do
    case :file.open(Path.join(root(instance, opts), key), [:read, :raw, :binary]) do
      {:ok, fd} ->
        result = :file.pread(fd, offset, length)
        :file.close(fd)

        case result do
          {:ok, bin} -> {:ok, bin}
          :eof -> {:error, :eof}
          {:error, reason} -> {:error, reason}
        end

      {:error, :enoent} ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def delete(instance, key, opts) do
    _ = File.rm(Path.join(root(instance, opts), key))
    :ok
  end

  # A root that does not exist yet holds no keys.
  @impl true
  def list(instance, prefix, opts) do
    root = root(instance, opts)

    keys =
      root
      |> Path.join(prefix_dir(prefix))
      |> Path.join("**")
      |> Path.wildcard()
      |> Enum.filter(&File.regular?/1)
      |> Enum.map(&Path.relative_to(&1, root))
      |> Enum.filter(&String.starts_with?(&1, prefix))
      |> Enum.sort()

    {:ok, keys}
  end

  @doc "The directory objects live under: `opts[:root]`, else the instance's `segments` dir."
  @spec root(atom(), keyword()) :: String.t()
  def root(instance, opts) do
    case Keyword.get(opts, :root) do
      nil -> Config.path(Ankusa.config(instance), "segments")
      root -> root
    end
  end

  # Walk only the prefix's containing directory, not the whole store root —
  # a deep prefix (`claims/<tenant>/`) shouldn't have to glob every segment
  # to find its own keys. A bare prefix with no `/` (e.g. `"seg"`) still
  # walks the whole root, same as before.
  defp prefix_dir(prefix) do
    case Path.dirname(prefix) do
      "." -> ""
      dir -> dir
    end
  end
end
