defmodule Ankusa.Fsync do
  @moduledoc false

  # Durable local-filesystem helpers (S4, W7): every directory creation,
  # file write and rename also fsyncs the containing directory, so a power
  # loss cannot roll back the namespace change and leave a name pointing at
  # nothing. Everything returns `:ok | {:error, reason}` and never raises.

  @doc """
  Fsyncs a directory: opens it as a directory (raw, falling back to a non-raw
  open on filesystems that refuse the raw flag), syncs, closes.
  """
  def fsync_dir(dir) do
    case open_dir(dir) do
      {:ok, fd} -> close_after(:file.sync(fd), fd)
      {:error, reason} -> {:error, reason}
    end
  end

  # Close in every path. A failed sync is the more telling error, so it wins; a
  # close error (a write-back failure the OS reports late) is returned when the
  # sync itself worked. Neither raises.
  defp close_after(result, fd) do
    closed = :file.close(fd)

    case result do
      :ok -> closed
      {:error, _reason} = error -> error
    end
  end

  defp open_dir(dir) do
    case :file.open(dir, [:read, :raw, :directory]) do
      {:ok, fd} ->
        {:ok, fd}

      {:error, _reason} ->
        :file.open(dir, [:read, :directory])
    end
  end

  @doc """
  Creates `dir` — `root` or a directory below it — and every missing
  ancestor, then fsyncs the parent of every directory from `root` down to
  `dir`, on every call. A caller that finds the directory already created by a
  concurrent caller still waits for the fsyncs that make it durable, and a
  parent fsync that failed once is retried by the next call.
  """
  def mkdir_p(dir, root) do
    with :ok <- File.mkdir_p(dir) do
      fsync_parents(chain(Path.expand(dir), Path.expand(root)))
    end
  end

  # `root`, then each directory below it down to `dir` (both absolute: data_dir
  # defaults to the relative "./data"). A `dir` outside `root` is just itself.
  defp chain(dir, root) do
    case Path.relative_to(dir, root) do
      "." -> [root]
      ^dir -> [dir]
      rel -> [root | rel |> Path.split() |> Enum.scan(root, &Path.join(&2, &1))]
    end
  end

  defp fsync_parents(dirs) do
    Enum.reduce_while(dirs, :ok, fn dir, :ok ->
      case fsync_dir(Path.dirname(dir)) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @doc """
  Writes `data` durably to `path`: a temp file in the same directory, synced,
  then renamed over `path`, then the directory is fsynced. The temp file is
  removed on any error.
  """
  def write_file(path, data) do
    tmp = path <> ".tmp." <> Integer.to_string(System.unique_integer([:positive]))

    result =
      with :ok <- write_sync(tmp, data),
           :ok <- :file.rename(tmp, path) do
        fsync_dir(Path.dirname(path))
      end

    case result do
      :ok ->
        :ok

      {:error, reason} ->
        _ = File.rm(tmp)
        {:error, reason}
    end
  end

  defp write_sync(tmp, data) do
    case :file.open(tmp, [:write, :raw, :binary]) do
      {:ok, fd} ->
        result =
          case :file.write(fd, data) do
            :ok -> :file.sync(fd)
            {:error, reason} -> {:error, reason}
          end

        close_after(result, fd)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Renames `from` to `to` and fsyncs both parents (one fsync when they are
  the same directory), so the rename itself is durable.
  """
  def rename(from, to) do
    with :ok <- :file.rename(from, to) do
      fsync_both(Path.dirname(from), Path.dirname(to))
    end
  end

  defp fsync_both(a, b) do
    with :ok <- fsync_dir(a) do
      if a == b, do: :ok, else: fsync_dir(b)
    end
  end
end
