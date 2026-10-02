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
  Creates `dir` and every missing ancestor, fsyncing the parent of each
  created directory top-down, so the creation survives power loss.
  """
  def mkdir_p(dir) do
    case missing_dirs(dir) do
      [] ->
        :ok

      dirs ->
        with :ok <- File.mkdir_p(dir),
             :ok <- fsync_parents(dirs) do
          :ok
        end
    end
  end

  # The missing directories, outermost first: `do_missing_dirs/2` walks up from
  # `dir` prepending as it goes, so the list already comes out that way.
  defp missing_dirs(dir), do: do_missing_dirs(dir, [])

  defp do_missing_dirs(dir, acc) do
    if File.dir?(dir) do
      acc
    else
      do_missing_dirs(Path.dirname(dir), [dir | acc])
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
