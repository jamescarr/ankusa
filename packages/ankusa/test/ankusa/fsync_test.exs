defmodule Ankusa.FsyncTest do
  @moduledoc """
  The durable-filesystem helpers promise `:ok | {:error, reason}` and never a
  raise: the compactor and the blob store call them from processes that must
  survive a full or failing disk.
  """

  use ExUnit.Case, async: true

  import Ankusa.TestHelpers, only: [unique_data_dir: 1]

  alias Ankusa.Fsync

  setup do
    dir = unique_data_dir(:fsync)
    on_exit(fn -> File.rm_rf(dir) end)
    %{dir: dir}
  end

  test "mkdir_p creates every missing level, and is a no-op when they exist", %{dir: dir} do
    deep = Path.join([dir, "a", "b", "c"])

    assert :ok = Fsync.mkdir_p(deep)
    assert File.dir?(deep)
    assert :ok = Fsync.mkdir_p(deep)
  end

  test "fsync_dir answers an error for a path it cannot open instead of raising", %{dir: dir} do
    File.mkdir_p!(dir)
    assert :ok = Fsync.fsync_dir(dir)

    assert {:error, _} = Fsync.fsync_dir(Path.join(dir, "missing"))

    file = Path.join(dir, "file")
    File.write!(file, "x")
    assert {:error, _} = Fsync.fsync_dir(file)
  end

  test "write_file replaces the content in one step and leaves no temp file", %{dir: dir} do
    File.mkdir_p!(dir)
    path = Path.join(dir, "obj")

    assert :ok = Fsync.write_file(path, "one")
    assert :ok = Fsync.write_file(path, "two")

    assert File.read!(path) == "two"
    assert File.ls!(dir) == ["obj"]
  end

  test "write_file into a directory that does not exist is an error, not a raise", %{dir: dir} do
    assert {:error, :enoent} = Fsync.write_file(Path.join([dir, "nope", "obj"]), "x")
  end

  test "rename moves the file and reports a missing source", %{dir: dir} do
    File.mkdir_p!(Path.join(dir, "to"))
    from = Path.join(dir, "from")
    File.write!(from, "x")

    assert :ok = Fsync.rename(from, Path.join([dir, "to", "moved"]))
    assert File.read!(Path.join([dir, "to", "moved"])) == "x"
    refute File.exists?(from)

    assert {:error, :enoent} = Fsync.rename(from, Path.join(dir, "again"))
  end
end
