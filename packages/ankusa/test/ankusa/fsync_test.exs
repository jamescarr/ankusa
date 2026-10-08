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

  test "mkdir_p creates every missing level, and succeeds when they exist", %{dir: dir} do
    deep = Path.join([dir, "a", "b", "c"])

    assert :ok = Fsync.mkdir_p(deep, dir)
    assert File.dir?(deep)
    assert :ok = Fsync.mkdir_p(deep, dir)
  end

  # W7: a caller that finds the directories already there (made by a concurrent
  # caller, or by an earlier call whose fsync failed) must still fsync them.
  test "mkdir_p fsyncs every parent from root down on every call", %{dir: dir} do
    root = Path.expand(dir)
    deep = Path.join([root, "a", "b", "c"])

    assert :ok = Fsync.mkdir_p(deep, root)

    {result, fsynced} = fsynced_during(fn -> Fsync.mkdir_p(deep, root) end)
    expected = [Path.dirname(root), root, Path.join(root, "a"), Path.join([root, "a", "b"])]

    assert result == :ok
    assert fsynced == expected
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

  # `{result, dirs}`: what `fun` returned, and the directories `Fsync.fsync_dir/1`
  # was called with while it ran in this process, in order. The calls are local
  # to the module, so the trace pattern must be `:local`. A process cannot trace
  # itself, so a helper process is the tracer and forwards each call here.
  # Trace messages are delivered late relative to ordinary sends, so
  # `trace_delivered/1` first waits until every one has reached the tracer, and
  # the `:done` round trip then until the tracer has forwarded them all.
  defp fsynced_during(fun) do
    me = self()
    {:module, Fsync} = Code.ensure_loaded(Fsync)
    tracer = spawn_link(fn -> forward_fsyncs(me) end)

    :erlang.trace_pattern({Fsync, :fsync_dir, 1}, true, [:local])
    :erlang.trace(me, true, [:call, {:tracer, tracer}])

    result =
      try do
        fun.()
      after
        :erlang.trace(me, false, [:call])
        :erlang.trace_pattern({Fsync, :fsync_dir, 1}, false, [:local])
      end

    delivered = :erlang.trace_delivered(me)
    assert_receive {:trace_delivered, ^me, ^delivered}, 1_000

    ref = make_ref()
    send(tracer, {:done, me, ref})
    assert_receive {:done, ^ref}, 1_000

    {result, fsynced_dirs([])}
  end

  defp forward_fsyncs(to) do
    receive do
      {:trace, _pid, :call, {Fsync, :fsync_dir, [dir]}} ->
        send(to, {:fsynced, dir})
        forward_fsyncs(to)

      {:done, from, ref} ->
        send(from, {:done, ref})
    end
  end

  defp fsynced_dirs(acc) do
    receive do
      {:fsynced, dir} -> fsynced_dirs([dir | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
