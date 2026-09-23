defmodule Ankusa.DurableLogTest do
  use ExUnit.Case, async: true

  alias Ankusa.DurableLog

  setup do
    dir = Path.join(System.tmp_dir!(), "ankusa_dlog_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf(dir) end)
    # a nested path that doesn't exist yet: append/2 is what creates it
    %{path: Path.join([dir, "nested", "log"])}
  end

  test "round-trips records in order, creating the parent directory", %{path: path} do
    assert :ok = DurableLog.append(path, [%{n: 1}, %{n: 2}])
    assert :ok = DurableLog.append(path, %{n: 3})

    assert DurableLog.read(path) == [%{n: 1}, %{n: 2}, %{n: 3}]
  end

  test "a torn trailing record is dropped rather than raised on", %{path: path} do
    :ok = DurableLog.append(path, [%{n: 1}])

    # a frame whose length prefix promises more bytes than were ever flushed —
    # exactly what a crash mid-write leaves behind. Built from `frame/1` so this
    # also pins the on-disk format three components depend on.
    frame = IO.iodata_to_binary(DurableLog.frame(%{n: 2}))
    <<prefix::binary-size(4), body::binary>> = frame
    File.write!(path, prefix <> binary_part(body, 0, div(byte_size(body), 2)), [:append])

    assert DurableLog.read(path) == [%{n: 1}]
  end

  test "a missing file reads as an empty log", %{path: path} do
    assert DurableLog.read(path) == []
  end
end
