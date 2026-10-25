defmodule Ankusa.LogLevelTest do
  # The Logger level is global to the VM.
  use ExUnit.Case, async: false

  alias Ankusa.LogLevel

  setup do
    original = Logger.level()
    on_exit(fn -> Logger.configure(level: original) end)
    :ok
  end

  test "levels are the four `log.level` takes, most verbose first" do
    assert LogLevel.levels() == [:debug, :info, :warning, :error]
  end

  test "parse reads an atom or its name, and nothing else" do
    for level <- LogLevel.levels() do
      assert LogLevel.parse(level) == {:ok, level}
      assert LogLevel.parse(Atom.to_string(level)) == {:ok, level}
    end

    for bad <- ["warn", "DEBUG", "", "notice", :notice, :all, nil, 1, %{}, ["debug"]] do
      assert LogLevel.parse(bad) == :error
    end
  end

  test "set changes the VM's level at once and answers the one it replaced" do
    Logger.configure(level: :info)

    assert LogLevel.set(:debug) == {:ok, :info}
    assert LogLevel.get() == :debug
    assert Logger.level() == :debug

    assert LogLevel.set("warning") == {:ok, :debug}
    assert LogLevel.get() == :warning
  end

  test "set refuses a level it does not know and changes nothing" do
    Logger.configure(level: :info)

    assert LogLevel.set("loud") == {:error, :invalid_level}
    assert LogLevel.set(:notice) == {:error, :invalid_level}
    assert LogLevel.get() == :info
  end

  test "trace_id is the 32-hex id of a stored trace context" do
    id = "0af7651916cd43dd8448eb211c80319c"

    assert Ankusa.Trace.trace_id(%{traceparent: "00-#{id}-b7ad6b7169203331-01", tracestate: nil}) ==
             id

    assert Ankusa.Trace.trace_id(nil) == nil
    assert Ankusa.Trace.trace_id(%{}) == nil
  end
end
