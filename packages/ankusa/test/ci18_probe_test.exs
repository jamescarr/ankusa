# Throwaway: CI-18's probe, never merged. It exists so the `ankusa (tests)`
# lane exits non-zero on a real test failure and its step summary can be read.
defmodule Ankusa.Ci18ProbeTest do
  use ExUnit.Case, async: true

  test "CI-18 probe: an assertion names itself" do
    assert 1 + 1 == 3
  end

  test "CI-18 probe: a raise names itself" do
    raise RuntimeError, "CI-18 probe boom"
  end
end
