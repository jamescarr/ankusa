defmodule Ankusa.SourceTest do
  use ExUnit.Case, async: true

  alias Ankusa.Source

  test "forwarded-header caps default to 64 headers and 16 KiB" do
    source = Source.new("s", [])

    assert source.max_headers == 64
    assert source.forward_headers_max_bytes == 16_384
  end

  test "a source's caps are its own" do
    source = Source.new("s", max_headers: 8, forward_headers_max_bytes: 1_024)

    assert source.max_headers == 8
    assert source.forward_headers_max_bytes == 1_024
  end

  test "a cap that is not a positive integer is refused" do
    for key <- [:max_headers, :forward_headers_max_bytes], bad <- [0, -1, 1.5, "64", nil] do
      assert_raise ArgumentError, ~r/source "s": #{key} must be a positive integer/, fn ->
        Source.new("s", [{key, bad}])
      end
    end
  end
end
