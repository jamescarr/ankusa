defmodule Ankusa.ConfigTest do
  @moduledoc """
  `Ankusa.Config` fails fast on a typo instead of silently accepting it or
  clobbering a defaults map — these pin the three bugs the release-readiness
  review found.
  """

  use ExUnit.Case, async: true

  alias Ankusa.Config

  describe "parse_roles!/1" do
    test "splits, trims, and maps to role atoms" do
      assert Config.parse_roles!("edge, dispatch") == [:edge, :dispatch]
    end

    test "raises ArgumentError on an unknown role name" do
      assert_raise ArgumentError, ~r/unknown Ankusa role/, fn ->
        Config.parse_roles!("edgee")
      end
    end
  end

  test "Config.new/1 raises on an unknown nested key" do
    assert_raise ArgumentError, ~r/batcher\.max_queu/, fn ->
      Config.new(batcher: %{max_queu: 1})
    end
  end

  test "Config.new/1 deep-merges a keyword-list section over the defaults" do
    config = Config.new(batcher: [max_queue: 5])

    assert config.batcher.max_batch == 256
    assert config.batcher.max_queue == 5
  end
end
