defmodule Ankusa.Sink.RedisDescribeTest do
  use ExUnit.Case, async: true

  alias Ankusa.Sink.Redis

  test "a rediss url keeps the database path and drops userinfo" do
    description =
      Redis.describe(%{source_id: "stripe", tenant_id: "acme"},
        url: "rediss://:pw@r:6380/2",
        channel: "ankusa.hooks"
      )

    assert description.protocol == "rediss"
    assert description.host == "r:6380"
    assert description.pathname == "/2"
    assert description.address == "ankusa.hooks"
    assert description.ankusa_headers == false
    assert description.channel_bindings == %{}
    assert description.message_bindings == %{}

    refute inspect(description) =~ "pw"
  end

  test "a url without a port or path uses the defaults" do
    description =
      Redis.describe(%{source_id: "stripe", tenant_id: nil}, url: "redis://r", channel: "c")

    assert description.protocol == "redis"
    assert description.host == "r:6379"
    assert description.pathname == nil
  end

  test "a function channel has no static address" do
    description =
      Redis.describe(%{source_id: "stripe", tenant_id: nil},
        url: "redis://r",
        channel: fn _ -> "x" end
      )

    assert description.address == nil
  end
end
