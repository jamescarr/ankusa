defmodule Ankusa.Sink.NATSDescribeTest do
  use ExUnit.Case, async: true

  alias Ankusa.Sink.NATS

  test "a comma-separated server string is split, trimmed and joined" do
    description =
      NATS.describe(%{source_id: "stripe", tenant_id: "acme"},
        servers: "n1:4222, n2:4223",
        subject: "ankusa.hooks"
      )

    assert description.protocol == "nats"
    assert description.host == "n1:4222,n2:4223"
    assert description.address == "ankusa.hooks"
    assert description.ankusa_headers == true
    assert description.channel_bindings == %{}
    assert description.message_bindings == %{}
  end

  test "a {host, port} tuple server works" do
    description =
      NATS.describe(%{source_id: "stripe", tenant_id: nil},
        servers: [{"n3", 4224}],
        subject: "ankusa.hooks"
      )

    assert description.host == "n3:4224"
  end

  test "a function subject has no static address" do
    description =
      NATS.describe(%{source_id: "stripe", tenant_id: nil},
        servers: ["n1:4222"],
        subject: fn _ -> "x" end
      )

    assert description.address == nil
  end
end
