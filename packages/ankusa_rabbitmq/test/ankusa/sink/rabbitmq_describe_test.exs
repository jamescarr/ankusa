defmodule Ankusa.Sink.RabbitMQDescribeTest do
  use ExUnit.Case, async: true

  alias Ankusa.Sink.RabbitMQ

  @subject %{source_id: "stripe", tenant_id: "acme"}

  test "url scheme, host and vhost, without userinfo" do
    description =
      RabbitMQ.describe(@subject, exchange: "ankusa.hooks", url: "amqp://u:p@mq:5671/prod")

    assert description.protocol == "amqp"
    assert description.host == "mq:5671"
    assert description.pathname == "/prod"
    assert description.address == "ankusa.stripe"
    assert description.ankusa_headers == false

    assert description.channel_bindings["amqp"]["bindingVersion"] == "0.3.0"
    assert description.channel_bindings["amqp"]["is"] == "routingKey"

    assert description.channel_bindings["amqp"]["exchange"] == %{
             "name" => "ankusa.hooks",
             "type" => "topic",
             "durable" => true,
             "vhost" => "prod"
           }

    refute inspect(description) =~ "u:p"
    refute inspect(description) =~ "p@"
  end

  test "the default url has the default vhost and no pathname" do
    description = RabbitMQ.describe(@subject, exchange: "ankusa.hooks")

    assert description.host == "localhost:5672"
    assert description.pathname == nil
    assert description.channel_bindings["amqp"]["exchange"]["vhost"] == "/"
  end

  test "a function routing key has no static address" do
    description =
      RabbitMQ.describe(@subject, exchange: "ankusa.hooks", routing_key: fn _ -> "x" end)

    assert description.address == nil
  end

  test "a static routing key is the address" do
    description = RabbitMQ.describe(@subject, exchange: "ankusa.hooks", routing_key: "rk")

    assert description.address == "rk"
  end
end
