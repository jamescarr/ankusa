defmodule Ankusa.Sink.KafkaDescribeTest do
  use ExUnit.Case, async: true

  alias Ankusa.Sink.Kafka

  @opts [brokers: ["a:9092", {"b", 9093}], topic: "ankusa.hooks"]

  test "host joins the configured brokers in order and address is the topic" do
    description = Kafka.describe(%{source_id: "stripe", tenant_id: "acme"}, @opts)

    assert description.protocol == "kafka"
    assert description.host == "a:9092,b:9093"
    assert description.address == "ankusa.hooks"
    assert description.ankusa_headers == true

    assert description.channel_bindings == %{
             "kafka" => %{"topic" => "ankusa.hooks", "bindingVersion" => "0.5.0"}
           }
  end

  test "the default key of a tenant-scoped source is a constant" do
    description = Kafka.describe(%{source_id: "stripe", tenant_id: "acme"}, @opts)

    assert description.message_bindings == %{
             "kafka" => %{
               "key" => %{"type" => "string", "const" => "acme/stripe"},
               "bindingVersion" => "0.5.0"
             }
           }
  end

  test "a varying tenant makes the default key a source-scoped pattern" do
    description = Kafka.describe(%{source_id: "stripe", tenant_id: nil}, @opts)
    key = description.message_bindings["kafka"]["key"]

    assert key["type"] == "string"
    refute Map.has_key?(key, "const")

    pattern = Regex.compile!(key["pattern"])
    assert Regex.match?(pattern, "acme/stripe")
    refute Regex.match?(pattern, "acme/other")
    refute Regex.match?(pattern, "/stripe")
  end

  test "a function key is described, never called" do
    key = fn _env -> raise "must not be called" end
    description = Kafka.describe(%{source_id: "stripe", tenant_id: "acme"}, @opts ++ [key: key])
    schema = description.message_bindings["kafka"]["key"]

    refute Map.has_key?(schema, "const")
    assert schema["description"] =~ "function"
  end

  test "a static key is the constant" do
    description = Kafka.describe(%{source_id: "stripe", tenant_id: "acme"}, @opts ++ [key: "k"])

    assert description.message_bindings["kafka"]["key"] == %{"type" => "string", "const" => "k"}
  end
end
