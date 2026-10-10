defmodule Ankusa.AsyncApiTest do
  @moduledoc """
  The AsyncAPI document an instance builds from its sources: which channels it
  advertises, how sources share them, and that the result is a valid document.
  """

  use ExUnit.Case, async: true

  import Ankusa.TestHelpers

  alias Ankusa.Test.DescribedSink

  defp document_map(overrides) do
    config = test_config(overrides)
    put_config(config)

    document = Ankusa.AsyncApi.document(config.instance)
    assert AsyncApiSpex.validate(document) == :ok

    document |> AsyncApiSpex.encode!() |> JSON.decode!()
  end

  defp static(sources), do: {Ankusa.SourceStore.Static, sources: sources}

  defp source(opts), do: [sinks: [{DescribedSink, opts}]]

  test "two sources publishing to one address share one channel, one message each" do
    map =
      document_map(
        source_store:
          static(%{
            "stripe" => source(address: "ankusa.hooks", headers: true),
            "github" => source(address: "ankusa.hooks", headers: true)
          })
      )

    assert map["asyncapi"] == "3.0.0"

    assert [{server_id, %{"host" => "broker:9092", "protocol" => "kafka"}}] =
             Map.to_list(map["servers"])

    assert [{channel_id, channel}] = Map.to_list(map["channels"])
    assert channel["address"] == "ankusa.hooks"
    assert channel["servers"] == [%{"$ref" => "#/servers/#{server_id}"}]
    assert Map.keys(channel["messages"]) == ["github", "stripe"]
    assert channel["description"] == "Hooks from: github, stripe"

    # Ankusa publishes: one `send` operation, naming both messages.
    assert [{"send-" <> ^channel_id, operation}] = Map.to_list(map["operations"])
    assert operation["action"] == "send"
    assert operation["channel"] == %{"$ref" => "#/channels/#{channel_id}"}

    assert operation["messages"] == [
             %{"$ref" => "#/channels/#{channel_id}/messages/github"},
             %{"$ref" => "#/channels/#{channel_id}/messages/stripe"}
           ]

    message = channel["messages"]["stripe"]
    assert message["name"] == "stripe"
    assert message["contentType"] == "application/json"
    assert message["headers"] == %{"$ref" => "#/components/schemas/SinkMessageHeadersV1"}

    assert message["payload"] == %{
             "allOf" => [
               %{"$ref" => "#/components/schemas/SinkMessageV1"},
               %{
                 "type" => "object",
                 "properties" => %{
                   "source_id" => %{"const" => "stripe"},
                   "tenant_id" => %{"const" => "default"}
                 }
               }
             ]
           }

    assert %{"SinkMessageV1" => %{"required" => required}} = map["components"]["schemas"]
    assert "id" in required
  end

  test "different addresses, brokers, and bindings are different channels and servers" do
    map =
      document_map(
        source_store:
          static(%{
            "a" => source(address: "a.topic"),
            "b" => source(address: "b.topic"),
            "c" => source(address: "a.topic", host: "other:9092"),
            "d" =>
              source(address: "a.topic", channel_bindings: %{"kafka" => %{"topic" => "a.topic"}})
          })
      )

    assert map["channels"] |> Map.values() |> Enum.map(& &1["address"]) |> Enum.sort() ==
             ["a.topic", "a.topic", "a.topic", "b.topic"]

    assert map["servers"] |> Map.values() |> Enum.map(& &1["host"]) |> Enum.sort() ==
             ["broker:9092", "other:9092"]

    assert map["operations"] |> Map.keys() |> length() == 4
  end

  test "a source with no messaging sink is absent" do
    map =
      document_map(
        source_store:
          static(%{
            "quiet" => [sinks: [{Ankusa.Sink.Log, []}]],
            "relay" => [sinks: [{Ankusa.Sink.Http, url: "https://sink.invalid/hook"}]]
          })
      )

    refute Map.has_key?(map, "channels")
    refute Map.has_key?(map, "servers")
    refute Map.has_key?(map, "operations")
  end

  test "an SQS sink is an sqs server and a queue channel with the sqs binding" do
    queue_url = "https://sqs.us-east-1.amazonaws.com/1/hooks.fifo"

    map =
      document_map(
        source_store:
          static(%{
            "stripe" => [sinks: [{Ankusa.Sink.SQS, queue_url: queue_url, region: "us-east-1"}]]
          })
      )

    assert [{_server_id, %{"host" => "sqs.us-east-1.amazonaws.com", "protocol" => "sqs"}}] =
             Map.to_list(map["servers"])

    assert [{_channel_id, channel}] = Map.to_list(map["channels"])
    assert channel["address"] == "hooks.fifo"

    assert channel["bindings"] == %{
             "sqs" => %{
               "queue" => %{"name" => "hooks.fifo", "fifoQueue" => true},
               "bindingVersion" => "0.3.0"
             }
           }

    refute Map.has_key?(channel["messages"]["stripe"], "headers")
  end

  test "a Google Pub/Sub sink is a googlepubsub server and a topic channel with the ordering key" do
    map =
      document_map(
        source_store:
          static(%{
            "stripe" => [
              sinks: [{Ankusa.Sink.GooglePubSub, project: "p", topic: "hooks", ordering_key: "k"}]
            ]
          })
      )

    assert [{_server_id, server}] = Map.to_list(map["servers"])

    assert Map.take(server, ["host", "protocol"]) == %{
             "host" => "pubsub.googleapis.com",
             "protocol" => "googlepubsub"
           }

    assert [{_channel_id, channel}] = Map.to_list(map["channels"])
    assert channel["address"] == "projects/p/topics/hooks"
    assert channel["bindings"] == %{"googlepubsub" => %{"bindingVersion" => "0.2.0"}}

    message = channel["messages"]["stripe"]

    assert message["bindings"] == %{
             "googlepubsub" => %{"orderingKey" => "k", "bindingVersion" => "0.2.0"}
           }

    refute Map.has_key?(message, "headers")
  end

  test "an address a function computes per hook is a channel of its own, with no address" do
    map =
      document_map(
        source_store:
          static(%{
            "a" => source(address: nil),
            "b" => source(address: nil),
            "fixed" => source(address: "fixed.topic")
          })
      )

    channels = Map.values(map["channels"])
    {dynamic, fixed} = Enum.split_with(channels, &(not Map.has_key?(&1, "address")))

    assert length(dynamic) == 2

    assert Enum.map(dynamic, & &1["title"]) |> Enum.sort() == [
             "Dynamic address (a)",
             "Dynamic address (b)"
           ]

    assert Enum.all?(dynamic, &(&1["description"] =~ "computed per hook"))
    assert [%{"address" => "fixed.topic"}] = fixed
  end

  test "the tenant is fixed in a message only when the route resolver takes it from the source" do
    sources = %{"a" => [tenant_id: "acme", sinks: [{DescribedSink, address: "t"}]]}

    path = document_map(source_store: static(sources))
    [%{"messages" => %{"a" => %{"payload" => path_payload}}}] = Map.values(path["channels"])

    assert get_in(path_payload, ["allOf", Access.at(1), "properties", "tenant_id"]) == %{
             "const" => "acme"
           }

    # `TenantPath` reads the tenant from the URL, so it varies per hook.
    url =
      document_map(
        source_store: static(sources),
        route_resolver: {Ankusa.RouteResolver.TenantPath, []}
      )

    [%{"messages" => %{"a" => %{"payload" => url_payload}}}] = Map.values(url["channels"])
    props = get_in(url_payload, ["allOf", Access.at(1), "properties"])
    assert props == %{"source_id" => %{"const" => "a"}}
  end

  test "ids that collide once sanitized stay distinct" do
    map =
      document_map(
        source_store:
          static(%{
            "acme.hooks" => source(address: "t"),
            "acme_hooks" => source(address: "t")
          })
      )

    [%{"messages" => messages}] = Map.values(map["channels"])
    assert Map.keys(messages) == ["acme_hooks", "acme_hooks_2"]
    assert Enum.sort(Enum.map(Map.values(messages), & &1["name"])) == ["acme.hooks", "acme_hooks"]
  end

  test "lifecycle sinks add a channel whose message names the CloudEvent" do
    map =
      document_map(
        source_store: static(%{"stripe" => source(address: "hooks")}),
        lifecycle: %{sinks: [{DescribedSink, address: "ankusa.lifecycle"}]}
      )

    channel = map["channels"] |> Map.values() |> Enum.find(&(&1["address"] == "ankusa.lifecycle"))

    assert channel["description"] ==
             "Ankusa lifecycle events (CloudEvents 1.0, structured), published from memory: " <>
               "retried, not persisted, not ordered"

    assert %{"ankusa_lifecycle" => message} = channel["messages"]
    assert message["name"] == "ankusa:lifecycle"

    assert message["x-ankusa-body"] == %{
             "contentType" => "application/cloudevents+json",
             "schema" => %{"$ref" => "#/components/schemas/LifecycleEventV1"}
           }

    props = get_in(message, ["payload", "allOf", Access.at(1), "properties"])
    assert props["content_type"] == %{"const" => "application/cloudevents+json"}
    # A lifecycle event's tenant varies per event.
    refute Map.has_key?(props, "tenant_id")

    assert %{"type" => %{"enum" => types}} =
             map["components"]["schemas"]["LifecycleEventV1"]["properties"]

    assert "io.ankusa.source.created" in types
  end

  test "lifecycle off adds nothing, and an instance with no sources is a valid empty document" do
    map = document_map([])

    assert map["asyncapi"] == "3.0.0"
    assert map["info"]["title"] == "Ankusa"
    refute Map.has_key?(map, "channels")
  end
end
