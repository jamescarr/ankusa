defmodule Shop.Events.LineItem do
  defstruct [:sku, :quantity]

  use AsyncApiSpex.Schema,
    name: "LineItem",
    fields: [sku: [type: :string, required: true], quantity: [type: :integer, required: true]]
end

defmodule Shop.Events.OrderCreated do
  defstruct [:id, :customer_id, :items, :placed_at]

  use AsyncApiSpex.Schema,
    name: "OrderCreated",
    title: "Order created",
    description: "Emitted once per checkout.",
    fields: [
      id: [type: :string, required: true],
      customer_id: :string,
      items: {:array, Shop.Events.LineItem},
      placed_at: :datetime
    ]
end

defmodule Shop.Events.OrderShipped do
  defstruct [:id, :carrier, :shipped_at]

  use AsyncApiSpex.Schema,
    name: "OrderShipped",
    fields: [
      id: [type: :string, required: true],
      carrier: {:enum, ["ups", "dhl"]},
      shipped_at: :datetime
    ]
end

defmodule Shop.Kafka.OrderProducer do
  # The README's producer calls :brod, which this package does not depend on.
  @compile {:no_warn_undefined, :brod}

  use AsyncApiSpex.Channel,
    address: "shop.orders",
    server: [
      id: "kafka",
      host: System.get_env("KAFKA_BOOTSTRAP", "kafka:9092"),
      protocol: "kafka"
    ],
    messages: [Shop.Events.OrderCreated, Shop.Events.OrderShipped],
    bindings: %{"kafka" => %{"partitions" => 12}},
    description: "One record per order event, keyed by order id."

  def publish(%{id: id} = event) do
    :brod.produce_sync(:shop, "shop.orders", :hash, id, JSON.encode!(Map.from_struct(event)))
  end
end

defmodule Shop.AsyncApi do
  use AsyncApiSpex.Spec,
    channels: [Shop.Kafka.OrderProducer],
    info: [title: "Shop", version: "1.0.0"]
end

defmodule Shop.AsyncApiByApp do
  use AsyncApiSpex.Spec,
    otp_app: :shop_fake,
    info: [title: "Shop", version: "1.0.0"]
end

defmodule Shop.AsyncApiMissingApp do
  use AsyncApiSpex.Spec,
    otp_app: :shop_missing,
    info: [title: "Shop", version: "1.0.0"]
end

defmodule Shop.Test.Partial do
  defstruct [:a, :b]
  use AsyncApiSpex.Schema, name: "Partial", fields: [a: :string]
end

defmodule Shop.Test.Described do
  defstruct [:item]

  use AsyncApiSpex.Schema,
    name: "Described",
    fields: [item: [type: Shop.Events.LineItem, description: "The only item."]]
end

defmodule Shop.Test.ExtendedSpec do
  use AsyncApiSpex.Spec,
    channels: [Shop.Kafka.OrderProducer],
    info: [title: "T", version: "1"]

  def spec do
    doc = super()
    %{doc | extensions: Map.put(doc.extensions, "x-team", "payments")}
  end
end

defmodule Shop.Test.Node do
  defstruct [:value, :children]

  use AsyncApiSpex.Schema,
    name: "Node",
    fields: [value: :integer, children: {:array, __MODULE__}]
end

defmodule Shop.Test.BadEvent do
  defstruct [:x]
  use AsyncApiSpex.Schema, name: "BadEvent", fields: [x: NotAModule]
end

defmodule Shop.Test.BadChannel do
  use AsyncApiSpex.Channel,
    address: "bad",
    server: [host: "h:1", protocol: "kafka"],
    messages: [Shop.Test.BadEvent]
end

defmodule Shop.Test.BadSpec do
  use AsyncApiSpex.Spec,
    channels: [Shop.Test.BadChannel],
    info: [title: "Bad", version: "1"]
end

defmodule Shop.Test.ChannelA do
  use AsyncApiSpex.Channel,
    address: "a.events",
    server: [id: "kafka", host: "k:9092", protocol: "kafka"],
    messages: [Shop.Events.OrderShipped]
end

defmodule Shop.Test.ChannelB do
  use AsyncApiSpex.Channel,
    address: "b.events",
    server: [id: "kafka", host: "k:9092", protocol: "kafka"],
    messages: [Shop.Events.OrderShipped],
    action: :receive
end

defmodule Shop.Test.ChannelOtherHost do
  use AsyncApiSpex.Channel,
    address: "c.events",
    server: [id: "kafka", host: "other:9092", protocol: "kafka"],
    messages: [Shop.Events.OrderShipped]
end

defmodule Shop.Test.ChannelOtherContentType do
  use AsyncApiSpex.Channel,
    address: "d.events",
    server: [id: "kafka", host: "k:9092", protocol: "kafka"],
    messages: [Shop.Events.OrderShipped],
    content_type: "application/cbor"
end

defmodule Shop.Test.ChannelDuplicateId do
  use AsyncApiSpex.Channel,
    id: "a_events",
    address: "other.address",
    server: [id: "kafka", host: "k:9092", protocol: "kafka"],
    messages: [Shop.Events.OrderShipped]
end

defmodule Shop.Test.CustomEvent do
  use AsyncApiSpex.Message,
    name: "Custom.Event",
    headers: %{"type" => "object", "properties" => %{"trace" => %{"type" => "string"}}},
    payload: Shop.Events.OrderShipped
end

defmodule Shop.Test.CustomChannel do
  use AsyncApiSpex.Channel,
    address: "custom",
    server: [host: "k:9092", protocol: "kafka"],
    messages: [Shop.Test.CustomEvent]
end

defmodule Shop.Test.CustomSpec do
  use AsyncApiSpex.Spec,
    channels: [Shop.Test.CustomChannel],
    info: [title: "Custom", version: "1"]
end

defmodule Shop.Test.ReceivingChannel do
  use AsyncApiSpex.Channel,
    address: "shop.{region}.orders",
    server: [host: "amqp:5672", protocol: "amqp"],
    messages: [Shop.Events.OrderShipped],
    action: :receive,
    parameters: %{"region" => %AsyncApiSpex.Parameter{}},
    operation: [title: "Consume shipments"]
end

defmodule AsyncApiSpexDecoratorsTest do
  use ExUnit.Case, async: true

  alias AsyncApiSpex.{Channel, Document, Info, Message, Reference}

  describe "a document derived from decorated structs and a publisher" do
    setup do
      spec = Shop.AsyncApi.spec()
      {:ok, spec: spec, encoded: spec |> AsyncApiSpex.encode!() |> JSON.decode!()}
    end

    test "declares the broker as a server", %{encoded: encoded} do
      assert encoded["servers"] == %{"kafka" => %{"host" => "kafka:9092", "protocol" => "kafka"}}
    end

    test "declares the topic as a channel on that server", %{encoded: encoded} do
      channel = encoded["channels"]["shop_orders"]

      assert channel["address"] == "shop.orders"
      assert channel["servers"] == [%{"$ref" => "#/servers/kafka"}]
      assert channel["bindings"] == %{"kafka" => %{"partitions" => 12}}
      assert channel["description"] == "One record per order event, keyed by order id."

      assert channel["messages"] == %{
               "OrderCreated" => %{"$ref" => "#/components/messages/OrderCreated"},
               "OrderShipped" => %{"$ref" => "#/components/messages/OrderShipped"}
             }
    end

    test "declares the publisher's operation over the channel's messages", %{encoded: encoded} do
      assert encoded["operations"]["send-shop_orders"] == %{
               "action" => "send",
               "channel" => %{"$ref" => "#/channels/shop_orders"},
               "messages" => [
                 %{"$ref" => "#/channels/shop_orders/messages/OrderCreated"},
                 %{"$ref" => "#/channels/shop_orders/messages/OrderShipped"}
               ]
             }
    end

    test "wraps each struct schema in a message", %{encoded: encoded} do
      assert encoded["components"]["messages"]["OrderCreated"] == %{
               "name" => "OrderCreated",
               "title" => "Order created",
               "contentType" => "application/json",
               "payload" => %{"$ref" => "#/components/schemas/OrderCreated"}
             }
    end

    test "derives the schemas from the struct keys and the declared fields", %{encoded: encoded} do
      schemas = encoded["components"]["schemas"]

      assert schemas["OrderCreated"] == %{
               "type" => "object",
               "title" => "Order created",
               "description" => "Emitted once per checkout.",
               "required" => ["id"],
               "properties" => %{
                 "id" => %{"type" => "string"},
                 "customer_id" => %{"type" => "string"},
                 "items" => %{
                   "type" => "array",
                   "items" => %{"$ref" => "#/components/schemas/LineItem"}
                 },
                 "placed_at" => %{"type" => "string", "format" => "date-time"}
               }
             }

      assert schemas["LineItem"]["required"] == ["sku", "quantity"]
      assert schemas["OrderShipped"]["properties"]["carrier"] == %{"enum" => ["ups", "dhl"]}
    end

    test "validates and encodes without nils", %{spec: spec, encoded: encoded} do
      assert AsyncApiSpex.validate(spec) == :ok
      refute_has_nils(encoded)
    end
  end

  describe "use AsyncApiSpex.Schema, fields:" do
    test "a struct key that is not declared is an unconstrained property" do
      schema = Shop.Test.Partial.schema()

      assert schema["properties"] == %{"a" => %{"type" => "string"}, "b" => %{}}
      refute Map.has_key?(schema, "required")
    end

    test "a described module type is wrapped because a reference takes no siblings" do
      assert Shop.Test.Described.schema()["properties"] == %{
               "item" => %{"allOf" => [Shop.Events.LineItem], "description" => "The only item."}
             }

      document = %Document{
        info: %Info{title: "T", version: "1"},
        channels: %{
          "c" => %Channel{
            address: nil,
            messages: %{"m" => %Message{name: "m", payload: Shop.Test.Described}}
          }
        }
      }

      encoded = document |> AsyncApiSpex.encode!() |> JSON.decode!()

      assert encoded["components"]["schemas"]["Described"]["properties"]["item"] ==
               %{
                 "allOf" => [%{"$ref" => "#/components/schemas/LineItem"}],
                 "description" => "The only item."
               }
    end

    test "a schema that references itself resolves to a reference to itself" do
      document = %Document{
        info: %Info{title: "T", version: "1"},
        channels: %{
          "tree" => %Channel{
            address: nil,
            messages: %{"node" => %Message{name: "node", payload: Shop.Test.Node}}
          }
        }
      }

      resolved = AsyncApiSpex.resolve(document)

      assert resolved.components.schemas["Node"]["properties"]["children"]["items"] ==
               %Reference{ref: "#/components/schemas/Node"}

      assert AsyncApiSpex.validate(document) == :ok
    end

    test "a field type that is not a schema module is a validation error naming the schema" do
      assert {:error, [message]} = AsyncApiSpex.validate(Shop.Test.BadSpec.spec())
      assert message =~ ~r/components.schemas.BadEvent: NotAModule is not a module using/
    end

    test "a hand-written schema may use atoms for values" do
      document = %Document{
        info: %Info{title: "T", version: "1"},
        channels: %{
          "c" => %Channel{
            address: nil,
            messages: %{"m" => %Message{name: "m", payload: %{type: :object}}}
          }
        },
        components: %AsyncApiSpex.Components{schemas: %{"S" => %{type: :object}}}
      }

      assert AsyncApiSpex.validate(document) == :ok
    end
  end

  describe "use AsyncApiSpex.Spec" do
    setup context do
      if app = context[:app] do
        :ok =
          :application.load(
            {:application, app,
             [
               description: ~c"fake",
               vsn: ~c"0.0.0",
               modules: [Shop.Kafka.OrderProducer, Shop.Events.OrderCreated, Enum],
               registered: [],
               applications: []
             ]}
          )

        on_exit(fn -> :application.unload(app) end)
      end

      :ok
    end

    @tag app: :shop_fake
    test "otp_app: includes exactly the channel modules of the application" do
      spec = Shop.AsyncApiByApp.spec()

      assert Map.keys(spec.channels) == ["shop_orders"]
      assert spec.operations |> Map.keys() == ["send-shop_orders"]
    end

    test "otp_app: naming an application that is not loaded raises" do
      assert_raise ArgumentError, ~r/is not loaded/, fn -> Shop.AsyncApiMissingApp.spec() end
    end

    test "channels sharing an identical server and message share one entry each" do
      spec =
        build_spec(channels: [Shop.Test.ChannelA, Shop.Test.ChannelB])

      assert map_size(spec.servers) == 1
      assert map_size(spec.components.messages) == 1
      assert Map.keys(spec.operations) |> Enum.sort() == ["receive-b_events", "send-a_events"]
    end

    test "a server id declared differently by two channels raises" do
      assert_raise ArgumentError, ~r/server id kafka is declared differently/, fn ->
        build_spec(channels: [Shop.Test.ChannelA, Shop.Test.ChannelOtherHost])
      end
    end

    test "a message name declared differently by two channels raises" do
      assert_raise ArgumentError, ~r/message id OrderShipped is declared differently/, fn ->
        build_spec(channels: [Shop.Test.ChannelA, Shop.Test.ChannelOtherContentType])
      end
    end

    test "a channel id declared by two modules raises" do
      assert_raise ArgumentError, ~r/channel id a_events is declared by/, fn ->
        build_spec(channels: [Shop.Test.ChannelA, Shop.Test.ChannelDuplicateId])
      end
    end

    test "channels: naming a module that declares no channel raises" do
      assert_raise ArgumentError, ~r/Enum does not use AsyncApiSpex.Channel/, fn ->
        build_spec(channels: [Enum])
      end
    end

    test "spec/0 can be extended with super/0" do
      encoded = Shop.Test.ExtendedSpec.spec() |> AsyncApiSpex.encode!() |> JSON.decode!()

      assert encoded["x-team"] == "payments"
      assert Map.keys(encoded["channels"]) == ["shop_orders"]
    end
  end

  describe "use AsyncApiSpex.Channel" do
    test "a message module is kept as the module and its dots become underscores in the key" do
      spec = Shop.Test.CustomSpec.spec()

      assert spec.channels["custom"].messages == %{"Custom_Event" => Shop.Test.CustomEvent}
      assert spec.components.messages == %{}

      encoded = spec |> AsyncApiSpex.encode!() |> JSON.decode!()

      assert encoded["channels"]["custom"]["messages"] ==
               %{"Custom_Event" => %{"$ref" => "#/components/messages/Custom.Event"}}

      assert encoded["components"]["messages"]["Custom.Event"]["headers"] ==
               %{"type" => "object", "properties" => %{"trace" => %{"type" => "string"}}}

      assert encoded["operations"]["send-custom"]["messages"] ==
               [%{"$ref" => "#/channels/custom/messages/Custom_Event"}]

      assert AsyncApiSpex.validate(spec) == :ok
    end

    test ":receive, parameters, and operation options reach the document" do
      %{channel: channel, operation: {operation_id, operation}} =
        Shop.Test.ReceivingChannel.__async_api_channel__()

      assert channel.address == "shop.{region}.orders"
      assert Map.keys(channel.parameters) == ["region"]
      assert operation_id == "receive-shop__region__orders"
      assert operation.action == :receive
      assert operation.title == "Consume shipments"

      spec = build_spec(channels: [Shop.Test.ReceivingChannel])
      assert AsyncApiSpex.validate(spec) == :ok
    end
  end

  describe "compile-time errors" do
    test "fields naming a key the struct does not have" do
      assert_compile_error(
        ~r/are not keys of the struct/,
        ~s|defmodule DecoratorsBad1 do defstruct [:a]; use AsyncApiSpex.Schema, name: "x", fields: [b: :string] end|
      )
    end

    test "fields in a module without defstruct" do
      assert_compile_error(
        ~r/requires defstruct/,
        ~s|defmodule DecoratorsBad2 do use AsyncApiSpex.Schema, name: "x", fields: [a: :string] end|
      )
    end

    test "schema and fields together" do
      assert_compile_error(
        ~r/exactly one of :schema or :fields/,
        ~s|defmodule DecoratorsBad3 do defstruct [:a]; use AsyncApiSpex.Schema, name: "x", schema: %{}, fields: [a: :string] end|
      )
    end

    test "neither schema nor fields" do
      assert_compile_error(
        ~r/exactly one of :schema or :fields/,
        ~s|defmodule DecoratorsBad4 do use AsyncApiSpex.Schema, name: "x" end|
      )
    end

    test "title with schema" do
      assert_compile_error(
        ~r/:title and :description go inside :schema/,
        ~s|defmodule DecoratorsBad5 do use AsyncApiSpex.Schema, name: "x", title: "t", schema: %{} end|
      )
    end

    test "an unknown field type" do
      assert_compile_error(
        ~r/field :a has unknown type :nope/,
        ~s|defmodule DecoratorsBad6 do defstruct [:a]; use AsyncApiSpex.Schema, name: "x", fields: [a: :nope] end|
      )
    end

    test "an unknown field option" do
      assert_compile_error(
        ~r/field :a has unknown option\(s\) \[:foo\]/,
        ~s|defmodule DecoratorsBad7 do defstruct [:a]; use AsyncApiSpex.Schema, name: "x", fields: [a: [type: :string, foo: 1]] end|
      )
    end

    test "a channel with a nil address and no id" do
      assert_compile_error(
        ~r/requires an :id when :address is nil/,
        ~s|defmodule DecoratorsBad8 do use AsyncApiSpex.Channel, address: nil, server: [host: "h", protocol: "p"], messages: [X] end|
      )
    end

    test "a channel server without a protocol" do
      assert_compile_error(
        ~r/requires :host and :protocol/,
        ~s|defmodule DecoratorsBad9 do use AsyncApiSpex.Channel, address: "a", server: [host: "h"], messages: [X] end|
      )
    end

    test "a channel without messages" do
      assert_compile_error(
        ~r/:messages must be a non-empty list of modules/,
        ~s|defmodule DecoratorsBad10 do use AsyncApiSpex.Channel, address: "a", server: [host: "h", protocol: "p"], messages: [] end|
      )
    end

    test "a channel action other than send or receive" do
      assert_compile_error(
        ~r/:action must be :send or :receive/,
        ~s|defmodule DecoratorsBad11 do use AsyncApiSpex.Channel, address: "a", server: [host: "h", protocol: "p"], messages: [X], action: :publish end|
      )
    end

    test "a spec with both channels and otp_app" do
      assert_compile_error(
        ~r/exactly one of :channels or :otp_app/,
        ~s|defmodule DecoratorsBad12 do use AsyncApiSpex.Spec, info: [title: "t", version: "1"], channels: [A], otp_app: :b end|
      )
    end

    test "a spec info without a version" do
      assert_compile_error(
        ~r/:info requires :title and :version/,
        ~s|defmodule DecoratorsBad13 do use AsyncApiSpex.Spec, info: [title: "t"], otp_app: :b end|
      )
    end
  end

  defp build_spec(opts) do
    AsyncApiSpex.Spec.Builder.build([info: [title: "T", version: "1"]] ++ opts)
  end

  defp assert_compile_error(regex, source) do
    assert_raise ArgumentError, regex, fn -> Code.compile_string(source) end
  end

  defp refute_has_nils(value) do
    case value do
      map when is_map(map) ->
        Enum.each(map, fn {key, item} -> refute_has_nils(key) && refute_has_nils(item) end)

      list when is_list(list) ->
        Enum.each(list, &refute_has_nils/1)

      nil ->
        flunk("found a nil value in the encoded document")

      _other ->
        :ok
    end
  end
end
