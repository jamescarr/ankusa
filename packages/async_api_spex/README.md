# AsyncApiSpex

Declaratively describe the messaging channels an Elixir application publishes to
or consumes from, as an [AsyncAPI 3.0](https://www.asyncapi.com/docs/reference/specification/v3.0.0)
document, and serve or serialize it. The API mirrors
[`open_api_spex`](https://hexdocs.pm/open_api_spex): plain structs, light `use`
macros, a Plug, and a mix task.

The library has no dependency on any application's domain code — only `plug`
(optional, for `AsyncApiSpex.Plug.RenderSpec`).

## Installation

```elixir
def deps do
  [{:async_api_spex, "~> 0.1"}]
end
```

## Usage

### Describe what you already have

Say your shop app already has event structs and a module that publishes them to
a Kafka topic with [`:brod`](https://hexdocs.pm/brod). Decorate those: the
structs become schemas, the producer declares its topic and its broker, and one
`use AsyncApiSpex.Spec` module turns them into an AsyncAPI document.

```elixir
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
  use AsyncApiSpex.Channel,
    address: "shop.orders",
    server: [id: "kafka", host: System.get_env("KAFKA_BOOTSTRAP", "kafka:9092"), protocol: "kafka"],
    messages: [Shop.Events.OrderCreated, Shop.Events.OrderShipped],
    bindings: %{"kafka" => %{"partitions" => 12}},
    description: "One record per order event, keyed by order id."

  def publish(%{id: id} = event) do
    :brod.produce_sync(:shop, "shop.orders", :hash, id, JSON.encode!(Map.from_struct(event)))
  end
end

defmodule Shop.AsyncApi do
  use AsyncApiSpex.Spec,
    otp_app: :shop,
    info: [title: "Shop", version: "1.0.0"]
end
```

Every key of a struct becomes a property of its schema, and `fields:` refines
the keys it names: a type, `required: true`, a `description`. A key that
`fields:` does not name is still a property, with no constraints; naming a key
the struct does not have is a compile error. Types are `:string`, `:integer`,
`:number`, `:boolean`, `:map`, `:any`, `:datetime`, `:date`, `{:array, type}`,
`{:enum, values}`, another schema module (`Shop.Events.LineItem` above), or a
raw schema map. See `AsyncApiSpex.Schema` for the table.

The producer declares where its events go: the topic (`address:`), the broker
(`server:`, evaluated when the document is built, so environment variables and
`Application.get_env/2` work), and the events it sends (`messages:`). The
operation is `send` unless you pass `action: :receive`, which is what a consumer
module declares. `publish/1` is yours; the library only reads the declaration.

`otp_app: :shop` finds every module in that application that uses
`AsyncApiSpex.Channel`. To list them instead, write
`channels: [Shop.Kafka.OrderProducer]`. Each channel module gets its own
channel, and the document shares a server between channels that declare the
same one.

### Serve or write it

Serve it from a Plug router:

```elixir
forward "/asyncapi.json", AsyncApiSpex.Plug.RenderSpec, spec: Shop.AsyncApi
```

Or write it to a file:

```sh
mix async_api_spex.gen --spec Shop.AsyncApi --output asyncapi.json
```

`AsyncApiSpex.encode!/1` emits JSON. YAML 1.2 is a superset of JSON, so any YAML
tool — including AsyncAPI Studio and `asyncapi validate` — reads the output.

### Let others subscribe

The document for the modules above, abridged:

```json
{
  "asyncapi": "3.0.0",
  "info": {"title": "Shop", "version": "1.0.0"},
  "servers": {
    "kafka": {"host": "kafka:9092", "protocol": "kafka"}
  },
  "channels": {
    "shop_orders": {
      "address": "shop.orders",
      "description": "One record per order event, keyed by order id.",
      "bindings": {"kafka": {"partitions": 12}},
      "servers": [{"$ref": "#/servers/kafka"}],
      "messages": {
        "OrderCreated": {"$ref": "#/components/messages/OrderCreated"},
        "OrderShipped": {"$ref": "#/components/messages/OrderShipped"}
      }
    }
  },
  "operations": {
    "send-shop_orders": {
      "action": "send",
      "channel": {"$ref": "#/channels/shop_orders"},
      "messages": [
        {"$ref": "#/channels/shop_orders/messages/OrderCreated"},
        {"$ref": "#/channels/shop_orders/messages/OrderShipped"}
      ]
    }
  },
  "components": {
    "messages": {
      "OrderCreated": {
        "name": "OrderCreated",
        "title": "Order created",
        "contentType": "application/json",
        "payload": {"$ref": "#/components/schemas/OrderCreated"}
      }
    },
    "schemas": {
      "OrderCreated": {
        "type": "object",
        "title": "Order created",
        "description": "Emitted once per checkout.",
        "required": ["id"],
        "properties": {
          "id": {"type": "string"},
          "customer_id": {"type": "string"},
          "items": {"type": "array", "items": {"$ref": "#/components/schemas/LineItem"}},
          "placed_at": {"type": "string", "format": "date-time"}
        }
      }
    }
  }
}
```

Another team subscribes with what is in the document and nothing else: connect
to `servers.kafka.host`, subscribe to `channels.shop_orders.address`, and decode
each record with the schemas under `components.schemas`. The AsyncAPI
[generator](https://www.asyncapi.com/tools/generator) templates read exactly this.

### Writing the document by hand

A channel that cannot be decorated (its address is computed, or you want full
control) is declared with the structs directly. `use AsyncApiSpex.Schema` with
`schema:` takes a JSON Schema map, and `use AsyncApiSpex.Message` declares a
message with headers or a correlation id:

```elixir
defmodule MyApp.AsyncApi.Schemas do
  use AsyncApiSpex.Schema,
    name: "OrderCreatedV1",
    schema: %{
      type: "object",
      required: ["id"],
      properties: %{id: %{type: "string"}}
    }
end

defmodule MyApp.AsyncApi.Messages do
  use AsyncApiSpex.Message,
    name: "OrderCreated",
    title: "Order created",
    content_type: "application/json",
    payload: MyApp.AsyncApi.Schemas
end

defmodule MyApp.AsyncApi do
  @behaviour AsyncApiSpex.Spec

  @impl true
  def spec do
    %AsyncApiSpex.Document{
      info: %AsyncApiSpex.Info{title: "My App", version: "1.0.0"},
      servers: %{"prod" => %AsyncApiSpex.Server{host: "kafka:9092", protocol: "kafka"}},
      channels: %{
        "orders" => %AsyncApiSpex.Channel{
          address: "orders",
          messages: %{"created" => MyApp.AsyncApi.Messages}
        }
      },
      operations: %{
        "send-orders" => %AsyncApiSpex.Operation{
          action: :send,
          channel: %AsyncApiSpex.Reference{ref: "#/channels/orders"},
          messages: [%AsyncApiSpex.Reference{ref: "#/channels/orders/messages/created"}]
        }
      }
    }
  end
end
```

A message module can also be listed in a decorated channel's `messages:`, next
to schema modules.
