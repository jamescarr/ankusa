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

Serve it from a Plug router:

```elixir
forward "/asyncapi.json", AsyncApiSpex.Plug.RenderSpec, spec: MyApp.AsyncApi
```

Or write it to a file:

```sh
mix async_api_spex.gen --spec MyApp.AsyncApi --output asyncapi.json
```

`AsyncApiSpex.encode!/1` emits JSON. YAML 1.2 is a superset of JSON, so any YAML
tool — including AsyncAPI Studio and `asyncapi validate` — reads the output.
