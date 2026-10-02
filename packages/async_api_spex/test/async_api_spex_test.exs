defmodule AsyncApiSpexTest.Schemas do
  use AsyncApiSpex.Schema,
    name: "OrderCreatedV1",
    schema: %{
      type: "object",
      required: ["id"],
      properties: %{id: %{type: "string"}}
    }
end

defmodule AsyncApiSpexTest.OtherSchema do
  use AsyncApiSpex.Schema, name: "Shared", schema: %{type: "string"}
end

defmodule AsyncApiSpexTest.ConflictingSchema do
  use AsyncApiSpex.Schema, name: "Shared", schema: %{type: "number"}
end

defmodule AsyncApiSpexTest.Messages do
  alias AsyncApiSpexTest.Schemas

  use AsyncApiSpex.Message,
    name: "OrderCreated",
    title: "Order created",
    content_type: "application/json",
    payload: Schemas,
    extensions: %{"x-foo" => "bar"}
end

defmodule AsyncApiSpexTest.Spec do
  @behaviour AsyncApiSpex.Spec

  @impl true
  def spec do
    %AsyncApiSpex.Document{
      info: %AsyncApiSpex.Info{title: "Test API", version: "1.0.0"},
      default_content_type: "application/json",
      servers: %{"prod" => %AsyncApiSpex.Server{host: "broker:9092", protocol: "kafka"}},
      channels: %{
        "orders" => %AsyncApiSpex.Channel{
          address: "orders",
          messages: %{"created" => AsyncApiSpexTest.Messages}
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

defmodule AsyncApiSpexTest do
  use ExUnit.Case, async: true

  alias AsyncApiSpex.{
    Channel,
    Document,
    Info,
    Message,
    Operation,
    Reference
  }

  describe "encode!/1" do
    test "resolves components and encodes lowerCamel JSON" do
      encoded = AsyncApiSpexTest.Spec.spec() |> AsyncApiSpex.encode!() |> JSON.decode!()

      assert encoded["asyncapi"] == "3.0.0"
      assert encoded["defaultContentType"] == "application/json"
      assert encoded["info"] == %{"title" => "Test API", "version" => "1.0.0"}

      assert %{"name" => "OrderCreated", "x-foo" => "bar", "payload" => payload} =
               encoded["components"]["messages"]["OrderCreated"]

      assert payload == %{"$ref" => "#/components/schemas/OrderCreatedV1"}
      assert encoded["components"]["schemas"]["OrderCreatedV1"]["type"] == "object"

      assert encoded["channels"]["orders"]["messages"]["created"] ==
               %{"$ref" => "#/components/messages/OrderCreated"}

      assert encoded["operations"]["send-orders"]["action"] == "send"

      assert encoded["operations"]["send-orders"]["channel"] ==
               %{"$ref" => "#/channels/orders"}

      refute_has_nils(encoded)
    end
  end

  describe "resolve/1" do
    test "two different modules with the same component name raise" do
      document = %Document{
        info: %Info{title: "T", version: "1"},
        channels: %{
          "c" => %Channel{
            address: "c",
            messages: %{
              "a" => %Message{name: "a", payload: AsyncApiSpexTest.OtherSchema},
              "b" => %Message{name: "b", payload: AsyncApiSpexTest.ConflictingSchema}
            }
          }
        }
      }

      assert_raise ArgumentError, ~r/component name Shared is used by/, fn ->
        AsyncApiSpex.resolve(document)
      end
    end

    test "the same module used twice is fine" do
      document = %Document{
        info: %Info{title: "T", version: "1"},
        channels: %{
          "c" => %Channel{
            address: "c",
            messages: %{
              "a" => %Message{name: "a", payload: AsyncApiSpexTest.OtherSchema},
              "b" => %Message{name: "b", payload: AsyncApiSpexTest.OtherSchema}
            }
          }
        }
      }

      assert %Document{} = resolved = AsyncApiSpex.resolve(document)
      assert %{"Shared" => _} = resolved.components.schemas
    end

    test "is idempotent" do
      once = AsyncApiSpex.resolve(AsyncApiSpexTest.Spec.spec())
      assert AsyncApiSpex.resolve(once) == once
    end
  end

  describe "validate/1" do
    test "a minimal valid document returns :ok" do
      assert :ok = AsyncApiSpex.validate(valid_document())
      assert :ok = AsyncApiSpex.validate(AsyncApiSpexTest.Spec.spec())
    end

    test "an address expression without a parameter reports one error" do
      document = put_channel(valid_document(), "c", %Channel{address: "orders.{id}"})

      assert {:error, [message]} = AsyncApiSpex.validate(document)
      assert message =~ "channels.c.address"
    end

    test "an operation referencing a missing channel reports one error" do
      document = %{
        valid_document()
        | operations: %{
            "op" => %Operation{action: :send, channel: %Reference{ref: "#/channels/nope"}}
          }
      }

      assert {:error, [message]} = AsyncApiSpex.validate(document)
      assert message =~ "operations.op.channel"
    end

    test "an operation message not in its channel reports one error" do
      channel = %Channel{
        address: nil,
        messages: %{"a" => %Message{name: "a"}}
      }

      document = %{
        valid_document()
        | channels: %{"c" => channel},
          operations: %{
            "op" => %Operation{
              action: :send,
              channel: %Reference{ref: "#/channels/c"},
              messages: [%Reference{ref: "#/channels/c/messages/nope"}]
            }
          }
      }

      assert {:error, [message]} = AsyncApiSpex.validate(document)
      assert message =~ "operations.op.messages"
    end

    test "a channel id containing a space reports one error" do
      document = put_channel(valid_document(), "bad id", %Channel{address: nil})

      assert {:error, [message]} = AsyncApiSpex.validate(document)
      assert message =~ "channels.bad id"
    end

    test "an extension key not starting with x- reports one error" do
      document = %{valid_document() | extensions: %{"foo" => 1}}

      assert {:error, [message]} = AsyncApiSpex.validate(document)
      assert message =~ "extensions"
      assert message =~ "x-"
    end
  end

  describe "use AsyncApiSpex.Message" do
    test "an unknown option raises at compile time" do
      assert_raise ArgumentError, ~r/allowed options/, fn ->
        Code.compile_string(~s|defmodule Bad do use AsyncApiSpex.Message, nme: "x" end|)
      end
    end

    test "a missing name raises at compile time" do
      assert_raise ArgumentError, ~r/requires a :name/, fn ->
        Code.compile_string(~s|defmodule Bad2 do use AsyncApiSpex.Message, title: "x" end|)
      end
    end
  end

  describe "use AsyncApiSpex.Schema" do
    test "a non-map schema raises at compile time" do
      assert_raise ArgumentError, ~r/:schema must be a map/, fn ->
        Code.compile_string(
          ~s|defmodule Bad3 do use AsyncApiSpex.Schema, name: "x", schema: "nope" end|
        )
      end
    end
  end

  describe "AsyncApiSpex.Plug.RenderSpec" do
    test "serves the encoded document with the AsyncAPI content type" do
      conn =
        Plug.Test.conn(:get, "/asyncapi.json")
        |> AsyncApiSpex.Plug.RenderSpec.call(
          AsyncApiSpex.Plug.RenderSpec.init(spec: AsyncApiSpexTest.Spec)
        )

      assert conn.status == 200
      assert [content_type] = Plug.Conn.get_resp_header(conn, "content-type")
      assert String.starts_with?(content_type, "application/asyncapi+json")
      assert %{"asyncapi" => "3.0.0"} = JSON.decode!(conn.resp_body)
    end

    test "init/1 without :spec raises" do
      assert_raise ArgumentError, ~r/requires a :spec option/, fn ->
        AsyncApiSpex.Plug.RenderSpec.init([])
      end
    end
  end

  describe "mix async_api_spex.gen" do
    test "writes the encoded document to the output path" do
      output =
        Path.join(
          System.tmp_dir!(),
          "async_api_spex_test_#{System.unique_integer([:positive])}.json"
        )

      on_exit(fn -> File.rm(output) end)

      Mix.Tasks.AsyncApiSpex.Gen.run([
        "--spec",
        "AsyncApiSpexTest.Spec",
        "--output",
        output
      ])

      assert %{"asyncapi" => "3.0.0"} = output |> File.read!() |> JSON.decode!()
    end
  end

  defp valid_document do
    %Document{
      info: %Info{title: "T", version: "1"},
      channels: %{"c" => %Channel{address: nil}}
    }
  end

  defp put_channel(document, id, channel), do: %{document | channels: %{id => channel}}

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
