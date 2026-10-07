defmodule AsyncApiSpex.Channel do
  @moduledoc """
  A channel: a topic, exchange and routing key, subject, or any other address a
  message is sent to or received from.

  `address` is `nil` when the address is computed at runtime and cannot be
  described statically; in that case `parameters` must be empty. `messages` maps
  a channel-local key to a message, which may be a `%AsyncApiSpex.Message{}`, a
  `%AsyncApiSpex.Reference{}`, or a module that exports `__async_api_message__/0`
  (moved into `components.messages` by `AsyncApiSpex.resolve/1`).

  ## Declaring a channel on the module that uses it

  `use AsyncApiSpex.Channel` decorates the module that publishes to (or consumes
  from) a channel. It declares the channel, the broker it lives on, and the
  operation, so `use AsyncApiSpex.Spec` can assemble a document from the modules
  your application already has:

      defmodule Shop.Kafka.OrderProducer do
        use AsyncApiSpex.Channel,
          address: "shop.orders",
          server: [id: "kafka", host: "kafka:9092", protocol: "kafka"],
          messages: [Shop.Events.OrderCreated, Shop.Events.OrderShipped],
          bindings: %{"kafka" => %{"partitions" => 12}}

        def publish(event), do: # ... produce to "shop.orders"
      end

  Options:

    * `:address` (required) — a string literal, or the literal `nil` when the
      address is computed at runtime.
    * `:id` — the channel id in the document; a string literal matching
      `#{inspect(~r/\A[A-Za-z0-9_-]+\z/)}`. Defaults to `:address` with every
      other character replaced by `_` (`"shop.orders"` becomes `"shop_orders"`).
      Required when `:address` is `nil`.
    * `:server` (required) — a keyword list literal with `:host` and `:protocol`
      and optionally `:id` (default: the protocol), `:protocol_version`,
      `:pathname`, `:title`, `:summary`, `:description`, `:tags`, `:bindings`.
      Values are evaluated when the document is built, so
      `System.get_env/2` and `Application.get_env/2` work.
    * `:messages` (required) — a non-empty list of modules, each using either
      `AsyncApiSpex.Message` or `AsyncApiSpex.Schema`. A schema module becomes a
      message in `components.messages`, named after the schema, with `:content_type`.
    * `:action` — `:send` (default) when this module publishes, `:receive` when
      it consumes.
    * `:content_type` — the content type of messages derived from schema
      modules. Defaults to `"application/json"`.
    * `:title`, `:summary`, `:description`, `:parameters`, `:tags`, `:bindings`,
      `:extensions` — the same fields of the channel struct.
    * `:operation` — a keyword list literal of `:title`, `:summary`,
      `:description`, `:tags`, `:bindings`, `:extensions` for the operation.

  Unknown options and invalid literals raise `ArgumentError` at compile time.

  The module gets `channel/0`, returning the `t:t/0`, and
  `__async_api_channel__/0`, returning a map with the channel `:id`, the
  `:server` as `{id, server}`, the `:channel`, the `:operation` as
  `{id, operation}` (its id is `"<action>-<channel id>"`), and `:messages`, the
  `components.messages` entries derived from schema modules. Message keys on the
  channel are the message names with every character outside
  `A-Za-z0-9_-` replaced by `_`.

  `channel/0` alone is not a complete document fragment: a message derived from
  a schema module is a reference to `#/components/messages/<name>`, and that
  message is only in `__async_api_channel__/0`'s `:messages`. Use
  `AsyncApiSpex.Spec`, which assembles all of it; if you build a
  `%AsyncApiSpex.Document{}` from `channel/0` yourself, add `:messages` to
  `components.messages`, the server, and the operation too, or
  `AsyncApiSpex.validate/1` reports the unresolved reference.
  """

  @allowed [
    :address,
    :id,
    :server,
    :messages,
    :action,
    :content_type,
    :title,
    :summary,
    :description,
    :parameters,
    :tags,
    :bindings,
    :extensions,
    :operation
  ]

  @channel_keys [:title, :summary, :description, :parameters, :tags, :bindings, :extensions]

  @server_keys [
    :id,
    :host,
    :protocol,
    :protocol_version,
    :pathname,
    :title,
    :summary,
    :description,
    :tags,
    :bindings
  ]

  @operation_keys [:title, :summary, :description, :tags, :bindings, :extensions]

  @id ~r/\A[A-Za-z0-9_-]+\z/

  defstruct address: nil,
            title: nil,
            summary: nil,
            description: nil,
            servers: nil,
            messages: %{},
            parameters: %{},
            tags: nil,
            bindings: nil,
            extensions: %{}

  @type t :: %__MODULE__{
          address: String.t() | nil,
          title: String.t() | nil,
          summary: String.t() | nil,
          description: String.t() | nil,
          servers: [AsyncApiSpex.Reference.t()] | nil,
          messages: %{
            optional(String.t()) =>
              AsyncApiSpex.Message.t() | AsyncApiSpex.Reference.t() | module()
          },
          parameters: %{optional(String.t()) => AsyncApiSpex.Parameter.t()},
          tags: [AsyncApiSpex.Tag.t()] | nil,
          bindings: map() | nil,
          extensions: map()
        }

  @doc """
  Declares the channel, its server, and its operation on the calling module.

  See the module documentation for the options.
  """
  defmacro __using__(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError,
            "use AsyncApiSpex.Channel expects a keyword list, got: #{inspect(opts)}"
    end

    case Keyword.keys(opts) -- @allowed do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "unknown option(s) #{inspect(unknown)} for use AsyncApiSpex.Channel; " <>
                "allowed options: #{inspect(@allowed)}"
    end

    address = address!(opts, __CALLER__)
    id = id!(opts, address, __CALLER__)
    action = action!(opts, __CALLER__)
    server_ast = server!(opts)
    messages_ast = messages!(opts)
    operation_ast = operation!(opts)
    content_type_ast = Keyword.get(opts, :content_type, "application/json")
    channel_ast = Keyword.take(opts, @channel_keys)

    quote do
      @doc "Returns the declared `t:AsyncApiSpex.Channel.t/0`."
      def channel, do: __async_api_channel__().channel

      @doc "Returns the channel, its server, and its operation for `use AsyncApiSpex.Spec`."
      def __async_api_channel__ do
        AsyncApiSpex.Channel.Builder.build(__MODULE__,
          id: unquote(id),
          action: unquote(action),
          address: unquote(address),
          server: unquote(server_ast),
          messages: unquote(messages_ast),
          content_type: unquote(content_type_ast),
          channel: unquote(channel_ast),
          operation: unquote(operation_ast)
        )
      end
    end
  end

  defp address!(opts, caller) do
    case Keyword.fetch(opts, :address) do
      :error ->
        raise ArgumentError, "use AsyncApiSpex.Channel requires an :address option"

      {:ok, ast} ->
        case Macro.expand(ast, caller) do
          address when is_binary(address) or is_nil(address) ->
            address

          _other ->
            raise ArgumentError,
                  "use AsyncApiSpex.Channel :address must be a string literal or nil"
        end
    end
  end

  defp id!(opts, address, caller) do
    id =
      case Keyword.fetch(opts, :id) do
        {:ok, ast} ->
          case Macro.expand(ast, caller) do
            id when is_binary(id) ->
              id

            other ->
              raise ArgumentError,
                    "use AsyncApiSpex.Channel :id must be a string literal, " <>
                      "got: #{Macro.to_string(other)}"
          end

        :error when is_nil(address) ->
          raise ArgumentError, "use AsyncApiSpex.Channel requires an :id when :address is nil"

        :error ->
          String.replace(address, ~r/[^A-Za-z0-9_-]/, "_")
      end

    unless Regex.match?(@id, id) do
      raise ArgumentError,
            "use AsyncApiSpex.Channel :id must match #{inspect(@id)}, got: #{inspect(id)}"
    end

    id
  end

  defp action!(opts, caller) do
    case Keyword.fetch(opts, :action) do
      :error ->
        :send

      {:ok, ast} ->
        case Macro.expand(ast, caller) do
          action when action in [:send, :receive] ->
            action

          _other ->
            raise ArgumentError, "use AsyncApiSpex.Channel :action must be :send or :receive"
        end
    end
  end

  defp server!(opts) do
    server =
      case Keyword.fetch(opts, :server) do
        {:ok, server} when is_list(server) ->
          if Keyword.keyword?(server) do
            server
          else
            raise ArgumentError, "use AsyncApiSpex.Channel :server must be a keyword list"
          end

        _ ->
          raise ArgumentError,
                "use AsyncApiSpex.Channel requires a :server keyword list with :host and :protocol"
      end

    check_keys!(server, @server_keys, ":server")

    unless Keyword.has_key?(server, :host) and Keyword.has_key?(server, :protocol) do
      raise ArgumentError, "use AsyncApiSpex.Channel :server requires :host and :protocol"
    end

    server
  end

  defp messages!(opts) do
    case Keyword.fetch(opts, :messages) do
      {:ok, [_ | _] = messages} ->
        messages

      _ ->
        raise ArgumentError,
              "use AsyncApiSpex.Channel :messages must be a non-empty list of modules"
    end
  end

  defp operation!(opts) do
    case Keyword.fetch(opts, :operation) do
      :error ->
        []

      {:ok, operation} when is_list(operation) ->
        unless Keyword.keyword?(operation) do
          raise ArgumentError, "use AsyncApiSpex.Channel :operation must be a keyword list"
        end

        check_keys!(operation, @operation_keys, ":operation")
        operation

      {:ok, _other} ->
        raise ArgumentError, "use AsyncApiSpex.Channel :operation must be a keyword list"
    end
  end

  defp check_keys!(keyword, allowed, option) do
    case Keyword.keys(keyword) -- allowed do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "unknown key(s) #{inspect(unknown)} in use AsyncApiSpex.Channel #{option}; " <>
                "allowed keys: #{inspect(allowed)}"
    end
  end
end
