defmodule AsyncApiSpex.Message do
  @moduledoc """
  A message on a channel: its metadata, headers, and payload.

  `payload` and `headers` are AsyncAPI Schema Objects, which may be plain maps,
  `%AsyncApiSpex.Reference{}` structs, or modules that export
  `__async_api_schema__/0`. A module is extracted into `components.schemas` by
  `AsyncApiSpex.resolve/1` and replaced with a reference.

  Use the `use AsyncApiSpex.Message` macro to declare a message:

      defmodule MyApp.Messages do
        use AsyncApiSpex.Message,
          name: "OrderCreated",
          payload: MyApp.Schemas.OrderCreatedV1
      end

  This defines `message/0` returning a `%AsyncApiSpex.Message{}` and
  `__async_api_message__/0` returning `{name, message}`.
  """

  @fields [
    title: nil,
    summary: nil,
    description: nil,
    content_type: nil,
    payload: nil,
    headers: nil,
    correlation_id: nil,
    tags: nil,
    bindings: nil,
    examples: nil,
    extensions: %{}
  ]

  defstruct [name: nil] ++ @fields

  @type t :: %__MODULE__{
          name: String.t() | nil,
          title: String.t() | nil,
          summary: String.t() | nil,
          description: String.t() | nil,
          content_type: String.t() | nil,
          headers: map() | AsyncApiSpex.Reference.t() | module() | nil,
          payload: map() | AsyncApiSpex.Reference.t() | module() | nil,
          correlation_id: AsyncApiSpex.CorrelationId.t() | nil,
          tags: [AsyncApiSpex.Tag.t()] | nil,
          bindings: map() | nil,
          examples: [term()] | nil,
          extensions: map()
        }

  @doc """
  Declares a message on the calling module.

  Options: `#{inspect(Keyword.keys(@fields))}` plus the required `:name` (a
  string matching `#{inspect(~r/\A[A-Za-z0-9_.-]+\z/)}`). Unknown options raise
  `ArgumentError`. The option values are evaluated in the calling module's
  context.
  """
  defmacro __using__(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError,
            "use AsyncApiSpex.Message expects a keyword list, got: #{inspect(opts)}"
    end

    allowed = [:name | Keyword.keys(@fields)]

    case Keyword.keys(opts) -- allowed do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "unknown option(s) #{inspect(unknown)} for use AsyncApiSpex.Message; " <>
                "allowed options: #{inspect(allowed)}"
    end

    name =
      AsyncApiSpex.MacroHelpers.expand_component_name!(
        opts,
        "use AsyncApiSpex.Message",
        __CALLER__
      )

    pairs =
      for {field, default} <- @fields do
        case Keyword.fetch(opts, field) do
          {:ok, ast} -> {field, ast}
          :error -> {field, Macro.escape(default)}
        end
      end

    map_ast = {:%{}, [], [__struct__: AsyncApiSpex.Message, name: name] ++ pairs}

    quote do
      @doc "Returns the declared `t:AsyncApiSpex.Message.t/0`."
      def message, do: unquote(map_ast)

      @doc "Returns `{name, message}` for `AsyncApiSpex.resolve/1`."
      def __async_api_message__, do: {unquote(name), message()}
    end
  end
end
