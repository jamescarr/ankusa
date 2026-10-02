defmodule AsyncApiSpex.Schema do
  @moduledoc """
  Declares a reusable AsyncAPI Schema Object (JSON Schema draft-07).

  Use it inside a module:

      defmodule MyApp.Schemas do
        use AsyncApiSpex.Schema,
          name: "OrderCreatedV1",
          schema: %{type: "object", required: ["id"], properties: %{id: %{type: "string"}}}
      end

  This defines `schema/0` returning the schema map and
  `__async_api_schema__/0` returning `{name, schema}`. `AsyncApiSpex.resolve/1`
  moves a schema module used from a message into `components.schemas` and
  replaces the use with a reference.

  `name` must be a string literal matching
  `#{inspect(~r/\A[A-Za-z0-9_.-]+\z/)}`; `schema` must be a map literal. Both
  requirements raise `ArgumentError` at compile time, as does any unknown
  option.
  """

  @allowed [:name, :schema]

  @doc """
  Declares `schema/0` and `__async_api_schema__/0` on the calling module.
  """
  defmacro __using__(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError, "use AsyncApiSpex.Schema expects a keyword list, got: #{inspect(opts)}"
    end

    case Keyword.keys(opts) -- @allowed do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "unknown option(s) #{inspect(unknown)} for use AsyncApiSpex.Schema; " <>
                "allowed options: #{inspect(@allowed)}"
    end

    name =
      AsyncApiSpex.MacroHelpers.expand_component_name!(
        opts,
        "use AsyncApiSpex.Schema",
        __CALLER__
      )

    schema_ast =
      case Keyword.fetch(opts, :schema) do
        {:ok, ast} ->
          AsyncApiSpex.MacroHelpers.validate_schema!(ast, "use AsyncApiSpex.Schema", __CALLER__)
          ast

        :error ->
          raise ArgumentError, "use AsyncApiSpex.Schema requires a :schema option"
      end

    quote do
      @doc "Returns the declared AsyncAPI Schema Object."
      def schema, do: unquote(schema_ast)

      @doc "Returns `{name, schema}` for `AsyncApiSpex.resolve/1`."
      def __async_api_schema__, do: {unquote(name), schema()}
    end
  end
end
