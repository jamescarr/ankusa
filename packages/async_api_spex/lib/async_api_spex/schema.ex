defmodule AsyncApiSpex.Schema do
  @moduledoc """
  Declares a reusable AsyncAPI Schema Object (JSON Schema draft-07).

  There are two forms. Pass exactly one of `:schema` and `:fields`.

  ## `schema:` — write the JSON Schema

      defmodule MyApp.Schemas do
        use AsyncApiSpex.Schema,
          name: "OrderCreatedV1",
          schema: %{type: "object", required: ["id"], properties: %{id: %{type: "string"}}}
      end

  `schema` must be a map literal.

  ## `fields:` — decorate an existing struct

      defmodule MyApp.Events.OrderCreated do
        defstruct [:id, :customer_id, :items, :placed_at]

        use AsyncApiSpex.Schema,
          name: "OrderCreated",
          title: "Order created",
          description: "Emitted once per checkout.",
          fields: [
            id: [type: :string, required: true],
            customer_id: :string,
            items: {:array, MyApp.Events.LineItem},
            placed_at: :datetime
          ]
      end

  Every key of the struct becomes a property of an `object` schema. `fields`
  refines the keys it names; a struct key it does not name is an unconstrained
  property (`%{}`). Naming a key the struct does not have, or using `fields`
  in a module without `defstruct`, raises `ArgumentError` at compile time.
  Struct defaults are not emitted and `additionalProperties` is left unset.
  `use` may come before or after `defstruct`.

  A field is either a type, or a keyword list with a required `:type` and the
  optional `:required` (boolean) and `:description` (string).

  | Type | Schema |
  |---|---|
  | `:string`, `:integer`, `:number`, `:boolean` | `%{"type" => "string"}` and so on |
  | `:map` | `%{"type" => "object"}` |
  | `:any` | `%{}` |
  | `:datetime` | `%{"type" => "string", "format" => "date-time"}` |
  | `:date` | `%{"type" => "string", "format" => "date"}` |
  | `{:array, type}` | `%{"type" => "array", "items" => <type>}` |
  | `{:enum, values}` | `%{"enum" => values}`; a non-empty list of strings, atoms, or numbers |
  | a map | used verbatim, for anything else |
  | a module | a reference to that module's schema; it must itself `use AsyncApiSpex.Schema` |

  A module may reference itself (`children: {:array, __MODULE__}`). A module
  type is not checked at compile time, because the module may not be compiled
  yet; `AsyncApiSpex.validate/1` reports one that does not use
  `AsyncApiSpex.Schema`.

  `:title` and `:description` are only valid with `:fields`; with `:schema`
  put them inside the map.

  ## Both forms

  Both define `schema/0` returning the schema map and
  `__async_api_schema__/0` returning `{name, schema}`. `AsyncApiSpex.resolve/1`
  moves a schema module used from a message into `components.schemas` and
  replaces the use with a reference.

  `name` must be a string literal matching
  `#{inspect(~r/\A[A-Za-z0-9_.-]+\z/)}`. A bad name, a bad `:schema` or
  `:fields`, and any unknown option raise `ArgumentError` at compile time.
  """

  alias AsyncApiSpex.{MacroHelpers, Schema.Fields}

  @allowed [:name, :schema, :fields, :title, :description]

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

    name = MacroHelpers.expand_component_name!(opts, "use AsyncApiSpex.Schema", __CALLER__)

    case {Keyword.fetch(opts, :schema), Keyword.fetch(opts, :fields)} do
      {{:ok, schema_ast}, :error} ->
        if Keyword.has_key?(opts, :title) or Keyword.has_key?(opts, :description) do
          raise ArgumentError,
                "use AsyncApiSpex.Schema :title and :description go inside :schema " <>
                  "when :schema is given"
        end

        MacroHelpers.validate_schema!(schema_ast, "use AsyncApiSpex.Schema", __CALLER__)
        schema_quoted(name, schema_ast)

      {:error, {:ok, fields_ast}} ->
        fields = fields_ast |> Code.eval_quoted([], __CALLER__) |> elem(0)
        Fields.validate!(fields, __CALLER__.module)

        meta =
          Enum.reject(
            [
              title: string_option!(opts, :title, __CALLER__),
              description: string_option!(opts, :description, __CALLER__)
            ],
            fn {_key, value} -> is_nil(value) end
          )

        fields_quoted(name, fields, meta)

      _ ->
        raise ArgumentError, "use AsyncApiSpex.Schema requires exactly one of :schema or :fields"
    end
  end

  defp schema_quoted(name, schema_ast) do
    quote do
      @doc "Returns the declared AsyncAPI Schema Object."
      def schema, do: unquote(schema_ast)

      @doc "Returns `{name, schema}` for `AsyncApiSpex.resolve/1`."
      def __async_api_schema__, do: {unquote(name), schema()}
    end
  end

  defp fields_quoted(name, fields, meta) do
    quote do
      @doc false
      def __async_api_fields__, do: unquote(Macro.escape(fields))

      @after_compile {AsyncApiSpex.Schema.Fields, :__after_compile__}

      @doc "Returns the AsyncAPI Schema Object derived from this module's struct."
      def schema,
        do: AsyncApiSpex.Schema.Fields.schema(__MODULE__, unquote(Macro.escape(meta)))

      @doc "Returns `{name, schema}` for `AsyncApiSpex.resolve/1`."
      def __async_api_schema__, do: {unquote(name), schema()}
    end
  end

  defp string_option!(opts, key, caller) do
    case Keyword.fetch(opts, key) do
      :error ->
        nil

      {:ok, ast} ->
        case Macro.expand(ast, caller) do
          value when is_binary(value) ->
            value

          other ->
            raise ArgumentError,
                  "use AsyncApiSpex.Schema #{inspect(key)} must be a string literal, " <>
                    "got: #{Macro.to_string(other)}"
        end
    end
  end
end
