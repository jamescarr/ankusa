defmodule AsyncApiSpex.Schema.Fields do
  @moduledoc false
  # Support for `use AsyncApiSpex.Schema, fields: [...]`: validates the field
  # declarations at compile time and derives the Schema Object from the
  # module's struct at runtime.

  @field_options [:type, :required, :description]

  @scalars %{
    string: %{"type" => "string"},
    integer: %{"type" => "integer"},
    number: %{"type" => "number"},
    boolean: %{"type" => "boolean"},
    map: %{"type" => "object"},
    any: %{},
    datetime: %{"type" => "string", "format" => "date-time"},
    date: %{"type" => "string", "format" => "date"}
  }

  @doc """
  Raises `ArgumentError` unless `fields` is a valid field declaration list.
  """
  @spec validate!(term(), module()) :: :ok
  def validate!(fields, module) do
    unless is_list(fields) and Keyword.keyword?(fields) do
      fail!(module, "fields must be a keyword list, got: #{inspect(fields)}")
    end

    Enum.each(fields, fn {name, spec} -> validate_field!(name, spec, module) end)
  end

  @doc """
  Builds the Schema Object for `module` from its struct keys and its
  `__async_api_fields__/0` declarations.
  """
  @spec schema(module(), keyword()) :: map()
  def schema(module, meta) do
    fields = module.__async_api_fields__()

    properties =
      Map.new(struct_keys(module), fn key ->
        {Atom.to_string(key), property(Keyword.fetch(fields, key))}
      end)

    required =
      for {name, spec} <- fields, {_type, true, _description} <- [normalize(spec)] do
        Atom.to_string(name)
      end

    %{"type" => "object", "properties" => properties}
    |> put_meta("title", meta[:title])
    |> put_meta("description", meta[:description])
    |> put_meta("required", if(required == [], do: nil, else: required))
  end

  @doc false
  def __after_compile__(env, _bytecode) do
    module = env.module

    unless function_exported?(module, :__struct__, 0) do
      raise ArgumentError,
            "use AsyncApiSpex.Schema with :fields requires defstruct in #{inspect(module)}"
    end

    case Keyword.keys(module.__async_api_fields__()) -- struct_keys(module) do
      [] ->
        :ok

      extra ->
        raise ArgumentError,
              "use AsyncApiSpex.Schema in #{inspect(module)}: " <>
                "fields #{inspect(extra)} are not keys of the struct"
    end
  end

  defp struct_keys(module), do: Map.keys(module.__struct__()) -- [:__struct__]

  defp put_meta(schema, _key, nil), do: schema
  defp put_meta(schema, key, value), do: Map.put(schema, key, value)

  defp property({:ok, spec}) do
    {type, _required, description} = normalize(spec)
    type |> type_schema() |> describe(description)
  end

  defp property(:error), do: %{}

  defp describe(schema, nil), do: schema

  defp describe(schema, description) when is_map(schema),
    do: Map.put(schema, "description", description)

  # A reference cannot carry sibling keywords, so a described module type is
  # wrapped.
  defp describe(module, description) when is_atom(module),
    do: %{"allOf" => [module], "description" => description}

  defp normalize(options) when is_list(options) do
    {Keyword.fetch!(options, :type), Keyword.get(options, :required, false),
     Keyword.get(options, :description)}
  end

  defp normalize(type), do: {type, false, nil}

  defp type_schema(type) when is_atom(type) do
    case @scalars do
      %{^type => schema} -> schema
      _ -> type
    end
  end

  defp type_schema({:array, type}), do: %{"type" => "array", "items" => type_schema(type)}
  defp type_schema({:enum, values}), do: %{"enum" => values}
  defp type_schema(schema) when is_map(schema), do: schema

  defp validate_field!(name, options, module) when is_list(options) do
    unless Keyword.keyword?(options) do
      fail!(
        module,
        "field #{inspect(name)} options must be a keyword list, got: #{inspect(options)}"
      )
    end

    case Keyword.keys(options) -- @field_options do
      [] ->
        :ok

      unknown ->
        fail!(
          module,
          "field #{inspect(name)} has unknown option(s) #{inspect(unknown)}; " <>
            "allowed: #{inspect(@field_options)}"
        )
    end

    case Keyword.fetch(options, :type) do
      {:ok, type} -> validate_type!(name, type, module)
      :error -> fail!(module, "field #{inspect(name)} requires a :type option")
    end

    case Keyword.fetch(options, :required) do
      {:ok, required} when not is_boolean(required) ->
        fail!(
          module,
          "field #{inspect(name)} :required must be a boolean, got: #{inspect(required)}"
        )

      _ ->
        :ok
    end

    case Keyword.fetch(options, :description) do
      {:ok, description} when not is_binary(description) ->
        fail!(
          module,
          "field #{inspect(name)} :description must be a string, got: #{inspect(description)}"
        )

      _ ->
        :ok
    end
  end

  defp validate_field!(name, type, module), do: validate_type!(name, type, module)

  defp validate_type!(name, type, module) do
    case invalid_type(type) do
      nil -> :ok
      invalid -> fail!(module, "field #{inspect(name)} has unknown type #{inspect(invalid)}")
    end
  end

  # Returns the offending sub-type, or nil when `type` is valid.
  defp invalid_type(type) when is_atom(type) do
    if Map.has_key?(@scalars, type) or module_alias?(type), do: nil, else: type
  end

  defp invalid_type({:array, type}), do: invalid_type(type)

  defp invalid_type({:enum, [_ | _] = values} = type) do
    if Enum.all?(values, &(is_binary(&1) or is_atom(&1) or is_number(&1))), do: nil, else: type
  end

  defp invalid_type(type) when is_map(type), do: nil
  defp invalid_type(type), do: type

  # An atom spelled like an alias (`Shop.Events.LineItem`) names a schema module.
  defp module_alias?(atom), do: String.starts_with?(Atom.to_string(atom), "Elixir.")

  defp fail!(module, message) do
    raise ArgumentError, "use AsyncApiSpex.Schema in #{inspect(module)}: #{message}"
  end
end
