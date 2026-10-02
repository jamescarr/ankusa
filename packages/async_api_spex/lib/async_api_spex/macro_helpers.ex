defmodule AsyncApiSpex.MacroHelpers do
  @moduledoc false
  # Compile-time helpers shared by the `use AsyncApiSpex.Schema` and
  # `use AsyncApiSpex.Message` macros.

  @component_name ~r/\A[A-Za-z0-9_.-]+\z/

  @doc false
  @spec expand_component_name!(keyword(), String.t(), Macro.Env.t()) :: String.t()
  def expand_component_name!(opts, context, caller) do
    case Keyword.fetch(opts, :name) do
      :error ->
        raise ArgumentError, "#{context} requires a :name option"

      {:ok, ast} ->
        case Macro.expand(ast, caller) do
          name when is_binary(name) ->
            unless Regex.match?(@component_name, name) do
              raise ArgumentError,
                    "#{context} :name must match #{inspect(@component_name)}, got: #{inspect(name)}"
            end

            name

          other ->
            raise ArgumentError,
                  "#{context} :name must be a string literal, got: #{Macro.to_string(other)}"
        end
    end
  end

  @doc false
  @spec validate_schema!(Macro.t(), String.t(), Macro.Env.t()) :: :ok
  def validate_schema!(ast, context, caller) do
    case ast do
      {:%{}, _, _} ->
        value = ast |> Code.eval_quoted([], caller) |> elem(0)

        unless is_map(value) do
          raise ArgumentError, "#{context} :schema must be a map, got: #{inspect(value)}"
        end

        :ok

      other ->
        case Macro.expand(other, caller) do
          value when is_map(value) ->
            :ok

          value when is_binary(value) or is_list(value) or is_number(value) or is_atom(value) ->
            raise ArgumentError, "#{context} :schema must be a map, got: #{inspect(value)}"

          _ ->
            :ok
        end
    end
  end
end
