defmodule AnkusaGatewayServer.Schema do
  @moduledoc """
  The typed accessors of the YAML walker both images share
  (`AnkusaServer.Config`, `AnkusaGatewayServer.Config`): a section of known
  keys, a string, an integer (a digit string is one), a boolean, an enum, a
  list or comma-separated string. Every failure raises
  `AnkusaGatewayServer.ConfigError` with the dotted path of the key.
  """

  alias AnkusaGatewayServer.ConfigError

  # ── schema helpers ──────────────────────────────────────────────────────────

  # A nested mapping of known keys. nil — absent, or present with every child
  # commented out — is an empty section.
  def section!(parent, key, allowed, path), do: section!(parent[key], allowed, path ++ [key])

  def section!(value, allowed, path) do
    section = expect_map(value, path)
    check_keys!(section, allowed, path)
    section
  end

  def check_keys!(map, allowed, path) do
    Enum.each(map, fn {key, _value} ->
      unless key in allowed do
        raise ConfigError, message: "#{render_path(path)}: unknown key \"#{key}\""
      end
    end)
  end

  def expect_map(nil, _path), do: %{}

  def expect_map(value, _path) when is_map(value), do: value

  def expect_map(value, path),
    do: raise(ConfigError, message: type_error(path, "a mapping", value))

  def expect_string(value, _path) when is_binary(value), do: value

  def expect_string(value, path),
    do: raise(ConfigError, message: type_error(path, "a string", value))

  def required_string!(map, key, path) do
    case Map.fetch(map, key) do
      {:ok, value} ->
        expect_string(value, path ++ [key])

      :error ->
        raise ConfigError, message: "#{render_path(path)}: missing required key \"#{key}\""
    end
  end

  def string_opt(map, key, path) do
    case map[key] do
      nil -> nil
      value -> expect_string(value, path ++ [key])
    end
  end

  def int_opt(map, key, path) do
    case map[key] do
      nil -> nil
      # Env overrides and YAML quoted values both arrive as strings; a digit
      # string is an integer as far as an operator is concerned.
      value when is_integer(value) -> value
      value when is_binary(value) -> parse_int!(value, path ++ [key])
      value -> raise ConfigError, message: type_error(path ++ [key], "an integer", value)
    end
  end

  # Like `int_opt/3`, for keys that take fractions (`quarantine.rate`).
  def number_opt(map, key, path) do
    case map[key] do
      nil -> nil
      value when is_number(value) -> value
      value when is_binary(value) -> parse_number!(value, path ++ [key])
      value -> raise ConfigError, message: type_error(path ++ [key], "a number", value)
    end
  end

  def parse_int!(value, path) do
    case Integer.parse(value) do
      {int, ""} -> int
      _ -> raise ConfigError, message: type_error(path, "an integer", value)
    end
  end

  # A required integer, where absent and explicit-nil are both "you didn't say".
  def required_int!(map, key, path) do
    case map[key] do
      nil -> raise ConfigError, message: "#{render_path(path)}: missing required key \"#{key}\""
      _value -> int_opt(map, key, path)
    end
  end

  def required_number!(map, key, path) do
    case map[key] do
      nil ->
        raise ConfigError, message: "#{render_path(path)}: missing required key \"#{key}\""

      value when is_number(value) ->
        value

      # Quoted YAML and `${VAR}` interpolation both arrive as strings.
      value when is_binary(value) ->
        parse_number!(value, path ++ [key])

      value ->
        raise ConfigError, message: type_error(path ++ [key], "a number", value)
    end
  end

  # Integer first, so `"100"` stays an integer; then float, so `"12.5"` does
  # not come back as `12` with a trailing ".5" nobody checked.
  def parse_number!(value, path) do
    case Integer.parse(value) do
      {int, ""} ->
        int

      _ ->
        case Float.parse(value) do
          {float, ""} -> float
          _ -> raise ConfigError, message: type_error(path, "a number", value)
        end
    end
  end

  def bool_opt(map, key, path) do
    case Map.fetch(map, key) do
      :error -> nil
      {:ok, value} -> bool!(value, path ++ [key])
    end
  end

  def bool!(value, _path) when is_boolean(value), do: value

  def bool!(value, path) when is_binary(value) do
    case String.downcase(value) do
      "true" -> true
      "false" -> false
      _ -> raise ConfigError, message: type_error(path, "a boolean", value)
    end
  end

  def bool!(value, path), do: raise(ConfigError, message: type_error(path, "a boolean", value))

  def nullable_bool_opt(map, key, path) do
    if map[key] == nil, do: nil, else: bool_opt(map, key, path)
  end

  def enum!(value, allowed, path) do
    if value in allowed do
      value
    else
      raise ConfigError,
        message:
          "#{render_path(path)}: unknown value #{inspect(value)}; " <>
            "expected one of #{Enum.join(allowed, ", ")}"
    end
  end

  # The string is checked against a closed list first (`enum!/3`), so the atoms are a fixed set.
  # sobelow_skip ["DOS.StringToAtom"]
  def atom_enum_opt(map, key, allowed, path) do
    case map[key] do
      nil -> nil
      value -> String.to_atom(enum!(value, allowed, path ++ [key]))
    end
  end

  # A list of strings, or one comma-separated string: the form a single env var
  # (`ANKUSA_ROLES`, `"${KAFKA_BROKERS}"`) can carry.
  def string_list!(value, path) do
    strings =
      case value do
        list when is_list(list) ->
          Enum.map(list, &expect_string(&1, path))

        string when is_binary(string) ->
          string |> String.split(",") |> Enum.map(&String.trim/1) |> Enum.reject(&(&1 == ""))

        other ->
          raise ConfigError,
            message: type_error(path, "a list or a comma-separated string", other)
      end

    if strings == [], do: raise(ConfigError, message: "#{render_path(path)}: must not be empty")
    strings
  end

  def type_error(path, expected, value) do
    "#{render_path(path)}: expected #{expected}, got #{inspect(value)}"
  end

  @doc false
  @spec render_path([term()]) :: String.t()
  def render_path([]), do: "config"

  def render_path(segments) do
    Enum.reduce(segments, "", fn
      index, "" when is_integer(index) -> "[#{index}]"
      index, acc when is_integer(index) -> acc <> "[#{index}]"
      segment, "" -> to_string(segment)
      segment, acc -> acc <> "." <> to_string(segment)
    end)
  end

  def put_opt(kw, _key, nil), do: kw

  def put_opt(kw, key, value), do: kw ++ [{key, value}]

  def put_map(map, _key, nil), do: map

  def put_map(map, key, value), do: Map.put(map, key, value)
end
