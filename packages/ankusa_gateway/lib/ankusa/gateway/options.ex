defmodule Ankusa.Gateway.Options do
  @moduledoc false
  # The validation helpers `Ankusa.Gateway.Config` and every authenticator and
  # authorizer share, so each message has one shape: it starts with the dotted
  # key (`"gateway.port must be ..."`), and an unknown key is
  # `"unknown Ankusa.Gateway.Config key: <key>.<k>"`.

  @name ~r/\A[a-z][a-z0-9_-]{0,31}\z/

  @doc "A map from a map or a keyword list."
  @spec map!(term(), String.t()) :: map()
  def map!(value, _key) when is_map(value) and not is_struct(value), do: value

  def map!(value, key) when is_list(value) do
    if Keyword.keyword?(value),
      do: Map.new(value),
      else: raise(ArgumentError, "#{key} must be a map or keyword list, got #{inspect(value)}")
  end

  def map!(other, key),
    do: raise(ArgumentError, "#{key} must be a map or keyword list, got #{inspect(other)}")

  @doc "Raise for the first key of `given` that is not in `known`."
  @spec unknown_keys!(map(), [atom()], String.t()) :: :ok
  def unknown_keys!(given, known, key) do
    case Enum.find(Map.keys(given), &(&1 not in known)) do
      nil -> :ok
      k -> raise ArgumentError, "unknown Ankusa.Gateway.Config key: #{key}.#{k}"
    end
  end

  @spec int_in!(term(), Range.t(), String.t()) :: integer()
  def int_in!(value, %Range{first: first, last: last} = range, key) do
    if is_integer(value) and value in range,
      do: value,
      else:
        raise(
          ArgumentError,
          "#{key} must be an integer from #{first} to #{last}, got #{inspect(value)}"
        )
  end

  @spec boolean!(term(), String.t()) :: boolean()
  def boolean!(value, _key) when is_boolean(value), do: value

  def boolean!(other, key),
    do: raise(ArgumentError, "#{key} must be true or false, got #{inspect(other)}")

  @spec string!(term(), String.t()) :: String.t()
  def string!(value, _key) when is_binary(value) and value != "", do: value

  def string!(other, key),
    do: raise(ArgumentError, "#{key} must be a non-empty string, got #{inspect(other)}")

  @doc "A list of strings (empty allowed)."
  @spec string_list!(term(), String.t()) :: [String.t()]
  def string_list!(value, key) when is_list(value) do
    if Enum.all?(value, &is_binary/1),
      do: value,
      else: raise(ArgumentError, "#{key} must be a list of strings, got #{inspect(value)}")
  end

  def string_list!(other, key),
    do: raise(ArgumentError, "#{key} must be a list of strings, got #{inspect(other)}")

  @doc "A name of the configuration (`~r/\\A[a-z][a-z0-9_-]{0,31}\\z/`), as a string."
  @spec name!(term(), String.t()) :: String.t()
  def name!(name, key) when is_atom(name) and not is_nil(name) and not is_boolean(name),
    do: name!(Atom.to_string(name), key)

  def name!(name, key) when is_binary(name) do
    if Regex.match?(@name, name),
      do: name,
      else:
        raise(
          ArgumentError,
          "#{key} must be 1 to 32 lowercase letters, digits, - or _ starting with a letter, " <>
            "got #{inspect(name)}"
        )
  end

  def name!(other, key),
    do: raise(ArgumentError, "#{key} must be a name string, got #{inspect(other)}")

  @doc """
  Scope strings for a static principal (a token entry, a certificate's
  subject): known operation scopes and `tenant:` scopes only, so a typo fails
  at boot instead of granting nothing. Empty is allowed.
  """
  @spec scopes!(term(), String.t()) :: [String.t()]
  def scopes!(scopes, key) do
    scopes = string_list!(scopes, key)

    case Ankusa.Principal.parse_scopes(scopes) do
      {_scopes, _tenants, []} ->
        scopes

      {_scopes, _tenants, unknown} ->
        raise ArgumentError,
              "#{key} has unknown scopes #{inspect(unknown)}; known: " <>
                "#{Enum.join(Ankusa.Principal.scopes(), ", ")}, tenant:<id>, tenant:*"
    end
  end

  @doc "Is `ip` (as `Ankusa.Config.listen_ip!/2` returns it) a loopback address?"
  @spec loopback?(:inet.ip_address()) :: boolean()
  def loopback?({127, _, _, _}), do: true
  def loopback?({0, 0, 0, 0, 0, 0, 0, 1}), do: true
  def loopback?({0, 0, 0, 0, 0, 0xFFFF, high, _low}), do: Bitwise.bsr(high, 8) == 127
  def loopback?(_ip), do: false

  @doc "A keyword list (the transport options of a call that goes out)."
  @spec keyword_list!(term(), String.t()) :: keyword()
  def keyword_list!(value, key) when is_list(value) do
    if Keyword.keyword?(value),
      do: value,
      else: raise(ArgumentError, "#{key} must be a keyword list, got #{inspect(value)}")
  end

  def keyword_list!(other, key),
    do: raise(ArgumentError, "#{key} must be a keyword list, got #{inspect(other)}")

  @doc "A module that is loaded and exports every function of `functions`."
  @spec implements?(term(), [{atom(), arity()}]) :: boolean()
  def implements?(module, functions) when is_atom(module) do
    Code.ensure_loaded?(module) and
      Enum.all?(functions, fn {fun, arity} -> function_exported?(module, fun, arity) end)
  end

  def implements?(_other, _functions), do: false
end
