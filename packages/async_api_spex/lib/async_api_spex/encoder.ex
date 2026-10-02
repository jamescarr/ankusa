defmodule AsyncApiSpex.Encoder do
  @moduledoc """
  Converts an `AsyncApiSpex.Document` into a JSON-encodable map.

  The document is resolved first. Struct field names become lowerCamel JSON
  keys, `nil`, empty maps, and empty lists are omitted, `extensions` are merged
  into their owning object, and an operation's `action` is written as a string.
  Plain maps and lists — AsyncAPI Schema Objects and bindings — are emitted
  with their keys unchanged apart from atoms becoming strings.
  """

  alias AsyncApiSpex.{Document, Reference}

  @doc """
  Encodes a document as a map. Calls `AsyncApiSpex.resolve/1` first.
  """
  @spec to_map(Document.t()) :: map()
  def to_map(%Document{} = doc) do
    doc |> AsyncApiSpex.Resolver.resolve() |> convert()
  end

  @doc """
  Encodes a document as a JSON string.
  """
  @spec encode!(Document.t()) :: binary()
  def encode!(%Document{} = doc), do: doc |> to_map() |> JSON.encode!()

  defp convert(%Reference{ref: ref}), do: %{"$ref" => ref}
  defp convert(%{__struct__: _} = struct), do: convert_struct(struct)
  defp convert(list) when is_list(list), do: Enum.map(list, &convert/1)
  defp convert(map) when is_map(map), do: convert_plain(map)

  defp convert(atom) when is_atom(atom) and atom not in [nil, true, false],
    do: Atom.to_string(atom)

  defp convert(other), do: other

  defp convert_struct(struct) do
    {extensions, fields} = struct |> Map.from_struct() |> Map.pop(:extensions, %{})

    body =
      Enum.reduce(fields, %{}, fn {key, value}, out ->
        put_field(out, key, convert_field(key, value))
      end)

    Enum.reduce(extensions, body, fn {key, value}, out ->
      Map.put(out, plain_key(key), convert(value))
    end)
  end

  defp convert_field(:action, value) when is_atom(value) and value not in [nil, true, false],
    do: Atom.to_string(value)

  defp convert_field(_key, value), do: convert(value)

  defp put_field(out, _key, nil), do: out
  defp put_field(out, _key, []), do: out

  defp put_field(out, key, value) when is_map(value) do
    if map_size(value) == 0, do: out, else: Map.put(out, camel(key), value)
  end

  defp put_field(out, key, value), do: Map.put(out, camel(key), value)

  # Plain maps: keys are not camel-cased, only atoms become strings.
  defp convert_plain(map) do
    Map.new(map, fn {key, value} -> {plain_key(key), convert(value)} end)
  end

  defp plain_key(key) when is_binary(key), do: key
  defp plain_key(key) when is_atom(key), do: Atom.to_string(key)
  defp plain_key(key), do: key

  defp camel(key) when is_atom(key), do: key |> Atom.to_string() |> camelize()
  defp camel(key) when is_binary(key), do: camelize(key)

  defp camelize(string) do
    case String.split(string, "_") do
      [first | rest] -> first <> Enum.map_join(rest, "", &String.capitalize/1)
    end
  end
end
