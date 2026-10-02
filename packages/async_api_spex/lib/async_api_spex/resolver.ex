defmodule AsyncApiSpex.Resolver do
  @moduledoc """
  Extracts reusable modules into `AsyncApiSpex.Components` and replaces their
  uses with `AsyncApiSpex.Reference` structs.

  A channel message that is a module exporting `__async_api_message__/0` moves
  into `components.messages`. Any atom inside a message's `payload`, `headers`,
  or `extensions` (recursively through maps and lists) that is a module
  exporting `__async_api_schema__/0` moves into `components.schemas`. Both are
  replaced with a reference at the point of use.

  Two different modules claiming the same component name raise
  `ArgumentError`. Using the same module twice is fine. `resolve/1` is
  idempotent.
  """

  alias AsyncApiSpex.{Channel, Components, Document, Message, Reference}

  @doc """
  Resolves `document`, returning a new document with component references.

  Raises `ArgumentError` when two different modules claim the same component
  name.
  """
  @spec resolve(Document.t()) :: Document.t()
  def resolve(%Document{} = doc) do
    acc = %{schemas: %{}, messages: %{}, owners: %{}}

    {acc, components} = resolve_components(acc, doc.components || %Components{})
    {acc, channels} = resolve_map(acc, doc.channels || %{}, &resolve_channel/2)

    components = %Components{
      components
      | schemas: Map.merge(components.schemas, acc.schemas),
        messages: Map.merge(components.messages, acc.messages)
    }

    %{doc | channels: channels, components: components}
  end

  defp resolve_components(acc, %Components{} = components) do
    {acc, schemas} = resolve_map(acc, components.schemas, &resolve_value/2)
    {acc, messages} = resolve_map(acc, components.messages, &resolve_component_message/2)
    {acc, %{components | schemas: schemas, messages: messages}}
  end

  defp resolve_map(acc, map, fun) do
    Enum.reduce(map, {acc, %{}}, fn {key, value}, {acc, out} ->
      {acc, value} = fun.(acc, value)
      {acc, Map.put(out, key, value)}
    end)
  end

  defp resolve_channel(acc, %Channel{} = channel) do
    {acc, messages} = resolve_map(acc, channel.messages || %{}, &resolve_channel_message/2)
    {acc, %{channel | messages: messages}}
  end

  defp resolve_channel(acc, other), do: {acc, other}

  defp resolve_channel_message(acc, module) when is_atom(module) do
    if message_module?(module) do
      {name, message} = module.__async_api_message__()
      acc = register!(acc, :message, name, module)
      {acc, message} = resolve_message(acc, message)
      {put_message(acc, name, message), %Reference{ref: "#/components/messages/#{name}"}}
    else
      {acc, module}
    end
  end

  defp resolve_channel_message(acc, %Reference{} = ref), do: {acc, ref}
  defp resolve_channel_message(acc, %Message{} = message), do: resolve_message(acc, message)
  defp resolve_channel_message(acc, other), do: {acc, other}

  defp resolve_component_message(acc, module) when is_atom(module) do
    if message_module?(module) do
      {name, message} = module.__async_api_message__()
      acc = register!(acc, :message, name, module)
      resolve_message(acc, message)
    else
      {acc, module}
    end
  end

  defp resolve_component_message(acc, %Message{} = message), do: resolve_message(acc, message)
  defp resolve_component_message(acc, other), do: {acc, other}

  defp resolve_message(acc, %Message{} = message) do
    {acc, payload} = resolve_value(acc, message.payload)
    {acc, headers} = resolve_value(acc, message.headers)
    {acc, extensions} = resolve_value(acc, message.extensions)
    {acc, %{message | payload: payload, headers: headers, extensions: extensions}}
  end

  defp resolve_value(acc, module) when is_atom(module) and module not in [nil, true, false] do
    if schema_module?(module) do
      {name, schema} = module.__async_api_schema__()
      acc = register!(acc, :schema, name, module)
      {acc, schema} = resolve_value(acc, schema)

      {%{acc | schemas: Map.put(acc.schemas, name, schema)},
       %Reference{ref: "#/components/schemas/#{name}"}}
    else
      {acc, module}
    end
  end

  defp resolve_value(acc, %Reference{} = ref), do: {acc, ref}
  defp resolve_value(acc, %{__struct__: _} = struct), do: {acc, struct}

  defp resolve_value(acc, map) when is_map(map) do
    Enum.reduce(map, {acc, %{}}, fn {key, value}, {acc, out} ->
      {acc, value} = resolve_value(acc, value)
      {acc, Map.put(out, key, value)}
    end)
  end

  defp resolve_value(acc, list) when is_list(list) do
    {acc, list} =
      Enum.reduce(list, {acc, []}, fn value, {acc, out} ->
        {acc, value} = resolve_value(acc, value)
        {acc, out ++ [value]}
      end)

    {acc, list}
  end

  defp resolve_value(acc, other), do: {acc, other}

  defp put_message(acc, name, message),
    do: %{acc | messages: Map.put(acc.messages, name, message)}

  defp register!(acc, kind, name, module) do
    key = {kind, name}

    case acc.owners do
      %{^key => ^module} ->
        acc

      %{^key => other} ->
        raise ArgumentError,
              "component name #{name} is used by #{inspect(other)} and #{inspect(module)}"

      _ ->
        %{acc | owners: Map.put(acc.owners, key, module)}
    end
  end

  defp message_module?(module) do
    Code.ensure_loaded?(module) and function_exported?(module, :__async_api_message__, 0)
  end

  defp schema_module?(module) do
    Code.ensure_loaded?(module) and function_exported?(module, :__async_api_schema__, 0)
  end
end
