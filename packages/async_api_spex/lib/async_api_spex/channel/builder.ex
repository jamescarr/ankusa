defmodule AsyncApiSpex.Channel.Builder do
  @moduledoc false
  # Runtime assembly for `use AsyncApiSpex.Channel`. The macro inlines the
  # option expressions into `__async_api_channel__/0`; this module turns the
  # evaluated options into the server, channel, and operation structs.

  alias AsyncApiSpex.{Channel, Message, Operation, Reference, Server}

  @id ~r/\A[A-Za-z0-9_-]+\z/
  @key_chars ~r/[^A-Za-z0-9_-]/

  @type built :: %{
          id: String.t(),
          server: {String.t(), Server.t()},
          channel: Channel.t(),
          operation: {String.t(), Operation.t()},
          messages: %{optional(String.t()) => Message.t()}
        }

  @doc """
  Builds the structs for the channel module `module` from its evaluated options.
  """
  @spec build(module(), keyword()) :: built()
  def build(module, opts) do
    id = Keyword.fetch!(opts, :id)
    action = Keyword.fetch!(opts, :action)

    {server_id, server} = build_server(module, Keyword.fetch!(opts, :server))

    {entries, messages} =
      build_messages(module, Keyword.fetch!(opts, :messages), Keyword.fetch!(opts, :content_type))

    channel =
      struct!(
        Channel,
        [
          address: Keyword.fetch!(opts, :address),
          servers: [%Reference{ref: "#/servers/#{server_id}"}],
          messages: Map.new(entries)
        ] ++ present(Keyword.get(opts, :channel, []))
      )

    operation =
      struct!(
        Operation,
        [
          action: action,
          channel: %Reference{ref: "#/channels/#{id}"},
          messages: for({key, _message} <- entries, do: message_ref(id, key))
        ] ++ present(Keyword.get(opts, :operation, []))
      )

    %{
      id: id,
      server: {server_id, server},
      channel: channel,
      operation: {"#{action}-#{id}", operation},
      messages: messages
    }
  end

  defp build_server(module, server_opts) do
    {id, server_opts} = Keyword.pop(server_opts, :id)

    for key <- [:host, :protocol] do
      unless non_empty_string?(server_opts[key]) do
        raise ArgumentError, "#{inspect(module)}: server #{key} must be a non-empty string"
      end
    end

    id = id || server_opts[:protocol]

    unless is_binary(id) and Regex.match?(@id, id) do
      raise ArgumentError,
            "#{inspect(module)}: server id #{inspect(id)} must match #{inspect(@id)}"
    end

    {id, struct!(Server, present(server_opts))}
  end

  # Returns the channel's `{key, message}` entries and the `components.messages`
  # entries (`%{name => %Message{}}`) derived from schema modules. A message
  # module stays in the channel as the module; `AsyncApiSpex.resolve/1` moves
  # it into `components.messages`.
  defp build_messages(module, messages, content_type) do
    {entries, {_seen, components}} =
      Enum.map_reduce(messages, {%{}, %{}}, fn message, {seen, components} ->
        {name, entry, component} = message_entry(module, message, content_type)
        key = String.replace(name, @key_chars, "_")

        case seen do
          %{^key => other} ->
            raise ArgumentError,
                  "#{inspect(module)}: messages #{inspect(other)} and #{inspect(message)} " <>
                    "both map to channel key #{key}"

          _ ->
            components = if component, do: Map.put(components, name, component), else: components
            {{key, entry}, {Map.put(seen, key, message), components}}
        end
      end)

    {entries, components}
  end

  defp message_entry(module, message, content_type) do
    cond do
      exports?(message, :__async_api_message__) ->
        {name, _message} = message.__async_api_message__()
        {name, message, nil}

      exports?(message, :__async_api_schema__) ->
        {name, schema} = message.__async_api_schema__()

        component = %Message{
          name: name,
          title: schema["title"],
          content_type: content_type,
          payload: message
        }

        {name, %Reference{ref: "#/components/messages/#{name}"}, component}

      true ->
        raise ArgumentError,
              "#{inspect(module)}: message #{inspect(message)} does not use " <>
                "AsyncApiSpex.Schema or AsyncApiSpex.Message"
    end
  end

  defp exports?(module, function),
    do:
      is_atom(module) and Code.ensure_loaded?(module) and function_exported?(module, function, 0)

  defp message_ref(channel_id, key),
    do: %Reference{ref: "#/channels/#{channel_id}/messages/#{key}"}

  # Options left unset by the macro arrive as nil; they keep the struct default.
  defp present(opts), do: Enum.reject(opts, fn {_key, value} -> is_nil(value) end)

  defp non_empty_string?(value), do: is_binary(value) and value != ""
end
