defmodule AsyncApiSpex.Validator do
  @moduledoc """
  Checks a resolved AsyncAPI document against the rules this library relies on.

  `validate/1` resolves the document first, then returns `:ok` or
  `{:error, messages}`. Every message names the JSON path it concerns. Rules
  checked:

    1. `asyncapi` is `"3.0.0"` and `info` has a non-empty `title` and `version`.
    2. Every server has a non-empty `host` and `protocol`.
    3. Server, channel, operation, channel message, and parameter ids match
       `#{inspect(~r/\A[A-Za-z0-9_-]+\z/)}`; component schemas and messages match
       `#{inspect(~r/\A[A-Za-z0-9_.-]+\z/)}`.
    4. A channel's `{name}` address expressions equal its parameter keys; a
       `nil` address has no parameters.
    5. Every channel server reference resolves.
    6. Every operation action is `send` or `receive`, its channel reference
       resolves, and its message references are messages of that channel.
    7. Every component schema and message reference resolves.
    8. Every extension key starts with `x-`.
  """

  alias AsyncApiSpex.{Channel, Document, Operation, Reference}

  @id ~r/\A[A-Za-z0-9_-]+\z/
  @component_id ~r/\A[A-Za-z0-9_.-]+\z/
  @expression ~r/\{([^}]*)\}/

  @doc """
  Validates a document, resolving it first.
  """
  @spec validate(Document.t()) :: :ok | {:error, [String.t()]}
  def validate(%Document{} = doc) do
    doc = AsyncApiSpex.Resolver.resolve(doc)

    errors =
      []
      |> check_version(doc)
      |> check_info(doc)
      |> check_servers(doc)
      |> check_ids(doc)
      |> check_addresses(doc)
      |> check_channel_servers(doc)
      |> check_operations(doc)
      |> check_component_refs(doc)
      |> check_extensions(doc)
      |> Enum.reverse()

    case errors do
      [] -> :ok
      errors -> {:error, errors}
    end
  end

  defp error(errors, message), do: [message | errors]

  defp check_version(errors, %Document{asyncapi: "3.0.0"}), do: errors

  defp check_version(errors, %Document{asyncapi: other}),
    do: error(errors, "asyncapi: must be \"3.0.0\", got: #{inspect(other)}")

  defp check_info(errors, %Document{info: nil}), do: error(errors, "info: is required")

  defp check_info(errors, %Document{info: info}) do
    errors =
      if non_empty_string?(info.title),
        do: errors,
        else: error(errors, "info.title: must be a non-empty string")

    if non_empty_string?(info.version),
      do: errors,
      else: error(errors, "info.version: must be a non-empty string")
  end

  defp check_servers(errors, %Document{servers: servers}) do
    Enum.reduce(sorted(servers), errors, fn {id, server}, errors ->
      errors =
        if non_empty_string?(server.host),
          do: errors,
          else: error(errors, "servers.#{id}.host: must be a non-empty string")

      if non_empty_string?(server.protocol),
        do: errors,
        else: error(errors, "servers.#{id}.protocol: must be a non-empty string")
    end)
  end

  defp check_ids(errors, %Document{} = doc) do
    errors =
      Enum.reduce(sorted(doc.servers), errors, fn {id, _server}, errors ->
        check_key(errors, "servers", id, @id)
      end)

    errors =
      Enum.reduce(sorted(doc.channels), errors, fn {id, channel}, errors ->
        errors = check_key(errors, "channels", id, @id)
        errors = check_keys(errors, "channels.#{id}.messages", channel.messages, @id)
        check_keys(errors, "channels.#{id}.parameters", channel.parameters, @id)
      end)

    errors =
      Enum.reduce(sorted(doc.operations), errors, fn {id, _operation}, errors ->
        check_key(errors, "operations", id, @id)
      end)

    errors = check_keys(errors, "components.schemas", doc.components.schemas, @component_id)
    check_keys(errors, "components.messages", doc.components.messages, @component_id)
  end

  defp check_keys(errors, path, map, regex) do
    Enum.reduce(sorted(map || %{}), errors, fn {id, _value}, errors ->
      check_key(errors, path, id, regex)
    end)
  end

  defp check_key(errors, path, id, regex) do
    if Regex.match?(regex, to_string(id)) do
      errors
    else
      error(errors, "#{path}.#{id}: key must match #{inspect(regex)}")
    end
  end

  defp check_addresses(errors, %Document{channels: channels}) do
    Enum.reduce(sorted(channels), errors, fn {id, channel}, errors ->
      check_address(errors, id, channel)
    end)
  end

  defp check_address(errors, id, %Channel{address: nil, parameters: parameters}) do
    if map_size(parameters || %{}) == 0 do
      errors
    else
      error(errors, "channels.#{id}.parameters: must be empty when address is null")
    end
  end

  defp check_address(errors, id, %Channel{address: address, parameters: parameters}) do
    expressions = expressions(address)
    keys = MapSet.new(parameters || %{}, &to_string(elem(&1, 0)))

    if MapSet.equal?(expressions, keys) do
      errors
    else
      error(
        errors,
        "channels.#{id}.address: expressions #{inspect(MapSet.to_list(expressions))} " <>
          "do not match parameters #{inspect(MapSet.to_list(keys))}"
      )
    end
  end

  defp check_channel_servers(errors, %Document{servers: servers, channels: channels}) do
    Enum.reduce(sorted(channels), errors, fn {id, channel}, errors ->
      Enum.reduce(channel.servers || [], errors, fn
        %Reference{ref: "#/servers/" <> server_id}, errors ->
          if Map.has_key?(servers, server_id) do
            errors
          else
            error(errors, "channels.#{id}.servers: references missing server #{server_id}")
          end

        ref, errors ->
          error(errors, "channels.#{id}.servers: #{inspect(ref)} is not #/servers/<id>")
      end)
    end)
  end

  defp check_operations(errors, %Document{channels: channels, operations: operations}) do
    Enum.reduce(sorted(operations), errors, fn {id, operation}, errors ->
      check_operation(errors, id, operation, channels)
    end)
  end

  defp check_operation(errors, id, %Operation{} = operation, channels) do
    errors = check_action(errors, id, operation.action)
    channel_id = referenced_channel_id(operation.channel)

    cond do
      channel_id == nil ->
        error(errors, "operations.#{id}.channel: must reference #/channels/<id>")

      not Map.has_key?(channels, channel_id) ->
        error(errors, "operations.#{id}.channel: references missing channel #{channel_id}")

      true ->
        Enum.reduce(operation.messages || [], errors, fn ref, errors ->
          check_operation_message(errors, id, ref, channel_id, channels)
        end)
    end
  end

  defp check_action(errors, _id, action) when action in [:send, :receive], do: errors

  defp check_action(errors, id, action),
    do:
      error(errors, "operations.#{id}.action: must be :send or :receive, got: #{inspect(action)}")

  defp check_operation_message(errors, id, %Reference{ref: ref}, channel_id, channels) do
    prefix = "#/channels/#{channel_id}/messages/"

    with true <- String.starts_with?(ref, prefix),
         key = String.replace_prefix(ref, prefix, ""),
         true <- Map.has_key?(channels[channel_id].messages, key) do
      errors
    else
      _ ->
        error(
          errors,
          "operations.#{id}.messages: #{inspect(ref)} is not a message of channel #{channel_id}"
        )
    end
  end

  defp check_operation_message(errors, id, ref, channel_id, _channels) do
    error(
      errors,
      "operations.#{id}.messages: #{inspect(ref)} is not a message of channel #{channel_id}"
    )
  end

  defp check_component_refs(errors, doc) do
    doc
    |> AsyncApiSpex.Encoder.to_map()
    |> collect_refs([])
    |> Enum.sort()
    |> Enum.reduce(errors, fn ref, errors ->
      case ref do
        "#/components/schemas/" <> name ->
          if Map.has_key?(doc.components.schemas, name),
            do: errors,
            else: error(errors, "$ref: #{ref} does not resolve")

        "#/components/messages/" <> name ->
          if Map.has_key?(doc.components.messages, name),
            do: errors,
            else: error(errors, "$ref: #{ref} does not resolve")

        _ ->
          errors
      end
    end)
  end

  defp collect_refs(%{"$ref" => ref}, acc) when is_binary(ref), do: [ref | acc]

  defp collect_refs(map, acc) when is_map(map) do
    Enum.reduce(map, acc, fn {_key, value}, acc -> collect_refs(value, acc) end)
  end

  defp collect_refs(list, acc) when is_list(list) do
    Enum.reduce(list, acc, fn value, acc -> collect_refs(value, acc) end)
  end

  defp collect_refs(_other, acc), do: acc

  defp check_extensions(errors, doc), do: walk_extensions(doc, "document", errors)

  defp walk_extensions(%{__struct__: _} = struct, path, errors) do
    errors =
      case Map.fetch(struct, :extensions) do
        {:ok, extensions} when is_map(extensions) ->
          Enum.reduce(Enum.sort(Map.keys(extensions)), errors, fn key, errors ->
            if extension_key?(key) do
              errors
            else
              error(errors, "#{path}.extensions: key #{inspect(key)} must start with \"x-\"")
            end
          end)

        _ ->
          errors
      end

    struct
    |> Map.from_struct()
    |> Enum.reduce(errors, fn {key, value}, errors ->
      walk_extensions(value, "#{path}.#{key}", errors)
    end)
  end

  defp walk_extensions(map, path, errors) when is_map(map) do
    Enum.reduce(map, errors, fn {key, value}, errors ->
      walk_extensions(value, "#{path}.#{key}", errors)
    end)
  end

  defp walk_extensions(list, path, errors) when is_list(list) do
    list
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {value, index}, errors ->
      walk_extensions(value, "#{path}[#{index}]", errors)
    end)
  end

  defp walk_extensions(_other, _path, errors), do: errors

  defp extension_key?(key), do: String.starts_with?(to_string(key), "x-")

  defp expressions(address) do
    @expression
    |> Regex.scan(address, capture: :all_but_first)
    |> List.flatten()
    |> MapSet.new()
  end

  defp referenced_channel_id(%Reference{ref: "#/channels/" <> id}), do: id
  defp referenced_channel_id(_other), do: nil

  defp sorted(map), do: map |> Map.to_list() |> Enum.sort_by(&elem(&1, 0))

  defp non_empty_string?(value), do: is_binary(value) and value != ""
end
