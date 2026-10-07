defmodule AsyncApiSpex.Spec.Builder do
  @moduledoc false
  # Runtime assembly for `use AsyncApiSpex.Spec`: folds every channel module's
  # `__async_api_channel__/0` into one `AsyncApiSpex.Document`.

  alias AsyncApiSpex.{Components, Document, Info, Message}

  @doc """
  Builds the document from the evaluated options of `use AsyncApiSpex.Spec`.
  """
  @spec build(keyword()) :: Document.t()
  def build(opts) do
    %{servers: servers, channels: channels, operations: operations, messages: messages} =
      opts
      |> channel_modules()
      |> Enum.reduce(
        %{servers: %{}, channels: %{}, operations: %{}, messages: %{}},
        &add_channel/2
      )

    struct!(
      Document,
      [
        info: struct!(Info, Keyword.fetch!(opts, :info)),
        servers: strip_owners(servers),
        channels: strip_owners(channels),
        operations: operations,
        components: %Components{messages: derived_messages(messages)}
      ] ++ present(Keyword.take(opts, [:id, :default_content_type, :extensions]))
    )
  end

  defp channel_modules(opts) do
    case Keyword.fetch(opts, :channels) do
      {:ok, modules} ->
        Enum.each(modules, fn module ->
          unless channel_module?(module) do
            raise ArgumentError, "#{inspect(module)} does not use AsyncApiSpex.Channel"
          end
        end)

        Enum.uniq(modules)

      :error ->
        app = Keyword.fetch!(opts, :otp_app)

        case Application.spec(app, :modules) do
          nil ->
            raise ArgumentError,
                  "application #{inspect(app)} is not loaded; use AsyncApiSpex.Spec " <>
                    ":otp_app must name an application in the release"

          modules ->
            modules |> Enum.filter(&channel_module?/1) |> Enum.sort()
        end
    end
  end

  defp channel_module?(module),
    do:
      is_atom(module) and Code.ensure_loaded?(module) and
        function_exported?(module, :__async_api_channel__, 0)

  defp add_channel(module, acc) do
    %{
      id: id,
      server: {server_id, server},
      channel: channel,
      operation: {operation_id, operation},
      messages: messages
    } = module.__async_api_channel__()

    case acc.channels do
      %{^id => {other, _channel}} ->
        raise ArgumentError,
              "channel id #{id} is declared by #{inspect(other)} and #{inspect(module)}"

      _ ->
        :ok
    end

    # A `use AsyncApiSpex.Message` module stays on the channel (the resolver
    # moves it into components) but still claims its name here, so it cannot
    # silently replace a derived message of the same name.
    claims =
      Map.to_list(messages) ++
        for {_key, message} <- channel.messages, is_atom(message) do
          {name, _message} = message.__async_api_message__()
          {name, message}
        end

    %{
      servers: share!(acc.servers, "server", server_id, server, module),
      channels: Map.put(acc.channels, id, {module, channel}),
      operations: Map.put(acc.operations, operation_id, operation),
      messages:
        Enum.reduce(claims, acc.messages, fn {name, message}, owned ->
          share!(owned, "message", name, message, module)
        end)
    }
  end

  # Several channel modules may declare the same server or message, but only
  # identically.
  defp share!(owned, kind, id, value, module) do
    case owned do
      %{^id => {_owner, ^value}} ->
        owned

      %{^id => {other, _value}} ->
        raise ArgumentError,
              "#{kind} id #{id} is declared differently by #{inspect(other)} and #{inspect(module)}"

      _ ->
        Map.put(owned, id, {module, value})
    end
  end

  defp strip_owners(map), do: Map.new(map, fn {id, {_module, value}} -> {id, value} end)

  # Only derived messages belong in components; message modules are moved
  # there by `AsyncApiSpex.resolve/1`.
  defp derived_messages(map) do
    for {name, {_module, %Message{} = message}} <- map, into: %{}, do: {name, message}
  end

  defp present(opts), do: Enum.reject(opts, fn {_key, value} -> is_nil(value) end)
end
