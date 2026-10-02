defmodule AsyncApiSpex.Channel do
  @moduledoc """
  A channel: a topic, exchange and routing key, subject, or any other address a
  message is sent to or received from.

  `address` is `nil` when the address is computed at runtime and cannot be
  described statically; in that case `parameters` must be empty. `messages` maps
  a channel-local key to a message, which may be a `%AsyncApiSpex.Message{}`, a
  `%AsyncApiSpex.Reference{}`, or a module that exports `__async_api_message__/0`
  (moved into `components.messages` by `AsyncApiSpex.resolve/1`).
  """

  defstruct address: nil,
            title: nil,
            summary: nil,
            description: nil,
            servers: nil,
            messages: %{},
            parameters: %{},
            tags: nil,
            bindings: nil,
            extensions: %{}

  @type t :: %__MODULE__{
          address: String.t() | nil,
          title: String.t() | nil,
          summary: String.t() | nil,
          description: String.t() | nil,
          servers: [AsyncApiSpex.Reference.t()] | nil,
          messages: %{
            optional(String.t()) =>
              AsyncApiSpex.Message.t() | AsyncApiSpex.Reference.t() | module()
          },
          parameters: %{optional(String.t()) => AsyncApiSpex.Parameter.t()},
          tags: [AsyncApiSpex.Tag.t()] | nil,
          bindings: map() | nil,
          extensions: map()
        }
end
