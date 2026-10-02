defmodule AsyncApiSpex.Components do
  @moduledoc """
  Reusable objects referenced from elsewhere in the document.

  Keys are the component names; `AsyncApiSpex.resolve/1` fills `schemas` and
  `messages` from `use AsyncApiSpex.Schema` and `use AsyncApiSpex.Message`
  declarations.
  """

  defstruct schemas: %{},
            messages: %{},
            servers: %{},
            channels: %{},
            operations: %{},
            parameters: %{},
            correlation_ids: %{},
            extensions: %{}

  @type t :: %__MODULE__{
          schemas: %{optional(String.t()) => map()},
          messages: %{optional(String.t()) => AsyncApiSpex.Message.t()},
          servers: %{optional(String.t()) => AsyncApiSpex.Server.t()},
          channels: %{optional(String.t()) => AsyncApiSpex.Channel.t()},
          operations: %{optional(String.t()) => AsyncApiSpex.Operation.t()},
          parameters: %{optional(String.t()) => AsyncApiSpex.Parameter.t()},
          correlation_ids: %{optional(String.t()) => AsyncApiSpex.CorrelationId.t()},
          extensions: map()
        }
end
