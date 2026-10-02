defmodule AsyncApiSpex.Document do
  @moduledoc """
  An AsyncAPI 3.0 document.

  The top-level object. `asyncapi` is fixed to `"3.0.0"`. `servers`, `channels`,
  and `operations` are maps keyed by the id used to reference them from
  elsewhere in the document. `components` holds reusable objects extracted by
  `AsyncApiSpex.resolve/1`.
  """

  alias AsyncApiSpex.Components

  @enforce_keys [:info]
  defstruct asyncapi: "3.0.0",
            id: nil,
            info: nil,
            servers: %{},
            default_content_type: nil,
            channels: %{},
            operations: %{},
            components: %Components{},
            extensions: %{}

  @type t :: %__MODULE__{
          asyncapi: String.t(),
          id: String.t() | nil,
          info: AsyncApiSpex.Info.t() | nil,
          servers: %{optional(String.t()) => AsyncApiSpex.Server.t()},
          default_content_type: String.t() | nil,
          channels: %{optional(String.t()) => AsyncApiSpex.Channel.t()},
          operations: %{optional(String.t()) => AsyncApiSpex.Operation.t()},
          components: Components.t(),
          extensions: map()
        }
end
