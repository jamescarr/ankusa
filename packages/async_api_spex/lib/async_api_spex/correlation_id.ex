defmodule AsyncApiSpex.CorrelationId do
  @moduledoc """
  Describes where a correlation identifier lives inside a message.
  """

  defstruct description: nil, location: nil, extensions: %{}

  @type t :: %__MODULE__{
          description: String.t() | nil,
          location: String.t() | nil,
          extensions: map()
        }
end
