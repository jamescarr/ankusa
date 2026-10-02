defmodule AsyncApiSpex.Parameter do
  @moduledoc """
  A parameter used in a channel address expression, such as `{tenant}`.
  """

  defstruct description: nil,
            enum: nil,
            default: nil,
            examples: nil,
            location: nil,
            extensions: %{}

  @type t :: %__MODULE__{
          description: String.t() | nil,
          enum: [term()] | nil,
          default: term(),
          examples: [term()] | nil,
          location: String.t() | nil,
          extensions: map()
        }
end
