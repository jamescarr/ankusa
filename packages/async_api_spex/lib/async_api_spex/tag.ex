defmodule AsyncApiSpex.Tag do
  @moduledoc """
  A named tag attached to a server, channel, operation, or message.
  """

  defstruct name: nil, description: nil, extensions: %{}

  @type t :: %__MODULE__{
          name: String.t() | nil,
          description: String.t() | nil,
          extensions: map()
        }
end
