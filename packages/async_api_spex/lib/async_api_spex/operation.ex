defmodule AsyncApiSpex.Operation do
  @moduledoc """
  An operation on a channel: a `:send` (the application publishes) or
  `:receive` (the application consumes).
  """

  defstruct action: nil,
            channel: nil,
            messages: nil,
            title: nil,
            summary: nil,
            description: nil,
            tags: nil,
            bindings: nil,
            extensions: %{}

  @type action :: :send | :receive

  @type t :: %__MODULE__{
          action: action() | nil,
          channel: AsyncApiSpex.Reference.t() | nil,
          messages: [AsyncApiSpex.Reference.t()] | nil,
          title: String.t() | nil,
          summary: String.t() | nil,
          description: String.t() | nil,
          tags: [AsyncApiSpex.Tag.t()] | nil,
          bindings: map() | nil,
          extensions: map()
        }
end
