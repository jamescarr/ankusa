defmodule AsyncApiSpex.Info do
  @moduledoc """
  Metadata about the API: its title and version, plus optional descriptive and
  legal fields.
  """

  defstruct title: nil,
            version: nil,
            description: nil,
            terms_of_service: nil,
            contact: nil,
            license: nil,
            tags: nil,
            external_docs: nil,
            extensions: %{}

  @type t :: %__MODULE__{
          title: String.t() | nil,
          version: String.t() | nil,
          description: String.t() | nil,
          terms_of_service: String.t() | nil,
          contact: map() | nil,
          license: map() | nil,
          tags: [AsyncApiSpex.Tag.t()] | nil,
          external_docs: map() | nil,
          extensions: map()
        }
end
