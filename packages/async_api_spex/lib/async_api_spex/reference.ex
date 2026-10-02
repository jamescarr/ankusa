defmodule AsyncApiSpex.Reference do
  @moduledoc """
  A reference to another object in the document. Encodes as `{"$ref": ref}`.
  """

  @enforce_keys [:ref]
  defstruct ref: nil, extensions: %{}

  @type t :: %__MODULE__{ref: String.t(), extensions: map()}
end
