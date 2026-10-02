defmodule AsyncApiSpex.Spec do
  @moduledoc """
  Behaviour for a module that returns a static AsyncAPI document.

  `AsyncApiSpex.Plug.RenderSpec` and `mix async_api_spex.gen` both call
  `spec/0`. Applications whose channels are known only at runtime do not
  implement this behaviour; they build the `AsyncApiSpex.Document` themselves.
  """

  @doc "Returns the AsyncAPI document to serve or serialize."
  @callback spec() :: AsyncApiSpex.Document.t()
end
