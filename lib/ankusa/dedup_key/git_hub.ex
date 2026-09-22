defmodule Ankusa.DedupKey.GitHub do
  @moduledoc """
  Extracts the GitHub delivery id from the `x-github-delivery` request header.
  """

  @behaviour Ankusa.DedupKey

  alias Ankusa.DedupKey.Rules
  alias Ankusa.Envelope

  @impl true
  @spec extract(Envelope.t(), keyword()) :: {:ok, String.t()} | :none
  def extract(%Envelope{} = env, _opts) do
    Rules.extract(env, header: "x-github-delivery")
  end
end
