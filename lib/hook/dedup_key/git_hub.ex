defmodule Hook.DedupKey.GitHub do
  @moduledoc """
  Extracts the GitHub delivery id from the `x-github-delivery` request header.
  """

  @behaviour Hook.DedupKey

  alias Hook.DedupKey.Rules
  alias Hook.Envelope

  @impl true
  @spec extract(Envelope.t(), keyword()) :: {:ok, String.t()} | :none
  def extract(%Envelope{} = env, _opts) do
    Rules.extract(env, header: "x-github-delivery")
  end
end
