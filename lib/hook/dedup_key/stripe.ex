defmodule Hook.DedupKey.Stripe do
  @moduledoc """
  Extracts the Stripe event id (`evt_...`) from the JSON body at path `["id"]`.
  """

  @behaviour Hook.DedupKey

  alias Hook.DedupKey.Rules
  alias Hook.Envelope

  @impl true
  @spec extract(Envelope.t(), keyword()) :: {:ok, String.t()} | :none
  def extract(%Envelope{} = env, _opts) do
    Rules.extract(env, json: ["id"])
  end
end
