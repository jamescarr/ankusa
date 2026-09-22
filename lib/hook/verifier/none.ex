defmodule Hook.Verifier.None do
  @moduledoc """
  No-op verifier: accepts every envelope. Use for sources whose authenticity is
  guaranteed by transport (mTLS, private network) or that are intentionally open.
  """

  @behaviour Hook.Verifier

  alias Hook.Envelope

  @impl true
  @spec verify(Envelope.t(), keyword()) :: :ok
  def verify(%Envelope{}, _opts), do: :ok
end
