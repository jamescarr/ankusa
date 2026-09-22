defmodule Hook.Verifier do
  @moduledoc """
  Signature and timestamp verification. HMAC is microseconds, so it runs inline
  on the hot path, before the ack.

  Adapters implement `verify/2` over the raw envelope (the exact bytes matter).
  `opts` carry the per-source secret and any tolerance window.
  """

  alias Hook.Envelope

  @doc """
  Verify an envelope. Return `:ok` to accept, or `{:error, reason}` to trigger
  the source's `on_verify_failure` policy (`:reject | :quarantine | :accept_flag`).
  """
  @callback verify(Envelope.t(), opts :: keyword()) :: :ok | {:error, term()}

  @doc "Constant-time compare of two binaries of equal length."
  @spec secure_compare(binary(), binary()) :: boolean()
  def secure_compare(a, b) when is_binary(a) and is_binary(b) do
    byte_size(a) == byte_size(b) and :crypto.hash_equals(a, b)
  end
end
