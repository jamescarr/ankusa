defmodule Hook.DedupKey do
  @moduledoc """
  Extract the provider event id used for idempotency, e.g. Stripe `evt_...`,
  GitHub `X-GitHub-Delivery`, or a JSON path. The `(tenant_id, source_id,
  dedup_key)` triple is unique in the WAL; duplicates still get a `2xx`.
  """

  alias Hook.Envelope

  @doc """
  Return `{:ok, key}` with the extracted idempotency key, or `:none` when the
  envelope carries no usable key (in which case dedup is skipped for it).
  """
  @callback extract(Envelope.t(), opts :: keyword()) :: {:ok, String.t()} | :none
end
