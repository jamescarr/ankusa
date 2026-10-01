defmodule Ankusa.SDK.ClaimRef do
  @moduledoc """
  A parsed claim-check ref: `urn:ankusa:claim:v1:<tenant>:<claim_id>`, the
  string a queue message carries in its `"claim"` field once its body outgrows
  the sink's `inline_max_bytes` (see `Ankusa.SDK.Message`).

  The claim id is a Crockford base32 ULID: 26 characters, no `I`, `L`, `O` or
  `U`, and a first character in `0`..`7` so the timestamp stays in range.
  """

  alias Ankusa.SDK.InvalidClaimRefError

  @type t :: %__MODULE__{tenant_id: String.t(), claim_id: String.t(), path: String.t()}

  @pattern ~r/\Aurn:ankusa:claim:v1:([A-Za-z0-9_-]{1,64}):([0-7][0-9A-HJKMNP-TV-Z]{25})\z/

  defstruct [:tenant_id, :claim_id, :path]

  @doc """
  Parse a ref, or return an `Ankusa.SDK.InvalidClaimRefError`.

  The gateway path a ref maps to is `/v1/claims/<tenant>/<claim_id>`.
  """
  @spec parse(term()) :: {:ok, t()} | {:error, InvalidClaimRefError.t()}
  def parse(ref) when is_binary(ref) do
    case Regex.run(@pattern, ref) do
      [_, tenant_id, claim_id] ->
        {:ok,
         %__MODULE__{tenant_id: tenant_id, claim_id: claim_id, path: path(tenant_id, claim_id)}}

      nil ->
        {:error, invalid(ref)}
    end
  end

  def parse(ref), do: {:error, invalid(ref)}

  defp path(tenant_id, claim_id), do: "/v1/claims/#{tenant_id}/#{claim_id}"

  defp invalid(ref) do
    %InvalidClaimRefError{message: "invalid claim ref: #{inspect(ref)}"}
  end
end
