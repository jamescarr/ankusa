defmodule Ankusa.SDK.ClaimRef do
  @moduledoc """
  A parsed claim-check ref: `urn:ankusa:claim:v1:<tenant>:<claim_id>` or the
  signed, expiring `urn:ankusa:claim:v2:<tenant>:<claim_id>:<exp_unix>:<signature>`,
  the string a queue message carries in its `"claim"` field once its body
  outgrows the sink's `inline_max_bytes` (see `Ankusa.SDK.Message`).

  The claim id is a Crockford base32 ULID: 26 characters, no `I`, `L`, `O` or
  `U`, and a first character in `0`..`7` so the timestamp stays in range.
  `exp_unix` is a decimal of 1-12 digits with no leading zero and `signature`
  43 unpadded base64url characters. The gateway checks the signature and the
  expiry; the SDK only carries them in the path.
  """

  alias Ankusa.SDK.InvalidClaimRefError

  @type t :: %__MODULE__{tenant_id: String.t(), claim_id: String.t(), path: String.t()}

  @pattern ~r/\Aurn:ankusa:claim:v1:([A-Za-z0-9_-]{1,64}):([0-7][0-9A-HJKMNP-TV-Z]{25})\z/
  # `x` mode, to keep the segments readable: whitespace here is not part of the ref.
  @pattern_v2 ~r/\A urn:ankusa:claim:v2:
                 ([A-Za-z0-9_-]{1,64}):
                 ([0-7][0-9A-HJKMNP-TV-Z]{25}):
                 ([1-9][0-9]{0,11}):
                 ([A-Za-z0-9_-]{43}) \z/x

  defstruct [:tenant_id, :claim_id, :path]

  @doc """
  Parse a ref, or return an `Ankusa.SDK.InvalidClaimRefError`.

  The gateway path a ref maps to is `/v1/claims/<tenant>/<claim_id>`, with
  `?exp=<exp_unix>&sig=<signature>` for a v2 ref.
  """
  @spec parse(term()) :: {:ok, t()} | {:error, InvalidClaimRefError.t()}
  def parse(ref) when is_binary(ref) do
    case {Regex.run(@pattern_v2, ref), Regex.run(@pattern, ref)} do
      {[_, tenant_id, claim_id, exp, sig], _} ->
        {:ok,
         %__MODULE__{
           tenant_id: tenant_id,
           claim_id: claim_id,
           path: path(tenant_id, claim_id) <> "?exp=#{exp}&sig=#{sig}"
         }}

      {nil, [_, tenant_id, claim_id]} ->
        {:ok,
         %__MODULE__{tenant_id: tenant_id, claim_id: claim_id, path: path(tenant_id, claim_id)}}

      {nil, nil} ->
        {:error, invalid(ref)}
    end
  end

  def parse(ref), do: {:error, invalid(ref)}

  defp path(tenant_id, claim_id), do: "/v1/claims/#{tenant_id}/#{claim_id}"

  defp invalid(ref) do
    %InvalidClaimRefError{message: "invalid claim ref: #{inspect(ref)}"}
  end
end
