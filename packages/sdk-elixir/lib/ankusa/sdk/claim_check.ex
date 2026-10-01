defmodule Ankusa.SDK.ClaimCheck do
  @moduledoc """
  Redeem claim-check refs against a deployment's claim-check gateway (the
  `:claim_check` role, `claim_check.port`, default `4001`).

  The gateway does no authentication or authorization (see
  [`docs/claim-check.md`](https://github.com/jamescarr/ankusa/blob/main/docs/claim-check.md)),
  and it does not verify integrity itself: `redeem/3` fetches the bytes and
  checks them against the message's `sha256` before returning them, so
  tampering in transit — or a claim object that has since been overwritten —
  surfaces as `Ankusa.SDK.ClaimIntegrityError` rather than as someone else's
  data.

  Every failure carries `retryable`, so a consumer needs exactly one bit to
  decide dead-letter (`false`) or retry (`true`):

  | Error | `retryable` | Cause |
  | --- | --- | --- |
  | `Ankusa.SDK.InvalidClaimRefError` | `false` | bad ref, or `sha256` not 64-char lowercase hex |
  | `Ankusa.SDK.ClaimNotFoundError` | `false` | gateway `404`: expired by retention, or never written |
  | `Ankusa.SDK.ClaimRejectedError` | `false` | gateway `4xx` other than `404` |
  | `Ankusa.SDK.ClaimIntegrityError` | `false` | sha256 of the returned bytes doesn't match |
  | `Ankusa.SDK.ClaimCheckUnavailableError` | `true` | gateway unreachable, or answered anything else |

  ```elixir
  client = Ankusa.SDK.ClaimCheck.new("http://localhost:4001")
  {:ok, bytes} = Ankusa.SDK.ClaimCheck.redeem(client, message.claim, message.sha256)
  ```
  """

  alias Ankusa.SDK.{ClaimRef, HTTP}

  alias Ankusa.SDK.{
    ClaimCheckUnavailableError,
    ClaimIntegrityError,
    ClaimNotFoundError,
    ClaimRejectedError,
    InvalidClaimRefError
  }

  @sha256_pattern ~r/\A[0-9a-f]{64}\z/

  defstruct [:http]

  @type t :: %__MODULE__{http: term()}

  @doc """
  Build a client for `base_url`.

  Options are `:headers` (a map or `{name, value}` list sent on every request),
  `:timeout_ms` (default `10_000`, applied to connect *and* receive — to receive
  only with a `:finch` pool, which owns its connect options), and
  `:req_options` (transport tuning: `:finch`, `:connect_options`,
  `:pool_timeout`, `:plug`; `:finch` and `:connect_options` are exclusive).
  """
  @spec new(String.t(), keyword()) :: t()
  def new(base_url, opts \\ []), do: %__MODULE__{http: HTTP.new(base_url, opts)}

  @doc """
  Fetch the bytes a claim ref names, verifying them against `sha256`.

  Returns `{:ok, bytes}` or `{:error, error}`. Nothing is requested when `ref`
  or `sha256` is malformed.
  """
  @spec redeem(t(), term(), term()) :: {:ok, binary()} | {:error, Exception.t()}
  def redeem(%__MODULE__{http: http}, ref, sha256) do
    with {:ok, parsed} <- ClaimRef.parse(ref),
         :ok <- validate_sha256(sha256) do
      case HTTP.request(http, :get, parsed.path) do
        {:ok, %{status: 200, body: body}} ->
          verify(parsed, sha256, body)

        {:ok, %{status: 404}} ->
          {:error,
           %ClaimNotFoundError{
             message: "claim not found: #{parsed.tenant_id}/#{parsed.claim_id}"
           }}

        {:ok, %{status: status, body: body}} when status >= 400 and status <= 499 ->
          {:error,
           %ClaimRejectedError{
             message:
               "claim-check rejected redeem (#{status}): #{inspect(HTTP.error_body(body))}",
             status: status,
             body: HTTP.error_body(body)
           }}

        {:ok, %{status: status, body: body}} ->
          {:error,
           unavailable(
             "claim-check gateway error (#{status}): #{inspect(HTTP.error_body(body))}",
             {:status, status}
           )}

        {:error, reason} ->
          {:error, unavailable("claim-check gateway unreachable: #{inspect(reason)}", reason)}
      end
    end
  end

  @doc """
  Liveness probe: `GET /health`.

  A `200` with a JSON body succeeds; anything else — a non-200 status, a
  transport error, or a non-JSON body — is a
  `Ankusa.SDK.ClaimCheckUnavailableError`.
  """
  @spec health(t()) :: {:ok, term()} | {:error, ClaimCheckUnavailableError.t()}
  def health(%__MODULE__{http: http}) do
    case HTTP.request(http, :get, "/health") do
      {:ok, %{status: 200, body: body}} ->
        case HTTP.decode_json(body) do
          {:ok, data} ->
            {:ok, data}

          :error ->
            {:error,
             unavailable(
               "claim-check gateway health check returned a non-JSON body (200)",
               :invalid_json
             )}
        end

      {:ok, %{status: status}} ->
        {:error,
         unavailable("claim-check gateway health check failed (#{status})", {:status, status})}

      {:error, reason} ->
        {:error, unavailable("claim-check gateway unreachable: #{inspect(reason)}", reason)}
    end
  end

  defp validate_sha256(sha256) when is_binary(sha256) do
    if Regex.match?(@sha256_pattern, sha256) do
      :ok
    else
      {:error, %InvalidClaimRefError{message: "invalid claim sha256: #{inspect(sha256)}"}}
    end
  end

  defp validate_sha256(sha256) do
    {:error, %InvalidClaimRefError{message: "invalid claim sha256: #{inspect(sha256)}"}}
  end

  defp verify(parsed, sha256, body) do
    digest = :crypto.hash(:sha256, body) |> Base.encode16(case: :lower)

    if digest == sha256 do
      {:ok, body}
    else
      {:error,
       %ClaimIntegrityError{
         message: "claim sha256 mismatch for #{parsed.tenant_id}/#{parsed.claim_id}"
       }}
    end
  end

  defp unavailable(message, reason) do
    %ClaimCheckUnavailableError{message: message, reason: reason}
  end
end
