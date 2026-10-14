defmodule Ankusa.TestHelpers.TokenTable do
  @moduledoc """
  An `Ankusa.Auth.Verifier` for tests: `opts[:tokens]` maps a token string to its
  claims, any other token is invalid, and the token `"down"` is a verifier that
  cannot decide. Tests of the guard, the audit log and SCIM are about roles and
  records, not about JWTs (`ankusa_oidc` tests those).
  """

  @behaviour Ankusa.Auth.Verifier

  @impl true
  def verify(_instance, "down", _opts), do: {:error, :unavailable}

  def verify(_instance, token, opts) do
    case Map.fetch(Keyword.fetch!(opts, :tokens), token) do
      {:ok, claims} -> {:ok, claims}
      :error -> {:error, :invalid_token}
    end
  end
end
