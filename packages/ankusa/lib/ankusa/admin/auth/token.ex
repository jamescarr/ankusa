defmodule Ankusa.Admin.Auth.Token do
  @moduledoc """
  Static bearer tokens (`mode: :token`), for development, CI and scripts. Use
  `Ankusa.Admin.Auth.JWT` or `Ankusa.Admin.Auth.MTLS` for people and for
  anything that must be revocable.

  The configuration holds each token's SHA-256, never the token:

      tokens: [
        %{id: "ci", sha256: "<hex of sha256(token)>", scopes: ["dlq:read", "tenant:*"]}
      ]

  Generate one with `openssl rand -hex 32` and hash it with
  `printf %s "$TOKEN" | shasum -a 256`. A token is as strong as its entropy:
  hash a random 32-byte value, not a password. The presented token is hashed
  and compared with every configured hash in constant time, so neither which
  entry matched nor how many exist shows in the response time.
  """

  @behaviour Ankusa.Admin.Auth

  alias Ankusa.Admin.Auth
  alias Ankusa.Principal

  @impl true
  def authenticate(conn, %{config: %{tokens: tokens}}) do
    with {:ok, token} <- Auth.bearer(conn),
         %{id: id, scopes: scopes} <- match(tokens, hash(token)) do
      {:ok, Principal.new(id, scopes)}
    else
      _ -> {:error, :unauthenticated}
    end
  end

  @doc "The lowercase hex SHA-256 of `token`: what `sha256:` in the configuration holds."
  @spec hash(binary()) :: String.t()
  def hash(token), do: :crypto.hash(:sha256, token) |> Base.encode16(case: :lower)

  # Every entry is compared, whether or not an earlier one matched.
  defp match(tokens, hash) do
    Enum.reduce(tokens, nil, fn entry, found ->
      if Auth.secure_compare(hash, entry.sha256) and found == nil, do: entry, else: found
    end)
  end
end
