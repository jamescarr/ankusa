defmodule Hook.Verifier.GitHub do
  @moduledoc """
  Verifier for GitHub webhook signatures.

  The `X-Hub-Signature-256` header is `"sha256=" <> hex`. The secret is used as
  the raw HMAC key over the exact request body. Compared constant-time against
  `"sha256=" <> lowercase_hex(hmac_sha256(secret, body))`.
  """

  @behaviour Hook.Verifier

  alias Hook.Envelope
  alias Hook.Verifier

  @impl true
  @spec verify(Envelope.t(), keyword()) :: :ok | {:error, term()}
  def verify(%Envelope{} = env, opts) do
    case Envelope.header(env, "X-Hub-Signature-256") do
      nil ->
        {:error, :missing_signature}

      header ->
        secret = Keyword.get(opts, :secret, "")

        expected =
          "sha256=" <> Base.encode16(:crypto.mac(:hmac, :sha256, secret, env.body), case: :lower)

        if Verifier.secure_compare(header, expected) do
          :ok
        else
          {:error, :no_match}
        end
    end
  end
end
