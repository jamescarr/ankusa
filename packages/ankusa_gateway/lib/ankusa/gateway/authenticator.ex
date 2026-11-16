defmodule Ankusa.Gateway.Authenticator do
  @moduledoc """
  One link of the gateway's authentication chain (`gateway.auth.authenticators`).

  The chain is tried in order. Each authenticator answers for the credential
  form it reads and steps aside for any other:

    * `{:ok, principal, expires_at}` — authenticated; `expires_at` is the unix
      second the credential stops being valid, or `nil` when it has no expiry.
      Nothing after it runs.
    * `:skip` — the request carries no credential in this authenticator's form
      (or one it does not own, like a bearer token no `token` entry matches):
      the next authenticator is asked.
    * `{:error, :unauthenticated}` — the credential is in this authenticator's
      form and is not valid: the chain stops with `401`.
    * `{:error, :link_expired}` — a share link whose signature holds and whose
      expiry has passed: `410`.
    * `{:error, {:unavailable, reason}}` — it could not decide (its identity
      provider did not answer): `503 auth_unavailable`.

  `init!/2` validates the authenticator's own options (raising `ArgumentError`
  naming `key`, `"gateway.auth.authenticators[<i>]"`) and returns what the
  other callbacks get as `ctx.config`. `credential/2` returns the bytes the
  authenticator would judge, or `:none`: an authenticator that answers `:none`
  is never asked to authenticate.

  A custom authenticator is a module implementing this behaviour, named as the
  entry's `type`.
  """

  @type ctx :: %{instance: atom(), index: non_neg_integer(), name: String.t(), config: map()}

  @callback init!(config :: map(), key :: String.t()) :: map()
  @callback child_specs(ctx()) :: [Supervisor.child_spec()]
  @doc "The bytes this authenticator would judge, or `:none` when the request carries none in its form."
  @callback credential(Plug.Conn.t(), ctx()) :: {:ok, binary()} | :none
  @callback authenticate(Plug.Conn.t(), ctx()) ::
              {:ok, Ankusa.Principal.t(), expires_at :: pos_integer() | nil}
              | :skip
              | {:error, :unauthenticated}
              | {:error, :link_expired}
              | {:error, {:unavailable, term()}}
  @doc "`true`: a refusal may be cached under the credential (authenticators that call out)."
  @callback cache_negative?() :: boolean()
  @optional_callbacks child_specs: 1, cache_negative?: 0
end
