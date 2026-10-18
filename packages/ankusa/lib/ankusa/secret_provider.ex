defmodule Ankusa.SecretProvider do
  @moduledoc """
  Where a secret comes from when it is not in the config file: a secret manager.

  `ankusa_server` resolves `${sm:<provider>:<ref>}` in its config by calling
  `c:fetch/2` on the module registered under `<provider>` (see
  `AnkusaServer.SecretProvider.AwsSecretsManager` and
  `AnkusaServer.SecretProvider.Vault`). The behaviour lives in core so an
  embedding application can write its own provider and hand it the same way.

  `ref` is whatever follows the provider name in the reference, opaque to the
  caller (`prod/stripe#whsec` for a JSON secret's `whsec` field, say). `opts` is
  the provider's own settings from the config (`region`, `address`, a token),
  never anything request-shaped.

  ## Three answers

    * `{:ok, secret}`: the bytes, exactly as the manager holds them. A trailing
      newline is not trimmed here.
    * `{:error, :not_found}`: the manager answered and the secret is not there,
      or the caller may not read it. Retrying changes nothing.
    * `{:error, :unavailable}`: the manager could not be asked or did not give a
      usable answer (a timeout, a 5xx, a refused connection, a malformed body).
      A later call may succeed.

  A provider never raises for an I/O failure, and never puts the secret, a
  credential or the full response in an error value: the reason is logged next
  to the dotted config key that asked for it.
  """

  @callback fetch(ref :: String.t(), opts :: keyword()) ::
              {:ok, binary()} | {:error, :unavailable | :not_found}
end
