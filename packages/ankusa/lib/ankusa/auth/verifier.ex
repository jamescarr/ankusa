defmodule Ankusa.Auth.Verifier do
  @moduledoc """
  Turns the bearer token of an admin request into the claims it asserts.

  Configured as `config.auth.verifier`, a `{module, opts}` pair. `verify/3` runs
  on every authenticated request, so it must not do blocking I/O per call: the
  OIDC adapter (`ankusa_oidc`) verifies against JWKS keys it caches.

    * `{:ok, claims}`: the token is genuine, claims is the decoded payload
      (string keys). What the claims mean is `Ankusa.Auth`'s business.
    * `{:error, :invalid_token}`: forged, expired, wrong audience, malformed.
      The request is `401`.
    * `{:error, :unavailable}`: the verifier cannot decide right now (its key
      source was never reachable). The request is `503`, never a `401`, so a
      client does not discard a good token.

  A verifier that exports `start_link/1` is supervised by the instance, started
  as `{module, instance: instance}`. `validate_opts!/1` is optional; when
  exported it runs at boot with the configured opts and raises
  `ArgumentError` for opts that cannot work.
  """

  @callback verify(instance :: atom(), token :: String.t(), opts :: keyword()) ::
              {:ok, claims :: map()} | {:error, :invalid_token} | {:error, :unavailable}

  @callback validate_opts!(keyword()) :: :ok

  @optional_callbacks validate_opts!: 1
end
