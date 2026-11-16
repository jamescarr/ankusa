defmodule Ankusa.Gateway.Authn.Okta do
  @moduledoc """
  `type: :okta`: Okta's issuer layout, expanded at boot into a `jwt` or an
  `introspection` authenticator (`Ankusa.Gateway.Config.new/2`; no network at
  boot):

      - type: okta
        domain: acme.okta.com
        authorization_server: default
        audience: api://default

  | Key | Default | |
  | --- | --- | --- |
  | `domain` | required | the Okta org's host: `[a-z0-9.-]+` |
  | `authorization_server` | `"default"` | an authorization server name, or `"org"` for Okta's org authorization server |
  | `audience` | required by the `jwt` form | the token's `aud` |
  | `client_id`, `client_secret` | required by `"org"` | an app allowed to introspect; the secret is redacted |
  | `name` | `"okta"` | the chain entry's name |

  A custom authorization server (`https://<domain>/oauth2/<server>`) issues
  JWTs: the entry becomes a `jwt` authenticator with
  `issuer: https://<domain>/oauth2/<server>`,
  `jwks_url: <issuer>/v1/keys` and `groups_claim: groups`. `client_id` and
  `client_secret` play no part in verifying a JWT and are dropped.

  `"org"` issues opaque access tokens, which cannot be verified locally: the
  entry becomes an `introspection` authenticator for
  `https://<domain>/oauth2/v1/introspect`, checking `iss: https://<domain>`,
  with the org's app credentials.

  Any `jwt` or `introspection` option in the entry — `subject_prefix`,
  `scope_claim`, `groups_claim`, `keys` instead of `jwks_url`, `timeout_ms` —
  overrides what the expansion derives; a key the expanded type does not know
  is refused by it.
  """

  alias Ankusa.Gateway.{Authn, Options}

  @own_keys [:domain, :authorization_server, :audience, :client_id, :client_secret]
  @domain ~r/\A[a-z0-9.-]+\z/
  @server ~r/\A[a-z0-9][a-z0-9_-]{0,63}\z/

  @doc """
  Expand one `okta` entry (whose `:type` is `:okta`) into `{type, opts}` for
  the type's own authenticator to validate and build. Raises `ArgumentError`
  naming `key.<option>`.
  """
  @spec expand!(map(), String.t()) :: {:jwt | :introspection, map()}
  def expand!(given, key) do
    Options.unknown_keys!(
      given,
      [:type, :name | @own_keys] ++ Authn.JWT.keys() ++ Authn.Introspection.keys(),
      key
    )

    domain = domain!(Map.get(given, :domain), key)
    server = server!(Map.get(given, :authorization_server, "default"), key)
    client_id = optional_secret(Map.get(given, :client_id), "#{key}.client_id")
    client_secret = optional_secret(Map.get(given, :client_secret), "#{key}.client_secret")
    given = Map.put_new(given, :name, "okta")

    case server do
      "org" ->
        opts =
          given
          |> Map.drop([:type, :domain, :authorization_server])
          |> secrets(client_id, client_secret)
          |> Map.put_new(:url, "https://#{domain}/oauth2/v1/introspect")
          |> Map.put_new(:issuer, "https://#{domain}")

        for name <- [:client_id, :client_secret], not present?(opts[name]) do
          raise ArgumentError,
                "#{key}.#{name} must be set: Okta's org authorization server issues opaque access " <>
                  "tokens, which are resolved by introspection"
        end

        {:introspection, opts}

      name ->
        issuer = "https://#{domain}/oauth2/#{name}"

        {:jwt,
         given
         |> Map.drop([:type, :domain, :authorization_server, :client_id, :client_secret])
         |> Map.put_new(:issuer, issuer)
         |> Map.put_new(:jwks_url, issuer <> "/v1/keys")
         |> Map.put_new(:groups_claim, "groups")}
    end
  end

  defp domain!(domain, key) do
    if is_binary(domain) and Regex.match?(@domain, domain) do
      domain
    else
      raise ArgumentError,
            "#{key}.domain must be a lowercase host name (A-Z 0-9 . -), got #{inspect(domain)}"
    end
  end

  defp server!(server, key) do
    if server == "org" or (is_binary(server) and Regex.match?(@server, server)) do
      server
    else
      raise ArgumentError,
            "#{key}.authorization_server must be \"org\" or an authorization server name " <>
              "([a-z0-9][a-z0-9_-]*), got #{inspect(server)}"
    end
  end

  defp optional_secret(nil, _key), do: nil
  defp optional_secret(value, _key) when is_binary(value) and value != "", do: value

  defp optional_secret(_other, key),
    do: raise(ArgumentError, "#{key} must be a non-empty string")

  defp secrets(opts, nil, nil), do: opts
  defp secrets(opts, client_id, nil), do: Map.put_new(opts, :client_id, client_id)
  defp secrets(opts, nil, client_secret), do: Map.put_new(opts, :client_secret, client_secret)

  defp secrets(opts, client_id, client_secret),
    do: opts |> Map.put_new(:client_id, client_id) |> Map.put_new(:client_secret, client_secret)

  defp present?(value), do: is_binary(value) and value != ""
end
