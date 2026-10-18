defmodule Ankusa.Admin.Auth.JWT do
  @moduledoc """
  OIDC / OAuth2 bearer tokens (`mode: :jwt`): the control planes accept the
  access tokens your identity provider issues, so operators sign in with SSO
  and revocation is the provider's.

      jwt: %{
        issuer: "https://login.example.com/",
        audience: "ankusa-admin",
        jwks_url: "https://login.example.com/.well-known/jwks.json"
      }

  A token is accepted when all of these hold:

    * it is a JWS signed with an asymmetric algorithm (`RS*`, `PS*`, `ES*`,
      `EdDSA`) by a key of the configured set. `none` and the `HS*` family are
      never accepted, so a token cannot be forged by presenting a public key
      as an HMAC secret;
    * `iss` equals `jwt.issuer` and `aud` is, or contains, `jwt.audience` (or
      one of its entries, when that is a list);
    * `exp` is present and not past, and `nbf`, when present, is not ahead,
      each with `jwt.leeway_s` seconds of clock-skew leeway;
    * `sub` names the caller (it becomes the principal's id).

  The principal's scopes come from the `jwt.scope_claim` claim (default
  `scope`: a space-delimited string as in RFC 9068, or a list), and its tenants
  from `jwt.tenants_claim` (default `ankusa_tenants`: a list of tenant ids, or
  `["*"]`) together with any `tenant:<id>` scopes. Scopes this system does not
  define (`openid`, `email`) are ignored.

  Keys come from `jwt.keys` (public JWKs or PEMs), from `jwt.jwks_url`, or
  both; see `Ankusa.Admin.Auth.JWT.Keys` for how the JWKS is cached and
  refreshed. With no key loaded every token is `401`.
  """

  @behaviour Ankusa.Admin.Auth

  alias Ankusa.Admin.Auth
  alias Ankusa.Admin.Auth.JWT.Keys
  alias Ankusa.Principal

  @algorithms ~w(RS256 RS384 RS512 PS256 PS384 PS512 ES256 ES384 ES512 EdDSA)

  @impl true
  def authenticate(conn, %{instance: instance, section: section, config: %{jwt: jwt}}) do
    with {:ok, token} <- Auth.bearer(conn),
         {:ok, kid} <- key_id(token),
         {:ok, claims} <-
           verify(token, Keys.candidates(instance, section, kid), instance, section, kid),
         :ok <- check_claims(claims, jwt, System.os_time(:second)),
         {:ok, principal} <- principal(claims, jwt) do
      {:ok, principal}
    else
      _ -> {:error, :unauthenticated}
    end
  end

  # The token's own `kid`, or `nil`. A token whose header is not a JSON object
  # is not a JWS at all.
  defp key_id(token) do
    case JOSE.JWS.peek_protected(token) |> JSON.decode() do
      {:ok, %{"kid" => kid}} when is_binary(kid) -> {:ok, kid}
      {:ok, %{} = header} when not is_map_key(header, "kid") -> {:ok, nil}
      _ -> :error
    end
  rescue
    _ -> :error
  catch
    _, _ -> :error
  end

  defp verify(_token, [], instance, section, kid) do
    # A kid we have no key for may be a rotation: ask for a refresh (rate
    # limited by the cache) and refuse this request.
    if kid, do: Keys.refresh(instance, section)
    :error
  end

  defp verify(token, keys, _instance, _section, _kid) do
    Enum.find_value(keys, :error, fn jwk ->
      case JOSE.JWT.verify_strict(jwk, @algorithms, token) do
        {true, %JOSE.JWT{fields: claims}, _jws} -> {:ok, claims}
        _ -> nil
      end
    end)
  rescue
    _ -> :error
  catch
    _, _ -> :error
  end

  defp check_claims(claims, jwt, now) do
    leeway = jwt.leeway_s

    with true <- claims["iss"] == jwt.issuer,
         true <- audience?(claims["aud"], jwt.audience),
         true <- is_number(claims["exp"]) and now - leeway < claims["exp"],
         true <- not_before?(claims["nbf"], now, leeway),
         true <- is_binary(claims["sub"]) and claims["sub"] != "" do
      :ok
    else
      _ -> :error
    end
  end

  defp not_before?(nil, _now, _leeway), do: true
  defp not_before?(nbf, now, leeway) when is_number(nbf), do: nbf - leeway <= now
  defp not_before?(_other, _now, _leeway), do: false

  defp audience?(aud, wanted) do
    presented = aud |> List.wrap() |> Enum.filter(&is_binary/1)
    Enum.any?(List.wrap(wanted), &(&1 in presented))
  end

  defp principal(claims, jwt) do
    scopes = words(claims[jwt.scope_claim])
    tenants = words(claims[jwt.tenants_claim])
    {:ok, Principal.new(claims["sub"], scopes, tenants)}
  end

  # A claim holding a list of strings, or a space-delimited string of them.
  defp words(value) when is_binary(value), do: String.split(value, " ", trim: true)
  defp words(value) when is_list(value), do: Enum.filter(value, &is_binary/1)
  defp words(_other), do: []
end
