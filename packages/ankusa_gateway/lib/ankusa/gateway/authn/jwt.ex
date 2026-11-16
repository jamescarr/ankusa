defmodule Ankusa.Gateway.Authn.JWT do
  @moduledoc """
  OIDC / OAuth2 access tokens (`type: :jwt`), verified with core's
  `Ankusa.Admin.Auth.JWT.verify/5` (asymmetric signature by a key of the set,
  `iss`, `aud`, `exp`, `nbf`, `sub`):

      %{type: :jwt, issuer: "https://login.example.com/", audience: "ankusa",
        jwks_url: "https://login.example.com/.well-known/jwks.json"}

  | Key | Default | |
  | --- | --- | --- |
  | `issuer`, `audience` | required | as `Ankusa.Admin.Auth.JWT` |
  | `jwks_url`, `keys` | one required | https (or http to loopback); static public keys |
  | `refresh_ms`, `min_refresh_ms`, `leeway_s` | `300000`, `10000`, `30` | |
  | `scope_claim`, `tenants_claim` | `"scope"`, `"ankusa_tenants"` | the principal's scopes and tenants |
  | `groups_claim` | `"groups"` | a list or a space-delimited string; the first 1000 strings |
  | `header` | `"authorization"` | where the token is: the bearer token, or exactly one header of another name |
  | `subject_prefix` | `""` | prepended to `sub` to form the principal id (at most 64 bytes of `[A-Za-z0-9_.\\|:-]`) |

  Several `jwt` authenticators can sit in one chain, one per issuer: a token
  whose unverified `iss` is not this authenticator's issuer is `:skip`, so the
  next one is asked. A token with this issuer that does not verify is `401`.
  Give each issuer a distinct `subject_prefix` when two of them can both name
  an `alice`: the principal id is what grants and table rows name.

  The keys of every `jwt` authenticator of the chain live in one
  `Ankusa.Admin.Auth.JWT.Keys` server the gateway starts (`server:
  :gateway_auth_keys`), one realm per authenticator, keyed by its index.
  """

  @behaviour Ankusa.Gateway.Authenticator

  alias Ankusa.Admin.Auth
  alias Ankusa.Gateway.Options

  @core_keys [
    :issuer,
    :audience,
    :jwks_url,
    :keys,
    :refresh_ms,
    :min_refresh_ms,
    :leeway_s,
    :scope_claim,
    :tenants_claim
  ]
  @own_keys [:groups_claim, :header, :subject_prefix]
  @subject_prefix ~r/\A[A-Za-z0-9_.|:-]{0,64}\z/
  @header ~r/\A[a-z0-9-]{1,64}\z/
  @max_header_bytes 8_192
  @max_groups 1_000

  @doc "The keys one `jwt` entry may set."
  @spec keys() :: [atom()]
  def keys, do: @core_keys ++ @own_keys

  @doc "The `Ankusa.Admin.Auth.JWT.Keys` server every `jwt` authenticator's keys live in."
  @spec keys_server() :: atom()
  def keys_server, do: :gateway_auth_keys

  @impl true
  def init!(config, key) do
    Options.unknown_keys!(config, @core_keys ++ @own_keys, key)
    jwt = Auth.jwt_block!(Map.take(config, @core_keys), key)

    groups_claim =
      Options.string!(Map.get(config, :groups_claim, "groups"), "#{key}.groups_claim")

    header = Map.get(config, :header, "authorization")

    unless is_binary(header) and Regex.match?(@header, header) do
      raise ArgumentError,
            "#{key}.header must be a lowercase header name, got #{inspect(header)}"
    end

    prefix = Map.get(config, :subject_prefix, "")

    unless is_binary(prefix) and Regex.match?(@subject_prefix, prefix) do
      raise ArgumentError,
            "#{key}.subject_prefix must be at most 64 bytes of A-Z a-z 0-9 _ . | : -, " <>
              "got #{inspect(prefix)}"
    end

    Map.merge(jwt, %{groups_claim: groups_claim, header: header, subject_prefix: prefix})
  end

  @impl true
  def credential(conn, %{config: %{header: "authorization"}}) do
    case Auth.bearer(conn) do
      {:ok, token} -> {:ok, token}
      :error -> :none
    end
  end

  def credential(conn, %{config: %{header: header}}) do
    case Plug.Conn.get_req_header(conn, header) do
      [token] when byte_size(token) in 1..@max_header_bytes -> {:ok, token}
      _ -> :none
    end
  end

  @impl true
  def authenticate(conn, %{instance: instance, index: index, config: jwt} = ctx) do
    with {:ok, token} <- credential(conn, ctx),
         true <- ours?(token, jwt.issuer) do
      verified(token, jwt, instance, index)
    else
      _ -> :skip
    end
  end

  defp verified(token, jwt, instance, index) do
    with {:ok, claims} <- Ankusa.Admin.Auth.JWT.verify(token, jwt, instance, index, keys_server()),
         {:ok, principal} <- Ankusa.Admin.Auth.JWT.principal(claims, jwt) do
      principal = %{
        principal
        | id: jwt.subject_prefix <> principal.id,
          claims: claims,
          groups: groups(claims[jwt.groups_claim])
      }

      {:ok, principal, expiry(claims["exp"])}
    else
      _ -> {:error, :unauthenticated}
    end
  end

  # Three dot-separated parts whose unverified `iss` is this authenticator's:
  # anything else belongs to another authenticator of the chain.
  defp ours?(token, issuer) do
    case String.split(token, ".") do
      [_, _, _] -> peek_issuer(token) == issuer
      _ -> false
    end
  end

  defp peek_issuer(token) do
    case JOSE.JWT.peek_payload(token) do
      %JOSE.JWT{fields: %{"iss" => iss}} -> iss
      _ -> nil
    end
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end

  @doc false
  # A list of strings, or a space-delimited string of them; the first 1000.
  @spec groups(term()) :: [String.t()]
  def groups(value) when is_binary(value),
    do: value |> String.split(" ", trim: true) |> Enum.take(@max_groups)

  def groups(value) when is_list(value),
    do: value |> Enum.filter(&is_binary/1) |> Enum.take(@max_groups)

  def groups(_other), do: []

  defp expiry(exp) when is_integer(exp), do: exp
  defp expiry(exp) when is_float(exp), do: trunc(exp)
  defp expiry(_other), do: nil
end
