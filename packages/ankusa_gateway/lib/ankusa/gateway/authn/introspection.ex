defmodule Ankusa.Gateway.Authn.Introspection do
  @moduledoc """
  Opaque OAuth2 access tokens, resolved by RFC 7662 token introspection
  (`type: :introspection`), for identity providers whose access tokens are not
  JWTs — Okta's org authorization server among them:

      %{type: :introspection, url: "https://acme.okta.com/oauth2/v1/introspect",
        client_id: "...", client_secret: "..."}

  | Key | Default | |
  | --- | --- | --- |
  | `url` | required | https (or http to a loopback host) |
  | `client_id`, `client_secret` | required | Basic credentials of an app allowed to introspect; the secret is redacted |
  | `token_type_hint` | `"access_token"` | |
  | `timeout_ms`, `max_inflight`, `queue_max` | `2000`, `32`, `256` | its `Ankusa.Gateway.Caller` |
  | `issuer` | none | the response's `iss` must be this |
  | `audience` | none | the response's `aud` must be (or contain) this; a list is accepted, as in `jwt` |
  | `subject_claim` | `"sub"` | the response claim that is the principal id |
  | `scope_claim`, `tenants_claim`, `groups_claim` | `"scope"`, `"ankusa_tenants"`, `"groups"` | the principal's scopes, tenants and groups |
  | `subject_prefix` | `""` | prepended to the subject to form the principal id, as for `jwt` |

  Every call is one `POST <url>` with the token form-encoded
  (`URI.encode_query/1`: a token is caller input, never concatenated into the
  body) and `Authorization: Basic base64(client_id:client_secret)`, through one
  `Ankusa.Gateway.Caller` (`{:authn, name}`): a hundred requests carrying one
  token make one call. `200` with `"active": true` (and the `iss`/`aud` checks,
  when configured) authenticates; `"active": false`, a failed check or an
  unidentifiable subject is `{:error, :unauthenticated}`, whose refusal is
  cached (`cache_negative?/0`); anything else — another status, a transport
  failure, a body over 64 KiB, an `active` that is not a boolean — is
  `{:error, {:unavailable, reason}}` (`503 auth_unavailable`).

  The principal's claims are the introspection response. Its expiry is the
  response's `exp`, or 60 seconds after the answer when it carries none.

  At most one `introspection` authenticator may sit in a chain, and no
  authenticator that reads the same bearer token (`token`, or `jwt` with
  `header: authorization`) may follow it: the first that answers for a token
  ends the chain, so a static token would never be reached.
  """

  @behaviour Ankusa.Gateway.Authenticator

  alias Ankusa.Admin.Auth
  alias Ankusa.Gateway.Authn.JWT
  alias Ankusa.Gateway.{Caller, Options, Url}
  alias Ankusa.HttpClient
  alias Ankusa.Principal

  @own_keys [
    :url,
    :client_id,
    :client_secret,
    :token_type_hint,
    :timeout_ms,
    :max_inflight,
    :queue_max,
    :issuer,
    :audience,
    :subject_claim,
    :scope_claim,
    :tenants_claim,
    :groups_claim,
    :subject_prefix,
    :req_options
  ]
  @subject_prefix ~r/\A[A-Za-z0-9_.|:-]{0,64}\z/
  @max_bytes 65_536
  @absent_expiry_s 60
  @breaker %{failures: 5, open_ms: 5_000}

  @doc "The keys one `introspection` entry may set."
  @spec keys() :: [atom()]
  def keys, do: @own_keys

  @impl true
  def init!(config, key) do
    Options.unknown_keys!(config, @own_keys, key)

    url = Map.get(config, :url)

    unless is_binary(url) and url != "" do
      raise ArgumentError, "#{key}.url must be set"
    end

    prefix = Map.get(config, :subject_prefix, "")

    unless is_binary(prefix) and Regex.match?(@subject_prefix, prefix) do
      raise ArgumentError,
            "#{key}.subject_prefix must be at most 64 bytes of A-Z a-z 0-9 _ . | : -, " <>
              "got #{inspect(prefix)}"
    end

    %{
      url: Url.https!(url, "#{key}.url"),
      client_id: credential!(config, :client_id, key),
      client_secret: credential!(config, :client_secret, key),
      token_type_hint:
        Options.string!(
          Map.get(config, :token_type_hint, "access_token"),
          "#{key}.token_type_hint"
        ),
      timeout_ms:
        Options.int_in!(Map.get(config, :timeout_ms, 2_000), 100..30_000, "#{key}.timeout_ms"),
      max_inflight:
        Options.int_in!(Map.get(config, :max_inflight, 32), 1..10_000, "#{key}.max_inflight"),
      queue_max:
        Options.int_in!(Map.get(config, :queue_max, 256), 0..100_000, "#{key}.queue_max"),
      issuer: issuer_opt(Map.get(config, :issuer), "#{key}.issuer"),
      audience: audience_opt(Map.get(config, :audience), "#{key}.audience"),
      subject_claim:
        Options.string!(Map.get(config, :subject_claim, "sub"), "#{key}.subject_claim"),
      scope_claim: Options.string!(Map.get(config, :scope_claim, "scope"), "#{key}.scope_claim"),
      tenants_claim:
        Options.string!(Map.get(config, :tenants_claim, "ankusa_tenants"), "#{key}.tenants_claim"),
      groups_claim:
        Options.string!(Map.get(config, :groups_claim, "groups"), "#{key}.groups_claim"),
      subject_prefix: prefix,
      req_options: Options.keyword_list!(Map.get(config, :req_options, []), "#{key}.req_options")
    }
  end

  defp credential!(config, name, key) do
    case Map.get(config, name) do
      value when is_binary(value) and value != "" ->
        value

      nil ->
        raise ArgumentError,
              "#{key}.#{name} must be set: token introspection needs an app's credentials"

      _other ->
        raise ArgumentError, "#{key}.#{name} must be a non-empty string"
    end
  end

  defp issuer_opt(nil, _key), do: nil
  defp issuer_opt(value, key), do: Options.string!(value, key)

  defp audience_opt(nil, _key), do: nil
  defp audience_opt(value, _key) when is_binary(value) and value != "", do: [value]

  defp audience_opt(value, key) when is_list(value) do
    if value != [] and Enum.all?(value, &(is_binary(&1) and &1 != "")),
      do: value,
      else: raise(ArgumentError, "#{key} must be an audience string or a list of them")
  end

  defp audience_opt(_other, key),
    do: raise(ArgumentError, "#{key} must be an audience string or a list of them")

  @impl true
  def credential(conn, _ctx) do
    case Auth.bearer(conn) do
      {:ok, token} -> {:ok, token}
      :error -> :none
    end
  end

  @impl true
  def authenticate(conn, %{instance: instance, name: name, config: config} = ctx) do
    case credential(conn, ctx) do
      {:ok, token} ->
        deadline = System.monotonic_time(:millisecond) + config.timeout_ms
        key = :crypto.hash(:sha256, token)

        case Caller.call(instance, {:authn, name}, key, :authn, token, deadline) do
          {:ok, {:ok, principal, expires_at}} -> {:ok, principal, expires_at}
          {:ok, :inactive} -> {:error, :unauthenticated}
          {:error, reason} -> {:error, {:unavailable, reason}}
        end

      :none ->
        :skip
    end
  end

  @impl true
  def child_specs(%{instance: instance, name: name, config: config}) do
    [
      Caller.child_spec(
        instance: instance,
        name: {:authn, name},
        label: "authn:#{name}",
        max_inflight: config.max_inflight,
        queue_max: config.queue_max,
        breaker: @breaker,
        run: fn token -> run(instance, config, token) end
      )
    ]
  end

  @impl true
  def cache_negative?, do: true

  defp run(instance, config, token) do
    body = URI.encode_query(%{"token" => token, "token_type_hint" => config.token_type_hint})

    headers = [
      {"content-type", "application/x-www-form-urlencoded"},
      {"accept", "application/json"},
      {"authorization",
       "Basic " <> Base.encode64(config.client_id <> ":" <> config.client_secret)}
    ]

    opts = [max_response_bytes: @max_bytes, instance: instance] ++ config.req_options

    case HttpClient.request_with_headers(
           :post,
           config.url,
           headers,
           body,
           config.timeout_ms,
           opts
         ) do
      {:ok, 200, _headers, response} -> verdict(response, config)
      {:ok, status, _headers, _body} -> {:error, {:unavailable, {:status, status}}}
      {:error, reason} -> {:error, {:unavailable, reason}}
    end
  end

  defp verdict(body, config) when is_binary(body) do
    case JSON.decode(body) do
      {:ok, %{"active" => true} = response} -> active(response, config)
      {:ok, %{"active" => false}} -> {:ok, :inactive}
      _other -> {:error, {:unavailable, :bad_response}}
    end
  end

  defp verdict({:truncated, _bytes}, _config), do: {:error, {:unavailable, :too_large}}
  defp verdict(_other, _config), do: {:error, {:unavailable, :bad_response}}

  defp active(response, config) do
    with :ok <- issuer_check(response["iss"], config.issuer),
         :ok <- audience_check(response["aud"], config.audience),
         {:ok, id} <- subject(response, config) do
      principal =
        Principal.new(
          id,
          words(response[config.scope_claim]),
          words(response[config.tenants_claim])
        )

      {:ok,
       {:ok,
        %{
          principal
          | groups: JWT.groups(response[config.groups_claim]),
            claims: response
        }, expiry(response["exp"])}}
    else
      _refused -> {:ok, :inactive}
    end
  end

  defp subject(response, config) do
    case response[config.subject_claim] do
      id when is_binary(id) and id != "" -> {:ok, config.subject_prefix <> id}
      _other -> :error
    end
  end

  defp issuer_check(_iss, nil), do: :ok
  defp issuer_check(iss, issuer) when iss == issuer, do: :ok
  defp issuer_check(_iss, _issuer), do: :error

  defp audience_check(_aud, nil), do: :ok

  defp audience_check(aud, audiences) do
    claimed =
      case aud do
        value when is_binary(value) -> [value]
        value when is_list(value) -> Enum.filter(value, &is_binary/1)
        _other -> []
      end

    if Enum.any?(audiences, &(&1 in claimed)), do: :ok, else: :error
  end

  defp expiry(exp) when is_integer(exp), do: exp
  defp expiry(exp) when is_float(exp), do: trunc(exp)
  defp expiry(_other), do: System.os_time(:second) + @absent_expiry_s

  # A list of strings, or a space-delimited string of them.
  defp words(value) when is_binary(value), do: String.split(value, " ", trim: true)
  defp words(value) when is_list(value), do: Enum.filter(value, &is_binary/1)
  defp words(_other), do: []
end
