defmodule Ankusa.Auth.OIDC do
  @moduledoc """
  An `Ankusa.Auth.Verifier` for OpenID Connect: it checks that a bearer token is
  a JWT the identity provider signed, for this audience, and still valid.

  Configure it as `auth.verifier` (YAML: `auth.oidc`):

      auth: %{
        enabled: true,
        verifier:
          {Ankusa.Auth.OIDC,
           issuer: "https://login.example.com/",
           audience: "ankusa"},
        bindings: [...]
      }

  ## Options

    * `:issuer` (required): the `iss` claim a token must carry, compared exactly.
      Unless `:jwks_uri` is set, the signing keys are found through
      `<issuer>/.well-known/openid-configuration`.
    * `:audience` (required): the `aud` claim must be this string, or a list
      containing it.
    * `:algorithms`: the signature algorithms accepted, default
      `["RS256", "ES256"]`; any of `RS256 RS384 RS512 ES256 ES384 PS256`.
      `none` and the `HS*` family are refused at boot: an HMAC "signature" made
      with the provider's *public* key is the classic way to forge a token.
    * `:leeway_seconds`: clock skew allowed on `exp` and `nbf`, default 60.
    * `:jwks_uri`: where the keys are, skipping discovery.
    * `:refresh_interval_ms`: how often the keys are fetched again, default one
      hour.
    * `:min_refresh_interval_ms`: the least time between two fetches, default
      one minute (see below).
    * `:http_timeout_ms`: per request to the identity provider, default 5 s.

  ## What is checked

  In this order, and any failure is `{:error, :invalid_token}`: the token parses
  as a JWS; its `alg` is one of `:algorithms`; a key for its `kid` is known (a
  token with no `kid` is tried against every key); the signature verifies under
  that algorithm (`JOSE.JWT.verify_strict/3`, which also refuses an algorithm
  the key does not allow); `iss`, `aud`, `exp` (required) and `nbf` (when
  present) hold. Nothing raises on a malformed token.

  ## Keys

  A process per instance (`Ankusa.via(instance, :oidc)`, supervised by the
  instance) holds the provider's signing keys, so verifying a token costs no
  network round trip. It fetches them at boot and every
  `:refresh_interval_ms`. A fetch that fails keeps the keys it has and is
  retried in 30 s: the node starts, and keeps accepting tokens, while the
  provider is down. Until the keys have been fetched once, `verify/3` answers
  `{:error, :unavailable}` (the admin API's `503`, never a `401` that would make
  a client discard a good token).

  A token whose `kid` is not known (the provider rotated keys) triggers one
  fetch, at most once per `:min_refresh_interval_ms`, so a client that sends
  garbage `kid`s cannot make the node hammer the provider.
  """

  use GenServer

  @behaviour Ankusa.Auth.Verifier

  require Logger

  @allowed_algorithms ~w(RS256 RS384 RS512 ES256 ES384 PS256)
  @default_algorithms ~w(RS256 ES256)
  @key_types ["RSA", "EC"]
  @retry_ms 30_000
  @call_timeout 15_000

  @defaults [
    algorithms: @default_algorithms,
    leeway_seconds: 60,
    refresh_interval_ms: 3_600_000,
    min_refresh_interval_ms: 60_000,
    http_timeout_ms: 5_000
  ]

  @known_opts [:issuer, :audience, :jwks_uri | Keyword.keys(@defaults)]

  # ── Ankusa.Auth.Verifier ───────────────────────────────────────────────────

  @impl Ankusa.Auth.Verifier
  def validate_opts!(opts) when is_list(opts) do
    case Keyword.keys(opts) -- @known_opts do
      [] -> :ok
      unknown -> raise ArgumentError, "unknown auth.oidc option(s): #{inspect(unknown)}"
    end

    require_string!(opts, :issuer)
    require_string!(opts, :audience)

    if Keyword.has_key?(opts, :jwks_uri), do: require_string!(opts, :jwks_uri)

    algorithms = Keyword.get(opts, :algorithms, @default_algorithms)

    unless is_list(algorithms) and algorithms != [] and Enum.all?(algorithms, &is_binary/1) do
      raise ArgumentError, "auth.oidc.algorithms must be a non-empty list of strings"
    end

    case algorithms -- @allowed_algorithms do
      [] ->
        :ok

      refused ->
        raise ArgumentError,
              "auth.oidc.algorithms #{inspect(refused)} not allowed; use any of " <>
                Enum.join(@allowed_algorithms, ", ") <> " (never none, never HS*)"
    end

    for key <- [:refresh_interval_ms, :min_refresh_interval_ms, :http_timeout_ms] do
      positive_integer!(opts, key, 1)
    end

    positive_integer!(opts, :leeway_seconds, 0)
    :ok
  end

  defp require_string!(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and value != "" ->
        :ok

      other ->
        raise ArgumentError, "auth.oidc.#{key} is required, a string; got #{inspect(other)}"
    end
  end

  defp positive_integer!(opts, key, min) do
    case Keyword.get(opts, key, @defaults[key]) do
      value when is_integer(value) and value >= min ->
        :ok

      other ->
        raise ArgumentError,
              "auth.oidc.#{key} must be an integer >= #{min}, got #{inspect(other)}"
    end
  end

  @impl Ankusa.Auth.Verifier
  def verify(instance, token, opts) when is_binary(token) do
    opts = Keyword.merge(@defaults, opts)

    with {:ok, header} <- header(token),
         :ok <- algorithm(header, opts[:algorithms]),
         {:ok, keys} <- keys(instance, header["kid"]),
         {:ok, claims} <- signature(keys, opts[:algorithms], token),
         :ok <- claims(claims, opts) do
      {:ok, claims}
    end
  rescue
    # a token is untrusted input: nothing it contains may crash the request
    _error -> {:error, :invalid_token}
  catch
    _kind, _reason -> {:error, :invalid_token}
  end

  def verify(_instance, _token, _opts), do: {:error, :invalid_token}

  defp header(token) do
    with json when is_binary(json) <- JOSE.JWS.peek_protected(token),
         {:ok, %{} = header} <- JSON.decode(json) do
      {:ok, header}
    else
      _ -> {:error, :invalid_token}
    end
  end

  defp algorithm(%{"alg" => alg}, allowed) when is_binary(alg) do
    if alg in allowed, do: :ok, else: {:error, :invalid_token}
  end

  defp algorithm(_header, _allowed), do: {:error, :invalid_token}

  defp keys(_instance, kid) when not is_nil(kid) and not is_binary(kid),
    do: {:error, :invalid_token}

  defp keys(instance, kid) do
    GenServer.call(Ankusa.via(instance, :oidc), {:keys, kid}, @call_timeout)
  catch
    :exit, _reason -> {:error, :unavailable}
  end

  defp signature(keys, algorithms, token) do
    Enum.find_value(keys, {:error, :invalid_token}, fn jwk ->
      case JOSE.JWT.verify_strict(jwk, algorithms, token) do
        {true, %JOSE.JWT{fields: claims}, _jws} -> {:ok, claims}
        _ -> nil
      end
    end)
  end

  defp claims(claims, opts) do
    now = System.system_time(:second)
    leeway = opts[:leeway_seconds]

    with true <- claims["iss"] == opts[:issuer],
         true <- audience?(claims["aud"], opts[:audience]),
         true <- number?(claims["exp"]) and claims["exp"] > now - leeway,
         true <-
           not Map.has_key?(claims, "nbf") or
             (number?(claims["nbf"]) and claims["nbf"] <= now + leeway) do
      :ok
    else
      _ -> {:error, :invalid_token}
    end
  end

  defp audience?(aud, expected) when is_binary(aud), do: aud == expected
  defp audience?(aud, expected) when is_list(aud), do: expected in aud
  defp audience?(_aud, _expected), do: false

  defp number?(value), do: is_integer(value) or is_float(value)

  # ── the key holder ─────────────────────────────────────────────────────────

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, instance, name: Ankusa.via(instance, :oidc))
  end

  @impl GenServer
  def init(instance) do
    {_module, opts} = Ankusa.config(instance).auth.verifier

    state = %{
      instance: instance,
      opts: Keyword.merge(@defaults, opts),
      jwks_uri: Keyword.get(opts, :jwks_uri),
      keys: %{},
      loaded?: false,
      last_fetch: nil,
      timer: nil
    }

    {:ok, state, {:continue, :load}}
  end

  @impl GenServer
  def handle_continue(:load, state), do: {:noreply, refresh(state)}

  @impl GenServer
  def handle_info(:refresh, state), do: {:noreply, refresh(state)}
  def handle_info(_other, state), do: {:noreply, state}

  @impl GenServer
  def handle_call({:keys, kid}, _from, state) do
    {reply, state} = lookup(state, kid)
    {:reply, reply, state}
  end

  # The keys a token may be verified with. A token without a `kid` is tried
  # against every key.
  defp lookup(state, nil) do
    state = maybe_fetch(state, not state.loaded?)

    cond do
      not state.loaded? -> {{:error, :unavailable}, state}
      state.keys == %{} -> {{:error, :invalid_token}, state}
      true -> {{:ok, Map.values(state.keys)}, state}
    end
  end

  defp lookup(state, kid) do
    case Map.fetch(state.keys, kid) do
      {:ok, jwk} ->
        {{:ok, [jwk]}, state}

      :error ->
        # Never loaded, or a key this node has not seen (rotation): one fetch,
        # when the cooldown allows.
        state = maybe_fetch(state, true)

        cond do
          not state.loaded? -> {{:error, :unavailable}, state}
          true -> lookup_known(state, kid)
        end
    end
  end

  defp lookup_known(state, kid) do
    case Map.fetch(state.keys, kid) do
      {:ok, jwk} -> {{:ok, [jwk]}, state}
      :error -> {{:error, :invalid_token}, state}
    end
  end

  defp maybe_fetch(state, false), do: state

  defp maybe_fetch(state, true) do
    min = state.opts[:min_refresh_interval_ms]
    now = System.monotonic_time(:millisecond)

    if state.last_fetch == nil or now - state.last_fetch >= min,
      do: refresh(state),
      else: state
  end

  defp refresh(state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    state = %{state | last_fetch: System.monotonic_time(:millisecond), timer: nil}

    case fetch(state) do
      {:ok, keys, jwks_uri} ->
        Logger.debug("[ankusa] oidc: #{map_size(keys)} signing key(s) from #{jwks_uri}")
        timer = Process.send_after(self(), :refresh, state.opts[:refresh_interval_ms])
        %{state | keys: keys, loaded?: true, jwks_uri: jwks_uri, timer: timer}

      {:error, reason} ->
        Logger.warning(
          "[ankusa] oidc: could not fetch signing keys for #{state.opts[:issuer]}: " <>
            "#{inspect(reason)}; " <>
            if(state.loaded?,
              do: "keeping the keys it has",
              else: "tokens cannot be verified yet"
            ) <>
            ", retrying in #{div(@retry_ms, 1000)} s"
        )

        %{state | timer: Process.send_after(self(), :refresh, @retry_ms)}
    end
  end

  defp fetch(state) do
    with {:ok, jwks_uri} <- jwks_uri(state),
         {:ok, body} <- get(jwks_uri, state.opts),
         {:ok, keys} <- parse_jwks(body) do
      {:ok, keys, jwks_uri}
    end
  end

  defp jwks_uri(%{jwks_uri: uri}) when is_binary(uri), do: {:ok, uri}

  defp jwks_uri(%{opts: opts}) do
    issuer = opts[:issuer]
    url = String.trim_trailing(issuer, "/") <> "/.well-known/openid-configuration"

    with {:ok, body} <- get(url, opts) do
      case body do
        %{"issuer" => other} when other != issuer ->
          {:error, {:issuer_mismatch, other}}

        %{"jwks_uri" => uri} when is_binary(uri) ->
          {:ok, uri}

        _ ->
          {:error, :no_jwks_uri}
      end
    end
  end

  defp get(url, opts) do
    timeout = opts[:http_timeout_ms]

    case Req.get(url,
           receive_timeout: timeout,
           connect_options: [timeout: timeout],
           retry: false
         ) do
      {:ok, %Req.Response{status: 200, body: body}} -> json(body)
      {:ok, %Req.Response{status: status}} -> {:error, {:status, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  # Req decodes `application/json`; a `application/jwk-set+json` body arrives
  # as the raw string.
  defp json(%{} = body), do: {:ok, body}

  defp json(body) when is_binary(body) do
    case JSON.decode(body) do
      {:ok, %{} = decoded} -> {:ok, decoded}
      _ -> {:error, :bad_json}
    end
  end

  defp json(_body), do: {:error, :bad_json}

  defp parse_jwks(%{"keys" => keys}) when is_list(keys) do
    keys =
      keys
      |> Enum.reject(&(not is_map(&1) or &1["use"] == "enc" or &1["kty"] not in @key_types))
      |> Enum.with_index()
      |> Enum.flat_map(fn {key, index} ->
        case to_jwk(key) do
          {:ok, jwk} -> [{key["kid"] || {:anonymous, index}, jwk}]
          :error -> []
        end
      end)
      |> Map.new()

    {:ok, keys}
  end

  defp parse_jwks(_body), do: {:error, :no_keys}

  defp to_jwk(key) do
    {:ok, JOSE.JWK.from_map(key)}
  rescue
    _error ->
      Logger.warning(
        "[ankusa] oidc: skipped a signing key that does not parse (kid #{inspect(key["kid"])})"
      )

      :error
  end
end
