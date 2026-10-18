defmodule Ankusa.Verifier.Token do
  @moduledoc """
  A static credential the provider sends with every request: a bearer token, a
  basic-auth login, a token in a header of its choosing, or a token in the
  query string. For providers that cannot sign (many internal systems, and
  several SaaS "webhook secret" fields that are just a header value). It proves
  the sender knows the secret, nothing about the body: a captured request can
  be replayed and a body can be swapped. Prefer `Ankusa.Verifier.Hmac` or
  `Ankusa.Verifier.Asymmetric` whenever the provider can sign.

  `opts`:

    * `:secret` — the credential, or a list of them (newest first; every one
      is tried, so a rotation is `secret: [new, old]`). A missing or empty
      secret, an empty list, more than 8 secrets, or an empty element fails
      every hook with `:bad_secret`, and a source declared in config refuses to
      boot: an empty credential would accept the empty request.
    * `:location` — where the credential travels:
        * `:bearer` (default): `Authorization: Bearer <secret>`;
        * `:basic`: `Authorization: Basic base64(<secret>)`, the secret being
          `user:password` (it must contain a `:`, as every client's does);
        * `{:header, name}`: the value of header `name`, which is the secret;
        * `{:query, param}`: the value of query parameter `param`. A URL is
          logged by every proxy on the way, so this is the weakest option.

  The comparison is `Ankusa.Verifier.secure_compare/2` over the SHA-256 of both
  sides, so neither the content nor the length of the secret is observable by
  timing, and every configured secret is compared whether or not an earlier one
  matched.

  The credential is *not* stored with the hook: `credential_headers/1` names the
  header it travels in, and `Ankusa.Edge.Ingest` adds it to the stored-header
  `drop` list for this source, so it never reaches the store, an archive
  segment, a claim pack or a sink. A quarantined hook is stored without it too,
  which means a quarantine replay cannot re-verify it (it fails closed with
  `:missing_signature` and stays in the pen): use `on_verify_failure: reject`
  for a token source.
  """

  @behaviour Ankusa.Verifier

  alias Ankusa.Envelope
  alias Ankusa.Verifier

  @max_secrets 8

  @type location :: :bearer | :basic | {:header, String.t()} | {:query, String.t()}

  @impl true
  @spec verify(Envelope.t(), keyword()) :: :ok | {:error, term()}
  def verify(%Envelope{} = env, opts) do
    with {:ok, location} <- location(opts),
         {:ok, secrets} <- secrets(opts, location),
         {:ok, presented} <- presented(env, location, opts) do
      presented_digest = :crypto.hash(:sha256, presented)

      # `or` evaluates its left side first, so every secret is compared.
      matched? =
        Enum.reduce(secrets, false, fn secret, matched? ->
          Verifier.secure_compare(presented_digest, :crypto.hash(:sha256, secret)) or matched?
        end)

      if matched?, do: :ok, else: {:error, :no_match}
    end
  end

  @doc "Human name of the scheme for telemetry: `bearer`, `basic`, `header` or `query`."
  @impl true
  @spec scheme_name(keyword()) :: String.t()
  def scheme_name(opts) do
    case opts[:location] || :bearer do
      :bearer -> "bearer"
      :basic -> "basic"
      {:header, _} -> "header"
      {:query, _} -> "query"
      _ -> "custom"
    end
  end

  @doc """
  Check `opts` the way `verify/2` would use them, without a request: the
  location is one of the four, and every secret is a non-empty binary (with a
  `:` for `:basic`).
  """
  @impl true
  @spec validate_opts(keyword()) :: :ok | {:error, :bad_scheme | :bad_secret}
  def validate_opts(opts) do
    with {:ok, location} <- location(opts),
         {:ok, _secrets} <- secrets(opts, location) do
      :ok
    end
  end

  @doc """
  The lowercase names of the request headers that carry this verifier's
  credential: what the source's stored-header policy must drop. `[]` for a
  location that is not a header, or opts that do not validate.
  """
  @spec credential_headers(keyword()) :: [String.t()]
  def credential_headers(opts) do
    case location(opts) do
      {:ok, :bearer} -> ["authorization"]
      {:ok, :basic} -> ["authorization"]
      {:ok, {:header, name}} -> [String.downcase(name, :ascii)]
      _ -> []
    end
  end

  defp location(opts) do
    case Keyword.get(opts, :location, :bearer) do
      loc when loc in [:bearer, :basic] ->
        {:ok, loc}

      {kind, name} = loc when kind in [:header, :query] and is_binary(name) and name != "" ->
        {:ok, loc}

      _ ->
        {:error, :bad_scheme}
    end
  end

  defp secrets(opts, location) do
    secrets = opts |> Keyword.get(:secret) |> List.wrap()

    if secrets != [] and length(secrets) <= @max_secrets and
         Enum.all?(secrets, &usable?(&1, location)),
       do: {:ok, secrets},
       else: {:error, :bad_secret}
  end

  defp usable?(secret, :basic), do: is_binary(secret) and String.contains?(secret, ":")
  defp usable?(secret, _location), do: is_binary(secret) and secret != ""

  defp presented(env, location, _opts) when location in [:bearer, :basic] do
    case Envelope.header(env, "authorization") do
      nil -> {:error, :missing_signature}
      value -> authorization(value, location)
    end
  end

  defp presented(env, {:header, name}, _opts) do
    case Envelope.header(env, name) do
      nil -> {:error, :missing_signature}
      "" -> {:error, :missing_signature}
      value -> {:ok, value}
    end
  end

  defp presented(_env, {:query, param}, opts) do
    case query_param(Keyword.get(opts, :query), param) do
      nil -> {:error, :missing_signature}
      "" -> {:error, :missing_signature}
      value -> {:ok, value}
    end
  end

  # The scheme name is case-insensitive (RFC 7235); the credential is not.
  defp authorization(value, location) do
    case String.split(value, " ", parts: 2) do
      [scheme, credential] when credential != "" ->
        if String.downcase(scheme, :ascii) == scheme_word(location),
          do: credential(credential, location),
          else: {:error, :malformed_signature}

      _ ->
        {:error, :malformed_signature}
    end
  end

  defp scheme_word(:bearer), do: "bearer"
  defp scheme_word(:basic), do: "basic"

  defp credential(credential, :bearer), do: {:ok, credential}

  defp credential(credential, :basic) do
    case Base.decode64(credential) do
      {:ok, decoded} when decoded != "" -> {:ok, decoded}
      _ -> {:error, :malformed_signature}
    end
  end

  # The first occurrence of `param`; a malformed percent-escape leaves the text
  # as it came (it then simply does not match).
  defp query_param(query, param) when is_binary(query) do
    query
    |> :binary.split("&", [:global])
    |> Enum.find_value(fn pair ->
      case :binary.split(pair, "=") do
        [key, value] -> if decode(key) == param, do: decode(value)
        _ -> nil
      end
    end)
  end

  defp query_param(_query, _param), do: nil

  defp decode(text) do
    URI.decode_www_form(text)
  rescue
    ArgumentError -> text
  end
end
