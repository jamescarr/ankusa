defmodule Ankusa.Admin.Auth do
  @moduledoc """
  Authentication and authorization for the three control planes: the admin API
  (`Ankusa.Admin.Router`), the route-management API (`Ankusa.Routes.Router`)
  and the claim-check gateway (`Ankusa.ClaimCheck.Router`).

  Each router mounts this module as a plug ahead of `:match`. The plug
  authenticates the request, assigns the `Ankusa.Principal` to
  `conn.assigns.principal` and halts with `401` when there is none; each route
  then calls `authorize/3`, `authorize_tenant/4` or `authorize_global/3` for
  the scope it needs, which answers `403` otherwise. `GET /health` and
  `GET /ready` are exempt: probes carry no credentials.

  ## Modes

  Each listener has its own `auth` block (`admin.auth`, `routes.admin.auth`,
  `claim_check.auth`):

      %{
        mode: :none | :token | :jwt | :mtls,
        allow_insecure: false,
        tokens: [%{id: "ci", sha256: "<64 hex>", scopes: ["dlq:read", "tenant:*"]}],
        jwt: %{issuer: ..., audience: ..., jwks_url: ..., keys: [...], ...},
        mtls: %{subject_map: %{"cn:ops-laptop" => ["dlq:read", "tenant:*"]}}
      }

    * `:none` (the default) authenticates nobody and hands every request the
      all-powerful `Ankusa.Principal.operator/1`. It is only accepted when the
      listener binds loopback: `Ankusa.Config.new/1` refuses `mode: :none` on
      any other address unless `allow_insecure: true` says the deployment is
      protected by something else (a mesh, a proxy, a network policy). Even
      `tls.verify: :verify_peer` does not count: it keeps strangers off the
      port but does not say what a certificate may do; use `mode: :mtls`.
    * `:token` — `Ankusa.Admin.Auth.Token`: static bearer tokens, stored as
      SHA-256 hashes, each with its scopes. For development, CI and
      machine-to-machine scripts.
    * `:jwt` — `Ankusa.Admin.Auth.JWT`: OIDC/OAuth2 access tokens signed by
      your identity provider, verified against its JWKS.
    * `:mtls` — `Ankusa.Admin.Auth.MTLS`: the verified client certificate of a
      `tls.verify: :verify_peer` listener, mapped to a principal by subject.
      Requires that listener's `tls` block.

  Further modes are modules implementing `c:authenticate/2`.

  ## Responses

    * `401` `{"error":"unauthenticated"}` with `www-authenticate: Bearer
      realm="ankusa"` when no credential, or a bad one, was presented. The
      response never says which.
    * `403` `{"error":"forbidden","scope":"dlq:read"}` when the principal lacks
      the named scope. A tenant scope names `tenant:<id>`.
  """

  import Plug.Conn

  alias Ankusa.{Config, Http, Principal, Verifier}

  @typedoc "Which control plane the plug guards."
  @type section :: :admin | :routes_admin | :claim_check

  @typedoc "What an implementation of `c:authenticate/2` is given."
  @type opts :: %{instance: atom(), section: section(), config: map()}

  @doc """
  Identify the caller of `conn`: a principal, or `{:error, :unauthenticated}`
  for a missing, malformed, expired or unknown credential. `opts.config` is the
  listener's normalised `auth` block.
  """
  @callback authenticate(Plug.Conn.t(), opts()) ::
              {:ok, Principal.t()} | {:error, :unauthenticated}

  @modes %{
    none: nil,
    token: Ankusa.Admin.Auth.Token,
    jwt: Ankusa.Admin.Auth.JWT,
    mtls: Ankusa.Admin.Auth.MTLS
  }

  @exempt [["health"], ["ready"]]
  @challenge ~s(Bearer realm="ankusa")
  @max_token_bytes 8_192

  @defaults %{
    mode: :none,
    allow_insecure: false,
    tokens: [],
    jwt: %{
      issuer: nil,
      audience: nil,
      jwks_url: nil,
      keys: [],
      refresh_ms: 300_000,
      min_refresh_ms: 10_000,
      leeway_s: 30,
      scope_claim: "scope",
      tenants_claim: "ankusa_tenants"
    },
    mtls: %{subject_map: %{}}
  }

  # ── the plug ────────────────────────────────────────────────────────────────

  @doc false
  @spec init(keyword()) :: section()
  def init(opts), do: Keyword.fetch!(opts, :section)

  @doc false
  @spec call(Plug.Conn.t(), section()) :: Plug.Conn.t()
  def call(%Plug.Conn{path_info: path} = conn, _section) when path in @exempt do
    assign(conn, :principal, %Principal{id: "anonymous"})
  end

  def call(conn, section) do
    instance = Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
    auth = instance |> Ankusa.config() |> auth_config(section)

    case authenticate(conn, instance, section, auth) do
      {:ok, %Principal{} = principal} -> assign(conn, :principal, principal)
      {:error, :unauthenticated} -> unauthenticated(conn)
    end
  end

  defp authenticate(_conn, _instance, _section, %{mode: :none}), do: {:ok, Principal.operator()}

  defp authenticate(conn, instance, section, %{mode: mode} = auth) do
    module = Map.fetch!(@modes, mode)
    module.authenticate(conn, %{instance: instance, section: section, config: auth})
  end

  @doc "The `auth` block of the control plane `section`."
  @spec auth_config(Config.t(), section()) :: map()
  def auth_config(%Config{admin: %{auth: auth}}, :admin), do: auth
  def auth_config(%Config{routes: %{admin: %{auth: auth}}}, :routes_admin), do: auth
  def auth_config(%Config{claim_check: %{auth: auth}}, :claim_check), do: auth

  defp unauthenticated(conn) do
    conn
    |> put_resp_header("www-authenticate", @challenge)
    |> Http.send_json(401, %{error: "unauthenticated"})
    |> halt()
  end

  # ── authorization ───────────────────────────────────────────────────────────

  @doc """
  Run `fun` when the principal holds `scope` (or every scope in a list), else
  answer `403`.
  """
  @spec authorize(Plug.Conn.t(), String.t() | [String.t()], (Plug.Conn.t() -> Plug.Conn.t())) ::
          Plug.Conn.t()
  def authorize(conn, scope, fun) when is_function(fun, 1) do
    case missing_scope(conn.assigns.principal, List.wrap(scope)) do
      nil -> fun.(conn)
      missing -> forbidden(conn, missing)
    end
  end

  @doc """
  `authorize/3` for a resource that belongs to one tenant: the principal must
  also hold `tenant:<tenant>` or `tenant:*`.
  """
  @spec authorize_tenant(
          Plug.Conn.t(),
          String.t() | [String.t()],
          term(),
          (Plug.Conn.t() -> Plug.Conn.t())
        ) :: Plug.Conn.t()
  def authorize_tenant(conn, scope, tenant, fun) when is_function(fun, 1) do
    principal = conn.assigns.principal

    cond do
      missing = missing_scope(principal, List.wrap(scope)) -> forbidden(conn, missing)
      Principal.tenant?(principal, tenant) -> fun.(conn)
      true -> forbidden(conn, "tenant:#{tenant}")
    end
  end

  @doc """
  `authorize/3` for a resource that spans tenants (the DLQ, the quarantine pen,
  replay jobs, route definitions): the principal must also hold `tenant:*`.
  """
  @spec authorize_global(
          Plug.Conn.t(),
          String.t() | [String.t()],
          (Plug.Conn.t() -> Plug.Conn.t())
        ) :: Plug.Conn.t()
  def authorize_global(conn, scope, fun) when is_function(fun, 1) do
    principal = conn.assigns.principal

    cond do
      missing = missing_scope(principal, List.wrap(scope)) -> forbidden(conn, missing)
      Principal.all_tenants?(principal) -> fun.(conn)
      true -> forbidden(conn, "tenant:*")
    end
  end

  defp missing_scope(principal, scopes),
    do: Enum.find(scopes, &(not Principal.scope?(principal, &1)))

  # Halted, so `authorize_global/3` also works as a plug ahead of `:dispatch`.
  defp forbidden(conn, scope) do
    conn
    |> Http.send_json(403, %{error: "forbidden", scope: scope})
    |> halt()
  end

  # ── credentials ─────────────────────────────────────────────────────────────

  @doc """
  The bearer token of `conn`: exactly one `Authorization: Bearer <token>`
  header, the scheme matched case-insensitively. A bare or oversized token, a
  second header, or another scheme is `:error`.
  """
  @spec bearer(Plug.Conn.t()) :: {:ok, binary()} | :error
  def bearer(conn) do
    case get_req_header(conn, "authorization") do
      [<<scheme::binary-size(6), " ", token::binary>>]
      when byte_size(token) in 1..@max_token_bytes ->
        if String.downcase(scheme) == "bearer" and not String.contains?(token, " "),
          do: {:ok, token},
          else: :error

      _ ->
        :error
    end
  end

  @doc "Constant-time comparison of two equal-length binaries."
  @spec secure_compare(binary(), binary()) :: boolean()
  defdelegate secure_compare(a, b), to: Verifier

  # ── configuration ───────────────────────────────────────────────────────────

  @doc """
  Validate and normalise one `auth` block of a listener. `key` is its dotted
  config name (`"admin.auth"`) and `listener` the section it sits in (its `ip`
  and `tls` are what `mode: :none` and `mode: :mtls` are checked against).
  `enabled?` says whether this instance runs the listener: a listener that is
  not started is not held to the loopback rule. Raises `ArgumentError` naming
  the key.
  """
  @spec normalize!(
          term(),
          String.t(),
          %{:ip => term(), :tls => map() | nil, optional(atom()) => term()},
          boolean()
        ) ::
          map()
  def normalize!(auth, key, %{ip: ip, tls: tls}, enabled?) do
    given = map!(auth, key)
    unknown_keys!(given, @defaults, key)
    auth = Map.merge(@defaults, given)

    mode = mode!(auth.mode, key)
    allow_insecure = boolean!(auth.allow_insecure, "#{key}.allow_insecure")
    tokens = tokens!(auth.tokens, "#{key}.tokens")
    jwt = jwt!(auth.jwt, "#{key}.jwt")
    mtls = mtls!(auth.mtls, "#{key}.mtls")

    case mode do
      :none -> if enabled?, do: none_needs_loopback!(ip, allow_insecure, key)
      :token -> if tokens == [], do: raise_needs(key, "tokens", "a non-empty list", mode)
      :jwt -> jwt_complete!(jwt, key)
      :mtls -> mtls_complete!(mtls, tls, key)
    end

    %{mode: mode, allow_insecure: allow_insecure, tokens: tokens, jwt: jwt, mtls: mtls}
  end

  defp none_needs_loopback!(ip, allow_insecure, key) do
    ip_key = key |> String.split(".") |> Enum.drop(-1) |> Enum.concat(["ip"]) |> Enum.join(".")
    address = Config.listen_ip!(ip, ip_key)

    unless allow_insecure or loopback?(address) do
      raise ArgumentError,
            "#{key}.mode is :none but #{ip_key} (#{:inet.ntoa(address)}) is not a loopback address: " <>
              "set #{key}.mode to :jwt, :mtls or :token, or #{key}.allow_insecure: true if " <>
              "something in front of the listener authenticates every caller"
    end
  end

  defp loopback?({127, _, _, _}), do: true
  defp loopback?({0, 0, 0, 0, 0, 0, 0, 1}), do: true
  defp loopback?({0, 0, 0, 0, 0, 0xFFFF, high, _low}), do: Bitwise.bsr(high, 8) == 127
  defp loopback?(_ip), do: false

  defp raise_needs(key, field, what, mode) do
    raise ArgumentError, "#{key}.#{field} must be #{what} with #{key}.mode: #{inspect(mode)}"
  end

  defp mode!(mode, _key) when is_map_key(@modes, mode), do: mode

  defp mode!(other, key) do
    raise ArgumentError,
          "#{key}.mode must be one of #{inspect(Map.keys(@modes) |> Enum.sort())}, got #{inspect(other)}"
  end

  # tokens ────────────────────────────────────────────────────────────────────

  defp tokens!(tokens, key) when is_list(tokens) do
    normalised =
      tokens
      |> Enum.with_index()
      |> Enum.map(fn {token, index} -> token!(token, "#{key}[#{index}]") end)

    ids = Enum.map(normalised, & &1.id)

    if length(Enum.uniq(ids)) != length(ids) do
      raise ArgumentError, "#{key} has two entries with the same id"
    end

    normalised
  end

  defp tokens!(other, key),
    do: raise(ArgumentError, "#{key} must be a list of token entries, got #{inspect(other)}")

  @token_keys [:id, :sha256, :scopes]

  defp token!(entry, key) do
    given = map!(entry, key)
    unknown_keys!(given, Map.new(@token_keys, &{&1, nil}), key)

    id = Map.get(given, :id)

    unless is_binary(id) and byte_size(id) in 1..128 and String.printable?(id) do
      raise ArgumentError, "#{key}.id must be a non-empty printable string, got #{inspect(id)}"
    end

    sha256 = Map.get(given, :sha256)

    unless is_binary(sha256) and Regex.match?(~r/\A[0-9a-fA-F]{64}\z/, sha256) do
      raise ArgumentError,
            "#{key}.sha256 must be the 64-character hex SHA-256 of the token " <>
              "(`printf %s \"$TOKEN\" | shasum -a 256`)"
    end

    %{
      id: id,
      sha256: String.downcase(sha256),
      scopes: scopes!(Map.get(given, :scopes), "#{key}.scopes")
    }
  end

  # A non-empty list of known scopes, at least one of them an operation scope.
  defp scopes!(scopes, key) when is_list(scopes) and scopes != [] do
    case Principal.parse_scopes(scopes) do
      {[], _tenants, _unknown} ->
        raise ArgumentError, "#{key} must name at least one scope besides tenant scopes"

      {_scopes, _tenants, []} ->
        scopes

      {_scopes, _tenants, unknown} ->
        raise ArgumentError,
              "#{key} has unknown scopes #{inspect(unknown)}; known: " <>
                "#{Enum.join(Principal.scopes(), ", ")}, tenant:<id>, tenant:*"
    end
  end

  defp scopes!(other, key) do
    raise ArgumentError, "#{key} must be a non-empty list of scope strings, got #{inspect(other)}"
  end

  # jwt ───────────────────────────────────────────────────────────────────────

  defp jwt!(jwt, key) do
    given = map!(jwt, key)
    unknown_keys!(given, @defaults.jwt, key)
    jwt = Map.merge(@defaults.jwt, given)

    jwt
    |> optional_string!(:issuer, key)
    |> audience!(key)
    |> jwks_url!(key)
    |> static_keys!(key)
    |> int_in!(:refresh_ms, 1_000..86_400_000, key)
    |> int_in!(:min_refresh_ms, 0..86_400_000, key)
    |> int_in!(:leeway_s, 0..3_600, key)
    |> claim_name!(:scope_claim, key)
    |> claim_name!(:tenants_claim, key)
  end

  defp jwt_complete!(jwt, key) do
    if is_nil(jwt.issuer), do: raise_needs(key, "jwt.issuer", "set", :jwt)
    if jwt.audience in [nil, []], do: raise_needs(key, "jwt.audience", "set", :jwt)

    if is_nil(jwt.jwks_url) and jwt.keys == [] do
      raise ArgumentError,
            "#{key}.jwt needs #{key}.jwt.jwks_url or #{key}.jwt.keys with #{key}.mode: :jwt"
    end
  end

  defp optional_string!(jwt, field, key) do
    case Map.fetch!(jwt, field) do
      nil ->
        jwt

      value when is_binary(value) and value != "" ->
        jwt

      other ->
        raise ArgumentError, "#{key}.#{field} must be a non-empty string, got #{inspect(other)}"
    end
  end

  defp audience!(jwt, key) do
    case jwt.audience do
      nil ->
        jwt

      aud when is_binary(aud) and aud != "" ->
        jwt

      [_ | _] = auds ->
        if Enum.all?(auds, &(is_binary(&1) and &1 != "")),
          do: jwt,
          else: raise(ArgumentError, "#{key}.audience must be a string or a list of strings")

      other ->
        raise ArgumentError,
              "#{key}.audience must be a string or a list of strings, got #{inspect(other)}"
    end
  end

  # The JWKS is fetched over TLS: a plain-HTTP URL would let anyone on the path
  # mint tokens, so http is accepted for loopback hosts only (a sidecar, a test).
  defp jwks_url!(jwt, key) do
    case jwt.jwks_url do
      nil ->
        jwt

      url when is_binary(url) ->
        case URI.parse(url) do
          %URI{scheme: "https", host: host} when is_binary(host) and host != "" ->
            jwt

          %URI{scheme: "http", host: host} when is_binary(host) ->
            if loopback_host?(host),
              do: jwt,
              else: raise(ArgumentError, "#{key}.jwks_url must be https:// (got #{inspect(url)})")

          _ ->
            raise ArgumentError, "#{key}.jwks_url must be an https:// URL, got #{inspect(url)}"
        end

      other ->
        raise ArgumentError, "#{key}.jwks_url must be a URL string, got #{inspect(other)}"
    end
  end

  defp loopback_host?("localhost"), do: true

  defp loopback_host?(host) do
    case :inet.parse_strict_address(String.to_charlist(host)) do
      {:ok, ip} -> loopback?(ip)
      {:error, _} -> false
    end
  end

  defp static_keys!(jwt, key) do
    case jwt.keys do
      keys when is_list(keys) ->
        keys
        |> Enum.with_index()
        |> Enum.each(fn {k, i} -> public_key!(k, "#{key}.keys[#{i}]") end)

        jwt

      other ->
        raise ArgumentError,
              "#{key}.keys must be a list of JWKs or PEM public keys, got #{inspect(other)}"
    end
  end

  @doc false
  # Parse one configured key (a JWK map or a PEM string) into a `JOSE.JWK`,
  # raising for anything that is not an asymmetric public key.
  @spec public_key!(term(), String.t()) :: JOSE.JWK.t()
  def public_key!(key, name) do
    jwk =
      try do
        case key do
          map when is_map(map) -> map |> stringify() |> JOSE.JWK.from_map()
          pem when is_binary(pem) -> JOSE.JWK.from_pem(pem)
        end
      rescue
        _ -> :error
      catch
        _, _ -> :error
      end

    with %JOSE.JWK{} = jwk <- jwk,
         {_, %{"kty" => kty} = fields} when kty in ["RSA", "EC", "OKP"] <- JOSE.JWK.to_map(jwk),
         false <- Map.has_key?(fields, "d") do
      jwk
    else
      _ ->
        raise ArgumentError,
              "#{name} must be a public RSA, EC or OKP key as a JWK or PEM (private and shared keys are refused)"
    end
  end

  defp stringify(map), do: Map.new(map, fn {k, v} -> {to_string(k), v} end)

  defp int_in!(jwt, field, range, key) do
    value = Map.fetch!(jwt, field)

    if is_integer(value) and value in range,
      do: jwt,
      else:
        raise(
          ArgumentError,
          "#{key}.#{field} must be an integer from #{range.first} to #{range.last}, got #{inspect(value)}"
        )
  end

  defp claim_name!(jwt, field, key) do
    case Map.fetch!(jwt, field) do
      name when is_binary(name) and name != "" -> jwt
      other -> raise ArgumentError, "#{key}.#{field} must be a claim name, got #{inspect(other)}"
    end
  end

  # mtls ──────────────────────────────────────────────────────────────────────

  defp mtls!(mtls, key) do
    given = map!(mtls, key)
    unknown_keys!(given, @defaults.mtls, key)

    subject_map =
      case Map.get(given, :subject_map, %{}) do
        map when is_map(map) ->
          Map.new(map, fn {subject, scopes} ->
            subject = to_string(subject)

            unless subject_form?(subject) do
              raise ArgumentError,
                    "#{key}.subject_map key #{inspect(subject)} must start with " <>
                      "cn:, dns:, email:, uri: or ip:"
            end

            {subject, scopes!(scopes, "#{key}.subject_map[#{inspect(subject)}]")}
          end)

        other ->
          raise ArgumentError, "#{key}.subject_map must be a map, got #{inspect(other)}"
      end

    %{subject_map: subject_map}
  end

  defp subject_form?(subject) do
    case String.split(subject, ":", parts: 2) do
      [form, rest] -> form in ~w(cn dns email uri ip) and rest != ""
      _ -> false
    end
  end

  defp mtls_complete!(mtls, tls, key) do
    if mtls.subject_map == %{}, do: raise_needs(key, "mtls.subject_map", "a non-empty map", :mtls)

    unless match?(%{verify: :verify_peer}, tls) do
      tls_key =
        key |> String.split(".") |> Enum.drop(-1) |> Enum.concat(["tls"]) |> Enum.join(".")

      raise ArgumentError,
            "#{key}.mode: :mtls needs #{tls_key} with verify: :verify_peer and a cacertfile: " <>
              "the client certificate is what identifies the caller"
    end
  end

  # helpers ───────────────────────────────────────────────────────────────────

  defp map!(value, _key) when is_map(value), do: Map.new(value)

  defp map!(value, key) when is_list(value) do
    if Keyword.keyword?(value),
      do: Map.new(value),
      else: raise(ArgumentError, "#{key} must be a map or keyword list")
  end

  defp map!(other, key),
    do: raise(ArgumentError, "#{key} must be a map or keyword list, got #{inspect(other)}")

  defp unknown_keys!(given, known, key) do
    for k <- Map.keys(given), not Map.has_key?(known, k) do
      raise ArgumentError, "unknown Ankusa.Config key: #{key}.#{k}"
    end
  end

  defp boolean!(value, _key) when is_boolean(value), do: value

  defp boolean!(other, key),
    do: raise(ArgumentError, "#{key} must be true or false, got #{inspect(other)}")
end
