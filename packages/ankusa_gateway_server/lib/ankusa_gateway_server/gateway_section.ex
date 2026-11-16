defmodule AnkusaGatewayServer.GatewaySection do
  @moduledoc """
  The `gateway:` section of a `config_version: 2` file, as
  `Ankusa.Gateway.Config` (which checks every range and cross-key rule; its
  messages name the dotted key). Both images read it: `jamescarr/ankusa` for a
  node whose `node.roles` lists `claim_check`, `jamescarr/ankusa-gateway`
  always.
  """

  import AnkusaGatewayServer.Schema
  import AnkusaGatewayServer.Sections, only: [jwt_opts: 2, tls_opt: 2, token_sha256!: 2]

  alias AnkusaGatewayServer.ConfigError

  @auth_jwt_keys ~w(issuer audience jwks_url keys refresh_ms min_refresh_ms leeway_s scope_claim tenants_claim)
  @auth_mtls_keys ~w(subject_map)
  @gateway_keys ~w(port ip tls auth max_concurrent_requests claims)
  @gateway_auth_keys ~w(allow_insecure authenticators)
  @gateway_authenticator_types ~w(token jwt mtls)
  @gateway_token_keys ~w(id token sha256 scopes groups claims)
  @gateway_jwt_keys @auth_jwt_keys ++ ~w(groups_claim header subject_prefix)
  @gateway_claims_keys ~w(enabled policy integrity_check)

  @doc """
  The gateway's config from `doc`. `check_exposure: false` accepts an empty
  authenticator chain on a non-loopback address (a node that does not run the
  gateway still has its section checked). Raises `AnkusaGatewayServer.ConfigError`.
  """
  @spec parse!(map(), keyword()) :: Ankusa.Gateway.Config.t()
  def parse!(doc, opts), do: gateway_section(doc, Keyword.fetch!(opts, :check_exposure))

  # ── gateway ─────────────────────────────────────────────────────────────────

  # The `gateway:` section, as `Ankusa.Gateway.Config` (which checks every
  # range and cross-key rule; its messages name the dotted key). Validated
  # whether or not this node runs the gateway, so `check-config` catches a bad
  # one; an empty chain off loopback is only refused when it does (`gateway?`).
  defp gateway_section(doc, gateway?) do
    gateway = section!(doc, "gateway", @gateway_keys, [])
    path = ["gateway"]

    opts =
      []
      |> put_opt(:port, int_opt(gateway, "port", path))
      |> put_opt(:ip, string_opt(gateway, "ip", path))
      |> put_opt(:tls, tls_opt(gateway, path))
      |> put_opt(:max_concurrent_requests, int_opt(gateway, "max_concurrent_requests", path))
      |> put_opt(:auth, gateway_auth(gateway, path))
      |> put_opt(:claims, gateway_claims(gateway, path))

    try do
      Ankusa.Gateway.Config.new(opts, check_exposure: gateway?)
    rescue
      error in ArgumentError ->
        reraise ConfigError, [message: error.message], __STACKTRACE__
    end
  end

  defp gateway_auth(gateway, path) do
    case gateway["auth"] do
      nil ->
        nil

      value ->
        path = path ++ ["auth"]
        auth = section!(value, @gateway_auth_keys, path)

        []
        |> put_opt(:allow_insecure, bool_opt(auth, "allow_insecure", path))
        |> put_opt(:authenticators, gateway_authenticators(auth, path))
    end
  end

  defp gateway_authenticators(auth, path) do
    case auth["authenticators"] do
      nil ->
        nil

      list when is_list(list) ->
        list
        |> Enum.with_index()
        |> Enum.map(fn {entry, index} ->
          authenticator!(entry, path ++ ["authenticators", index])
        end)

      other ->
        raise ConfigError,
          message: type_error(path ++ ["authenticators"], "a list of authenticators", other)
    end
  end

  # One authenticator: its `type` picks the keys it may have. The atoms are a
  # closed set (`enum!/3` first).
  # sobelow_skip ["DOS.StringToAtom"]
  defp authenticator!(entry, path) do
    map = expect_map(entry, path)
    type = enum!(map["type"], @gateway_authenticator_types, path ++ ["type"])
    entry = section!(map, ~w(type name) ++ authenticator_keys(type), path)

    [type: String.to_atom(type)]
    |> put_opt(:name, string_opt(entry, "name", path))
    |> Kernel.++(authenticator_opts(type, entry, path))
  end

  defp authenticator_keys("token"), do: ~w(tokens)

  defp authenticator_keys("jwt"), do: @gateway_jwt_keys

  defp authenticator_keys("mtls"), do: @auth_mtls_keys

  defp authenticator_opts("token", entry, path) do
    case entry["tokens"] do
      list when is_list(list) ->
        tokens =
          list
          |> Enum.with_index()
          |> Enum.map(fn {token, index} -> gateway_token!(token, path ++ ["tokens", index]) end)

        [tokens: tokens]

      other ->
        raise ConfigError,
          message: type_error(path ++ ["tokens"], "a list of token entries", other)
    end
  end

  defp authenticator_opts("jwt", entry, path) do
    entry
    |> jwt_opts(path)
    |> put_opt(:groups_claim, string_opt(entry, "groups_claim", path))
    |> put_opt(:header, string_opt(entry, "header", path))
    |> put_opt(:subject_prefix, string_opt(entry, "subject_prefix", path))
  end

  defp authenticator_opts("mtls", entry, path) do
    case entry["subject_map"] do
      map when is_map(map) ->
        [
          subject_map:
            Map.new(map, fn {identity, value} ->
              identity = to_string(identity)
              {identity, subject_entry(value, path ++ ["subject_map", identity])}
            end)
        ]

      other ->
        raise ConfigError, message: type_error(path ++ ["subject_map"], "a mapping", other)
    end
  end

  defp subject_entry(%{} = map, path) do
    entry = section!(map, ~w(scopes groups), path)

    %{}
    |> put_map(:scopes, entry["scopes"] && string_list!(entry["scopes"], path ++ ["scopes"]))
    |> put_map(:groups, entry["groups"] && string_list!(entry["groups"], path ++ ["groups"]))
  end

  defp subject_entry(scopes, path), do: string_list!(scopes, path)

  # As a control plane's token (`auth_token!/2`): the plaintext is hashed here.
  defp gateway_token!(entry, path) do
    token = section!(entry, @gateway_token_keys, path)

    %{id: required_string!(token, "id", path), sha256: token_sha256!(token, path)}
    |> put_map(:scopes, token["scopes"] && string_list!(token["scopes"], path ++ ["scopes"]))
    |> put_map(:groups, token["groups"] && string_list!(token["groups"], path ++ ["groups"]))
    |> put_map(:claims, token["claims"] && expect_map(token["claims"], path ++ ["claims"]))
  end

  defp gateway_claims(gateway, path) do
    case gateway["claims"] do
      nil ->
        nil

      value ->
        path = path ++ ["claims"]
        claims = section!(value, @gateway_claims_keys, path)

        []
        |> put_opt(:enabled, bool_opt(claims, "enabled", path))
        |> put_opt(:policy, string_opt(claims, "policy", path))
        |> put_opt(
          :integrity_check,
          atom_enum_opt(claims, "integrity_check", ~w(off manifest), path)
        )
    end
  end
end
