defmodule AnkusaGatewayServer.GatewaySection do
  @moduledoc """
  The `gateway:` section of a `config_version: 2` file, as
  `Ankusa.Gateway.Config` (which checks every range and cross-key rule; its
  messages name the dotted key). Both images read it: `jamescarr/ankusa` for a
  node whose `node.roles` lists `claim_check`, `jamescarr/ankusa-gateway`
  always.
  """

  import AnkusaGatewayServer.Schema

  import AnkusaGatewayServer.Sections,
    only: [claim_store!: 2, jwt_opts: 2, tls_opt: 2, token_sha256!: 2]

  alias AnkusaGatewayServer.ConfigError

  @auth_jwt_keys ~w(issuer audience jwks_url keys refresh_ms min_refresh_ms leeway_s scope_claim tenants_claim)
  @auth_mtls_keys ~w(subject_map)
  @gateway_keys ~w(port ip tls auth authz max_concurrent_requests claims spaces objects sharing)
  @gateway_sharing_ints ~w(link_max_ttl_s acl_cache_ttl_ms acl_cache_max_entries acl_cache_max_bytes
                           max_grants_per_resource)a
  @gateway_auth_keys ~w(allow_insecure authenticators cache)
  @gateway_auth_cache_keys ~w(ttl_ms negative_ttl_ms max_entries max_bytes)a
  @gateway_authz_keys ~w(deadline_ms cache authorizers policies)
  @gateway_authz_cache_keys ~w(max_entries max_bytes allow_ttl_ms deny_ttl_ms refresh_ahead serve_stale_ms)a
  @gateway_policy_keys ~w(mode steps)
  @gateway_step_keys ~w(use on_unavailable)
  # Every authorizer type the YAML knows, and its module.
  @authorizer_types %{"rules" => Ankusa.Gateway.Authz.Rules}
  # A rule's keys and its `when` keys, string to atom (closed sets).
  @rule_keys Map.new(~w(effect actions spaces keys when)a, &{Atom.to_string(&1), &1})
  @rule_when_keys Map.new(
                    ~w(groups_any groups_all scopes_any scopes_all tenants_any principals claims)a,
                    &{Atom.to_string(&1), &1}
                  )
  @gateway_authenticator_types ~w(token jwt mtls)
  @gateway_token_keys ~w(id token sha256 scopes groups claims)
  @gateway_jwt_keys @auth_jwt_keys ++ ~w(groups_claim header subject_prefix)
  @gateway_claims_keys ~w(enabled policy integrity_check)
  @gateway_space_keys ~w(store prefix policy max_object_bytes read_first_bytes read_only)
  @gateway_objects_keys ~w(max_inflight_bytes op_deadline_ms timeout_ms retries)

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
      |> put_opt(:authz, gateway_authz(gateway, path))
      |> put_opt(:claims, gateway_claims(gateway, path))
      |> put_opt(:spaces, gateway_spaces(gateway, path))
      |> put_opt(:objects, gateway_objects(gateway, path))
      |> put_opt(:sharing, gateway_sharing(gateway, path))

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
        |> put_opt(:cache, int_section(auth, "cache", @gateway_auth_cache_keys, path))
    end
  end

  # A section whose keys (atoms, a closed set) all take integers.
  defp int_section(parent, key, keys, path) do
    case parent[key] do
      nil ->
        nil

      value ->
        path = path ++ [key]
        section = section!(value, Enum.map(keys, &Atom.to_string/1), path)
        Enum.reduce(keys, [], &put_opt(&2, &1, int_opt(section, Atom.to_string(&1), path)))
    end
  end

  # ── authz ───────────────────────────────────────────────────────────────────

  defp gateway_authz(gateway, path) do
    case gateway["authz"] do
      nil ->
        nil

      value ->
        path = path ++ ["authz"]
        authz = section!(value, @gateway_authz_keys, path)

        []
        |> put_opt(:deadline_ms, int_opt(authz, "deadline_ms", path))
        |> put_opt(:cache, authz_cache(authz, path))
        |> put_opt(:authorizers, authorizers(authz, path))
        |> put_opt(:policies, policies(authz, path))
    end
  end

  defp authz_cache(authz, path) do
    case authz["cache"] do
      nil ->
        nil

      value ->
        ints = @gateway_authz_cache_keys -- [:refresh_ahead]

        cache =
          int_section(
            %{"cache" => Map.delete(expect_map(value, path ++ ["cache"]), "refresh_ahead")},
            "cache",
            ints,
            path
          )

        put_opt(cache, :refresh_ahead, bool_opt(value, "refresh_ahead", path ++ ["cache"]))
    end
  end

  defp authorizers(authz, path) do
    case authz["authorizers"] do
      nil ->
        nil

      %{} = map ->
        Map.new(map, fn {name, entry} ->
          name = to_string(name)
          {name, authorizer_entry!(entry, path ++ ["authorizers", name])}
        end)

      other ->
        raise ConfigError,
          message: type_error(path ++ ["authorizers"], "a mapping of authorizers", other)
    end
  end

  # `{type: <t>, ...}`: the type picks the module; the rest are its options,
  # which the module validates (its messages name the dotted key).
  defp authorizer_entry!(entry, path) do
    map = expect_map(entry, path)

    case Map.fetch(@authorizer_types, map["type"]) do
      {:ok, module} ->
        {module, authorizer_opts(module, Map.delete(map, "type"), path)}

      :error ->
        known = @authorizer_types |> Map.keys() |> Enum.sort() |> Enum.join(", ")

        raise ConfigError,
          message:
            "#{render_path(path)}.type: unknown type #{inspect(map["type"])} (known: #{known})"
    end
  end

  defp authorizer_opts(Ankusa.Gateway.Authz.Rules, map, path) do
    section!(map, ~w(rules), path)

    case map["rules"] do
      list when is_list(list) ->
        rules =
          list
          |> Enum.with_index()
          |> Enum.map(fn {rule, index} -> rule!(rule, path ++ ["rules", index]) end)

        %{rules: rules}

      other ->
        raise ConfigError, message: type_error(path ++ ["rules"], "a list of rules", other)
    end
  end

  # The values (`effect`, `actions`, patterns, templates) are the module's to
  # check; its messages name the same dotted key.
  defp rule!(rule, path) do
    rule = section!(rule, Map.keys(@rule_keys), path)

    Map.new(rule, fn
      {"when", value} ->
        conditions = section!(value, Map.keys(@rule_when_keys), path ++ ["when"])
        {:when, Map.new(conditions, fn {k, v} -> {Map.fetch!(@rule_when_keys, k), v} end)}

      {key, value} ->
        {Map.fetch!(@rule_keys, key), value}
    end)
  end

  defp policies(authz, path) do
    case authz["policies"] do
      nil ->
        nil

      %{} = map ->
        Map.new(map, fn {name, policy} ->
          name = to_string(name)
          {name, policy!(policy, path ++ ["policies", name])}
        end)

      other ->
        raise ConfigError,
          message: type_error(path ++ ["policies"], "a mapping of policies", other)
    end
  end

  defp policy!(policy, path) do
    policy = section!(policy, @gateway_policy_keys, path)

    %{}
    |> put_map(:mode, atom_enum_opt(policy, "mode", ~w(first all any), path))
    |> put_map(:steps, steps!(policy["steps"], path ++ ["steps"]))
  end

  defp steps!(list, path) when is_list(list) do
    list
    |> Enum.with_index()
    |> Enum.map(fn
      {name, _index} when is_binary(name) ->
        name

      {step, index} ->
        step_path = path ++ [index]
        step = section!(step, @gateway_step_keys, step_path)

        %{use: step["use"]}
        |> put_map(
          :on_unavailable,
          atom_enum_opt(step, "on_unavailable", ~w(fail abstain deny), step_path)
        )
    end)
  end

  defp steps!(other, path),
    do: raise(ConfigError, message: type_error(path, "a list of steps", other))

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

  # ── spaces and objects ──────────────────────────────────────────────────────

  # A space's `store` keeps the `claim_check.store` shape, so YAML reaches the
  # stores that parser knows (`local` with a `root`, `s3`, `gcs`). Every range
  # and cross-space rule is `Ankusa.Gateway.Spaces`'s, which names the same
  # dotted key; a missing `store` is left to it, so the message names it too.
  defp gateway_spaces(gateway, path) do
    case gateway["spaces"] do
      nil ->
        nil

      %{} = spaces ->
        path = path ++ ["spaces"]

        Map.new(spaces, fn {name, space} ->
          name = to_string(name)
          {name, gateway_space!(space, path ++ [name])}
        end)

      other ->
        raise ConfigError,
          message: type_error(path ++ ["spaces"], "a mapping of spaces", other)
    end
  end

  defp gateway_space!(entry, path) do
    space = section!(entry, @gateway_space_keys, path)

    %{}
    |> put_map(:store, space["store"] && claim_store!(space["store"], path ++ ["store"]))
    |> put_map(:prefix, string_opt(space, "prefix", path))
    |> put_map(:policy, string_opt(space, "policy", path))
    |> put_map(:max_object_bytes, int_opt(space, "max_object_bytes", path))
    |> put_map(:read_first_bytes, int_opt(space, "read_first_bytes", path))
    |> put_map(:read_only, bool_opt(space, "read_only", path))
  end

  defp gateway_objects(gateway, path) do
    case gateway["objects"] do
      nil ->
        nil

      value ->
        path = path ++ ["objects"]
        objects = section!(value, @gateway_objects_keys, path)

        []
        |> put_opt(:max_inflight_bytes, int_opt(objects, "max_inflight_bytes", path))
        |> put_opt(:op_deadline_ms, int_opt(objects, "op_deadline_ms", path))
        |> put_opt(:timeout_ms, int_opt(objects, "timeout_ms", path))
        |> put_opt(:retries, int_opt(objects, "retries", path))
    end
  end

  # `signing_keys` is a list or one comma-separated string (`${VAR}`), as
  # `claim_check.ref_signing_keys`; empty or null leaves it unset.
  defp gateway_sharing(gateway, path) do
    case gateway["sharing"] do
      nil ->
        nil

      value ->
        path = path ++ ["sharing"]
        keys = ~w(enabled signing_keys) ++ Enum.map(@gateway_sharing_ints, &Atom.to_string/1)
        sharing = section!(value, keys, path)

        @gateway_sharing_ints
        |> Enum.reduce([], &put_opt(&2, &1, int_opt(sharing, Atom.to_string(&1), path)))
        |> put_opt(:enabled, bool_opt(sharing, "enabled", path))
        |> put_opt(:signing_keys, signing_keys(sharing["signing_keys"], path ++ ["signing_keys"]))
    end
  end

  defp signing_keys(empty, _path) when empty in [nil, [], ""], do: nil
  defp signing_keys(keys, path), do: string_list!(keys, path)
end
