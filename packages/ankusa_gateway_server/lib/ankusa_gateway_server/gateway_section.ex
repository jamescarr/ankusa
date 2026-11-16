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
    only: [claim_store!: 2, jwt_opts: 2, jwt_audience: 2, tls_opt: 2, token_sha256!: 2]

  alias AnkusaGatewayServer.ConfigError

  @auth_jwt_keys ~w(issuer audience jwks_url keys refresh_ms min_refresh_ms leeway_s scope_claim tenants_claim)
  @auth_mtls_keys ~w(subject_map)
  @gateway_keys ~w(port ip tls auth authz max_concurrent_requests claims spaces objects sharing)
  @gateway_sharing_ints ~w(link_max_ttl_s acl_cache_ttl_ms acl_cache_max_entries acl_cache_max_bytes
                           max_grants_per_resource)a
  @gateway_auth_keys ~w(allow_insecure authenticators cache enrichers)
  @gateway_enricher_types ~w(okta_groups http)
  @gateway_enricher_common_keys ~w(name only on_unavailable ttl_ms timeout_ms)
  @gateway_okta_groups_keys ~w(domain api_token user_id max_groups)
  @gateway_http_enricher_keys ~w(url headers groups_path claims_path)
  # A closed set, so no `String.to_atom/1` on a file's value.
  @gateway_enricher_atoms %{"okta_groups" => :okta_groups, "http" => :http}
  @max_enrichers 4
  @gateway_auth_cache_keys ~w(ttl_ms negative_ttl_ms max_entries max_bytes)a
  @gateway_authz_keys ~w(deadline_ms cache authorizers policies)
  @gateway_authz_cache_keys ~w(max_entries max_bytes allow_ttl_ms deny_ttl_ms refresh_ahead serve_stale_ms)a
  @gateway_policy_keys ~w(mode steps)
  @gateway_step_keys ~w(use on_unavailable)
  # Every authorizer type the YAML knows, and its module.
  @authorizer_types %{
    "rules" => Ankusa.Gateway.Authz.Rules,
    "http" => Ankusa.Gateway.Authz.Http,
    "openfga" => Ankusa.Gateway.Authz.OpenFGA,
    "table" => Ankusa.Gateway.Authz.Table
  }
  # The common `:remote` options, as integers; the module checks their ranges.
  @remote_authorizer_keys ~w(timeout_ms max_inflight queue_max batch_max batch_window_ms
                             breaker_failures breaker_open_ms allow_ttl_ms deny_ttl_ms
                             allow_ttl_max_ms headers allow_insecure_http)
  # The integer remote keys, as option atoms (a literal map: a name must never
  # create an atom).
  @remote_authorizer_int_keys %{
    "timeout_ms" => :timeout_ms,
    "max_inflight" => :max_inflight,
    "queue_max" => :queue_max,
    "batch_max" => :batch_max,
    "batch_window_ms" => :batch_window_ms,
    "breaker_failures" => :breaker_failures,
    "breaker_open_ms" => :breaker_open_ms,
    "allow_ttl_ms" => :allow_ttl_ms,
    "deny_ttl_ms" => :deny_ttl_ms,
    "allow_ttl_max_ms" => :allow_ttl_max_ms
  }
  @http_authorizer_keys ~w(protocol url evaluation_path evaluations_path forward_authorization
                           subject resource actions context request allow_path ttl_path batch) ++
                          @remote_authorizer_keys
  @openfga_authorizer_keys ~w(url store_id authorization_model_id api_token consistency user
                              object resources relations contextual_tuples batch) ++
                             @remote_authorizer_keys
  # The five action names, and the two consistency modes: closed sets, so the
  # atoms are fixed.
  @authorizer_action_keys %{
    "read" => :read,
    "write" => :write,
    "delete" => :delete,
    "list" => :list,
    "share" => :share
  }
  @consistency_keys %{
    "minimize_latency" => :minimize_latency,
    "higher_consistency" => :higher_consistency
  }
  @authorizer_batch_keys ~w(url items_key results_path)
  @openfga_resource_keys ~w(space match object)
  @openfga_tuple_keys ~w(user relation object)
  @table_keys ~w(mode source columns identities max_identities ready_timeout_ms refresh_ms max_rows
                 max_bytes stale_after_ms timeout_ms max_inflight queue_max batch_max
                 batch_window_ms breaker_failures breaker_open_ms allow_ttl_ms deny_ttl_ms
                 allow_ttl_max_ms)
  @table_remote_atoms Map.new(
                        ~w(timeout_ms max_inflight queue_max batch_max batch_window_ms
                           breaker_failures breaker_open_ms allow_ttl_ms deny_ttl_ms
                           allow_ttl_max_ms),
                        &{&1, String.to_atom(&1)}
                      )
  @table_columns %{
    "principal" => :principal,
    "action" => :action,
    "space" => :space,
    "prefix" => :prefix,
    "effect" => :effect
  }
  @table_source_types %{
    "object" => Ankusa.Gateway.Authz.Table.Source.Object,
    "http" => Ankusa.Gateway.Authz.Table.Source.Http,
    "postgres" => Ankusa.Gateway.Postgres.Source
  }
  @object_source_keys ~w(type store key prefix max_object_bytes)
  @http_source_keys ~w(type url headers rows_path next_path cursor_param timeout_ms max_pages)
  @postgres_source_keys ~w(type url hostname port database username password ssl query
                            version_query lookup_query pool_size timeout_ms)
  # A rule's keys and its `when` keys, string to atom (closed sets).
  @rule_keys Map.new(~w(effect actions spaces keys when)a, &{Atom.to_string(&1), &1})
  @rule_when_keys Map.new(
                    ~w(groups_any groups_all scopes_any scopes_all tenants_any principals claims)a,
                    &{Atom.to_string(&1), &1}
                  )
  @gateway_authenticator_types ~w(token jwt mtls okta introspection)
  @gateway_token_keys ~w(id token sha256 scopes groups claims)
  @gateway_jwt_keys @auth_jwt_keys ++ ~w(groups_claim header subject_prefix)
  @gateway_introspection_keys ~w(client_id client_secret url token_type_hint timeout_ms max_inflight
                                 queue_max issuer audience subject_claim scope_claim tenants_claim
                                 groups_claim subject_prefix)
  # `okta` takes the jwt block's keys and the introspection block's: which of
  # them apply is its `authorization_server` ("org" introspects).
  @gateway_okta_keys Enum.uniq(
                       @gateway_introspection_keys ++
                         @gateway_jwt_keys ++ ~w(domain authorization_server)
                     )
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
        |> put_opt(:enrichers, gateway_enrichers(auth, path))
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

  # `type: http`: the protocol picks its keys; action names and a bulk
  # endpoint block are converted here, everything else is the module's to
  # check (its messages name the same dotted key).
  defp authorizer_opts(Ankusa.Gateway.Authz.Http, map, path) do
    given = section!(map, @http_authorizer_keys, path)

    %{}
    |> put_map(:protocol, enum_opt(given, "protocol", ~w(authzen template), path))
    |> put_map(:url, given["url"])
    |> put_map(:evaluation_path, given["evaluation_path"])
    |> put_map(:evaluations_path, given["evaluations_path"])
    |> put_map(:forward_authorization, bool_opt(given, "forward_authorization", path))
    |> put_map(:subject, given["subject"])
    |> put_map(:resource, given["resource"])
    |> put_map(:actions, actions!(given["actions"], path ++ ["actions"]))
    |> put_map(:context, given["context"])
    |> put_map(:request, given["request"])
    |> put_map(:allow_path, given["allow_path"])
    |> put_map(:ttl_path, given["ttl_path"])
    |> put_map(:batch, http_batch!(given["batch"], path ++ ["batch"]))
    |> put_map(:headers, given["headers"])
    |> put_map(:allow_insecure_http, bool_opt(given, "allow_insecure_http", path))
    |> remote_authorizer_opts(given, path)
  end

  # `type: openfga`: the relation map and the consistency mode are closed
  # sets; resources and contextual tuples keep their string leaves for the
  # module.
  defp authorizer_opts(Ankusa.Gateway.Authz.OpenFGA, map, path) do
    given = section!(map, @openfga_authorizer_keys, path)

    %{}
    |> put_map(:url, given["url"])
    |> put_map(:store_id, given["store_id"])
    |> put_map(:authorization_model_id, given["authorization_model_id"])
    |> put_map(:api_token, given["api_token"])
    |> put_map(:consistency, consistency!(given["consistency"], path ++ ["consistency"]))
    |> put_map(:user, given["user"])
    |> put_map(:object, given["object"])
    |> put_map(:resources, openfga_resources!(given["resources"], path ++ ["resources"]))
    |> put_map(:relations, actions!(given["relations"], path ++ ["relations"]))
    |> put_map(
      :contextual_tuples,
      openfga_tuples!(given["contextual_tuples"], path ++ ["contextual_tuples"])
    )
    |> put_map(:batch, bool_opt(given, "batch", path))
    |> put_map(:headers, given["headers"])
    |> put_map(:allow_insecure_http, bool_opt(given, "allow_insecure_http", path))
    |> remote_authorizer_opts(given, path)
  end

  defp authorizer_opts(Ankusa.Gateway.Authz.Table, map, path) do
    section!(map, @table_keys, path)

    %{}
    |> put_map(:mode, atom_enum_opt(map, "mode", ~w(snapshot lookup), path))
    |> put_map(:source, table_source(map["source"], path ++ ["source"]))
    |> put_map(:columns, table_columns(map["columns"], path ++ ["columns"]))
    |> put_map(
      :identities,
      map["identities"] && string_list!(map["identities"], path ++ ["identities"])
    )
    |> put_map(:max_identities, int_opt(map, "max_identities", path))
    |> put_map(:ready_timeout_ms, int_opt(map, "ready_timeout_ms", path))
    |> put_map(:refresh_ms, int_opt(map, "refresh_ms", path))
    |> put_map(:max_rows, int_opt(map, "max_rows", path))
    |> put_map(:max_bytes, int_opt(map, "max_bytes", path))
    |> put_map(:stale_after_ms, int_opt(map, "stale_after_ms", path))
    |> then(&table_remote(&1, map, path))
  end

  defp table_remote(opts, map, path) do
    Enum.reduce(@table_remote_atoms, opts, fn {name, atom}, acc ->
      put_opt(acc, atom, int_opt(map, name, path))
    end)
  end

  defp table_columns(nil, _path), do: nil

  defp table_columns(%{} = columns, path) do
    given = section!(columns, Map.keys(@table_columns), path)

    Map.new(given, fn {name, value} ->
      {Map.fetch!(@table_columns, name), expect_string(value, path ++ [name])}
    end)
  end

  defp table_columns(other, path),
    do: raise(ConfigError, message: type_error(path, "a mapping of column names", other))

  defp table_source(nil, path),
    do: raise(ConfigError, message: "#{render_path(path)} is required")

  defp table_source(%{} = source, path) do
    case Map.fetch(@table_source_types, source["type"]) do
      {:ok, module} ->
        keys =
          case module do
            Ankusa.Gateway.Authz.Table.Source.Object -> @object_source_keys
            Ankusa.Gateway.Authz.Table.Source.Http -> @http_source_keys
            Ankusa.Gateway.Postgres.Source -> @postgres_source_keys
          end

        entry = section!(source, keys, path)
        {module, table_source_opts(module, entry, path)}

      :error ->
        known = @table_source_types |> Map.keys() |> Enum.sort() |> Enum.join(", ")

        raise ConfigError,
          message:
            "#{render_path(path)}.type: unknown type #{inspect(source["type"])} (known: #{known})"
    end
  end

  defp table_source(other, path),
    do: raise(ConfigError, message: type_error(path, "a mapping", other))

  defp table_source_opts(Ankusa.Gateway.Authz.Table.Source.Object, entry, path) do
    %{}
    |> put_map(:store, entry["store"] && claim_store!(entry["store"], path ++ ["store"]))
    |> put_map(:key, string_opt(entry, "key", path))
    |> put_map(:prefix, string_opt(entry, "prefix", path))
    |> put_map(:max_object_bytes, int_opt(entry, "max_object_bytes", path))
  end

  defp table_source_opts(Ankusa.Gateway.Authz.Table.Source.Http, entry, path) do
    %{}
    |> put_map(:url, string_opt(entry, "url", path))
    |> put_map(:headers, table_headers(entry["headers"], path ++ ["headers"]))
    |> put_map(:rows_path, string_opt(entry, "rows_path", path))
    |> put_map(:next_path, string_opt(entry, "next_path", path))
    |> put_map(:cursor_param, string_opt(entry, "cursor_param", path))
    |> put_map(:timeout_ms, int_opt(entry, "timeout_ms", path))
    |> put_map(:max_pages, int_opt(entry, "max_pages", path))
  end

  defp table_source_opts(Ankusa.Gateway.Postgres.Source, entry, path) do
    %{}
    |> put_map(:url, string_opt(entry, "url", path))
    |> put_map(:hostname, string_opt(entry, "hostname", path))
    |> put_map(:port, int_opt(entry, "port", path))
    |> put_map(:database, string_opt(entry, "database", path))
    |> put_map(:username, string_opt(entry, "username", path))
    |> put_map(:password, string_opt(entry, "password", path))
    |> put_map(:ssl, bool_opt(entry, "ssl", path))
    |> put_map(:query, string_opt(entry, "query", path))
    |> put_map(:version_query, string_opt(entry, "version_query", path))
    |> put_map(:lookup_query, string_opt(entry, "lookup_query", path))
    |> put_map(:pool_size, int_opt(entry, "pool_size", path))
    |> put_map(:timeout_ms, int_opt(entry, "timeout_ms", path))
  end

  defp table_headers(nil, _path), do: nil

  defp table_headers(%{} = headers, path) do
    Map.new(headers, fn {name, value} ->
      {to_string(name), expect_string(value, path ++ [name])}
    end)
  end

  defp table_headers(other, path),
    do: raise(ConfigError, message: type_error(path, "a mapping of headers", other))

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

  defp remote_authorizer_opts(opts, given, path) do
    Enum.reduce(@remote_authorizer_int_keys, opts, fn {name, key}, acc ->
      put_map(acc, key, int_opt(given, name, path))
    end)
  end

  defp enum_opt(map, key, allowed, path) do
    case map[key] do
      nil -> nil
      value -> enum!(value, allowed, path ++ [key])
    end
  end

  defp actions!(nil, _path), do: nil

  defp actions!(value, path) do
    map = expect_map(value, path)

    Map.new(map, fn {name, action} ->
      case Map.fetch(@authorizer_action_keys, name) do
        {:ok, action_name} ->
          {action_name, expect_string(action, path)}

        :error ->
          raise ConfigError,
            message:
              "#{render_path(path)}: unknown action #{inspect(name)}; " <>
                "expected one of read, write, delete, list, share"
      end
    end)
  end

  defp consistency!(nil, _path), do: nil

  defp consistency!(value, path) do
    case Map.fetch(@consistency_keys, value) do
      {:ok, consistency} ->
        consistency

      :error ->
        raise ConfigError,
          message:
            "#{render_path(path)}: unknown value #{inspect(value)}; " <>
              "expected minimize_latency, higher_consistency"
    end
  end

  # For the authzen protocol `batch` is a boolean; for the template protocol
  # it is null or a bulk endpoint block.
  defp http_batch!(nil, _path), do: nil
  defp http_batch!(value, _path) when is_boolean(value), do: value
  defp http_batch!(%{} = map, path), do: batch_block!(map, path)

  defp http_batch!(other, path) do
    raise ConfigError,
      message: type_error(path, "true, false, null, or a bulk endpoint block", other)
  end

  defp batch_block!(map, path) do
    section!(map, @authorizer_batch_keys, path)

    Map.new(map, fn {key, value} ->
      case key do
        "url" -> {:url, value}
        "items_key" -> {:items_key, value}
        "results_path" -> {:results_path, value}
      end
    end)
  end

  defp openfga_resources!(nil, _path), do: nil

  defp openfga_resources!(list, path) when is_list(list) do
    list
    |> Enum.with_index()
    |> Enum.map(fn {entry, index} ->
      entry = section!(entry, @openfga_resource_keys, path ++ [index])

      Map.new(entry, fn
        {"space", value} -> {:space, value}
        {"match", value} -> {:match, value}
        {"object", value} -> {:object, value}
      end)
    end)
  end

  defp openfga_resources!(other, path),
    do: raise(ConfigError, message: type_error(path, "a list of entries", other))

  defp openfga_tuples!(nil, _path), do: nil

  defp openfga_tuples!(list, path) when is_list(list) do
    list
    |> Enum.with_index()
    |> Enum.map(fn {entry, index} ->
      entry = section!(entry, @openfga_tuple_keys, path ++ [index])

      Map.new(entry, fn
        {"user", value} -> {:user, value}
        {"relation", value} -> {:relation, value}
        {"object", value} -> {:object, value}
      end)
    end)
  end

  defp openfga_tuples!(other, path),
    do: raise(ConfigError, message: type_error(path, "a list of entries", other))

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

  defp authenticator_keys("okta"), do: @gateway_okta_keys

  defp authenticator_keys("introspection"), do: @gateway_introspection_keys

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

  # Okta's layout: `domain` + `authorization_server` pick the issuer; the rest,
  # jwt and introspection keys alike, override what `Ankusa.Gateway.Authn.Okta`
  # derives (it refuses one the expanded type does not know).
  defp authenticator_opts("okta", entry, path) do
    []
    |> put_opt(:domain, string_opt(entry, "domain", path))
    |> put_opt(:authorization_server, string_opt(entry, "authorization_server", path))
    |> put_opt(:client_id, string_opt(entry, "client_id", path))
    |> put_opt(:client_secret, string_opt(entry, "client_secret", path))
    |> put_opt(:header, string_opt(entry, "header", path))
    |> put_opt(:subject_claim, string_opt(entry, "subject_claim", path))
    |> put_opt(:token_type_hint, string_opt(entry, "token_type_hint", path))
    |> put_opt(:timeout_ms, int_opt(entry, "timeout_ms", path))
    |> put_opt(:max_inflight, int_opt(entry, "max_inflight", path))
    |> put_opt(:queue_max, int_opt(entry, "queue_max", path))
    |> put_opt(:subject_prefix, string_opt(entry, "subject_prefix", path))
    |> Kernel.++(jwt_opts(entry, path))
    |> put_opt(:groups_claim, string_opt(entry, "groups_claim", path))
  end

  defp authenticator_opts("introspection", entry, path) do
    []
    |> put_opt(:url, string_opt(entry, "url", path))
    |> put_opt(:client_id, string_opt(entry, "client_id", path))
    |> put_opt(:client_secret, string_opt(entry, "client_secret", path))
    |> put_opt(:token_type_hint, string_opt(entry, "token_type_hint", path))
    |> put_opt(:timeout_ms, int_opt(entry, "timeout_ms", path))
    |> put_opt(:max_inflight, int_opt(entry, "max_inflight", path))
    |> put_opt(:queue_max, int_opt(entry, "queue_max", path))
    |> put_opt(:issuer, string_opt(entry, "issuer", path))
    |> put_opt(:audience, jwt_audience(entry, path))
    |> put_opt(:subject_claim, string_opt(entry, "subject_claim", path))
    |> put_opt(:scope_claim, string_opt(entry, "scope_claim", path))
    |> put_opt(:tenants_claim, string_opt(entry, "tenants_claim", path))
    |> put_opt(:groups_claim, string_opt(entry, "groups_claim", path))
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

  # ── enrichers ───────────────────────────────────────────────────────────────

  defp gateway_enrichers(auth, path) do
    case auth["enrichers"] do
      nil ->
        nil

      list when is_list(list) ->
        if length(list) > @max_enrichers,
          do:
            raise(
              ConfigError,
              message:
                "#{render_path(path ++ ["enrichers"])} has #{length(list)} entries; " <>
                  "at most #{@max_enrichers}"
            )

        list
        |> Enum.with_index()
        |> Enum.map(fn {entry, index} -> enricher!(entry, path ++ ["enrichers", index]) end)

      other ->
        raise ConfigError,
          message: type_error(path ++ ["enrichers"], "a list of enrichers", other)
    end
  end

  defp enricher!(entry, path) do
    map = expect_map(entry, path)
    type = enum!(map["type"], @gateway_enricher_types, path ++ ["type"])

    section =
      section!(
        map,
        ~w(type) ++ @gateway_enricher_common_keys ++ enricher_keys(type),
        path
      )

    [type: Map.fetch!(@gateway_enricher_atoms, type)]
    |> put_opt(:name, string_opt(section, "name", path))
    |> put_opt(:only, section["only"] && string_list!(section["only"], path ++ ["only"]))
    |> put_opt(:on_unavailable, atom_enum_opt(section, "on_unavailable", ~w(fail skip), path))
    |> put_opt(:ttl_ms, int_opt(section, "ttl_ms", path))
    |> put_opt(:timeout_ms, int_opt(section, "timeout_ms", path))
    |> Kernel.++(enricher_opts(type, section, path))
  end

  defp enricher_keys("okta_groups"), do: @gateway_okta_groups_keys
  defp enricher_keys("http"), do: @gateway_http_enricher_keys

  defp enricher_opts("okta_groups", entry, path) do
    []
    |> put_opt(:domain, string_opt(entry, "domain", path))
    |> put_opt(:api_token, string_opt(entry, "api_token", path))
    |> put_opt(:user_id, string_opt(entry, "user_id", path))
    |> put_opt(:max_groups, int_opt(entry, "max_groups", path))
  end

  defp enricher_opts("http", entry, path) do
    []
    |> put_opt(:url, string_opt(entry, "url", path))
    |> put_opt(:headers, headers_opt(entry, path))
    |> put_opt(:groups_path, string_opt(entry, "groups_path", path))
    |> put_opt(:claims_path, string_opt(entry, "claims_path", path))
  end

  defp headers_opt(entry, path) do
    case entry["headers"] do
      nil ->
        nil

      %{} = headers ->
        Map.new(headers, fn {name, value} ->
          {to_string(name), expect_string(value, path ++ ["headers", to_string(name)])}
        end)

      other ->
        raise ConfigError, message: type_error(path ++ ["headers"], "a mapping", other)
    end
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
