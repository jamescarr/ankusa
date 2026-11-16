defmodule Ankusa.Gateway.Config do
  @moduledoc """
  The gateway's configuration: the `gateway:` section of a `config_version: 2`
  file, or the options an embedder passes to `new/2`.

      Ankusa.Gateway.Config.new(
        instance: :default,
        port: 4001,
        ip: "0.0.0.0",
        auth: %{
          authenticators: [
            %{type: :token, tokens: [%{id: "worker", sha256: "<64 hex>", scopes: ["claims:read", "tenant:*"]}]},
            %{type: :jwt, issuer: "https://idp.example/", audience: "ankusa", jwks_url: "https://idp.example/jwks"}
          ]
        }
      )

  | Key | Default | |
  | --- | --- | --- |
  | `instance` | `:default` | the core instance it runs beside |
  | `port` | `4001` | 0..65535 |
  | `ip` | `"127.0.0.1"` | the address it binds (`Ankusa.Config.listen_ip!/2`) |
  | `tls` | `nil` | a TLS block shaped like `listener.tls` (`Ankusa.Listener.normalize_tls!/2`) |
  | `max_concurrent_requests` | `1024` | requests served at once on this node; `0` = no cap |
  | `auth.allow_insecure` | `false` | allow an empty chain on a non-loopback `ip` |
  | `auth.authenticators` | `[]` | the chain, at most 8, at most one `mtls` |
  | `auth.cache.ttl_ms` | `60000` | how long an authenticated credential is remembered (never past its own expiry); `0` = off |
  | `auth.cache.negative_ttl_ms` | `5000` | how long a refusal by an authenticator that calls out is remembered |
  | `auth.cache.max_entries`, `auth.cache.max_bytes` | `100000`, `33554432` | the authentication cache's bounds |
  | `auth.enrichers` | `[]` | at most 4: groups and claims added to a freshly authenticated principal (see `Ankusa.Gateway.Enricher`) |
  | `authz.deadline_ms` | `250` | the whole policy's budget per request |
  | `authz.cache.max_entries`, `authz.cache.max_bytes` | `200000`, `67108864` | the decision cache's bounds |
  | `authz.cache.allow_ttl_ms`, `authz.cache.deny_ttl_ms` | `5000`, `1000` | how long a `:cached`/`:remote` authorizer's allow, and deny or abstain, is reused (`0` = never) |
  | `authz.cache.refresh_ahead` | `true` | a `:remote` decision in the last fifth of its TTL is refreshed in the background |
  | `authz.cache.serve_stale_ms` | `0` | how long past its TTL a decision is kept, to answer with when its backend is unavailable |
  | `authz.authorizers` | `%{}` | `%{name => {module, opts}}`; `"scopes"` is reserved and always defined |
  | `authz.policies` | `%{}` | `%{name => %{mode: :first \\| :all \\| :any, steps: [...]}}`; `"claims"` defaults to `[scopes]` |
  | `claims.enabled` | `true` | serve the claims API (else its routes are `404`) |
  | `claims.policy` | `"claims"` | the policy a claim read is authorized by |
  | `claims.integrity_check` | `:manifest` | `:off` or `:manifest` |
  | `spaces` | `%{}` | the objects API's spaces (`Ankusa.Gateway.Spaces`) |
  | `objects.max_inflight_bytes` | `536870912` | object bytes held in memory at once across requests; at least every space's `max_object_bytes` |
  | `objects.op_deadline_ms` | `30000` | 1000..600000: one store operation's deadline, every attempt included |
  | `objects.timeout_ms` | `10000` | 1..600000: one store request |
  | `objects.retries` | `1` | 0..10: extra attempts after a transient store failure |
  | `sharing.enabled` | `false` | native grants and share links: defines the `grants` authorizer, adds the `link` authenticator, records ownership on `PUT` |
  | `sharing.signing_keys` | `[]` | `[current \\| previous]`, each at least 16 bytes; required when enabled |
  | `sharing.link_max_ttl_s` | `604800` | 1..31536000: the longest a share link may live |
  | `sharing.acl_cache_ttl_ms` | `2000` | 0..60000: how long an ACL document or an object's owner is reused; the cross-node bound of a revocation |
  | `sharing.acl_cache_max_entries`, `sharing.acl_cache_max_bytes` | `100000`, `67108864` | the ACL cache's bounds |
  | `sharing.max_grants_per_resource` | `256` | 1..1024 |

  An authenticator is `%{type: :token | :jwt | :mtls | :introspection | :okta |
  MyModule, name: "..."}` plus its own keys (see `Ankusa.Gateway.Authn.Token`,
  `Ankusa.Gateway.Authn.JWT`, `Ankusa.Gateway.Authn.MTLS`,
  `Ankusa.Gateway.Authn.Introspection` and `Ankusa.Gateway.Authn.Okta`);
  `name` defaults to `"<type>-<index>"`. At most one authenticator calls out to
  introspect a token, and nothing that reads the bearer token may follow it. An
  enricher is `%{type: :okta_groups | :http | MyModule, ...}` plus
  `Ankusa.Gateway.Enricher`'s common keys (`only`, `on_unavailable`, `ttl_ms`,
  `timeout_ms`). A policy step is an authorizer name or `%{use: name,
  on_unavailable: :fail | :abstain | :deny}` (`:fail`, the default, answers
  `503 authz_unavailable` when the step cannot decide).

  `new/2` raises `ArgumentError`; every message starts with the dotted key.
  With `check_exposure: false` an empty chain on a non-loopback address is
  accepted (a loader that has not decided yet whether the gateway runs).
  """

  alias Ankusa.Gateway.{Authn, Authz, Enricher, Options}

  defstruct instance: :default,
            port: 4001,
            ip: "127.0.0.1",
            tls: nil,
            max_concurrent_requests: 1024,
            auth: %{
              allow_insecure: false,
              authenticators: [],
              enrichers: [],
              cache: %{
                ttl_ms: 60_000,
                negative_ttl_ms: 5_000,
                max_entries: 100_000,
                max_bytes: 33_554_432
              }
            },
            authz: %{
              authorizers: %{},
              policies: %{},
              deadline_ms: 250,
              cache: %{
                max_entries: 200_000,
                max_bytes: 67_108_864,
                allow_ttl_ms: 5_000,
                deny_ttl_ms: 1_000,
                refresh_ahead: true,
                serve_stale_ms: 0
              }
            },
            claims: %{enabled: true, policy: "claims", integrity_check: :manifest},
            spaces: %{},
            objects: %{
              max_inflight_bytes: 536_870_912,
              op_deadline_ms: 30_000,
              timeout_ms: 10_000,
              retries: 1
            },
            sharing: %{
              enabled: false,
              signing_keys: [],
              link_max_ttl_s: 604_800,
              acl_cache_ttl_ms: 2_000,
              acl_cache_max_entries: 100_000,
              acl_cache_max_bytes: 67_108_864,
              max_grants_per_resource: 256
            }

  @type authenticator :: %{type: atom(), name: String.t(), module: module(), config: map()}
  @type enricher :: %{type: atom(), name: String.t(), module: module(), config: map()}
  @type authorizer :: %{module: module(), state: map()}
  @type step :: %{use: String.t(), on_unavailable: :fail | :abstain | :deny}
  @type policy :: %{mode: :first | :all | :any, steps: [step()]}

  @type t :: %__MODULE__{
          instance: atom(),
          port: :inet.port_number(),
          ip: String.t() | :inet.ip_address(),
          tls: map() | nil,
          max_concurrent_requests: non_neg_integer(),
          auth: %{
            allow_insecure: boolean(),
            authenticators: [authenticator()],
            enrichers: [enricher()],
            cache: map()
          },
          authz: %{
            authorizers: %{String.t() => authorizer()},
            policies: %{String.t() => policy()},
            deadline_ms: pos_integer(),
            cache: map()
          },
          claims: %{enabled: boolean(), policy: String.t(), integrity_check: :off | :manifest},
          spaces: %{String.t() => Ankusa.Gateway.Spaces.space()},
          objects: %{
            max_inflight_bytes: pos_integer(),
            op_deadline_ms: pos_integer(),
            timeout_ms: pos_integer(),
            retries: non_neg_integer()
          },
          sharing: %{
            enabled: boolean(),
            signing_keys: [String.t()],
            link_max_ttl_s: pos_integer(),
            acl_cache_ttl_ms: non_neg_integer(),
            acl_cache_max_entries: pos_integer(),
            acl_cache_max_bytes: pos_integer(),
            max_grants_per_resource: pos_integer()
          }
        }

  @top_keys [
    :instance,
    :port,
    :ip,
    :tls,
    :max_concurrent_requests,
    :auth,
    :authz,
    :claims,
    :spaces,
    :objects,
    :sharing
  ]
  @sharing %{
    link_max_ttl_s: 1..31_536_000,
    acl_cache_ttl_ms: 0..60_000,
    acl_cache_max_entries: 1_000..5_000_000,
    acl_cache_max_bytes: 1_048_576..1_073_741_824,
    max_grants_per_resource: 1..1024
  }
  @objects %{
    max_inflight_bytes: 1..1_099_511_627_776,
    op_deadline_ms: 1_000..600_000,
    timeout_ms: 1..600_000,
    retries: 0..10
  }
  @auth_keys [:allow_insecure, :authenticators, :cache, :enrichers]
  @authz_keys [:authorizers, :policies, :deadline_ms, :cache]
  @auth_cache %{
    ttl_ms: 0..3_600_000,
    negative_ttl_ms: 0..60_000,
    max_entries: 1_000..5_000_000,
    max_bytes: 1_048_576..1_073_741_824
  }
  @authz_cache %{
    max_entries: 1_000..10_000_000,
    max_bytes: 1_048_576..4_294_967_296,
    allow_ttl_ms: 0..3_600_000,
    deny_ttl_ms: 0..3_600_000,
    serve_stale_ms: 0..3_600_000
  }
  @claims_keys [:enabled, :policy, :integrity_check]
  @policy_keys [:mode, :steps]
  @step_keys [:use, :on_unavailable]

  @authenticators %{
    token: Authn.Token,
    jwt: Authn.JWT,
    mtls: Authn.MTLS,
    introspection: Authn.Introspection
  }
  # `okta` is sugar: `Ankusa.Gateway.Authn.Okta` expands it into one of the above.
  @authenticator_types [:okta | Map.keys(@authenticators)]
  @max_authenticators 8
  @max_enrichers 4
  @enrichers %{okta_groups: Enricher.OktaGroups, http: Enricher.Http}
  @reserved_authorizers %{"scopes" => Authz.Scopes}
  # Defined only with `sharing.enabled`, but never a name of your own.
  @sharing_authorizers %{"grants" => Authz.Grants}

  # M8's PDP authorizers add keys that are operator configuration, never
  # credentials: the protocol, the paths and templates (a value under `url`
  # still goes through the URL rules, which hide its query values), the action
  # and relation names, and a store or model's id. `headers` and `api_token`
  # stay out: closed mode hides them.
  @shown ~w(instance ip name type issuer audience jwks_url header scope_claim tenants_claim
            groups_claim subject_prefix id scopes groups policy mode use on_unavailable steps
            prefix protocol evaluation_path evaluations_path allow_path ttl_path actions relations
            consistency store_id authorization_model_id user object match space items_key
            results_path domain authorization_server subject_claim token_type_hint only
            groups_path claims_path user_id)

  @doc "Validate `opts` into a config. Raises `ArgumentError` naming the key."
  @spec new(keyword() | map(), keyword()) :: t()
  def new(opts \\ [], options \\ []) do
    check_exposure? = Keyword.get(options, :check_exposure, true)
    given = Options.map!(opts, "gateway")
    Options.unknown_keys!(given, @top_keys, "gateway")
    base = %__MODULE__{}

    instance = Map.get(given, :instance, base.instance)

    unless is_atom(instance) and instance not in [nil, true, false] do
      raise ArgumentError, "gateway.instance must be an atom, got #{inspect(instance)}"
    end

    ip = Map.get(given, :ip, base.ip)
    address = Ankusa.Config.listen_ip!(ip, "gateway.ip")
    tls = Ankusa.Listener.normalize_tls!(Map.get(given, :tls), "gateway.tls")

    authz = given |> Map.get(:authz, %{}) |> section!("gateway.authz", @authz_keys)
    sharing = sharing!(Map.get(given, :sharing, %{}))
    authorizers = authorizers!(authz, sharing, instance)
    policies = policies!(authz, authorizers)
    auth = auth!(Map.get(given, :auth, %{}), tls)

    if check_exposure?, do: exposure!(auth, address)
    auth = with_link(auth, sharing)

    %__MODULE__{
      instance: instance,
      port: Options.int_in!(Map.get(given, :port, base.port), 0..65_535, "gateway.port"),
      ip: ip,
      tls: tls,
      max_concurrent_requests:
        non_negative!(
          Map.get(given, :max_concurrent_requests, base.max_concurrent_requests),
          "gateway.max_concurrent_requests"
        ),
      auth: auth,
      authz: %{
        authorizers: authorizers,
        policies: policies,
        deadline_ms:
          Options.int_in!(
            Map.get(authz, :deadline_ms, 250),
            10..30_000,
            "gateway.authz.deadline_ms"
          ),
        cache: authz_cache!(Map.get(authz, :cache, %{}))
      },
      claims: claims!(Map.get(given, :claims, %{}), policies),
      sharing: sharing
    }
    |> objects!(Map.get(given, :spaces, %{}), Map.get(given, :objects, %{}))
  end

  defp objects!(config, spaces, objects) do
    spaces = Ankusa.Gateway.Spaces.config!(spaces, config.authz.policies)
    objects = ranged!(objects, "gateway.objects", @objects, %__MODULE__{}.objects)

    case Enum.find(Map.values(spaces), &(&1.max_object_bytes > objects.max_inflight_bytes)) do
      nil ->
        %{config | spaces: spaces, objects: objects}

      space ->
        raise ArgumentError,
              "gateway.objects.max_inflight_bytes (#{objects.max_inflight_bytes}) must be at " <>
                "least gateway.spaces.#{space.name}.max_object_bytes (#{space.max_object_bytes})"
    end
    |> permissions!()
  end

  # A table source that reads a store must not read a space's keys: the
  # objects API would expose (and a grant could be written over) its files.
  defp permissions!(%__MODULE__{authz: %{authorizers: authorizers}, spaces: spaces} = config) do
    for {name, %{module: Ankusa.Gateway.Authz.Table, state: state}} <- authorizers,
        reach = permission_reach(state),
        reach != nil,
        {space_name, space} <- spaces,
        reach.store == space.store,
        overlap?(reach.prefix, space.prefix) do
      raise ArgumentError,
            "gateway.authz.authorizers.#{name}.source: its store and prefix overlap " <>
              "gateway.spaces.#{space_name}; keep permission files outside every space"
    end

    config
  end

  defp permission_reach(%{source: %{module: module, opts: opts}}) do
    if function_exported?(module, :reach, 1), do: module.reach(opts), else: nil
  end

  defp permission_reach(_state), do: nil

  defp overlap?(a, b), do: String.starts_with?(a, b) or String.starts_with?(b, a)

  @doc """
  The store options of one call to `space`'s store: its own, with
  `objects.op_deadline_ms`, `timeout_ms` and `retries` over them.
  """
  @spec store_opts(t(), Ankusa.Gateway.Spaces.space()) :: keyword()
  def store_opts(%__MODULE__{objects: objects}, %{store: {_module, opts}}) do
    Keyword.merge(opts,
      op_deadline_ms: objects.op_deadline_ms,
      timeout_ms: objects.timeout_ms,
      retries: objects.retries
    )
  end

  @doc """
  A JSON-encodable view of `config` safe to print: closed-mode redaction
  (`Ankusa.Admin.Redact.closed_view/2`), so every string it is not told to
  show (a token's hash, a static key, a secret option of a custom
  authorizer) is `"[REDACTED]"`.
  """
  @spec redact(t()) :: map()
  def redact(%__MODULE__{} = config),
    do: Ankusa.Admin.Redact.closed_view(Map.from_struct(config), @shown)

  # ── auth ────────────────────────────────────────────────────────────────────

  defp auth!(auth, tls) do
    given = section!(auth, "gateway.auth", @auth_keys)

    allow_insecure =
      Options.boolean!(Map.get(given, :allow_insecure, false), "gateway.auth.allow_insecure")

    authenticators =
      case Map.get(given, :authenticators, []) do
        list when is_list(list) ->
          list |> Enum.with_index() |> Enum.map(fn {entry, i} -> authenticator!(entry, i) end)

        other ->
          raise ArgumentError,
                "gateway.auth.authenticators must be a list, got #{inspect(other)}"
      end

    chain!(authenticators, tls)
    enrichers = enrichers!(Map.get(given, :enrichers, []), authenticators)

    %{
      allow_insecure: allow_insecure,
      authenticators: authenticators,
      enrichers: enrichers,
      cache:
        ranged!(
          Map.get(given, :cache, %{}),
          "gateway.auth.cache",
          @auth_cache,
          %__MODULE__{}.auth.cache
        )
    }
  end

  # A section of integer keys with ranges, over its defaults.
  defp ranged!(value, key, ranges, defaults) do
    given = section!(value, key, Map.keys(defaults))

    Map.new(defaults, fn {k, default} ->
      case Map.fetch(ranges, k) do
        {:ok, range} -> {k, Options.int_in!(Map.get(given, k, default), range, "#{key}.#{k}")}
        :error -> {k, Map.get(given, k, default)}
      end
    end)
  end

  defp authenticator!(entry, index) do
    key = "gateway.auth.authenticators[#{index}]"
    given = Options.map!(entry, key)

    # `okta` is Okta's layout for one of the real types, expanded here so the
    # chain holds a `jwt` or an `introspection` authenticator either way.
    case Map.get(given, :type) do
      :okta ->
        {type, given} = Authn.Okta.expand!(given, key)
        build_authenticator(type, given, index, key)

      type ->
        build_authenticator(type, given, index, key)
    end
  end

  defp build_authenticator(type, given, index, key) do
    {type, module} = authenticator_type!(type, key)
    name = Options.name!(Map.get(given, :name, default_name(type, index)), "#{key}.name")
    config = module.init!(Map.drop(given, [:type, :name]), key)
    %{type: type, name: name, module: module, config: config}
  end

  defp authenticator_type!(type, _key) when is_map_key(@authenticators, type),
    do: {type, Map.fetch!(@authenticators, type)}

  defp authenticator_type!(module, key) when is_atom(module) and module != nil do
    if Options.implements?(module, init!: 2, credential: 2, authenticate: 2),
      do: {module, module},
      else: raise(ArgumentError, bad_type(module, key))
  end

  defp authenticator_type!(other, key), do: raise(ArgumentError, bad_type(other, key))

  defp bad_type(type, key) do
    "#{key}.type must be one of #{inspect(Enum.sort(@authenticator_types))} or a " <>
      "module implementing Ankusa.Gateway.Authenticator, got #{inspect(type)}"
  end

  defp default_name(type, index) when is_map_key(@authenticators, type), do: "#{type}-#{index}"
  defp default_name(_module, index), do: "custom-#{index}"

  defp chain!(authenticators, tls) do
    if length(authenticators) > @max_authenticators do
      raise ArgumentError,
            "gateway.auth.authenticators has #{length(authenticators)} entries; at most " <>
              "#{@max_authenticators}"
    end

    names = Enum.map(authenticators, & &1.name)

    case names -- Enum.uniq(names) do
      [] ->
        :ok

      [dup | _] ->
        raise ArgumentError, "gateway.auth.authenticators: two entries are named #{dup}"
    end

    mtls = for {%{type: :mtls}, i} <- Enum.with_index(authenticators), do: i

    if length(mtls) > 1 do
      raise ArgumentError,
            "gateway.auth.authenticators has #{length(mtls)} mtls entries; at most one"
    end

    for i <- mtls, not match?(%{verify: :verify_peer}, tls) do
      raise ArgumentError,
            "gateway.auth.authenticators[#{i}]: mtls needs gateway.tls with verify: :verify_peer " <>
              "and a cacertfile"
    end

    introspection!(authenticators)
  end

  # At most one introspection entry, and nothing that reads the same bearer
  # token after it: the first that answers for a token ends the chain, so a
  # token entry behind it would never be reached.
  defp introspection!(authenticators) do
    entries =
      for {%{type: :introspection}, index} <- Enum.with_index(authenticators), do: index

    if length(entries) > 1 do
      raise ArgumentError,
            "gateway.auth.authenticators has #{length(entries)} introspection entries; at most one"
    end

    case entries do
      [introspection] ->
        for {%{} = later, index} <- Enum.with_index(authenticators),
            index > introspection,
            reads_bearer?(later) do
          raise ArgumentError,
                "gateway.auth.authenticators[#{index}] reads the bearer token after the " <>
                  "introspection authenticator at [#{introspection}], which would answer for it " <>
                  "first; put the introspection authenticator last"
        end

        :ok

      [] ->
        :ok
    end
  end

  defp reads_bearer?(%{type: :token}), do: true
  defp reads_bearer?(%{type: :jwt, config: %{header: "authorization"}}), do: true
  defp reads_bearer?(_authenticator), do: false

  # ── enrichers ───────────────────────────────────────────────────────────────

  defp enrichers!(list, authenticators) when is_list(list) do
    if length(list) > @max_enrichers do
      raise ArgumentError,
            "gateway.auth.enrichers has #{length(list)} entries; at most #{@max_enrichers}"
    end

    names = Enum.map(authenticators, & &1.name)
    list |> Enum.with_index() |> Enum.map(fn {entry, i} -> enricher!(entry, i, names) end)
  end

  defp enrichers!(other, _authenticators) do
    raise ArgumentError, "gateway.auth.enrichers must be a list, got #{inspect(other)}"
  end

  defp enricher!(entry, index, authenticator_names) do
    key = "gateway.auth.enrichers[#{index}]"
    given = Options.map!(entry, key)
    {type, module} = enricher_type!(Map.get(given, :type), key)
    name = Options.name!(Map.get(given, :name, "#{type}-#{index}"), "#{key}.name")
    config = module.init!(Map.drop(given, [:type, :name]), key)

    for only <- config.only, only not in authenticator_names do
      raise ArgumentError,
            "#{key}.only names #{only}, which is not an authenticator of the chain (defined: " <>
              "#{authenticator_names |> Enum.sort() |> Enum.join(", ")})"
    end

    %{type: type, name: name, module: module, config: config}
  end

  defp enricher_type!(type, _key) when is_map_key(@enrichers, type),
    do: {type, Map.fetch!(@enrichers, type)}

  defp enricher_type!(module, key) when is_atom(module) and module != nil do
    if Options.implements?(module, init!: 2, enrich: 3),
      do: {module, module},
      else: raise(ArgumentError, enricher_bad_type(module, key))
  end

  defp enricher_type!(other, key), do: raise(ArgumentError, enricher_bad_type(other, key))

  defp enricher_bad_type(type, key) do
    "#{key}.type must be one of #{inspect(@enrichers |> Map.keys() |> Enum.sort())} or a " <>
      "module implementing Ankusa.Gateway.Enricher, got #{inspect(type)}"
  end

  defp exposure!(%{authenticators: [], allow_insecure: false}, address) do
    unless Options.loopback?(address) do
      raise ArgumentError,
            "gateway.auth.authenticators is empty but gateway.ip (#{:inet.ntoa(address)}) is not " <>
              "a loopback address: add an authenticator, or set gateway.auth.allow_insecure: true " <>
              "if something in front of the listener authenticates every caller"
    end
  end

  defp exposure!(_auth, _address), do: :ok

  # ── authz ───────────────────────────────────────────────────────────────────

  defp authorizers!(%{} = authz, sharing, instance) do
    given =
      authz
      |> Map.get(:authorizers, %{})
      |> Options.map!("gateway.authz.authorizers")

    defined =
      Map.new(given, fn {name, spec} ->
        name = Options.name!(name, "gateway.authz.authorizers")
        key = "gateway.authz.authorizers.#{name}"

        if Map.has_key?(@reserved_authorizers, name) or Map.has_key?(@sharing_authorizers, name) do
          raise ArgumentError, "#{key}: the name #{name} is reserved for the built-in authorizer"
        end

        {name, authorizer!(spec, key, instance)}
      end)

    built_in =
      if sharing.enabled,
        do: Map.merge(@reserved_authorizers, @sharing_authorizers),
        else: @reserved_authorizers

    Map.merge(
      defined,
      Map.new(built_in, fn {name, module} ->
        {name,
         put_instance(
           %{module: module, state: module.init!(%{}, "gateway.authz.authorizers.#{name}")},
           instance
         )}
      end)
    )
  end

  # ── sharing ─────────────────────────────────────────────────────────────────

  defp sharing!(value) do
    key = "gateway.sharing"
    defaults = %__MODULE__{}.sharing
    given = section!(value, key, Map.keys(defaults))

    sharing =
      ranged!(
        Map.drop(given, [:enabled, :signing_keys]),
        key,
        @sharing,
        Map.drop(defaults, [:enabled, :signing_keys])
      )

    enabled = Options.boolean!(Map.get(given, :enabled, false), "#{key}.enabled")
    keys = signing_keys!(Map.get(given, :signing_keys, []), "#{key}.signing_keys")

    if enabled and keys == [] do
      raise ArgumentError,
            "#{key}.signing_keys must hold at least one key when gateway.sharing.enabled is true"
    end

    Map.merge(sharing, %{enabled: enabled, signing_keys: keys})
  end

  defp signing_keys!(keys, key) when is_list(keys) do
    Enum.map(keys, fn
      value when is_binary(value) and byte_size(value) >= 16 -> value
      _other -> raise ArgumentError, "#{key}: every key must be a string of at least 16 bytes"
    end)
  end

  defp signing_keys!(_other, key),
    do: raise(ArgumentError, "#{key} must be a list of strings of at least 16 bytes")

  # With sharing on, the share-link authenticator closes the chain.
  defp with_link(auth, %{enabled: false}), do: auth

  defp with_link(auth, sharing) do
    link = %{
      type: :link,
      name: "link",
      module: Authn.Link,
      config: Authn.Link.init!(sharing, "gateway.sharing")
    }

    %{auth | authenticators: auth.authenticators ++ [link]}
  end

  defp authorizer!({module, opts}, key, instance) when is_atom(module) do
    unless Options.implements?(module, init!: 2, kind: 1, decide: 3) do
      raise ArgumentError,
            "#{key} must name a module implementing Ankusa.Gateway.Authorizer, got #{inspect(module)}"
    end

    module.init!(Options.map!(opts, key), key)
    |> put_instance(instance)
    |> then(&%{module: module, state: &1})
  end

  defp authorizer!(other, key, _instance),
    do: raise(ArgumentError, "#{key} must be {module, opts}, got #{inspect(other)}")

  # The instance is part of every authorizer's state: `cache_scope/2` takes no
  # instance, and a `table` authorizer's scope names its snapshot generation.
  defp put_instance(%{module: module, state: state}, instance),
    do: %{module: module, state: Map.put(state, :instance, instance)}

  defp put_instance(state, instance), do: Map.put(state, :instance, instance)

  defp policies!(authz, authorizers) do
    given =
      authz
      |> Options.map!("gateway.authz")
      |> Map.get(:policies, %{})
      |> Options.map!("gateway.authz.policies")

    policies =
      Map.new(given, fn {name, policy} ->
        name = Options.name!(name, "gateway.authz.policies")
        {name, policy!(policy, "gateway.authz.policies.#{name}", authorizers)}
      end)

    Map.put_new(policies, "claims", %{
      mode: :first,
      steps: [%{use: "scopes", on_unavailable: :fail}]
    })
  end

  defp policy!(policy, key, authorizers) do
    given = section!(policy, key, @policy_keys)
    mode = Map.get(given, :mode, :first)

    unless mode in [:first, :all, :any] do
      raise ArgumentError, "#{key}.mode must be :first, :all or :any, got #{inspect(mode)}"
    end

    steps =
      case Map.get(given, :steps) do
        [_ | _] = steps ->
          steps
          |> Enum.with_index()
          |> Enum.map(fn {step, i} -> step!(step, "#{key}.steps[#{i}]", authorizers) end)

        other ->
          raise ArgumentError, "#{key}.steps must be a non-empty list, got #{inspect(other)}"
      end

    %{mode: mode, steps: steps}
  end

  defp step!(name, key, authorizers) when is_binary(name) or is_atom(name),
    do: step!(%{use: name}, key, authorizers)

  defp step!(step, key, authorizers) do
    given = section!(step, key, @step_keys)
    name = Options.name!(Map.get(given, :use), "#{key}.use")

    unless Map.has_key?(authorizers, name) do
      raise ArgumentError,
            "#{key}.use names #{name}, which is not an authorizer (defined: " <>
              "#{authorizers |> Map.keys() |> Enum.sort() |> Enum.join(", ")})"
    end

    on_unavailable = Map.get(given, :on_unavailable, :fail)

    unless on_unavailable in [:fail, :abstain, :deny] do
      raise ArgumentError,
            "#{key}.on_unavailable must be :fail, :abstain or :deny, got #{inspect(on_unavailable)}"
    end

    %{use: name, on_unavailable: on_unavailable}
  end

  defp authz_cache!(value) do
    key = "gateway.authz.cache"
    cache = ranged!(value, key, @authz_cache, %__MODULE__{}.authz.cache)
    %{cache | refresh_ahead: Options.boolean!(cache.refresh_ahead, "#{key}.refresh_ahead")}
  end

  # ── claims ──────────────────────────────────────────────────────────────────

  defp claims!(claims, policies) do
    given = section!(claims, "gateway.claims", @claims_keys)
    policy = Map.get(given, :policy, "claims")

    policy =
      if is_binary(policy) and Map.has_key?(policies, policy),
        do: policy,
        else:
          raise(
            ArgumentError,
            "gateway.claims.policy must name a policy of gateway.authz.policies, got #{inspect(policy)}"
          )

    integrity = Map.get(given, :integrity_check, :manifest)

    unless integrity in [:off, :manifest] do
      raise ArgumentError,
            "gateway.claims.integrity_check must be :off or :manifest, got #{inspect(integrity)}"
    end

    %{
      enabled: Options.boolean!(Map.get(given, :enabled, true), "gateway.claims.enabled"),
      policy: policy,
      integrity_check: integrity
    }
  end

  # ── helpers ─────────────────────────────────────────────────────────────────

  defp section!(value, key, known) do
    given = Options.map!(value, key)
    Options.unknown_keys!(given, known, key)
    given
  end

  defp non_negative!(value, _key) when is_integer(value) and value >= 0, do: value

  defp non_negative!(other, key),
    do: raise(ArgumentError, "#{key} must be a non-negative integer, got #{inspect(other)}")
end
