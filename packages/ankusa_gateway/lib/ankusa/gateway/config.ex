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

  An authenticator is `%{type: :token | :jwt | :mtls | MyModule, name: "..."}`
  plus its own keys (see `Ankusa.Gateway.Authn.Token`, `Ankusa.Gateway.Authn.JWT`
  and `Ankusa.Gateway.Authn.MTLS`); `name` defaults to `"<type>-<index>"`. A
  policy step is an authorizer name or `%{use: name, on_unavailable: :fail |
  :abstain | :deny}` (`:fail`, the default, answers `503 authz_unavailable`
  when the step cannot decide).

  `new/2` raises `ArgumentError`; every message starts with the dotted key.
  With `check_exposure: false` an empty chain on a non-loopback address is
  accepted (a loader that has not decided yet whether the gateway runs).
  """

  alias Ankusa.Gateway.{Authn, Authz, Options}

  defstruct instance: :default,
            port: 4001,
            ip: "127.0.0.1",
            tls: nil,
            max_concurrent_requests: 1024,
            auth: %{
              allow_insecure: false,
              authenticators: [],
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
            claims: %{enabled: true, policy: "claims", integrity_check: :manifest}

  @type authenticator :: %{type: atom(), name: String.t(), module: module(), config: map()}
  @type authorizer :: %{module: module(), state: map()}
  @type step :: %{use: String.t(), on_unavailable: :fail | :abstain | :deny}
  @type policy :: %{mode: :first | :all | :any, steps: [step()]}

  @type t :: %__MODULE__{
          instance: atom(),
          port: :inet.port_number(),
          ip: String.t() | :inet.ip_address(),
          tls: map() | nil,
          max_concurrent_requests: non_neg_integer(),
          auth: %{allow_insecure: boolean(), authenticators: [authenticator()], cache: map()},
          authz: %{
            authorizers: %{String.t() => authorizer()},
            policies: %{String.t() => policy()},
            deadline_ms: pos_integer(),
            cache: map()
          },
          claims: %{enabled: boolean(), policy: String.t(), integrity_check: :off | :manifest}
        }

  @top_keys [:instance, :port, :ip, :tls, :max_concurrent_requests, :auth, :authz, :claims]
  @auth_keys [:allow_insecure, :authenticators, :cache]
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

  @authenticators %{token: Authn.Token, jwt: Authn.JWT, mtls: Authn.MTLS}
  @max_authenticators 8
  @reserved_authorizers %{"scopes" => Authz.Scopes}

  @shown ~w(instance ip name type issuer audience jwks_url header scope_claim tenants_claim
            groups_claim subject_prefix id scopes groups policy mode use on_unavailable steps)

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
    authorizers = authorizers!(authz)
    policies = policies!(authz, authorizers)
    auth = auth!(Map.get(given, :auth, %{}), tls)

    if check_exposure?, do: exposure!(auth, address)

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
      claims: claims!(Map.get(given, :claims, %{}), policies)
    }
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

    %{
      allow_insecure: allow_insecure,
      authenticators: authenticators,
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
    {type, module} = authenticator_type!(Map.get(given, :type), key)
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
    "#{key}.type must be one of #{inspect(Map.keys(@authenticators) |> Enum.sort())} or a " <>
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

    :ok
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

  defp authorizers!(%{} = authz) do
    given =
      authz
      |> Map.get(:authorizers, %{})
      |> Options.map!("gateway.authz.authorizers")

    defined =
      Map.new(given, fn {name, spec} ->
        name = Options.name!(name, "gateway.authz.authorizers")
        key = "gateway.authz.authorizers.#{name}"

        if Map.has_key?(@reserved_authorizers, name) do
          raise ArgumentError, "#{key}: the name #{name} is reserved for the built-in authorizer"
        end

        {name, authorizer!(spec, key)}
      end)

    Map.merge(
      defined,
      Map.new(@reserved_authorizers, fn {name, module} ->
        {name, %{module: module, state: module.init!(%{}, "gateway.authz.authorizers.#{name}")}}
      end)
    )
  end

  defp authorizer!({module, opts}, key) when is_atom(module) do
    unless Options.implements?(module, init!: 2, kind: 1, decide: 3) do
      raise ArgumentError,
            "#{key} must name a module implementing Ankusa.Gateway.Authorizer, got #{inspect(module)}"
    end

    state = module.init!(Options.map!(opts, key), key)
    %{module: module, state: state}
  end

  defp authorizer!(other, key),
    do: raise(ArgumentError, "#{key} must be {module, opts}, got #{inspect(other)}")

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
