defmodule Ankusa.Auth do
  @moduledoc """
  Single sign-on for the admin (`admin.port`) and route-management
  (`routes.admin.port`) listeners: who a bearer token names, and what they may
  do.

  Off by default (`auth.enabled: false`), and off means both listeners answer
  everyone, as they always did. On, every request except `GET /health` and
  `GET /ready` carries `Authorization: Bearer <token>`, and
  `Ankusa.Admin.Guard` runs three steps:

    1. **Verify.** `auth.verifier` (`Ankusa.Auth.Verifier`, for example
       `Ankusa.Auth.OIDC` from the `ankusa_oidc` package) turns the token into
       claims, or rejects it.
    2. **Resolve.** `resolve/2` turns the claims into a `Ankusa.Auth.Principal`
       holding grants, from `auth.bindings`.
    3. **Authorize.** `allowed?/3` compares the grants with the role and scope
       the action needs (`Ankusa.Admin.Actions`).

  ## Roles and bindings

  Roles are ranked `viewer < operator < admin`: a grant of a higher role
  includes every lower one. A binding grants one role to everyone in a group,
  or to one subject, either everywhere (`tenants: :all`, the default) or only
  in the listed tenants:

      auth: %{
        enabled: true,
        verifier: {Ankusa.Auth.OIDC, issuer: "https://idp.example.com", audience: "ankusa"},
        bindings: [
          %{group: "Platform", role: :admin},
          %{group: "Acme Ops", role: :operator, tenants: ["acme"]},
          %{subject: "ci-deployer", role: :viewer}
        ]
      }

  A tenant-scoped grant applies only to actions on that tenant's resources
  (sources, the tenant's audit trail). Platform-wide actions (the DLQ, replays,
  the quarantine pen, route management, a tenant's rate limit, which is a
  platform control) need a grant with `tenants: :all`.

  ## Where groups come from

    * **With SCIM** (`scim.enabled`), the directory. A token is matched to a
      provisioned user by `auth.user_match`: the value of the token's `claim`
      (default `preferred_username`) against the user's `userName`
      (case-insensitively) or `externalId` (exactly). The user's groups are the
      directory groups they belong to, and their `displayName` is what a
      `group` binding names. A user the IdP deprovisioned (`active: false`) is
      refused (`403 user_inactive`) even while their token is still valid, and
      a token matching no user is `403 user_not_provisioned`. A subject binding
      still works with no directory user: a machine identity, which is how a CI
      job gets in without being provisioned.
    * **Without SCIM**, the token's own groups claim (`auth.groups_claim`,
      default `groups`).

  Group names compare exactly. A binding that names a group nobody is in grants
  nothing: unmatched bindings fail closed.

  ## The SCIM client

  The IdP's SCIM client (Okta, Entra) does not hold a user token. It sends the
  static `scim.token`, which opens `/scim/v2` and nothing else.
  """

  alias Ankusa.Auth.Principal
  alias Ankusa.Config

  @roles [:viewer, :operator, :admin]
  @rank %{viewer: 1, operator: 2, admin: 3}
  @attributes [:user_name, :external_id]
  @binding_keys [:role, :group, :subject, :tenants]
  @tenant_regex ~r/\A[A-Za-z0-9_-]{1,64}\z/
  @min_scim_token_bytes 32

  @type scope :: :global | {:tenant, String.t()}

  @doc "The roles, lowest first."
  @spec roles() :: [:viewer | :operator | :admin]
  def roles, do: @roles

  @doc """
  Reject an auth or SCIM configuration that cannot work. Raises
  `ArgumentError` naming the key.
  """
  @spec validate_config!(Config.t()) :: :ok
  def validate_config!(%Config{auth: auth, scim: scim} = config) do
    validate_verifier!(auth)
    validate_user_match!(auth.user_match)
    validate_claim_name!(auth.subject_claim, "auth.subject_claim")
    validate_claim_name!(auth.groups_claim, "auth.groups_claim")
    validate_bindings!(auth.bindings)
    validate_scim!(config, auth, scim)
    :ok
  end

  defp validate_verifier!(%{enabled: true, verifier: {mod, opts}})
       when is_atom(mod) and is_list(opts) do
    unless Code.ensure_loaded?(mod) and function_exported?(mod, :verify, 3) do
      raise ArgumentError,
            "auth.verifier #{inspect(mod)} is not available or does not implement " <>
              "Ankusa.Auth.Verifier (is its package a dependency?)"
    end

    if function_exported?(mod, :validate_opts!, 1), do: mod.validate_opts!(opts)
    :ok
  end

  defp validate_verifier!(%{enabled: true}) do
    raise ArgumentError, "auth.verifier must be a {module, opts} pair when auth.enabled is true"
  end

  defp validate_verifier!(_auth), do: :ok

  defp validate_user_match!(user_match) when is_map(user_match) do
    unless is_binary(user_match[:claim]) and user_match[:claim] != "" do
      raise ArgumentError,
            "auth.user_match.claim must be a non-empty string, got: #{inspect(user_match[:claim])}"
    end

    unless user_match[:attribute] in @attributes do
      raise ArgumentError,
            "auth.user_match.attribute must be :user_name or :external_id, got: " <>
              inspect(user_match[:attribute])
    end
  end

  defp validate_user_match!(other) do
    raise ArgumentError, "auth.user_match must be a map, got: #{inspect(other)}"
  end

  defp validate_claim_name!(claim, key) do
    unless is_binary(claim) and claim != "" do
      raise ArgumentError, "#{key} must be a non-empty string, got: #{inspect(claim)}"
    end
  end

  defp validate_bindings!(bindings) when is_list(bindings) do
    bindings
    |> Enum.with_index()
    |> Enum.each(fn {binding, i} -> validate_binding!(binding, i) end)
  end

  defp validate_bindings!(other) do
    raise ArgumentError, "auth.bindings must be a list, got: #{inspect(other)}"
  end

  defp validate_binding!(binding, i) do
    key = "auth.bindings[#{i}]"
    binding = binding_map!(binding, key)

    case Map.keys(binding) -- @binding_keys do
      [] -> :ok
      unknown -> raise ArgumentError, "#{key} has unknown key(s): #{inspect(unknown)}"
    end

    group = Map.get(binding, :group)
    subject = Map.get(binding, :subject)

    unless one_identity?(group, subject) do
      raise ArgumentError, "#{key} must set exactly one of group or subject"
    end

    unless Map.get(binding, :role) in @roles do
      raise ArgumentError,
            "#{key}.role must be one of #{inspect(@roles)}, got: #{inspect(Map.get(binding, :role))}"
    end

    case Map.get(binding, :tenants, :all) do
      :all ->
        :ok

      [_ | _] = tenants ->
        unless Enum.all?(tenants, &(is_binary(&1) and Regex.match?(@tenant_regex, &1))) do
          raise ArgumentError,
                "#{key}.tenants must be :all or a non-empty list of tenant ids " <>
                  "([A-Za-z0-9_-], up to 64 characters), got: #{inspect(tenants)}"
        end

      other ->
        raise ArgumentError,
              "#{key}.tenants must be :all or a non-empty list of tenant ids, got: #{inspect(other)}"
    end
  end

  defp binding_map!(binding, _key) when is_map(binding), do: binding

  defp binding_map!(binding, key) do
    if is_list(binding) and Keyword.keyword?(binding) do
      Map.new(binding)
    else
      raise ArgumentError, "#{key} must be a map, got: #{inspect(binding)}"
    end
  end

  defp one_identity?(group, subject) do
    (non_empty_binary?(group) and is_nil(subject)) or
      (non_empty_binary?(subject) and is_nil(group))
  end

  defp non_empty_binary?(value), do: is_binary(value) and value != ""

  defp validate_scim!(config, auth, scim) do
    validate_directory!(scim.directory)

    if scim.enabled do
      unless auth.enabled, do: raise(ArgumentError, "scim.enabled requires auth.enabled")
      unless config.admin.enabled, do: raise(ArgumentError, "scim.enabled requires admin.enabled")

      unless is_binary(scim.token) and byte_size(scim.token) >= @min_scim_token_bytes do
        raise ArgumentError,
              "scim.token must be a string of at least #{@min_scim_token_bytes} bytes"
      end
    end

    unless is_nil(scim.base_url) or is_binary(scim.base_url) do
      raise ArgumentError, "scim.base_url must be a string or nil, got: #{inspect(scim.base_url)}"
    end
  end

  defp validate_directory!({mod, opts}) when is_atom(mod) and is_list(opts), do: :ok

  defp validate_directory!(other) do
    raise ArgumentError, "scim.directory must be a {module, opts} pair, got: #{inspect(other)}"
  end

  # ── authentication ─────────────────────────────────────────────────────────

  @doc """
  The anonymous principal of an instance with auth off: every grant, so the
  listeners behave as they did before auth existed.
  """
  @spec anonymous(String.t() | nil) :: Principal.t()
  def anonymous(ip) do
    %Principal{type: :anonymous, grants: [%{role: :admin, tenants: :all}], ip: ip}
  end

  @doc """
  Verify `token` with the configured verifier and resolve its claims into a
  principal.
  """
  @spec authenticate(atom(), String.t(), String.t() | nil) ::
          {:ok, Principal.t()}
          | {:error, :invalid_token | :user_not_provisioned | :user_inactive | :unavailable}
  def authenticate(instance, token, ip) do
    {mod, opts} = Ankusa.config(instance).auth.verifier

    with {:ok, claims} <- mod.verify(instance, token, opts),
         {:ok, principal} <- resolve(instance, claims) do
      {:ok, %{principal | ip: ip}}
    end
  end

  @doc """
  Turn verified `claims` into a principal with its grants, as described in the
  module doc.
  """
  @spec resolve(atom(), map()) ::
          {:ok, Principal.t()} | {:error, :user_not_provisioned | :user_inactive | :unavailable}
  def resolve(instance, claims) when is_map(claims) do
    config = Ankusa.config(instance)
    subject = binary_claim(claims, config.auth.subject_claim)
    username = binary_claim(claims, config.auth.user_match.claim)

    if config.scim.enabled do
      resolve_directory(instance, config.auth, subject, username)
    else
      resolve_claims(config.auth, claims, subject, username)
    end
  end

  defp resolve_claims(auth, claims, subject, username) do
    groups =
      case Map.get(claims, auth.groups_claim) do
        list when is_list(list) -> Enum.filter(list, &is_binary/1)
        _ -> []
      end

    {:ok,
     %Principal{
       type: if(username, do: :user, else: :service),
       subject: subject,
       username: username,
       groups: groups,
       grants: grants(auth.bindings, groups, subject)
     }}
  end

  defp resolve_directory(instance, auth, subject, username) do
    case find_user(instance, auth.user_match.attribute, username) do
      {:ok, %{"active" => false}} ->
        {:error, :user_inactive}

      {:ok, user} ->
        with {:ok, groups} <- Ankusa.Directory.groups_for_user(instance, user["id"]) do
          names = Enum.map(groups, & &1["displayName"])

          {:ok,
           %Principal{
             type: :user,
             subject: subject,
             username: user["userName"],
             user_id: user["id"],
             groups: names,
             grants: grants(auth.bindings, names, subject)
           }}
        end

      :not_found ->
        case grants(auth.bindings, [], subject) do
          [] ->
            {:error, :user_not_provisioned}

          grants ->
            {:ok,
             %Principal{type: :service, subject: subject, username: username, grants: grants}}
        end

      {:error, :unavailable} ->
        {:error, :unavailable}
    end
  end

  defp find_user(_instance, _attribute, nil), do: :not_found

  defp find_user(instance, attribute, value),
    do: Ankusa.Directory.find_user(instance, attribute, value)

  defp binary_claim(claims, name) do
    case Map.get(claims, name) do
      value when is_binary(value) and value != "" -> value
      _ -> nil
    end
  end

  # Group bindings for the groups the principal is in, plus the subject
  # bindings that name its subject (which always apply).
  defp grants(bindings, groups, subject) do
    for binding <- bindings,
        binding = Map.new(binding),
        matches?(binding, groups, subject) do
      %{role: binding.role, tenants: Map.get(binding, :tenants, :all)}
    end
  end

  defp matches?(%{group: group}, groups, _subject) when is_binary(group), do: group in groups

  defp matches?(%{subject: bound}, _groups, subject) when is_binary(bound),
    do: subject != nil and bound == subject

  defp matches?(_binding, _groups, _subject), do: false

  # ── authorization ──────────────────────────────────────────────────────────

  @doc """
  Whether `principal` holds a grant of at least `required` that covers `scope`:
  `:all` tenants cover everything, a tenant list covers `{:tenant, t}` for each
  tenant in it and never `:global`.
  """
  @spec allowed?(Principal.t(), :viewer | :operator | :admin, scope()) :: boolean()
  def allowed?(%Principal{grants: grants}, required, scope) do
    needed = Map.fetch!(@rank, required)

    Enum.any?(grants, fn %{role: role, tenants: tenants} ->
      Map.fetch!(@rank, role) >= needed and covers?(tenants, scope)
    end)
  end

  defp covers?(:all, _scope), do: true
  defp covers?(tenants, {:tenant, tenant}) when is_list(tenants), do: tenant in tenants
  defp covers?(_tenants, _scope), do: false
end
