defmodule Ankusa.Principal do
  @moduledoc """
  Who is calling a control plane, and what they may do: the result of
  `c:Ankusa.Admin.Auth.authenticate/2`, assigned to `conn.assigns.principal`
  for every handler of the admin API, the route-management API and the
  claim-check gateway.

    * `id` — a stable name for audit and logs (a token entry's `id`, a JWT's
      `sub`, a certificate's mapped subject).
    * `scopes` — operation scopes, e.g. `"dlq:read"`.
    * `tenants` — `:all` (the `tenant:*` scope) or the list of tenant ids the
      principal may act on (one `tenant:<id>` scope each).

  ## Scopes

  | Scope | Grants |
  | --- | --- |
  | `metrics:read` | `GET /metrics`, `GET /v1/wal` |
  | `config:read` | `GET /v1/config`, `GET /asyncapi.json` |
  | `config:write` | `PUT /v1/log-level` and `POST /v1/config/reload` (with `tenant:*`: both are the node's, for every tenant) |
  | `sources:read`, `sources:write` | a tenant's sources |
  | `dlq:read` | `GET /v1/dlq`; `dlq:write`: starting a `dlq` replay |
  | `replays:read`, `replays:write` | replay jobs (`dlq:write` / `quarantine:write` as well for those kinds) |
  | `quarantine:read`, `quarantine:write` | the quarantine pen |
  | `rate_limits:read`, `rate_limits:write` | per-tenant rate limits |
  | `tenants:write` | `DELETE /v1/tenants/:tenant` (a tenant erase) and `GET /v1/tenants/:tenant/erase`; with `tenant:*` |
  | `routes:read`, `routes:write` | route definitions, IP rules, the dry run |
  | `claims:read` | claim-check gateway reads |
  | `audit:read` | reserved for the audit log; no route yet |
  | `tenant:<id>`, `tenant:*` | the tenants the other scopes apply to |

  A resource that spans tenants is filtered by tenant, not refused. The DLQ,
  the quarantine pen, replay jobs and the rate-limit listing take an optional
  `tenant` filter: a principal limited to some tenants gets rows for those
  tenants only (forced server-side) and `403` for a `tenant` it does not hold;
  `tenant:*` is required to read or change them unfiltered. Route definitions
  and IP rules belong to no tenant and still need `tenant:*`.
  """

  @enforce_keys [:id]
  defstruct id: nil, scopes: [], tenants: []

  @type t :: %__MODULE__{
          id: String.t(),
          scopes: [String.t()],
          tenants: :all | [String.t()]
        }

  @scopes ~w(
    metrics:read config:read config:write
    sources:read sources:write
    dlq:read dlq:write
    replays:read replays:write
    quarantine:read quarantine:write
    rate_limits:read rate_limits:write
    tenants:write
    routes:read routes:write
    claims:read audit:read
  )

  @doc "Every operation scope (the `tenant:` scopes are separate)."
  @spec scopes() :: [String.t()]
  def scopes, do: @scopes

  @doc """
  Split scope strings into operation scopes and tenants. Returns
  `{scopes, tenants, unknown}`: `unknown` is every entry that is neither a known
  scope nor a `tenant:` scope, in order.
  """
  @spec parse_scopes([String.t()]) :: {[String.t()], :all | [String.t()], [String.t()]}
  def parse_scopes(entries) when is_list(entries) do
    {scopes, tenants, unknown} =
      Enum.reduce(entries, {[], [], []}, fn
        "tenant:*", {s, t, u} ->
          {s, [:all | t], u}

        "tenant:" <> tenant, {s, t, u} when tenant != "" ->
          {s, [tenant | t], u}

        scope, {s, t, u} when scope in @scopes ->
          {[scope | s], t, u}

        other, {s, t, u} ->
          {s, t, [other | u]}
      end)

    {Enum.uniq(Enum.reverse(scopes)), tenants(Enum.reverse(tenants)), Enum.reverse(unknown)}
  end

  @doc """
  A principal from `id`, scope strings and an extra list of tenant ids (the
  `ankusa_tenants` claim: `"*"` means every tenant). Entries that are not
  scopes of this system are dropped: an identity provider's token carries
  scopes of its own (`openid`, `email`) that mean nothing here.
  """
  @spec new(String.t(), [String.t()], [String.t()]) :: t()
  def new(id, scope_entries, extra_tenants \\ []) when is_binary(id) do
    {scopes, tenants, _unknown} = parse_scopes(scope_entries)

    extra =
      Enum.map(extra_tenants, fn
        "*" -> :all
        tenant -> tenant
      end)

    %__MODULE__{id: id, scopes: scopes, tenants: merge_tenants(tenants, tenants(extra))}
  end

  @doc """
  The principal of an unauthenticated listener (`mode: :none`, loopback only):
  every scope, every tenant.
  """
  @spec operator(String.t()) :: t()
  def operator(id \\ "anonymous"), do: %__MODULE__{id: id, scopes: @scopes, tenants: :all}

  @doc "Does the principal hold `scope`?"
  @spec scope?(t(), String.t()) :: boolean()
  def scope?(%__MODULE__{scopes: scopes}, scope), do: scope in scopes

  @doc "May the principal act on `tenant`?"
  @spec tenant?(t(), term()) :: boolean()
  def tenant?(%__MODULE__{tenants: :all}, _tenant), do: true
  def tenant?(%__MODULE__{tenants: tenants}, tenant), do: tenant in tenants

  @doc "Does the principal hold `tenant:*`?"
  @spec all_tenants?(t()) :: boolean()
  def all_tenants?(%__MODULE__{tenants: :all}), do: true
  def all_tenants?(%__MODULE__{}), do: false

  defp tenants(entries) do
    if :all in entries, do: :all, else: Enum.uniq(entries)
  end

  defp merge_tenants(:all, _other), do: :all
  defp merge_tenants(_other, :all), do: :all
  defp merge_tenants(a, b), do: Enum.uniq(a ++ b)
end
