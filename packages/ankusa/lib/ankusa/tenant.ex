defmodule Ankusa.Tenant do
  @moduledoc """
  Tenant erase: remove everything this node's store, the claim stores and the
  edge's tables hold for one tenant, as one resumable, audited job.

      Ankusa.Tenant.delete(:default, "acme", confirm: "acme", audit: context)

  `DELETE /v1/tenants/:tenant?confirm=<tenant>` (scope `tenants:write` and
  `tenant:*`) is the same call.

  ## What an erase does, in order

  `delete/3` writes the job's progress row (`m:erase:<tenant>`, synced) and
  answers; `Ankusa.Tenant.Eraser` runs the job in the background. Its first act
  is to **close the tenant** (the tenant gate): from then on the queue
  writer answers the tenant's hooks `503 tenant_erasing`, the quarantine pen
  refuses to hold them, and dispatch stops claiming its rows. Then, each stage
  done and recorded before the next:

  | Stage | Removes |
  | --- | --- |
  | `sources` | the tenant's API-managed sources, through `Ankusa.SourceStore.delete/4`: each is an audited change with its own `source.deleted` lifecycle event |
  | `routes` | the tenant's routes (`Ankusa.Routes.delete/3`: one `route.deleted` event each) |
  | `rate_limit` | the tenant's runtime rate-limit override |
  | `queue` | waits for running attempts to settle (the tenant's `inflight` counter reaches 0), then every due, inflight and dead row (`Ankusa.Queue.purge_tenant/3`), reclaiming hooks nobody else holds, and the tenant's active markers |
  | `archive` | the tenant's archive obligations (`?a`), reclaiming their hooks |
  | `quarantine` | the tenant's held hooks |
  | `claims` | every object under `claims/tenant=<tenant>/` in every claim store |
  | `dedupe` | the tenant's dedupe keys (`?u`, a range delete) and their expiry entries |
  | `entities` | the redacted entities of the tenant's lifecycle events, once the lifecycle publisher has delivered them |
  | `commit` | one synced batch: the tombstone `m:erased:<tenant>`, the job's progress row, the tenant's ordinal (retired, never reused), its counters and source revisions, and **one audit row** (`tenant.delete`, which becomes the lifecycle event `io.ankusa.tenant.deleted`) |

  A stage that fails (a store that does not answer, a sink that is still
  running) is retried every few seconds; a node that is killed resumes the job
  from its progress row at the next boot. Every stage is idempotent.

  ## What stays, and why

    * **The audit trail.** Its rows hold hashes, counts and names, never a body
      or a secret (`Ankusa.Audit`); they are the record that the erase happened
      and who asked for it, and they age out with `audit.retention_days`. Only
      the lifecycle *entities* (the redacted views of the tenant's sources and
      routes that events carry) are deleted.
    * **Archived segments.** A segment is multi-tenant and immutable. The
      tombstone makes `Ankusa.Storage.fetch/2` answer `{:error, :gone}` (HTTP
      `410` semantics) for every hook of the tenant received at or before it,
      and archive replay skip them; the bytes leave the bucket when the bucket's
      lifecycle rule expires the segment (`docs/storage.md`).
    * **Backups.** A restored backup that predates the erase brings the tenant
      back, tombstone and all: erase again after such a restore.
    * **The ordinal.** Ordinals are never reused (ADR 0001), so an erased tenant
      still counts towards `tenants.max`.
    * **Replay job records** that name the tenant in their filter: they hold no
      hook data.

  A tenant that sends again after the erase is a new tenant; the tombstone only
  covers what was received before it.

  ## Limits

  `"default"` cannot be erased. A tenant that a *configured* source (static,
  or seeded into the source store) names cannot be erased either: remove it
  from the configuration first, or the next hook would recreate the tenant.
  The erase needs the `:edge` role on this node when routes are enabled (the
  route store is the edge's). Sources, routes and overrides written for the
  tenant *while* its erase runs can outlive it; the `commit` stage re-checks
  them right before it commits and the job fails and retries until none is left.
  """

  alias Ankusa.Audit.Context
  alias Ankusa.{Config, SourceStore, Store}
  alias Ankusa.Store.Keys
  alias Ankusa.Tenant.{Eraser, Gate}

  @identity_regex ~r/\A[A-Za-z0-9_-]{1,64}\z/

  @type reason ::
          :confirm
          | :invalid_tenant
          | :default_tenant
          | :no_store
          | :store_unavailable
          | {:role_not_enabled, :edge}
          | {:static_sources, [String.t()]}

  @doc """
  Start erasing `tenant`, or report the erase already running.

  Options: `confirm:` (required: must equal `tenant`) and `audit:`, the
  `Ankusa.Audit.Context` the erase's audit rows are written under (the actor of
  the request, which is kept across a restart).

  `{:ok, :started}` or `{:ok, :erasing}` (one is already running for it).
  """
  @spec delete(atom(), String.t(), keyword()) :: {:ok, :started | :erasing} | {:error, reason()}
  def delete(instance, tenant, opts \\ []) do
    cond do
      not valid?(tenant) ->
        {:error, :invalid_tenant}

      Keyword.get(opts, :confirm) != tenant ->
        {:error, :confirm}

      tenant == "default" ->
        {:error, :default_tenant}

      true ->
        config = Ankusa.config(instance)

        with :ok <- require_store(config),
             :ok <- require_edge(config),
             :ok <- no_configured_sources(instance, tenant) do
          Eraser.start(instance, tenant, Context.from_opts(opts))
        end
    end
  end

  defp valid?(tenant), do: is_binary(tenant) and Regex.match?(@identity_regex, tenant)

  defp require_store(config),
    do: if(Ankusa.Instance.store?(config), do: :ok, else: {:error, :no_store})

  defp require_edge(config) do
    if config.routes.enabled and not Config.role?(config, :edge),
      do: {:error, {:role_not_enabled, :edge}},
      else: :ok
  end

  defp no_configured_sources(instance, tenant) do
    case configured_sources(instance, tenant) do
      [] -> :ok
      ids -> {:error, {:static_sources, ids}}
    end
  end

  @doc """
  The sources that name `tenant` and that an erase cannot delete: configured
  ones (the static source store, or seeds of the writable one), as opposed to
  the ones written through the API. Walks the source ids, so it costs the number
  of sources.
  """
  @spec configured_sources(atom(), String.t()) :: [String.t()]
  def configured_sources(instance, tenant) do
    managed =
      instance
      |> SourceStore.list_tenant(tenant)
      |> MapSet.new(&SourceStore.Table.source_id(tenant, &1.name))

    for id <- SourceStore.list(instance),
        not MapSet.member?(managed, id),
        {:ok, %{tenant_id: ^tenant}} <- [SourceStore.fetch(instance, id)],
        do: id
  end

  @doc """
  Where an erase of `tenant` stands: `{:ok, %{state: :erasing, stage: atom,
  started_at: ms}}` while its job exists, `{:ok, %{state: :erased, erased_at:
  ms}}` once committed, `:not_found` for a tenant that was never erased here.
  """
  @spec status(atom(), String.t()) ::
          {:ok, %{state: :erasing | :erased} | map()} | :not_found | {:error, term()}
  def status(instance, tenant) do
    with {:ok, progress} <- progress(instance, tenant) do
      case progress do
        %{stage: stage, started_at: started_at} ->
          {:ok, %{state: :erasing, stage: stage, started_at: started_at}}

        nil ->
          case erased_at(instance, tenant) do
            {:ok, at} -> {:ok, %{state: :erased, erased_at: at}}
            other -> other
          end
      end
    end
  end

  @doc false
  # The job's progress row, `nil` when there is none.
  @spec progress(atom(), String.t()) :: {:ok, map() | nil} | {:error, term()}
  def progress(instance, tenant) do
    case Store.get(instance, :default, Keys.erase(tenant)) do
      {:ok, bin} -> {:ok, decode_progress(bin)}
      :not_found -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc false
  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  def decode_progress(bin), do: :erlang.binary_to_term(bin)

  @doc false
  def encode_progress(progress), do: :erlang.term_to_binary(progress)

  @doc "When `tenant` was erased: `{:ok, ms}` or `:not_found`."
  @spec erased_at(atom(), String.t()) :: {:ok, non_neg_integer()} | :not_found | {:error, term()}
  def erased_at(instance, tenant) do
    case Store.get(instance, :default, Keys.erased(tenant)) do
      {:ok, <<at::64>>} -> {:ok, at}
      {:ok, _bad} -> {:error, {:corrupt_value, :erased}}
      other -> other
    end
  end

  @doc """
  Every tombstone: `{:ok, %{tenant => erased_at_ms}}`. One range of the store,
  read once per page by the archive and lifecycle replays.
  """
  @spec erased(atom()) :: {:ok, %{String.t() => non_neg_integer()}} | {:error, term()}
  def erased(instance) do
    %{lo: lo, hi: hi} = Keys.family(:erased)

    Store.fold(instance, :erased, {lo, hi}, %{}, fn key, <<at::64>>, acc ->
      {:cont, Map.put(acc, Keys.decode_erased(key), at)}
    end)
  end

  @doc """
  Whether a hook of `tenant` received at `received_at` was erased, given the
  tombstones `erased/1` returned.
  """
  @spec erased?(%{String.t() => non_neg_integer()}, String.t() | nil, integer()) :: boolean()
  def erased?(erased, tenant, received_at) do
    case erased do
      %{^tenant => at} -> received_at <= at
      _ -> false
    end
  end

  @doc """
  Whether a lifecycle replay must not re-emit the audit row `row`: it belongs
  to a tenant that is being erased, or one that was erased after the row was
  written. The erase's own `tenant.delete` row is the one exception.
  """
  @spec suppressed?(atom(), %{String.t() => non_neg_integer()}, map()) :: boolean()
  def suppressed?(_instance, _erased, %{action: "tenant.delete"}), do: false

  def suppressed?(instance, erased, row) do
    tenant = Map.get(row, :tenant) || "default"
    Gate.closed?(instance, tenant) or erased?(erased, tenant, Map.get(row, :at, 0))
  end
end
