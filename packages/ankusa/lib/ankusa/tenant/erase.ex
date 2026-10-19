defmodule Ankusa.Tenant.Erase do
  @moduledoc false

  # The stages of a tenant erase (`Ankusa.Tenant`), one function each. A stage is
  # idempotent and returns `{:ok, counts}` (what it removed, summed into the
  # job's progress and the final audit row) or `{:error, reason}`, after which
  # `Ankusa.Tenant.Eraser` runs it again: a stage that stopped part-way, or a
  # node that was killed, costs a repeat, never a gap.
  #
  # The tenant is already closed (`Ankusa.Tenant.Gate`) when stage one runs, so
  # nothing new is committed for it while the stages remove what is there.

  alias Ankusa.{Audit, Config, Lifecycle, Routes, SourceStore, Store}
  alias Ankusa.BlobStore.LocalFS
  alias Ankusa.ClaimCheck.{Ref, Stores}
  alias Ankusa.Edge.{Quarantine, RateLimiter}
  alias Ankusa.Lifecycle.Publisher
  alias Ankusa.Queue.{Deliveries, Tenants}
  alias Ankusa.Queue.Deliveries.Counters
  alias Ankusa.Store.{Keys, Sweep}
  alias Ankusa.Tenant

  @stages [
    :sources,
    :routes,
    :rate_limit,
    :queue,
    :archive,
    :quarantine,
    :claims,
    :dedupe,
    :entities,
    :commit
  ]

  # How long `queue` waits past `dispatch.attempt_timeout_ms` for the outcomes of
  # the tenant's running attempts to be recorded, and how often it looks.
  @drain_slack_ms 15_000
  @drain_poll_ms 50

  # How long `entities` waits for the lifecycle publisher to deliver the events
  # of the erase's own changes before it deletes their entities.
  @publisher_wait_ms 300_000
  @publisher_poll_ms 100

  @type job :: %{
          required(:instance) => atom(),
          required(:tenant) => String.t(),
          required(:context) => Ankusa.Audit.Context.t(),
          required(:counts) => %{optional(atom()) => non_neg_integer()}
        }

  @spec stages() :: [atom()]
  def stages, do: @stages

  @doc "The stage after `stage`, `:done` after the last."
  @spec next(atom()) :: atom()
  def next(stage) do
    case Enum.drop_while(@stages, &(&1 != stage)) do
      [_stage, next | _] -> next
      _ -> :done
    end
  end

  @spec run(atom(), job()) :: {:ok, map()} | {:error, term()}
  def run(:sources, %{instance: instance, tenant: tenant, context: context}) do
    with {:ok, deleted} <- delete_sources(instance, tenant, context),
         [] <- Tenant.configured_sources(instance, tenant) do
      {:ok, %{sources: deleted}}
    else
      ids when is_list(ids) -> {:error, {:static_sources, ids}}
      {:error, reason} -> {:error, reason}
    end
  end

  def run(:routes, %{instance: instance, tenant: tenant, context: context}) do
    config = Ankusa.config(instance)

    cond do
      not (Routes.enabled?(instance) and Config.role?(config, :edge)) -> {:ok, %{}}
      Routes.meta(instance) == nil -> {:error, :routes_not_ready}
      true -> delete_routes(instance, tenant, context, 0)
    end
  end

  def run(:rate_limit, %{instance: instance, tenant: tenant, context: context}) do
    override =
      if Ankusa.whereis(instance, :rate_limiter) do
        case RateLimiter.delete_override(instance, tenant, audit: context) do
          :ok -> {:ok, 1}
          {:error, :not_found} -> {:ok, 0}
          {:error, reason} -> {:error, reason}
        end
      else
        {:ok, 0}
      end

    # The row itself too, whether or not a limiter was there to forget it, and
    # the tenant's fleet buckets (a no-op unless `rate_limits.scope: :fleet`;
    # best effort, the buckets expire once full again). Fleet dedupe claims
    # expire on their own TTL: Redis has no prefix delete to remove them by.
    with {:ok, n} <- override,
         :ok <- Store.write(instance, [{:delete, :default, Keys.rate_limit(tenant)}], sync: true),
         :ok <- Ankusa.Edge.Fleet.reset_tenant(instance, tenant) do
      {:ok, %{rate_limits: n}}
    end
  end

  def run(:queue, %{instance: instance, tenant: tenant}) do
    case Tenants.ord(instance, tenant) do
      :not_found ->
        {:ok, %{}}

      {:error, reason} ->
        {:error, reason}

      {:ok, ord} ->
        with :ok <- drain(instance, ord),
             {:ok, removed} <-
               Deliveries.purge_tenant(instance, tenant, [:pending, :inflight, :dead]),
             {:ok, markers} <- Deliveries.purge_markers(instance, ord) do
          {:ok, Map.put(removed, :markers, markers)}
        end
    end
  end

  def run(:archive, %{instance: instance, tenant: tenant}) do
    case Tenants.ord(instance, tenant) do
      :not_found ->
        {:ok, %{}}

      {:error, reason} ->
        {:error, reason}

      {:ok, ord} ->
        with {:ok, n} <- Deliveries.purge_archive(instance, ord), do: {:ok, %{archive: n}}
    end
  end

  def run(:quarantine, %{instance: instance, tenant: tenant}) do
    {lower, upper} = Quarantine.range(nil, nil)
    sweep_pen(instance, tenant, lower, upper, %{quarantine: 0, quarantine_bytes: 0})
  end

  def run(:claims, %{instance: instance, tenant: tenant}) do
    prefix = Ref.claims_prefix() <> "tenant=" <> tenant <> "/"

    instance
    |> Ankusa.config()
    |> Stores.list()
    |> Enum.reduce_while({:ok, 0}, fn {_index, mod, opts}, {:ok, n} ->
      case mod.list(instance, prefix, opts) do
        {:ok, keys} ->
          # `BlobStore.delete/3` is `:ok` by contract (an absent key is deleted).
          Enum.each(keys, &mod.delete(instance, &1, opts))
          if mod == LocalFS, do: remove_local_dir(instance, opts, prefix)
          {:cont, {:ok, n + length(keys)}}

        {:error, reason} ->
          {:halt, {:error, {:claims_unavailable, reason}}}
      end
    end)
    |> case do
      {:ok, n} -> {:ok, %{claims: n}}
      {:error, reason} -> {:error, reason}
    end
  end

  def run(:dedupe, %{instance: instance, tenant: tenant}) do
    # The tenant id has no NUL byte, so `?u <tenant> 0` is exactly its keys.
    {lower, upper} = Keys.range(<<?u, tenant::binary, 0>>)

    with :ok <- Store.write(instance, [{:delete_range, :index, lower, upper}], sync: true),
         {:ok, expiries} <-
           Sweep.run(
             instance,
             :dedupe_expiry,
             fn key, _value ->
               case Keys.decode_dedupe_expiry(key) do
                 {_at, ^tenant, _source, _key} -> {:ok, {:delete, :index, key}}
                 _ -> :skip
               end
             end,
             fn deletes ->
               with :ok <- Store.write(instance, deletes, sync: false), do: {:ok, length(deletes)}
             end
           ) do
      {:ok, %{dedupe_expiries: expiries}}
    end
  end

  def run(:entities, %{instance: instance, tenant: tenant}) do
    with {:ok, bound} <- Audit.last_seq(instance),
         :ok <- await_publisher(instance, bound),
         {:ok, n} <-
           Sweep.run(
             instance,
             :audit,
             fn
               <<?a, seq::64, _at::64>>, value when seq <= bound ->
                 case Audit.decode_row(value) do
                   {:ok, %{tenant: ^tenant, publish: true}} -> {:ok, seq}
                   _ -> :skip
                 end

               _key, _value ->
                 :skip
             end,
             fn seqs ->
               deletes = Enum.map(seqs, &{:delete, :audit, Keys.lifecycle_entity(&1)})
               with :ok <- Store.write(instance, deletes, sync: false), do: {:ok, length(seqs)}
             end
           ) do
      {:ok, %{entities: n}}
    end
  end

  def run(:commit, %{instance: instance, tenant: tenant, context: context, counts: counts} = job) do
    with :ok <- settled(job),
         {:ok, retire} <- retire_ops(instance, tenant) do
      at = System.system_time(:millisecond)

      ops =
        [
          {:put, :default, Keys.erased(tenant), <<at::64>>},
          {:delete, :default, Keys.erase(tenant)}
          | retire
        ]

      attrs =
        Map.merge(
          %{
            action: "tenant.delete",
            tenant: tenant,
            resource: tenant,
            detail: Map.new(counts, fn {key, n} -> {Atom.to_string(key), n} end)
          },
          Lifecycle.outbox(instance, fn -> %{"tenant" => tenant} end)
        )

      with :ok <- Audit.commit(instance, context, attrs, ops), do: {:ok, %{}}
    end
  end

  # ── sources and routes ─────────────────────────────────────────────────────

  defp delete_sources(instance, tenant, context) do
    instance
    |> SourceStore.list_tenant(tenant)
    |> Enum.reduce_while({:ok, 0}, fn stored, {:ok, n} ->
      case SourceStore.delete(instance, tenant, stored.name, audit: context) do
        :ok -> {:cont, {:ok, n + 1}}
        {:error, :not_found} -> {:cont, {:ok, n}}
        {:error, :read_only} -> {:cont, {:ok, n}}
        {:error, :invalid, message} -> {:halt, {:error, {:source_not_deletable, message}}}
        {:error, :store_unavailable} -> {:halt, {:error, :store_unavailable}}
      end
    end)
  end

  defp delete_routes(instance, tenant, context, deleted) do
    {:ok, %{routes: routes}} = Routes.list(instance, tenant: tenant, limit: 200)

    if routes == [] do
      {:ok, %{routes: deleted}}
    else
      results = Enum.map(routes, &Routes.delete(instance, &1.id, tenant: tenant, audit: context))

      cond do
        {:error, :store_unavailable} in results ->
          {:error, :store_unavailable}

        :ok not in results ->
          {:error, :routes_not_settled}

        true ->
          delete_routes(instance, tenant, context, deleted + Enum.count(results, &(&1 == :ok)))
      end
    end
  end

  # ── queue ──────────────────────────────────────────────────────────────────

  # With dispatch running here, the tenant's claimed rows belong to attempts
  # that will record an outcome over whatever is deleted, so wait for them: the
  # tenant is closed, so nothing new is claimed, and the `inflight` counter only
  # goes down. No dispatch here: a claimed row is a crashed node's leftover.
  defp drain(instance, ord) do
    case Ankusa.whereis(instance, :dispatch) do
      nil ->
        :ok

      _pipeline ->
        timeout = Ankusa.config(instance).dispatch.attempt_timeout_ms + @drain_slack_ms
        await_drained(instance, ord, System.monotonic_time(:millisecond) + timeout)
    end
  end

  defp await_drained(instance, ord, deadline) do
    case Counters.read_field(instance, [ord], :inflight) do
      {:ok, [inflight]} when inflight <= 0 ->
        :ok

      {:ok, _} ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, :draining}
        else
          Process.sleep(@drain_poll_ms)
          await_drained(instance, ord, deadline)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ── quarantine ─────────────────────────────────────────────────────────────

  defp sweep_pen(instance, tenant, lower, upper, counts) do
    case Quarantine.page(instance, {lower, upper}, %{tenants: [tenant]}, 500, 50_000) do
      {:ok, hits, last, exhausted?, _scanned} ->
        ops = Enum.flat_map(hits, fn {key, _summary} -> Quarantine.delete_ops(key) end)
        freed = Enum.sum(for {_key, summary} <- hits, do: Map.get(summary, :size, 0))

        with :ok <- Store.write(instance, ops, sync: true) do
          Quarantine.released(instance, freed)

          counts = %{
            counts
            | quarantine: counts.quarantine + length(hits),
              quarantine_bytes: counts.quarantine_bytes + freed
          }

          if exhausted? or last == nil,
            do: {:ok, counts},
            else: sweep_pen(instance, tenant, last <> <<0>>, upper, counts)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ── claims ─────────────────────────────────────────────────────────────────

  # A local store leaves its (empty) partition directories behind object by
  # object; the whole tenant directory goes. The path is the operator's root plus
  # a tenant id that passed `[A-Za-z0-9_-]{1,64}`, never request input.
  # sobelow_skip ["Traversal.FileModule"]
  defp remove_local_dir(instance, opts, prefix) do
    instance |> LocalFS.root(opts) |> Path.join(prefix) |> File.rm_rf()
  end

  # ── entities ───────────────────────────────────────────────────────────────

  # The erase's own source and route deletions are lifecycle events: their
  # entities must survive until the publisher has delivered them.
  defp await_publisher(instance, bound) do
    if Lifecycle.enabled?(instance) and Ankusa.whereis(instance, :lifecycle) != nil do
      await_cursor(instance, bound, System.monotonic_time(:millisecond) + @publisher_wait_ms)
    else
      :ok
    end
  end

  defp await_cursor(instance, bound, deadline) do
    case Store.get(instance, :default, Keys.lifecycle_cursor()) do
      {:ok, <<cursor::64>>} when cursor >= bound ->
        :ok

      {:error, reason} ->
        {:error, reason}

      _behind ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, :publisher_behind}
        else
          Publisher.wake(instance)
          Process.sleep(@publisher_poll_ms)
          await_cursor(instance, bound, deadline)
        end
    end
  end

  # ── commit ─────────────────────────────────────────────────────────────────

  # Sources, routes and an override written for the tenant while the job ran
  # (the admin APIs do not know the tenant is closing) are removed now, and the
  # commit waits for the next attempt, so what it commits is what is left.
  defp settled(job) do
    swept =
      Enum.reduce_while([:sources, :routes, :rate_limit], {:ok, 0}, fn stage, {:ok, n} ->
        case run(stage, job) do
          {:ok, counts} -> {:cont, {:ok, n + counts_sum(counts)}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)

    case swept do
      {:ok, 0} -> :ok
      {:ok, _changed} -> {:error, :changed_during_erase}
      {:error, reason} -> {:error, reason}
    end
  end

  defp counts_sum(counts), do: counts |> Map.values() |> Enum.sum()

  # The tenant's ordinal is retired (a tombstone row with no name: never reused),
  # its counters go, and whatever they still held leaves the node aggregate, so
  # the aggregate stays the sum of the tenants.
  defp retire_ops(instance, tenant) do
    case Tenants.ord(instance, tenant) do
      :not_found ->
        {:ok, [revisions(tenant)]}

      {:error, reason} ->
        {:error, reason}

      {:ok, ord} ->
        with {:ok, held} <- Counters.read(instance, ord) do
          indexed = Enum.with_index(Counters.fields(), 1)

          aggregate =
            for {field, index} <- indexed, (value = Map.fetch!(held, field)) != 0 do
              {:merge, :counters, Keys.counter(0, index), -value}
            end

          rows =
            for {_field, index} <- indexed, do: {:delete, :counters, Keys.counter(ord, index)}

          {:ok,
           [
             {:delete, :tenants, Keys.tenant_ord(tenant)},
             {:put, :tenants, Keys.tenant(ord), <<>>},
             revisions(tenant)
             | aggregate ++ rows
           ]}
        end
    end
  end

  # A source's revision counter outlives the source (`Ankusa.SourceStore`), so
  # the tenant's go with it.
  defp revisions(tenant) do
    {lower, upper} = Keys.range(<<"v:", tenant::binary, 0>>)
    {:delete_range, :default, lower, upper}
  end
end
