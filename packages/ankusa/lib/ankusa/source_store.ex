defmodule Ankusa.SourceStore do
  @moduledoc """
  Source config, secrets, and policy. This module is both the behaviour and the
  instance-scoped facade (resolves `config.source_store` and delegates).

  Reads happen on every request, so adapters should be read-mostly and fast.

  ## Reads and writes

  `fetch/2` and `list/1` are the ingest-facing, read-only callbacks every store
  implements. The optional admin callbacks (`put/5` or `put/6`, `get/3`,
  `list_tenant/2`, `delete/3` or `delete/4`) back the admin API's tenant-scoped
  source management; a store that only reads (like `Ankusa.SourceStore.Static`)
  leaves them out and the facade answers `{:error, :read_only}` / `:error` /
  `[]`. See `Ankusa.SourceStore.Persistent` for the writable implementation.

  ## Audit

  Every change to a source is audited (`Ankusa.Audit`). A store that keeps its
  sources in this node's `Ankusa.Store` exports `put/6` and `delete/4`, which
  take `audit: %Ankusa.Audit.Context{}` and commit the audit row in the same
  synced batch as the change. A store that keeps them elsewhere exports
  `put/5` and `delete/3`; the facade then brackets the call with a pending
  audit row (a dual write: see `Ankusa.Audit`).

  ## Three answers to `fetch/2`

  `{:ok, source}` and `:error` are verdicts: the source exists, or it does not
  (the edge answers `404`, dispatch dead-letters the source's rows as
  `{:source_gone, id}`). A store backed by something that can fail — a
  database, a network service — returns `{:error, :unavailable}` when it could
  not tell: the edge answers `503 store_unavailable` with `Retry-After: 1`,
  dispatch reschedules the row without spending an attempt, and a replay
  stops at that entry and retries it on its next tick. Never answer `:error`
  for "I could not look": that turns an outage into lost hooks.

  Writes follow the same rule: a store that could not persist a `put` or
  `delete` answers `{:error, :store_unavailable}` (the admin API's
  `503 store_unavailable`), never `{:error, :invalid, _}`, which tells the
  caller its request is wrong.

  A store that runs a process exports `start_link/1`. `Ankusa.Instance` starts
  it with `[instance: instance]`, and it reads its options from
  `Ankusa.config(instance).source_store`. Never put configuration in a child
  spec: a supervisor prints its children's start arguments.
  """

  alias Ankusa.Admin.Redact
  alias Ankusa.{Audit, Config, Lifecycle, Source}
  alias Ankusa.Audit.Context

  @typedoc """
  One tenant-scoped source as the store keeps it. `spec` is the submitted JSON
  map with string keys, secrets included; redaction is the caller's job (the
  admin router), never the store's.
  """
  @type stored :: %{
          required(:tenant) => String.t(),
          required(:name) => String.t(),
          required(:source_id) => String.t(),
          required(:spec) => map(),
          optional(:revision) => pos_integer()
        }

  @callback fetch(instance :: atom(), source_id :: String.t()) ::
              {:ok, Source.t()} | :error | {:error, :unavailable}
  @callback list(instance :: atom()) :: [String.t()]

  @callback put(
              instance :: atom(),
              tenant :: String.t(),
              name :: String.t(),
              spec :: map(),
              mode :: :create | :update
            ) ::
              {:ok, stored()}
              | {:error, :invalid, String.t()}
              | {:error, :retry_exceeds_claim_expiry, String.t()}
              | {:error, :exists}
              | {:error, :not_found}
              | {:error, :store_unavailable}
  @callback get(instance :: atom(), tenant :: String.t(), name :: String.t()) ::
              {:ok, stored()} | :error
  @callback list_tenant(instance :: atom(), tenant :: String.t()) :: [stored()]

  @callback delete(instance :: atom(), tenant :: String.t(), name :: String.t()) ::
              :ok
              | {:error, :not_found}
              | {:error, :invalid, String.t()}
              | {:error, :store_unavailable}

  @callback put(
              instance :: atom(),
              tenant :: String.t(),
              name :: String.t(),
              spec :: map(),
              mode :: :create | :update,
              opts :: keyword()
            ) ::
              {:ok, stored()}
              | {:error, :invalid, String.t()}
              | {:error, :retry_exceeds_claim_expiry, String.t()}
              | {:error, :exists}
              | {:error, :not_found}
              | {:error, :store_unavailable}

  @callback delete(
              instance :: atom(),
              tenant :: String.t(),
              name :: String.t(),
              opts :: keyword()
            ) ::
              :ok
              | {:error, :not_found}
              | {:error, :invalid, String.t()}
              | {:error, :store_unavailable}

  @doc false
  @callback ready?(instance :: atom()) :: boolean()

  @optional_callbacks put: 5, put: 6, get: 3, list_tenant: 2, delete: 3, delete: 4, ready?: 1

  # Same rule as `Ankusa.ClaimCheck.Ref.valid_tenant?/1`: an identity that names
  # a storage partition and a URL path segment must not need encoding.
  @identity_regex ~r/\A[A-Za-z0-9_-]{1,64}\z/

  @spec fetch(atom(), String.t()) :: {:ok, Source.t()} | :error | {:error, :unavailable}
  def fetch(instance, source_id) do
    %Config{source_store: {mod, _}} = Ankusa.config(instance)
    mod.fetch(instance, source_id)
  end

  @spec list(atom()) :: [String.t()]
  def list(instance) do
    %Config{source_store: {mod, _}} = Ankusa.config(instance)
    mod.list(instance)
  end

  @doc """
  Whether the store has loaded what it serves. A store that can start empty and
  fill from elsewhere (Redis) says so with the optional `ready?/1` callback; one
  without it is always ready. `Ankusa.Lifecycle.reconcile/2` waits for it before
  it reads a source to settle a change nobody saw finish.
  """
  @spec ready?(atom()) :: boolean()
  def ready?(instance) do
    %Config{source_store: {mod, _}} = Ankusa.config(instance)
    not exports?(mod, :ready?, 1) or mod.ready?(instance)
  end

  @doc """
  Create (`:create`) or replace (`:update`) one tenant-scoped source.

  `tenant` and `name` are validated here, before the store sees them, so a bad
  identity is `{:error, :invalid, message}` no matter which store is configured.
  A store that exports neither `put/6` nor `put/5` is read-only:
  `{:error, :read_only}`. A source that routes to a claim-checking sink in an
  object store whose retry horizon outlasts `claim_check.bucket_expiry_days`
  (`Ankusa.ClaimCheck.check_retry_horizon/2`) is
  `{:error, :retry_exceeds_claim_expiry, message}`.

  Options: `audit:` the `Ankusa.Audit.Context` of the change (default
  `Ankusa.Audit.Context.system/0`).
  """
  @spec put(atom(), String.t(), String.t(), map(), :create | :update, keyword()) ::
          {:ok, stored()}
          | {:error, :invalid, String.t()}
          | {:error, :retry_exceeds_claim_expiry, String.t()}
          | {:error, :exists}
          | {:error, :not_found}
          | {:error, :store_unavailable}
          | {:error, :read_only}
  def put(instance, tenant, name, spec, mode, opts \\ []) do
    with :ok <- validate_identity(tenant, "tenant"),
         :ok <- validate_identity(name, "name") do
      %Config{source_store: {mod, _}} = Ankusa.config(instance)
      context = Context.from_opts(opts)

      cond do
        exports?(mod, :put, 6) ->
          mod.put(instance, tenant, name, spec, mode, audit: context)

        exports?(mod, :put, 5) ->
          audited_put(mod, instance, context, {tenant, name}, spec, mode)

        true ->
          {:error, :read_only}
      end
    end
  end

  # A store whose sources live elsewhere (Redis): the change cannot share a
  # batch with its audit row, so the row is written first, pending, and
  # finished with the outcome. A request the store rejects (it exists, it
  # does not, the spec is invalid) changed nothing and leaves no row.
  defp audited_put(mod, instance, context, {tenant, name}, spec, mode) do
    attrs = %{
      action: if(mode == :create, do: "source.create", else: "source.update"),
      tenant: tenant,
      resource: "#{tenant}.#{name}",
      before_hash: prior_hash(mod, instance, tenant, name),
      after_hash: Audit.digest(instance, spec)
    }

    Audit.record(instance, context, attrs, fn ->
      result = mod.put(instance, tenant, name, spec, mode)
      {result, put_outcome(instance, result)}
    end)
  end

  # The outcome of a store call that changed a source; the entity of its event
  # when lifecycle events are on.
  defp put_outcome(instance, {:ok, stored}) do
    outcome = %{
      result: "ok",
      after_hash: Audit.digest(instance, stored.spec),
      revision: Map.get(stored, :revision)
    }

    case Lifecycle.outbox(instance, fn -> Redact.source_entry(stored) end) do
      %{entity: _} = outbox -> outcome |> Map.merge(outbox) |> Map.put(:publish, true)
      none -> Map.merge(outcome, none)
    end
  end

  defp put_outcome(_instance, {:error, :store_unavailable}), do: %{result: "failed"}
  defp put_outcome(_instance, _rejected), do: :discard

  defp prior_hash(mod, instance, tenant, name) do
    if exports?(mod, :get, 3) do
      case mod.get(instance, tenant, name) do
        {:ok, stored} -> Audit.digest(instance, stored.spec)
        :error -> nil
      end
    end
  end

  @doc "Fetch one tenant-scoped source, or `:error` if there is no such source."
  @spec get(atom(), String.t(), String.t()) :: {:ok, stored()} | :error
  def get(instance, tenant, name) do
    %Config{source_store: {mod, _}} = Ankusa.config(instance)

    if exports?(mod, :get, 3), do: mod.get(instance, tenant, name), else: :error
  end

  @doc "Every source owned by `tenant`, or `[]` for a read-only store."
  @spec list_tenant(atom(), String.t()) :: [stored()]
  def list_tenant(instance, tenant) do
    %Config{source_store: {mod, _}} = Ankusa.config(instance)

    if exports?(mod, :list_tenant, 2), do: mod.list_tenant(instance, tenant), else: []
  end

  @doc """
  Delete one tenant-scoped source.

  `tenant` and `name` are validated here, before the store sees them, so a bad
  identity is `{:error, :invalid, message}` no matter which store is configured.
  A store that exports neither `delete/4` nor `delete/3` is read-only:
  `{:error, :read_only}`. Options: `audit:`, as for `put/6`.
  """
  @spec delete(atom(), String.t(), String.t(), keyword()) ::
          :ok
          | {:error, :not_found}
          | {:error, :invalid, String.t()}
          | {:error, :store_unavailable}
          | {:error, :read_only}
  def delete(instance, tenant, name, opts \\ []) do
    with :ok <- validate_identity(tenant, "tenant"),
         :ok <- validate_identity(name, "name") do
      %Config{source_store: {mod, _}} = Ankusa.config(instance)
      context = Context.from_opts(opts)

      cond do
        exports?(mod, :delete, 4) ->
          mod.delete(instance, tenant, name, audit: context)

        exports?(mod, :delete, 3) ->
          prior = prior(mod, instance, tenant, name)
          audited_delete(mod, instance, context, tenant, name, prior)

        true ->
          {:error, :read_only}
      end
    end
  end

  # Read before the delete: the event's `data` is the last view of the source,
  # and there is nothing left to read afterwards.
  defp prior(mod, instance, tenant, name),
    do: if(exports?(mod, :get, 3), do: mod.get(instance, tenant, name), else: :error)

  defp audited_delete(mod, instance, context, tenant, name, prior) do
    entity =
      Lifecycle.outbox(instance, fn ->
        case prior do
          {:ok, stored} -> Redact.source_entry(stored)
          :error -> %{"tenant" => tenant, "name" => name}
        end
      end)

    attrs =
      Map.merge(
        %{
          action: "source.delete",
          tenant: tenant,
          resource: "#{tenant}.#{name}",
          before_hash: with({:ok, stored} <- prior, do: Audit.digest(instance, stored.spec))
        },
        entity
      )

    Audit.record(instance, context, attrs, fn ->
      result = mod.delete(instance, tenant, name)

      outcome =
        case result do
          :ok -> %{result: "ok", publish: Map.has_key?(attrs, :entity)}
          {:error, :store_unavailable} -> %{result: "failed"}
          _rejected -> :discard
        end

      {result, outcome}
    end)
  end

  defp validate_identity(value, label) when is_binary(value) do
    if Regex.match?(@identity_regex, value) do
      :ok
    else
      {:error, :invalid, invalid_message(label, value)}
    end
  end

  defp validate_identity(value, label), do: {:error, :invalid, invalid_message(label, value)}

  defp invalid_message(label, value) do
    "#{label} #{inspect(value)} must match [A-Za-z0-9_-]{1,64}"
  end

  defp exports?(mod, fun, arity) do
    Code.ensure_loaded?(mod) and function_exported?(mod, fun, arity)
  end
end

defmodule Ankusa.SourceStore.Static do
  @moduledoc """
  Default source store: sources are declared in config as a map of
  `source_id => keyword/map` (see `Ankusa.Source.new/2`) and cached in
  `:persistent_term`. Change rarely, read on every request.

  It implements only the read callbacks, so the admin API's write routes answer
  `409 source_store_read_only` against it. Use
  `Ankusa.SourceStore.Persistent` to manage sources at runtime.
  """

  @behaviour Ankusa.SourceStore

  alias Ankusa.{Config, Source}

  @impl true
  def fetch(instance, source_id) do
    case Map.fetch(sources(instance), source_id) do
      {:ok, %Source{} = s} -> {:ok, s}
      {:ok, opts} -> {:ok, Source.new(source_id, opts)}
      :error -> :error
    end
  end

  @impl true
  def list(instance), do: Map.keys(sources(instance))

  defp sources(instance) do
    %Config{source_store: {_mod, opts}} = Ankusa.config(instance)
    Keyword.get(opts, :sources, %{})
  end

  @doc """
  Replace the sources a running instance serves with `sources` (the same
  `source_id => keyword/map` shape as the `:sources` option), without a restart.

  This is how a config reload rotates a secret: the caller re-reads its config,
  resolves its secrets again and publishes the result. Every source is built
  and checked with the rules the instance boots under (`wal: :none` durability,
  a shared source with no verifier, the verifier's own options, egress), so a
  spec that would refuse to boot is refused here and the instance keeps serving
  the sources it has. Nothing is applied unless all of them pass.

  Reads see the new config on their next `fetch/2`, so a hook already being
  verified finishes against the secret it started with. Returns the ids that
  were added, removed, or changed, sorted. Raises `ArgumentError` naming the
  source when a spec is refused, and `ArgumentError` when the instance does
  not run `Ankusa.SourceStore.Static`: a writable store keeps its sources
  elsewhere.
  """
  @spec publish(atom(), %{String.t() => keyword() | map() | Source.t()}) :: {:ok, [String.t()]}
  def publish(instance, sources) when is_map(sources) do
    %Config{source_store: {__MODULE__, opts}} =
      config =
      case Ankusa.config(instance) do
        %Config{source_store: {__MODULE__, _}} = config ->
          config

        %Config{source_store: {mod, _}} ->
          raise ArgumentError,
                "#{inspect(__MODULE__)}.publish/2 needs the static source store, " <>
                  "instance #{inspect(instance)} runs #{inspect(mod)}"
      end

    old = config |> Ankusa.Queue.static_sources() |> Map.new()
    updated = %Config{config | source_store: {__MODULE__, Keyword.put(opts, :sources, sources)}}

    Ankusa.Queue.validate_config!(updated)
    Ankusa.Verifier.validate_config!(updated)
    Ankusa.Verifier.validate_shared!(updated)
    Ankusa.HttpClient.Egress.validate_config!(updated)

    new = updated |> Ankusa.Queue.static_sources() |> Map.new()
    :ok = Ankusa.put_config(updated)

    changed =
      (Map.keys(old) ++ Map.keys(new))
      |> Enum.uniq()
      |> Enum.filter(&(Map.get(old, &1) != Map.get(new, &1)))
      |> Enum.sort()

    {:ok, changed}
  end
end
