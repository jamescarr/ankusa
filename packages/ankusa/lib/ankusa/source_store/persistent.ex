defmodule Ankusa.SourceStore.Persistent do
  @moduledoc """
  Writable source store: seed sources from config plus API-managed, tenant-scoped
  sources persisted to disk.

  Seeds (`sources:` in the store opts, the same shape `Ankusa.SourceStore.Static`
  takes) are resolved at boot and are read-only: they never appear in
  `list_tenant/2` and cannot be written through `put/5`. Sources created or
  updated through the admin API are kept in ETS and written to this node's
  `Ankusa.Store` (one key per source), so they survive a restart. They are this
  node's alone: a fleet that manages sources through the API on more than one
  node shares them through `Ankusa.SourceStore.Redis` (`ankusa_redis`) instead.

  ## Reads and writes

  Reads (`fetch/2`, `get/3`, `list_tenant/2`) go straight to a `:protected` ETS
  table, so they never serialize behind the GenServer. Writes go through the
  GenServer, which is the table's owner and the only process that may modify it,
  and persist the one changed key, synced, before replying.

  ## The decoder

  The store does not know how to turn a JSON spec into a `Ankusa.Source` — that
  is the operator's schema (`AnkusaServer.Config.source_from_map!/2` in the
  server). It takes a `decoder:` function of `(source_id, spec_map -> keyword)`
  and calls it on every write, and on every entry loaded from disk. A decoder
  that raises fails the write with `{:error, :invalid, Exception.message(e)}`,
  and a persisted entry that no longer decodes is skipped with a warning rather
  than taken down the boot.
  """

  @behaviour Ankusa.SourceStore

  use GenServer

  require Logger

  alias Ankusa.{Config, SourceStore.Table, Store}
  alias Ankusa.Store.Keys

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, instance, name: Ankusa.via(instance, :source_store))
  end

  # ── reads, straight from ETS (`Ankusa.SourceStore.Table`) ──────────────────

  @impl true
  defdelegate fetch(instance, source_id), to: Table

  @impl true
  defdelegate list(instance), to: Table

  @impl true
  defdelegate get(instance, tenant, name), to: Table

  @impl true
  defdelegate list_tenant(instance, tenant), to: Table

  # ── writes, through the GenServer ───────────────────────────────────────────

  @impl true
  def put(instance, tenant, name, spec, mode) do
    GenServer.call(Ankusa.via(instance, :source_store), {:put, tenant, name, spec, mode})
  end

  @impl true
  def delete(instance, tenant, name) do
    GenServer.call(Ankusa.via(instance, :source_store), {:delete, tenant, name})
  end

  # ── GenServer ───────────────────────────────────────────────────────────────

  @impl true
  def init(instance) do
    %Config{source_store: {__MODULE__, opts}} = config = Ankusa.config(instance)
    table = Table.new(instance)
    decoder = Keyword.get(opts, :decoder)
    Table.insert_seeds(table, Keyword.get(opts, :sources, %{}))

    case load_persisted(instance, table, decoder) do
      :ok ->
        Ankusa.Verifier.warn_stored_shared(config, Table.stored_sources(table))
        {:ok, %{instance: instance, table: table, decoder: decoder}}

      # Booting without the persisted sources would 404 every hook for them.
      {:error, reason} ->
        {:stop, {:source_store_load_failed, reason}}
    end
  end

  # A crash report prints the last message: a `put` carries the new source's
  # sinks and secrets.
  @impl true
  def format_status(status) do
    Map.new(status, fn
      {:message, {:put, tenant, name, _spec, mode}} ->
        {:message, {:put, tenant, name, :redacted, mode}}

      other ->
        other
    end)
  end

  @impl true
  def handle_call({:put, tenant, name, spec, mode}, _from, state) do
    source_id = Table.source_id(tenant, name)

    reply =
      cond do
        Table.seed?(state.table, source_id) ->
          {:error, :invalid, "source #{source_id} is seeded from configuration and is read-only"}

        not is_map(spec) ->
          {:error, :invalid, "spec must be a JSON object"}

        true ->
          apply_put(state, tenant, name, source_id, spec, mode)
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, tenant, name}, _from, state) do
    source_id = Table.source_id(tenant, name)

    reply =
      cond do
        Table.seed?(state.table, source_id) ->
          {:error, :invalid, "source #{source_id} is seeded from configuration and is read-only"}

        is_nil(Table.lookup_stored(state.table, tenant, name)) ->
          {:error, :not_found}

        true ->
          apply_delete(state, tenant, name, source_id)
      end

    {:reply, reply, state}
  end

  defp apply_delete(state, tenant, name, source_id) do
    ops = [{:delete, :default, Keys.source(tenant, name)}]

    case Store.write(state.instance, ops, sync: true) do
      :ok ->
        Table.delete_rows(state.table, tenant, name)
        :ok

      {:error, reason} ->
        Logger.error("[ankusa] could not persist source #{source_id}: #{inspect(reason)}")
        {:error, :store_unavailable}
    end
  end

  defp apply_put(state, tenant, name, source_id, spec, mode) do
    current = Table.lookup_stored(state.table, tenant, name)

    mode
    |> allowed?(current)
    |> case do
      :ok -> write(state, tenant, name, source_id, spec, current)
      error -> error
    end
  end

  defp allowed?(:create, nil), do: :ok
  defp allowed?(:create, _stored), do: {:error, :exists}
  defp allowed?(:update, nil), do: {:error, :not_found}
  defp allowed?(:update, _stored), do: :ok

  defp write(state, tenant, name, source_id, spec, current) do
    spec = Table.normalize(spec, current)

    with {:ok, stored, source} <- Table.build(state.decoder, tenant, name, spec),
         :ok <- Table.check_write(state.instance, source) do
      ops = [{:put, :default, Keys.source(tenant, name), JSON.encode!(spec)}]

      case Store.write(state.instance, ops, sync: true) do
        :ok ->
          Table.put_rows(state.table, stored, source)
          {:ok, stored}

        {:error, reason} ->
          Logger.error("[ankusa] could not persist source #{source_id}: #{inspect(reason)}")
          {:error, :store_unavailable}
      end
    end
  end

  # ── persistence ─────────────────────────────────────────────────────────────

  # One key per source (`s:<tenant>\0<name>` -> the spec as JSON), written
  # synced: a successful `PUT` answers "stored". A row that is rejected on boot
  # (a seed collision, a spec the decoder no longer accepts) is skipped with a
  # warning and stays in the store untouched, so rolling a config change back
  # brings it straight back.
  defp load_persisted(instance, table, decoder) do
    %{lo: lo, hi: hi} = Keys.family(:sources)

    result =
      Store.fold(instance, :sources, {lo, hi}, :ok, fn key, value, :ok ->
        load_entry(Keys.decode_source(key), value, table, decoder)
        {:cont, :ok}
      end)

    case result do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp load_entry({tenant, name}, value, table, decoder) do
    source_id = Table.source_id(tenant, name)

    case JSON.decode(value) do
      {:ok, spec} when is_map(spec) ->
        Table.load_spec(table, decoder, tenant, name, spec)

      _ ->
        Logger.warning(
          "[ankusa] skipping persisted source #{source_id}: stored spec is not a JSON object"
        )
    end
  end

  defp load_entry(nil, _value, _table, _decoder) do
    Logger.warning("[ankusa] skipping a persisted source with a malformed key")
  end
end
