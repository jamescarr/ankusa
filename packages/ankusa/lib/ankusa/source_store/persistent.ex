defmodule Ankusa.SourceStore.Persistent do
  @moduledoc """
  Writable source store: seed sources from config plus API-managed, tenant-scoped
  sources persisted to disk.

  Seeds (`sources:` in the store opts, the same shape `Ankusa.SourceStore.Static`
  takes) are resolved at boot and are read-only: they never appear in
  `list_tenant/2` and cannot be written through `put/5`. Sources created or
  updated through the admin API are kept in ETS and written to this node's
  `Ankusa.Store` (one key per source), so they survive a restart.

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

  alias Ankusa.{Config, Source, Store}
  alias Ankusa.Store.Keys

  @spec start_link(Config.t()) :: GenServer.on_start()
  def start_link(%Config{} = config) do
    GenServer.start_link(__MODULE__, config, name: Ankusa.via(config.instance, :source_store))
  end

  # ── reads, straight from ETS ────────────────────────────────────────────────

  @impl true
  def fetch(instance, source_id) do
    case :ets.lookup(table(instance), {:source, source_id}) do
      [{_, {_stored, source}}] -> {:ok, source}
      [] -> :error
    end
  end

  @impl true
  def list(instance) do
    :ets.select(table(instance), [{{{:source, :"$1"}, :_}, [], [:"$1"]}])
  end

  @impl true
  def get(instance, tenant, name) do
    case :ets.lookup(table(instance), {:stored, tenant, name}) do
      [{_, stored}] -> {:ok, stored}
      [] -> :error
    end
  end

  @impl true
  def list_tenant(instance, tenant) do
    table(instance)
    |> :ets.select([{{{:stored, tenant, :_}, :"$1"}, [], [:"$1"]}])
    |> Enum.sort_by(& &1.name)
  end

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
  def init(%Config{source_store: {__MODULE__, opts}} = config) do
    table =
      :ets.new(table(config.instance), [:named_table, :protected, :set, read_concurrency: true])

    decoder = Keyword.get(opts, :decoder)
    seeds = Keyword.get(opts, :sources, %{})

    Enum.each(seeds, fn {source_id, source_opts} ->
      source = Source.new(source_id, source_opts)
      :ets.insert(table, {{:source, source_id}, {nil, source}})
    end)

    case load_persisted(config.instance, table, decoder) do
      :ok ->
        {:ok, %{config: config, table: table, decoder: decoder}}

      # Booting without the persisted sources would 404 every hook for them.
      {:error, reason} ->
        {:stop, {:source_store_load_failed, reason}}
    end
  end

  @impl true
  def handle_call({:put, tenant, name, spec, mode}, _from, state) do
    source_id = source_id(tenant, name)

    reply =
      cond do
        seed?(state.table, source_id) ->
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
    source_id = source_id(tenant, name)

    reply =
      cond do
        seed?(state.table, source_id) ->
          {:error, :invalid, "source #{source_id} is seeded from configuration and is read-only"}

        is_nil(lookup_stored(state.table, tenant, name)) ->
          {:error, :not_found}

        true ->
          apply_delete(state, tenant, name, source_id)
      end

    {:reply, reply, state}
  end

  defp apply_delete(state, tenant, name, source_id) do
    ops = [{:delete, :default, Keys.source(tenant, name)}]

    case Store.write(state.config.instance, ops, sync: true) do
      :ok ->
        :ets.delete(state.table, {:source, source_id})
        :ets.delete(state.table, {:stored, tenant, name})
        :ok

      {:error, reason} ->
        Logger.error("[ankusa] could not persist source #{source_id}: #{inspect(reason)}")
        {:error, :invalid, "could not persist source: #{inspect(reason)}"}
    end
  end

  defp apply_put(state, tenant, name, source_id, spec, mode) do
    current = lookup_stored(state.table, tenant, name)

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
    spec = normalize(spec, current)

    case decode(state.decoder, source_id, spec) do
      {:ok, source_opts} ->
        entry = %{tenant: tenant, name: name, source_id: source_id, spec: spec}
        source = Source.new(source_id, Keyword.put(source_opts, :tenant_id, tenant))
        ops = [{:put, :default, Keys.source(tenant, name), JSON.encode!(spec)}]

        case Store.write(state.config.instance, ops, sync: true) do
          :ok ->
            :ets.insert(state.table, [
              {{:source, source_id}, {entry, source}},
              {{:stored, tenant, name}, entry}
            ])

            {:ok, entry}

          {:error, reason} ->
            Logger.error("[ankusa] could not persist source #{source_id}: #{inspect(reason)}")
            {:error, :invalid, "could not persist source: #{inspect(reason)}"}
        end

      {:error, :invalid, message} ->
        {:error, :invalid, message}
    end
  end

  # Identity lives in the URL, so a `tenant`/`name` key smuggled into a spec is
  # dropped before validation. On update, a verify block that keeps its type but
  # omits its secret inherits the stored one, so a client can edit a source
  # without ever re-sending the secret.
  defp normalize(spec, current) do
    spec
    |> Map.drop(["tenant", "name"])
    |> merge_secret(current)
  end

  defp merge_secret(spec, %{spec: stored_spec}) do
    new_verify = spec["verify"]

    with true <- is_map(new_verify),
         false <- Map.has_key?(new_verify, "secret"),
         stored_verify when is_map(stored_verify) <- stored_spec["verify"],
         true <- Map.get(new_verify, "type") == Map.get(stored_verify, "type"),
         secret when is_binary(secret) <- Map.get(stored_verify, "secret") do
      put_in(spec, ["verify", "secret"], secret)
    else
      _ -> spec
    end
  end

  defp merge_secret(spec, nil), do: spec

  defp decode(decoder, source_id, spec) when is_function(decoder, 2) do
    {:ok, decoder.(source_id, spec)}
  rescue
    error -> {:error, :invalid, Exception.message(error)}
  end

  defp decode(_decoder, _source_id, _spec), do: {:error, :invalid, "no decoder configured"}

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
    source_id = source_id(tenant, name)

    case JSON.decode(value) do
      {:ok, spec} when is_map(spec) ->
        load_spec(tenant, name, source_id, spec, table, decoder)

      _ ->
        Logger.warning(
          "[ankusa] skipping persisted source #{source_id}: stored spec is not a JSON object"
        )
    end
  end

  defp load_entry(nil, _value, _table, _decoder) do
    Logger.warning("[ankusa] skipping a persisted source with a malformed key")
  end

  defp load_spec(tenant, name, source_id, spec, table, decoder) do
    if seed?(table, source_id) do
      Logger.warning(
        "[ankusa] skipping persisted source #{source_id}: it is seeded from configuration"
      )
    else
      case decode(decoder, source_id, spec) do
        {:ok, source_opts} ->
          source = Source.new(source_id, Keyword.put(source_opts, :tenant_id, tenant))
          stored = %{tenant: tenant, name: name, source_id: source_id, spec: spec}

          :ets.insert(table, [
            {{:source, source_id}, {stored, source}},
            {{:stored, tenant, name}, stored}
          ])

        {:error, :invalid, message} ->
          Logger.warning("[ankusa] skipping persisted source #{source_id}: #{message}")
      end
    end
  end

  # ── ETS helpers ─────────────────────────────────────────────────────────────

  defp lookup_stored(table, tenant, name) do
    case :ets.lookup(table, {:stored, tenant, name}) do
      [{_, stored}] -> stored
      [] -> nil
    end
  end

  defp seed?(table, source_id) do
    case :ets.lookup(table, {:source, source_id}) do
      [{_, {nil, _source}}] -> true
      _ -> false
    end
  end

  defp source_id(tenant, name), do: "#{tenant}.#{name}"

  @doc false
  @spec table(atom()) :: atom()
  def table(instance), do: :"ankusa_source_store_#{instance}"
end
