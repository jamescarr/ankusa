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

  ## Secrets at rest

  With `secrets.kek` (or `secrets.kek_provider`) configured, the secret-bearing
  fields of a spec (`Ankusa.Admin.Redact.reduce_hidden/3`: verify secrets, sink
  credentials, header values) are encrypted before the row is written, so
  neither the node's store files nor a backup of them hold a secret in the
  clear (`Ankusa.Crypto.Envelope`). Rows are decrypted as they are loaded; ETS
  and the admin API's merge-the-stored-secret update see plaintext, as before.
  A row this node cannot decrypt (no KEK, a key it was not given, a damaged
  value) **refuses the boot**: skipping it would `404` every hook for that
  source. `rewrap/1` (and `mix ankusa.secrets.rewrap` for a stopped node)
  moves every row to the current key, and encrypts rows written before a KEK
  existed.
  """

  @behaviour Ankusa.SourceStore

  use GenServer

  require Logger

  alias Ankusa.{Config, Crypto.Envelope, SourceStore.Table, Store}
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

  @doc """
  Rewrite every stored row under the current KEK: rows still in plaintext, and
  rows encrypted under a `secrets.kek_previous` key. Returns how many rows were
  rewritten and how many were already current.
  """
  @spec rewrap(atom()) ::
          {:ok, %{rewrapped: non_neg_integer(), current: non_neg_integer()}} | {:error, term()}
  def rewrap(instance) do
    GenServer.call(Ankusa.via(instance, :source_store), :rewrap, :infinity)
  end

  # ── GenServer ───────────────────────────────────────────────────────────────

  @impl true
  def init(instance) do
    %Config{source_store: {__MODULE__, opts}} = config = Ankusa.config(instance)

    case Envelope.keyring(config.secrets) do
      {:ok, keyring} ->
        boot(
          instance,
          config,
          Keyword.get(opts, :decoder),
          Keyword.get(opts, :sources, %{}),
          keyring
        )

      {:error, reason} ->
        Logger.error(
          "[ankusa] source store could not resolve its secrets key: #{inspect(reason)}"
        )

        {:stop, {:source_store_load_failed, {:secrets, reason}}}
    end
  end

  defp boot(instance, config, decoder, seeds, keyring) do
    table = Table.new(instance)
    Table.insert_seeds(table, seeds)

    case load_persisted(instance, table, decoder, keyring) do
      :ok ->
        Ankusa.Verifier.warn_stored_shared(config, Table.stored_sources(table))
        {:ok, %{instance: instance, table: table, decoder: decoder, keyring: keyring}}

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

  @impl true
  def handle_call(:rewrap, _from, state) do
    {:reply, rewrap_rows(state.instance, state.keyring), state}
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
      ops = [{:put, :default, Keys.source(tenant, name), encode(state.keyring, spec)}]

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

  defp encode(keyring, spec), do: keyring |> Envelope.seal(spec) |> JSON.encode!()

  # One key per source (`s:<tenant>\0<name>` -> the spec as JSON, its secrets
  # encrypted when a KEK is configured), written synced: a successful `PUT`
  # answers "stored". A row that is rejected on boot (a seed collision, a spec
  # the decoder no longer accepts) is skipped with a warning and stays in the
  # store untouched, so rolling a config change back brings it straight back. A
  # row that cannot be decrypted is not skipped: it stops the boot.
  defp load_persisted(instance, table, decoder, keyring) do
    %{lo: lo, hi: hi} = Keys.family(:sources)

    result =
      Store.fold(instance, :sources, {lo, hi}, :ok, fn key, value, :ok ->
        case load_entry(Keys.decode_source(key), value, table, decoder, keyring) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    case result do
      {:ok, :ok} -> :ok
      {:ok, {:error, reason}} -> {:error, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  defp load_entry({tenant, name}, value, table, decoder, keyring) do
    source_id = Table.source_id(tenant, name)

    with {:ok, spec} when is_map(spec) <- JSON.decode(value),
         {:ok, spec} <- unseal(keyring, source_id, spec) do
      Table.load_spec(table, decoder, tenant, name, spec)
      :ok
    else
      {:error, {:secrets, ^source_id, _reason}} = error ->
        error

      _not_a_spec ->
        Logger.warning(
          "[ankusa] skipping persisted source #{source_id}: stored spec is not a JSON object"
        )
    end
  end

  defp load_entry(nil, _value, _table, _decoder, _keyring) do
    Logger.warning("[ankusa] skipping a persisted source with a malformed key")
  end

  defp unseal(keyring, source_id, spec) do
    case Envelope.unseal(keyring, spec) do
      {:ok, spec} ->
        {:ok, spec}

      {:error, reason} ->
        Logger.error(
          "[ankusa] persisted source #{source_id} cannot be decrypted (#{inspect(reason)}); " <>
            "configure secrets.kek, or secrets.kek_previous for the key it was written under. " <>
            "Refusing to start: skipping it would 404 every hook for the source."
        )

        {:error, {:secrets, source_id, reason}}
    end
  end

  # ── rewrap ──────────────────────────────────────────────────────────────────

  # Rows move in synced batches, so a store of a million sources is never held
  # in memory, and an interrupted run leaves every row readable under one key
  # or the other.
  @rewrap_batch 256

  @doc """
  `rewrap/1` on a store nobody has open as a source store (the
  `mix ankusa.secrets.rewrap` task, a test): `Ankusa.Store` must be running for
  `instance`. `dry_run: true` counts and writes nothing.
  """
  @spec rewrap_rows(atom(), Envelope.keyring(), keyword()) ::
          {:ok, %{rewrapped: non_neg_integer(), current: non_neg_integer()}} | {:error, term()}
  def rewrap_rows(instance, keyring, opts \\ []) do
    %{lo: lo, hi: hi} = Keys.family(:sources)
    dry_run? = Keyword.get(opts, :dry_run, false)
    rewrap_from(instance, keyring, dry_run?, {lo, hi}, %{rewrapped: 0, current: 0})
  end

  defp rewrap_from(instance, keyring, dry_run?, {_lower, upper} = range, totals) do
    fresh = %{ops: [], current: 0, last: nil, error: nil}

    scan =
      Store.fold(instance, :sources, range, fresh, fn key, value, acc ->
        case rewrap_row(keyring, key, value) do
          {:ok, row} ->
            acc = %{acc | ops: [{:put, :default, key, row} | acc.ops], last: key}
            if length(acc.ops) >= @rewrap_batch, do: {:halt, acc}, else: {:cont, acc}

          :current ->
            {:cont, %{acc | current: acc.current + 1, last: key}}

          {:error, reason} ->
            {:halt, %{acc | error: reason}}
        end
      end)

    with {:ok, %{error: nil} = acc} <- scan,
         :ok <- write_rewrapped(instance, acc.ops, dry_run?) do
      totals = %{
        rewrapped: totals.rewrapped + length(acc.ops),
        current: totals.current + acc.current
      }

      if length(acc.ops) >= @rewrap_batch,
        do: rewrap_from(instance, keyring, dry_run?, {acc.last <> <<0>>, upper}, totals),
        else: {:ok, totals}
    else
      {:ok, %{error: reason}} -> {:error, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  defp write_rewrapped(_instance, [], _dry_run?), do: :ok
  defp write_rewrapped(_instance, _ops, true), do: :ok
  defp write_rewrapped(instance, ops, false), do: Store.write(instance, ops, sync: true)

  # A row that is not a JSON object is left as it is, as it is at boot.
  defp rewrap_row(keyring, key, value) do
    case JSON.decode(value) do
      {:ok, spec} when is_map(spec) -> rewrap_spec(keyring, key, spec)
      _not_a_spec -> :current
    end
  end

  defp rewrap_spec(keyring, key, spec) do
    case Envelope.rewrap_spec(keyring, spec) do
      {:ok, sealed} -> {:ok, JSON.encode!(sealed)}
      :current -> :current
      {:error, reason} -> {:error, {:secrets, row_id(key), reason}}
    end
  end

  defp row_id(key) do
    case Keys.decode_source(key) do
      {tenant, name} -> Table.source_id(tenant, name)
      nil -> "(malformed key)"
    end
  end
end
