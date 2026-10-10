defmodule Ankusa.SourceStore.Table do
  @moduledoc false
  # The in-memory side every writable source store shares
  # (`Ankusa.SourceStore.Persistent`, and `Ankusa.SourceStore.Redis` in
  # `ankusa_redis`): one `:protected` ETS table the reads go straight to, and
  # the spec handling every write and every load runs through, so a source
  # resolves the same way whichever store keeps it.
  #
  # Rows:
  #
  #   * `{{:source, source_id}, {stored | nil, %Ankusa.Source{}}}` — `nil` for a
  #     seed from configuration (read-only), the stored entry otherwise
  #   * `{{:stored, tenant, name}, stored}` — API-managed sources only
  #
  # A store may add rows of its own under other keys (the Redis store keeps a
  # `{:meta, version}` row).

  require Logger

  alias Ankusa.{Source, SourceStore}

  @spec table(atom()) :: atom()
  def table(instance), do: :"ankusa_source_store_#{instance}"

  @doc "Create the instance's table, owned by the caller."
  @spec new(atom(), list()) :: :ets.table()
  def new(instance, extra_opts \\ []) do
    :ets.new(
      table(instance),
      [:named_table, :protected, :set, {:read_concurrency, true} | extra_opts]
    )
  end

  @doc "Insert the configuration seeds (read-only sources)."
  @spec insert_seeds(:ets.table(), map()) :: :ok
  def insert_seeds(table, seeds) do
    Enum.each(seeds, fn {source_id, source_opts} ->
      :ets.insert(table, {{:source, source_id}, {nil, Source.new(source_id, source_opts)}})
    end)
  end

  # ── reads ─────────────────────────────────────────────────────────────────
  #
  # A missing table is a store between restarts: `fetch/2` answers
  # `{:error, :unavailable}` (the edge `503`s, dispatch reschedules) rather than
  # `:error`, which would be a verdict that the source is gone.

  @spec fetch(atom(), String.t()) :: {:ok, Source.t()} | :error | {:error, :unavailable}
  def fetch(instance, source_id) do
    case :ets.lookup(table(instance), {:source, source_id}) do
      [{_, {_stored, source}}] -> {:ok, source}
      [] -> :error
    end
  rescue
    ArgumentError -> {:error, :unavailable}
  end

  @spec list(atom()) :: [String.t()]
  def list(instance) do
    :ets.select(table(instance), [{{{:source, :"$1"}, :_}, [], [:"$1"]}])
  rescue
    ArgumentError -> []
  end

  @spec get(atom(), String.t(), String.t()) :: {:ok, SourceStore.stored()} | :error
  def get(instance, tenant, name) do
    case :ets.lookup(table(instance), {:stored, tenant, name}) do
      [{_, stored}] -> {:ok, stored}
      [] -> :error
    end
  rescue
    ArgumentError -> :error
  end

  @spec list_tenant(atom(), String.t()) :: [SourceStore.stored()]
  def list_tenant(instance, tenant) do
    instance
    |> table()
    |> :ets.select([{{{:stored, tenant, :_}, :"$1"}, [], [:"$1"]}])
    |> Enum.sort_by(& &1.name)
  rescue
    ArgumentError -> []
  end

  @spec lookup_stored(:ets.table(), String.t(), String.t()) :: SourceStore.stored() | nil
  def lookup_stored(table, tenant, name) do
    case :ets.lookup(table, {:stored, tenant, name}) do
      [{_, stored}] -> stored
      [] -> nil
    end
  end

  @spec seed?(:ets.table(), String.t()) :: boolean()
  def seed?(table, source_id) do
    case :ets.lookup(table, {:source, source_id}) do
      [{_, {nil, _source}}] -> true
      _ -> false
    end
  end

  @doc "Every API-managed `{tenant, name}` in the table (seeds excluded)."
  @spec stored_keys(:ets.table()) :: [{String.t(), String.t()}]
  def stored_keys(table) do
    :ets.select(table, [{{{:stored, :"$1", :"$2"}, :_}, [], [{{:"$1", :"$2"}}]}])
  end

  @doc "Every API-managed source in the table (seeds excluded)."
  @spec stored_sources(:ets.table()) :: [Source.t()]
  def stored_sources(table) do
    :ets.select(table, [{{{:source, :_}, {:"$1", :"$2"}}, [{:"=/=", :"$1", nil}], [:"$2"]}])
  end

  @spec source_id(String.t(), String.t()) :: String.t()
  def source_id(tenant, name), do: "#{tenant}.#{name}"

  # ── specs ─────────────────────────────────────────────────────────────────

  @doc """
  The spec a write stores. Identity lives in the URL, so a `tenant`/`name` key
  smuggled into a spec is dropped before validation. On update, a verify block
  that keeps its type but omits its secret inherits the stored one, so a client
  can edit a source without ever re-sending the secret.
  """
  @spec normalize(map(), SourceStore.stored() | nil) :: map()
  def normalize(spec, current) do
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

  @doc """
  Run the store's decoder (`(source_id, spec_map -> keyword)`). A decoder that
  raises is `{:error, :invalid, Exception.message(e)}`.
  """
  @spec decode(function() | nil, String.t(), map()) ::
          {:ok, keyword()} | {:error, :invalid, String.t()}
  def decode(decoder, source_id, spec) when is_function(decoder, 2) do
    {:ok, decoder.(source_id, spec)}
  rescue
    error -> {:error, :invalid, Exception.message(error)}
  end

  def decode(_decoder, _source_id, _spec), do: {:error, :invalid, "no decoder configured"}

  @doc "Decode `spec` into the stored entry and the `Ankusa.Source` it resolves to."
  @spec build(function() | nil, String.t(), String.t(), map()) ::
          {:ok, SourceStore.stored(), Source.t()} | {:error, :invalid, String.t()}
  def build(decoder, tenant, name, spec) do
    source_id = source_id(tenant, name)

    with {:ok, source_opts} <- decode(decoder, source_id, spec) do
      stored = %{tenant: tenant, name: name, source_id: source_id, spec: spec}
      {:ok, stored, Source.new(source_id, Keyword.put(source_opts, :tenant_id, tenant))}
    end
  end

  @doc """
  Refuse a write of a source the instance's config would not run
  (`Ankusa.Verifier.check_shared/2`). `build/4` and `load_spec/5` stay
  permissive: refusing a stored source at load would `404` it, and dispatch
  would dead-letter its pending hooks as `source_gone`.
  """
  @spec check_write(atom(), Source.t()) :: :ok | {:error, :invalid, String.t()}
  def check_write(instance, %Source{} = source) do
    case Ankusa.Verifier.check_shared(Ankusa.config(instance), source) do
      :ok -> :ok
      {:error, message} -> {:error, :invalid, message}
    end
  end

  @spec put_rows(:ets.table(), SourceStore.stored(), Source.t()) :: true
  def put_rows(table, %{tenant: tenant, name: name, source_id: source_id} = stored, source) do
    :ets.insert(table, [
      {{:source, source_id}, {stored, source}},
      {{:stored, tenant, name}, stored}
    ])
  end

  @spec delete_rows(:ets.table(), String.t(), String.t()) :: true
  def delete_rows(table, tenant, name) do
    :ets.delete(table, {:source, source_id(tenant, name)})
    :ets.delete(table, {:stored, tenant, name})
  end

  @doc """
  Load one persisted spec into the table. A spec that collides with a seed, or
  that the decoder no longer accepts, is skipped with a warning (and left where
  it is persisted, so rolling a config change back brings it straight back).
  Returns whether it was loaded.
  """
  @spec load_spec(:ets.table(), function() | nil, String.t(), String.t(), map()) :: boolean()
  def load_spec(table, decoder, tenant, name, spec) do
    source_id = source_id(tenant, name)

    if seed?(table, source_id) do
      Logger.warning(
        "[ankusa] skipping persisted source #{source_id}: it is seeded from configuration"
      )

      false
    else
      case build(decoder, tenant, name, spec) do
        {:ok, stored, source} ->
          put_rows(table, stored, source)
          true

        {:error, :invalid, message} ->
          Logger.warning("[ankusa] skipping persisted source #{source_id}: #{message}")
          false
      end
    end
  end
end
