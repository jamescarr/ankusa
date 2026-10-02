defmodule Ankusa.SourceStore do
  @moduledoc """
  Source config, secrets, and policy. This module is both the behaviour and the
  instance-scoped facade (resolves `config.source_store` and delegates).

  Reads happen on every request, so adapters should be read-mostly and fast.

  ## Reads and writes

  `fetch/2` and `list/1` are the ingest-facing, read-only callbacks every store
  implements. The optional admin callbacks (`put/5`, `get/3`, `list_tenant/2`,
  `delete/3`) back the admin API's tenant-scoped source management; a store that
  only reads (like `Ankusa.SourceStore.Static`) leaves them out and the facade
  answers `{:error, :read_only}` / `:error` / `[]`. See
  `Ankusa.SourceStore.Persistent` for the writable implementation.
  """

  alias Ankusa.{Config, Source}

  @typedoc """
  One tenant-scoped source as the store keeps it. `spec` is the submitted JSON
  map with string keys, secrets included; redaction is the caller's job (the
  admin router), never the store's.
  """
  @type stored :: %{
          tenant: String.t(),
          name: String.t(),
          source_id: String.t(),
          spec: map()
        }

  @callback fetch(instance :: atom(), source_id :: String.t()) ::
              {:ok, Source.t()} | :error
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
              | {:error, :exists}
              | {:error, :not_found}
  @callback get(instance :: atom(), tenant :: String.t(), name :: String.t()) ::
              {:ok, stored()} | :error
  @callback list_tenant(instance :: atom(), tenant :: String.t()) :: [stored()]

  @callback delete(instance :: atom(), tenant :: String.t(), name :: String.t()) ::
              :ok | {:error, :not_found} | {:error, :invalid, String.t()}

  @optional_callbacks put: 5, get: 3, list_tenant: 2, delete: 3

  # Same rule as `Ankusa.ClaimCheck.Ref.valid_tenant?/1`: an identity that names
  # a storage partition and a URL path segment must not need encoding.
  @identity_regex ~r/\A[A-Za-z0-9_-]{1,64}\z/

  @spec fetch(atom(), String.t()) :: {:ok, Source.t()} | :error
  def fetch(instance, source_id) do
    %Config{source_store: {mod, _}} = Ankusa.config(instance)
    mod.fetch(instance, source_id)
  end

  @doc """
  The sinks a delivered hook of `source_id` goes to; `[]` for an unknown source.

  Dispatch resolves sinks here rather than through `fetch/2` so it can also
  deliver the reserved lifecycle source (`Ankusa.Lifecycle`), which the edge must
  never resolve: ingest keeps using `fetch/2`, and `POST /webhooks/ankusa:lifecycle`
  stays a `404`.
  """
  @spec sinks(atom(), String.t()) :: [{module(), keyword()}]
  def sinks(instance, source_id) do
    lifecycle_id = Ankusa.Lifecycle.source_id()

    case source_id do
      ^lifecycle_id ->
        case Ankusa.Lifecycle.source(instance) do
          {:ok, source} -> source.sinks
          :error -> []
        end

      _ ->
        case fetch(instance, source_id) do
          {:ok, source} -> source.sinks
          :error -> []
        end
    end
  end

  @spec list(atom()) :: [String.t()]
  def list(instance) do
    %Config{source_store: {mod, _}} = Ankusa.config(instance)
    mod.list(instance)
  end

  @doc """
  Create (`:create`) or replace (`:update`) one tenant-scoped source.

  `tenant` and `name` are validated here, before the store sees them, so a bad
  identity is `{:error, :invalid, message}` no matter which store is configured.
  A store that does not export `put/5` is read-only: `{:error, :read_only}`.
  """
  @spec put(atom(), String.t(), String.t(), map(), :create | :update) ::
          {:ok, stored()}
          | {:error, :invalid, String.t()}
          | {:error, :exists}
          | {:error, :not_found}
          | {:error, :read_only}
  def put(instance, tenant, name, spec, mode) do
    with :ok <- validate_identity(tenant, "tenant"),
         :ok <- validate_identity(name, "name") do
      %Config{source_store: {mod, _}} = Ankusa.config(instance)

      if exports?(mod, :put, 5) do
        result = mod.put(instance, tenant, name, spec, mode)

        with {:ok, stored} <- result do
          action = if mode == :create, do: :created, else: :updated
          Ankusa.Lifecycle.source_changed(instance, action, stored)
        end

        result
      else
        {:error, :read_only}
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
  A store that does not export `delete/3` is read-only: `{:error, :read_only}`.
  """
  @spec delete(atom(), String.t(), String.t()) ::
          :ok | {:error, :not_found} | {:error, :invalid, String.t()} | {:error, :read_only}
  def delete(instance, tenant, name) do
    with :ok <- validate_identity(tenant, "tenant"),
         :ok <- validate_identity(name, "name") do
      %Config{source_store: {mod, _}} = Ankusa.config(instance)

      if exports?(mod, :delete, 3) do
        # Read before the delete: the event's `data` is the last view of the
        # source, and there is nothing left to read afterwards.
        prior = if exports?(mod, :get, 3), do: mod.get(instance, tenant, name), else: :error

        with :ok <- mod.delete(instance, tenant, name) do
          entry =
            case prior do
              {:ok, stored} -> stored
              :error -> %{tenant: tenant, name: name}
            end

          Ankusa.Lifecycle.source_changed(instance, :deleted, entry)
          :ok
        end
      else
        {:error, :read_only}
      end
    end
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
end
