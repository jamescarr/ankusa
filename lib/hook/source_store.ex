defmodule Hook.SourceStore do
  @moduledoc """
  Source config, secrets, and policy. This module is both the behaviour and the
  instance-scoped facade (resolves `config.source_store` and delegates).

  Reads happen on every request, so adapters should be read-mostly and fast.
  """

  alias Hook.{Config, Source}

  @callback fetch(instance :: atom(), source_id :: String.t()) ::
              {:ok, Source.t()} | :error
  @callback list(instance :: atom()) :: [String.t()]

  @spec fetch(atom(), String.t()) :: {:ok, Source.t()} | :error
  def fetch(instance, source_id) do
    %Config{source_store: {mod, _}} = Hook.config(instance)
    mod.fetch(instance, source_id)
  end

  @spec list(atom()) :: [String.t()]
  def list(instance) do
    %Config{source_store: {mod, _}} = Hook.config(instance)
    mod.list(instance)
  end
end

defmodule Hook.SourceStore.Static do
  @moduledoc """
  Default source store: sources are declared in config as a map of
  `source_id => keyword/map` (see `Hook.Source.new/2`) and cached in
  `:persistent_term`. Change rarely, read on every request.
  """

  @behaviour Hook.SourceStore

  alias Hook.{Config, Source}

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
    %Config{source_store: {_mod, opts}} = Hook.config(instance)
    Keyword.get(opts, :sources, %{})
  end
end
