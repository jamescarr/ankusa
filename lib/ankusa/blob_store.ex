defmodule Ankusa.BlobStore do
  @moduledoc """
  Segment object storage: `PUT` whole immutable segments, range `GET` one record,
  `DELETE` for retention. The default is the local filesystem; S3/GCS/R2 are
  drop-in adapters.

  This module is both the behaviour and the instance-scoped facade
  (resolves `config.storage.blob_store` and delegates).
  """

  alias Ankusa.Config

  @callback put(instance :: atom(), key :: String.t(), data :: iodata(), opts :: keyword()) ::
              :ok | {:error, term()}
  @callback get(instance :: atom(), key :: String.t(), opts :: keyword()) ::
              {:ok, binary()} | {:error, term()}
  @callback get_range(
              instance :: atom(),
              key :: String.t(),
              offset :: non_neg_integer(),
              length :: pos_integer(),
              opts :: keyword()
            ) :: {:ok, binary()} | {:error, term()}
  @callback delete(instance :: atom(), key :: String.t(), opts :: keyword()) :: :ok
  # Returns *all* keys under the prefix, not one page: the caller (index repair,
  # the claim-check sweeper) needs the complete set, and pagination is the
  # adapter's business.
  @callback list(instance :: atom(), prefix :: String.t(), opts :: keyword()) :: [String.t()]

  defp resolve(instance) do
    %Config{storage: %{blob_store: {mod, opts}}} = Ankusa.config(instance)
    {mod, opts}
  end

  def put(instance, key, data) do
    {mod, opts} = resolve(instance)
    mod.put(instance, key, data, opts)
  end

  def get(instance, key) do
    {mod, opts} = resolve(instance)
    mod.get(instance, key, opts)
  end

  def get_range(instance, key, offset, length) do
    {mod, opts} = resolve(instance)
    mod.get_range(instance, key, offset, length, opts)
  end

  def delete(instance, key) do
    {mod, opts} = resolve(instance)
    mod.delete(instance, key, opts)
  end

  def list(instance, prefix) do
    {mod, opts} = resolve(instance)
    mod.list(instance, prefix, opts)
  end
end
