defmodule Ankusa.Config do
  @moduledoc """
  The configuration struct passed down the supervision tree at start. No
  `Application.get_env/2` buried in call sites — instance-scoped config falls
  out of this for free.
  """

  defstruct instance: :default,
            data_dir: "./data",
            # which OTP roles boot in this node (ANKUSA_ROLES in production)
            roles: [:edge, :dispatch, :storage],
            # Bandit edge
            port: 4000,
            # {module, opts} implementing Ankusa.RouteResolver (URL scheme → identity)
            route_resolver: {Ankusa.RouteResolver.Path, []},
            max_body_bytes: 8_000_000,
            # {module, opts} implementing Ankusa.SourceStore
            source_store: {Ankusa.SourceStore.Static, sources: %{}},
            # {module, opts} implementing Ankusa.WAL
            wal: {Ankusa.WAL.DiskLog, []},
            # group-commit batcher
            batcher: %{
              partitions: System.schedulers_online(),
              max_batch: 256,
              max_delay_ms: 5,
              max_queue: 10_000
            },
            # dispatch pipeline
            dispatch: %{
              poll_ms: 200,
              batch: 128,
              retry: {Ankusa.RetryPolicy.Exponential, []}
            },
            # segment compaction
            storage: %{
              blob_store: {Ankusa.BlobStore.LocalFS, []},
              codec: {Ankusa.Codec.Raw, []},
              roll_bytes: 16 * 1024 * 1024,
              roll_ms: 30_000,
              interval_ms: 1_000
            },
            # claim check gateway — check bytes in, get a ticket back
            claim_check: %{
              adapter: {Ankusa.ClaimCheck.Direct, []},
              max_bytes: 8_000_000,
              # :claim_check role only
              port: 4001,
              api_tokens: %{},
              # LocalFS retention only; nil disables the sweeper
              retention_days: nil,
              sweep_interval_ms: 3_600_000
            }

  @type t :: %__MODULE__{}

  @doc """
  Build a `%Ankusa.Config{}` from a keyword list, deep-merging the map-valued
  sections (`:batcher`, `:dispatch`, `:storage`, `:claim_check`) over the
  defaults.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    base = %__MODULE__{}

    Enum.reduce(opts, base, fn {k, v}, acc ->
      cond do
        k in [:batcher, :dispatch, :storage, :claim_check] and is_map(v) ->
          Map.put(acc, k, Map.merge(Map.get(acc, k), Map.new(v)))

        Map.has_key?(base, k) ->
          Map.put(acc, k, v)

        true ->
          raise ArgumentError, "unknown Ankusa.Config key: #{inspect(k)}"
      end
    end)
  end

  @doc "Absolute path for an instance-scoped data sub-directory."
  @spec path(t(), Path.t()) :: Path.t()
  def path(%__MODULE__{data_dir: dir, instance: instance}, sub) do
    Path.join([dir, to_string(instance), sub])
  end

  @doc "Is a role enabled for this node?"
  @spec role?(t(), atom()) :: boolean()
  def role?(%__MODULE__{roles: roles}, role), do: role in roles
end
