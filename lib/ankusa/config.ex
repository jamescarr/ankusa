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
            # group-commit batcher. Both WALs serialize commits themselves (the
            # DiskLog GenServer, Postgres's per-instance advisory lock), so more
            # partitions only add contention now that a partition commits
            # asynchronously instead of holding the caller's message queue.
            batcher: %{
              partitions: 2,
              max_batch: 256,
              # 0 = commit as soon as the batch fills, no linger: the WAL
              # append is a Task, so waiting costs a scheduling hop, not
              # head-of-line blocking.
              max_delay_ms: 0,
              max_queue: 10_000
            },
            # dispatch pipeline
            dispatch: %{
              poll_ms: 200,
              # bounds one WAL read's worth of memory
              batch: 128,
              # max sink deliveries in flight at once
              concurrency: 32,
              # max admitted (not yet fully handled) envelopes...
              max_inflight: 4096,
              # ...and the max sum of their body bytes
              max_inflight_bytes: 134_217_728,
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
            },
            # operator HTTP API + Prometheus /metrics, unauthenticated; off by
            # default for embedded use
            admin: %{enabled: false, port: 4002}

  @type t :: %__MODULE__{}

  @roles [:edge, :dispatch, :storage, :claim_check]

  @role_names %{
    "edge" => :edge,
    "dispatch" => :dispatch,
    "storage" => :storage,
    "claim_check" => :claim_check
  }

  @doc """
  Parse a comma-separated `ANKUSA_ROLES`-shaped string into role atoms.
  Never calls `String.to_atom/1` — each part must name one of the fixed
  roles (`edge`, `dispatch`, `storage`, `claim_check`).
  """
  @spec parse_roles!(String.t()) :: [atom()]
  def parse_roles!(value) do
    roles =
      value
      |> String.split(",")
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.map(fn name ->
        Map.get(@role_names, name) ||
          raise ArgumentError,
                "unknown Ankusa role #{inspect(name)}; expected one of: edge, dispatch, storage, claim_check"
      end)

    if roles == [] do
      raise ArgumentError, "ANKUSA_ROLES must name at least one role"
    end

    roles
  end

  @doc """
  Build a `%Ankusa.Config{}` from a keyword list, deep-merging the map-valued
  sections (`:batcher`, `:dispatch`, `:storage`, `:claim_check`, `:admin`) over
  the defaults.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    base = %__MODULE__{}

    Enum.reduce(opts, base, fn {k, v}, acc ->
      cond do
        k == :roles ->
          bad = Enum.reject(v, &(&1 in @roles))

          if bad != [] do
            raise ArgumentError, "unknown Ankusa role(s) #{inspect(bad)} in :roles"
          end

          Map.put(acc, k, v)

        k in [:batcher, :dispatch, :storage, :claim_check, :admin] ->
          put_section(acc, k, v)

        Map.has_key?(base, k) ->
          Map.put(acc, k, v)

        true ->
          raise ArgumentError, "unknown Ankusa.Config key: #{inspect(k)}"
      end
    end)
  end

  defp put_section(acc, k, v) do
    cond do
      is_map(v) -> merge_section(acc, k, Map.new(v))
      Keyword.keyword?(v) -> merge_section(acc, k, Map.new(v))
      true -> raise ArgumentError, "Ankusa.Config #{k} must be a map or keyword list"
    end
  end

  defp merge_section(acc, k, v) do
    defaults = Map.get(acc, k)

    Enum.each(Map.keys(v), fn nk ->
      unless Map.has_key?(defaults, nk) do
        raise ArgumentError, "unknown Ankusa.Config key: #{k}.#{nk}"
      end
    end)

    Map.put(acc, k, Map.merge(defaults, v))
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
