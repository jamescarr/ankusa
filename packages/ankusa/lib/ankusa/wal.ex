defmodule Ankusa.WAL do
  @moduledoc """
  Durable, ordered write-ahead log: the fast tier that makes the ack honest.

  This module is both the **behaviour** every WAL adapter implements and the
  instance-scoped **facade** the rest of the framework calls. The facade resolves
  the configured adapter module (`config.wal`) and the registered server for the
  instance, then delegates.

  ## Contract

    * `append/2` is a **group commit**. Given a list of records it writes them all
      and issues a *single* `fsync`, then returns per-record results in order.
    * A committed record is assigned a strictly increasing `seq`, and seq order
      is **commit order**: once a reader has observed seq `N`, no record with
      seq ≤ `N` may become visible later. `seq` values may have gaps; readers
      use them only as a cursor.
    * After a crash, replay MUST drop a torn trailing record (a commit that never
      `fsync`'d) so no un-acked write is ever surfaced.

  A record is `%{envelope: Ankusa.Envelope.t()}`. Adapters set `envelope.seq` on
  the returned committed envelope.
  """

  alias Ankusa.{Config, Envelope}

  @type server :: GenServer.server()
  @type entry :: %{envelope: Envelope.t()}
  @type result :: {:committed, Envelope.t()}

  @callback append(server(), [entry()]) :: {:ok, [result()]}
  @callback read(server(), after_seq :: non_neg_integer(), limit :: pos_integer()) ::
              [Envelope.t()]
  @callback get_cursor(server(), name :: atom()) :: non_neg_integer()
  @callback put_cursor(server(), name :: atom(), seq :: non_neg_integer()) :: :ok
  @callback truncate_through(server(), seq :: non_neg_integer()) :: :ok
  @callback stats(server()) :: map()

  # ── facade ──────────────────────────────────────────────────────────────

  @spec append(atom(), [entry()]) :: {:ok, [result()]}
  def append(instance, records), do: apply_mod(instance, :append, [records])

  @spec read(atom(), non_neg_integer(), pos_integer()) :: [Envelope.t()]
  def read(instance, after_seq, limit), do: apply_mod(instance, :read, [after_seq, limit])

  @spec get_cursor(atom(), atom()) :: non_neg_integer()
  def get_cursor(instance, name), do: apply_mod(instance, :get_cursor, [name])

  @spec put_cursor(atom(), atom(), non_neg_integer()) :: :ok
  def put_cursor(instance, name, seq), do: apply_mod(instance, :put_cursor, [name, seq])

  @spec truncate_through(atom(), non_neg_integer()) :: :ok
  def truncate_through(instance, seq), do: apply_mod(instance, :truncate_through, [seq])

  @spec stats(atom()) :: map()
  def stats(instance), do: apply_mod(instance, :stats, [])

  @doc """
  The configured adapter's name, for boot banners and `check-config`: `"none"`
  under `wal: :none`, otherwise the adapter module.
  """
  @spec label({module(), keyword()} | :none) :: String.t()
  def label(:none), do: "none"
  def label({mod, _opts}), do: inspect(mod)

  # ── boot validation ─────────────────────────────────────────────────────

  @doc """
  Reject a configuration that would ack without durability.

  Under `wal: :none` the provider's `2xx` means "a sink confirmed", so every
  statically configured source must have at least one sink whose `:ok` means the
  hook is durably accepted by something that outlives this node — see
  `Ankusa.Sink.durable?/2`. Raises `ArgumentError` naming the first source that
  cannot make that promise.

  Only sources in `config.source_store` are checked. A source created at
  runtime through the admin API is not: the store's decoder has no instance
  config, so `Ankusa.SourceStore.put/5` is the place runtime enforcement would
  have to live. Use `wal.type: disk` if sources are edited at runtime and a
  log-only sink cannot be ruled out.
  """
  @spec validate_config!(Ankusa.Config.t()) :: :ok
  def validate_config!(%Ankusa.Config{wal: :none} = config) do
    Enum.each(static_sources(config), fn {id, %Ankusa.Source{sinks: sinks}} ->
      unless Enum.any?(sinks, fn {mod, opts} -> Ankusa.Sink.durable?(mod, opts) end) do
        raise ArgumentError,
              "source #{inspect(id)}: wal: :none acks the provider on a sink's confirm, " <>
                "but none of its sinks is durable. Configure a durable sink, or use wal.type: disk."
      end
    end)
  end

  def validate_config!(%Ankusa.Config{}), do: :ok

  defp static_sources(config) do
    {_mod, opts} = config.source_store

    opts
    |> Keyword.get(:sources, %{})
    |> Enum.map(fn
      {id, %Ankusa.Source{} = source} -> {id, source}
      {id, source_opts} -> {id, Ankusa.Source.new(id, source_opts)}
    end)
  end

  defp apply_mod(instance, fun, args) do
    %Config{wal: {mod, _opts}} = Ankusa.config(instance)
    apply(mod, fun, [Ankusa.via(instance, :wal) | args])
  end
end
