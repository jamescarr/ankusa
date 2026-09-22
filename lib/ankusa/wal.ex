defmodule Ankusa.WAL do
  @moduledoc """
  Durable, ordered write-ahead log: the fast tier that makes the ack honest.

  This module is both the **behaviour** every WAL adapter implements and the
  instance-scoped **facade** the rest of the framework calls. The facade resolves
  the configured adapter module (`config.wal`) and the registered server for the
  instance, then delegates.

  ## Contract

    * `append/2` is a **group commit**. Given a list of records it writes them all
      and issues a *single* `fsync`, then returns per-record results in order. A
      record whose `dedup_key` collides with an already-committed one is returned
      as `{:duplicate, existing_seq}` and is *not* written — but the caller still
      acks `2xx` (the provider retried; dedup absorbed it).
    * A committed record is assigned a strictly increasing `seq`. `seq` values are
      dense and monotonic; readers use them as a cursor.
    * After a crash, replay MUST drop a torn trailing record (a commit that never
      `fsync`'d) so no un-acked write is ever surfaced.

  A record is `%{envelope: Ankusa.Envelope.t()}`; the `dedup_key` is read from the
  envelope. Adapters set `envelope.seq` on the returned committed envelope.
  """

  alias Ankusa.{Config, Envelope}

  @type server :: GenServer.server()
  @type entry :: %{envelope: Envelope.t()}
  @type result :: {:committed, Envelope.t()} | {:duplicate, non_neg_integer()}

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

  defp apply_mod(instance, fun, args) do
    %Config{wal: {mod, _opts}} = Ankusa.config(instance)
    apply(mod, fun, [Ankusa.via(instance, :wal) | args])
  end
end
