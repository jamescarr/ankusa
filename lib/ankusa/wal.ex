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
    * A committed record is assigned a strictly increasing `seq`, and seq order
      is **commit order**: once a reader has observed seq `N`, no record with
      seq ≤ `N` may become visible later. `seq` values may have gaps; readers
      use them only as a cursor.
    * After a crash, replay MUST drop a torn trailing record (a commit that never
      `fsync`'d) so no un-acked write is ever surfaced.
    * Cursors never decrease: `put_cursor/4` is a maximum, not an assignment.
    * A cursor write or a truncation requires the token of a live lease for that
      cursor's lease name (`lease_for_cursor/1`), and is rejected with
      `{:error, :fenced}` otherwise.
    * A write carrying a stale token is rejected with `{:error, :fenced}` and the
      caller must re-acquire the lease before trying again.

  ## Leases

  Every cursor is owned by a *lease*: `:dispatch`'s cursor by the `:dispatch`
  lease, `:compactor`'s by the `:storage` lease. Only the holder of a live lease
  may advance its cursor or truncate the log, and each acquisition allocates a
  strictly increasing `token`. A holder that lost its lease (a live token it did
  not issue, or an expired one) is *fenced*: its writes are refused, so a
  paused-then-resumed zombie can never move a cursor backwards or truncate
  records a new holder still needs.

  A record is `%{envelope: Ankusa.Envelope.t()}`; the `dedup_key` is read from the
  envelope. Adapters set `envelope.seq` on the returned committed envelope.
  """

  alias Ankusa.{Config, Envelope}

  @type server :: GenServer.server()
  @type entry :: %{envelope: Envelope.t()}
  @type result :: {:committed, Envelope.t()} | {:duplicate, non_neg_integer()}

  @type lease :: %{
          name: atom(),
          holder: String.t(),
          token: pos_integer(),
          ttl_ms: pos_integer(),
          expires_at: integer()
        }

  @callback append(server(), [entry()]) :: {:ok, [result()]}
  @callback read(server(), after_seq :: non_neg_integer(), limit :: pos_integer()) ::
              [Envelope.t()]
  @callback get_cursor(server(), name :: atom()) :: non_neg_integer()
  @callback put_cursor(server(), name :: atom(), seq :: non_neg_integer(), token :: pos_integer()) ::
              :ok | {:error, :fenced}
  @callback truncate_through(server(), seq :: non_neg_integer(), token :: pos_integer()) ::
              :ok | {:error, :fenced}
  @callback stats(server()) :: map()

  @callback acquire_lease(
              server(),
              name :: atom(),
              holder :: String.t(),
              ttl_ms :: pos_integer()
            ) :: {:ok, lease()} | {:error, {:held, holder :: String.t()}}
  @callback renew_lease(server(), lease()) :: {:ok, lease()} | {:error, :lost}
  @callback release_lease(server(), lease()) :: :ok

  # ── facade ──────────────────────────────────────────────────────────────

  @spec append(atom(), [entry()]) :: {:ok, [result()]}
  def append(instance, records), do: apply_mod(instance, :append, [records])

  @spec read(atom(), non_neg_integer(), pos_integer()) :: [Envelope.t()]
  def read(instance, after_seq, limit), do: apply_mod(instance, :read, [after_seq, limit])

  @spec get_cursor(atom(), atom()) :: non_neg_integer()
  def get_cursor(instance, name), do: apply_mod(instance, :get_cursor, [name])

  @spec put_cursor(atom(), atom(), non_neg_integer(), pos_integer()) ::
          :ok | {:error, :fenced}
  def put_cursor(instance, name, seq, token),
    do: apply_mod(instance, :put_cursor, [name, seq, token])

  @spec truncate_through(atom(), non_neg_integer(), pos_integer()) :: :ok | {:error, :fenced}
  def truncate_through(instance, seq, token),
    do: apply_mod(instance, :truncate_through, [seq, token])

  @spec stats(atom()) :: map()
  def stats(instance), do: apply_mod(instance, :stats, [])

  @spec acquire_lease(atom(), atom(), String.t(), pos_integer()) ::
          {:ok, lease()} | {:error, {:held, String.t()}}
  def acquire_lease(instance, name, holder, ttl_ms),
    do: apply_mod(instance, :acquire_lease, [name, holder, ttl_ms])

  @spec renew_lease(atom(), lease()) :: {:ok, lease()} | {:error, :lost}
  def renew_lease(instance, lease), do: apply_mod(instance, :renew_lease, [lease])

  @spec release_lease(atom(), lease()) :: :ok
  def release_lease(instance, lease), do: apply_mod(instance, :release_lease, [lease])

  @doc """
  The lease name that fences a given cursor.

  `:compactor` is the storage role's cursor, so it is fenced by the `:storage`
  lease; every other cursor (`:dispatch`) is fenced by a lease of the same name.
  Defined once here so all adapters agree.
  """
  @spec lease_for_cursor(atom()) :: atom()
  def lease_for_cursor(:compactor), do: :storage
  def lease_for_cursor(other), do: other

  defp apply_mod(instance, fun, args) do
    %Config{wal: {mod, _opts}} = Ankusa.config(instance)
    apply(mod, fun, [Ankusa.via(instance, :wal) | args])
  end
end
