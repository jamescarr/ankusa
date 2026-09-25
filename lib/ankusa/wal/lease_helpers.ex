defmodule Ankusa.WAL.LeaseHelpers do
  @moduledoc """
  Small helpers around `Ankusa.WAL`'s lease callbacks, shared by the components
  that *hold* leases — `Ankusa.Dispatch.Pipeline` (`:dispatch`) and
  `Ankusa.Storage.Compactor` (`:storage`) — and by tests and operator tooling
  that need to act as a holder.

  Production holders are the pipeline and the compactor. Everything here is
  generic over the adapter, so the same code works against DiskLog, Postgres
  and Ra.

  `emit/2` is the single place lease telemetry is emitted from, so every
  adapter's `[:ankusa, :lease, *]` events carry the same metadata shape.
  """

  alias Ankusa.WAL

  @default_ttl_ms 60_000

  @doc """
  Acquire `name` for a fresh holder (or the one given in `:holder`) and return
  `{:ok, lease}`. Raises if the lease is currently held by someone else — this
  is for tests and tooling where "someone else holds it" is a bug, not a state
  to handle; the pipeline and compactor use `WAL.acquire_lease/4` directly.

  Options: `:ttl_ms` (default #{@default_ttl_ms}), `:holder` (default a unique
  `"test/…"` string).
  """
  @spec hold_lease(atom(), atom(), keyword()) :: {:ok, WAL.lease()}
  def hold_lease(instance, name, opts \\ []) do
    ttl_ms = Keyword.get(opts, :ttl_ms, @default_ttl_ms)
    holder = Keyword.get(opts, :holder, "test/#{System.unique_integer([:positive])}")

    case WAL.acquire_lease(instance, name, holder, ttl_ms) do
      {:ok, lease} ->
        {:ok, Map.put(lease, :instance, instance)}

      {:error, {:held, other}} ->
        raise "lease #{inspect(name)} on #{inspect(instance)} is held by #{inspect(other)}"
    end
  end

  @doc """
  Acquire `name`, run `fun.(lease)`, and release the lease in an `after` — even
  if `fun` raises. Returns whatever `fun` returns.
  """
  @spec with_lease(atom(), atom(), (WAL.lease() -> result)) :: result when result: term()
  def with_lease(instance, name, fun) do
    {:ok, lease} = hold_lease(instance, name)

    try do
      fun.(lease)
    after
      WAL.release_lease(instance, lease)
    end
  end

  @doc """
  Emit `[:ankusa, :lease, event]` for `lease`.

  The lease must carry an `:instance` key (the adapters' leases do not, so a
  holder adds it once when it stores the lease). `event` is `:acquired`,
  `:renewed` or `:lost`.
  """
  @spec emit(atom(), map()) :: :ok
  def emit(event, %{instance: instance, name: name, holder: holder, token: token}) do
    Ankusa.Telemetry.emit([:lease, event], %{}, %{
      instance: instance,
      name: name,
      holder: holder,
      token: token
    })
  end
end
