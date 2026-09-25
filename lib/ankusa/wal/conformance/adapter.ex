defmodule Ankusa.WAL.Conformance.Adapter do
  @moduledoc """
  What an adapter package implements to run the shared WAL conformance suite
  (`Ankusa.WAL.ConformanceCase`).

  The suite needs to *boot* and *restart* the WAL under test, which is
  adapter-specific: DiskLog is a `GenServer` plus a directory, Postgres needs a
  connection pool and DDL, Ra needs a Raft cluster. Implementing this behaviour
  is how an adapter says how, so the same 13 cases run against all three.
  """

  @doc """
  Start the adapter for `instance` and return `:ok`. Called once per test with a
  fresh instance name and a temp `data_dir` already present in `config`.
  """
  @callback start(instance :: atom(), config :: Ankusa.Config.t()) :: :ok

  @doc """
  Stop the adapter for `instance`, durably flushing anything it buffers. After
  this the adapter's registered process must be gone, so a subsequent `restart`
  proves the state really was persisted.
  """
  @callback stop(instance :: atom()) :: :ok

  @doc """
  Start the adapter again for `instance` with the same `config` — the crash /
  reboot path. Must not wipe state.
  """
  @callback restart(instance :: atom(), config :: Ankusa.Config.t()) :: :ok
end
