defmodule Ankusa.WAL.Conformance.Postgres do
  @moduledoc """
  `Ankusa.WAL.Conformance.Adapter` for `Ankusa.WAL.Postgres`.

  The conformance suite hands each adapter a config built from the framework
  defaults, so this module swaps the WAL entry for the Postgres one (the local
  `docker compose` instance) before starting the pool — and re-`put_config`s it,
  because the facade resolves `config.wal` to find the adapter module.
  """

  @behaviour Ankusa.WAL.Conformance.Adapter

  alias Ankusa.WAL.Postgres

  @pg_opts [
    hostname: "localhost",
    port: 5433,
    username: "ankusa",
    password: "ankusa",
    database: "ankusa_dev",
    pool_size: 4
  ]

  @impl true
  def start(instance, config) do
    config = %{config | wal: {Postgres, @pg_opts}}
    Ankusa.put_config(config)
    {:ok, _pid} = Postgres.start_link(instance: instance, config: config)
    :ok
  end

  @impl true
  def stop(instance) do
    case Ankusa.whereis(instance, :wal) do
      nil ->
        :ok

      pid ->
        try do
          GenServer.stop(pid)
        catch
          :exit, _ -> :ok
        end
    end

    :ok
  end

  @impl true
  def restart(instance, config) do
    :ok = stop(instance)
    start(instance, config)
  end
end
