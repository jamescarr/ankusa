defmodule Ankusa.Routes.TableOwner do
  @moduledoc """
  Keeps an instance's route snapshot table alive across a routes-store
  restart. It creates the table and is its heir: the store writes into it
  (`Ankusa.Routes.Snapshot` asks for it with `hand_over/2`), and when the
  store dies the table comes back here instead of being deleted. The
  restarted store takes it over with `Ankusa.Routes.Snapshot.adopt/1` and
  resumes from the routes in it, rather than starting again from its config.

  Started by `Ankusa.Instance` just before the routes store; nothing else
  needs to call it.
  """

  use GenServer

  alias Ankusa.Routes.Snapshot

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, instance, name: Ankusa.via(instance, :routes_table))
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc """
  Give the table to `pid` (the routes store). `{:error, :not_running}` when
  this instance has no owner process (a store started on its own, in tests).
  """
  @spec hand_over(atom(), pid()) :: :ok | {:error, :not_running | :owned_elsewhere}
  def hand_over(instance, pid) do
    case Ankusa.whereis(instance, :routes_table) do
      nil -> {:error, :not_running}
      owner -> GenServer.call(owner, {:hand_over, pid})
    end
  end

  @impl true
  def init(instance) do
    table =
      :ets.new(Snapshot.table(instance), [
        :named_table,
        :protected,
        :ordered_set,
        {:read_concurrency, true},
        {:heir, self(), :returned}
      ])

    {:ok, table}
  end

  @impl true
  def handle_call({:hand_over, pid}, _from, table) do
    if :ets.info(table, :owner) == self() do
      true = :ets.give_away(table, pid, :routes)
      {:reply, :ok, table}
    else
      {:reply, {:error, :owned_elsewhere}, table}
    end
  end

  @impl true
  def handle_info({:"ETS-TRANSFER", _table, _from, :returned}, table), do: {:noreply, table}
end
