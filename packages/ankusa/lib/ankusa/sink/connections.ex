defmodule Ankusa.Sink.Connections do
  @moduledoc """
  The broker connections a sink adapter opens on demand. An adapter package
  starts one tree with `start_link/1` from its `Application`: a
  `DynamicSupervisor` (`<sink>.Supervisor`) that owns the connections and an
  `Ankusa.Sink.Reaper` (`<sink>.Reaper`) that closes idle ones, under
  `<sink>.TopSup`. A sink calls `ensure/5` before every publish.

  The connections live in the adapter's tree, not the instance's, so a lost
  broker never takes ingest or dispatch down: it makes `publish/4` (see
  `Ankusa.Sink.Messaging`) return `{:error, _}` for the source's
  `Ankusa.RetryPolicy`. A connection that belongs to one instance registers
  through that instance's registry (`Ankusa.via/2`, found again with
  `Ankusa.whereis/2`), so nothing is registered globally and two instances
  never share one; core itself does not know which adapters are loaded.
  """

  alias Ankusa.Sink.Reaper

  @doc "Start `sink`'s connection tree; an adapter's `Application.start/2` returns this."
  @spec start_link(module()) :: Supervisor.on_start()
  def start_link(sink) when is_atom(sink) do
    supervisor = supervisor(sink)

    children = [
      {DynamicSupervisor, name: supervisor, strategy: :one_for_one},
      # Stops a connection no delivery has used for its `:idle_timeout_ms`.
      {Reaper, name: reaper(sink), supervisor: supervisor}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Module.concat(sink, "TopSup"))
  end

  # Strings, not aliases: `Reaper` here is `Ankusa.Sink.Reaper`, and
  # `Module.concat(sink, Reaper)` would name `<sink>.Ankusa.Sink.Reaper`.

  @doc "The `DynamicSupervisor` that owns `sink`'s connections."
  @spec supervisor(module()) :: atom()
  def supervisor(sink), do: Module.concat(sink, "Supervisor")

  @doc "The reaper (and its table) that closes `sink`'s idle connections."
  @spec reaper(module()) :: atom()
  def reaper(sink), do: Module.concat(sink, "Reaper")

  @doc """
  The connection `key` names, started on demand and about to be used.

  `lookup` finds the running one (`Ankusa.whereis/2`, `Process.whereis/1`);
  `nil` starts `child.()` under `sink`'s supervisor — only then is the child
  spec built, and a start that loses the race (`{:error, {:already_started,
  pid}}`) is that pid. The pid is touched in `sink`'s reaper under `key` with
  `idle_ms` (`Ankusa.Sink.Reaper.touch/4`; `0` never reaps), so `key` is
  printed when the connection is closed and must carry no credential.
  """
  @spec ensure(
          module(),
          term(),
          (-> pid() | nil),
          (-> Supervisor.child_spec()),
          non_neg_integer()
        ) ::
          {:ok, pid()} | {:error, term()}
  def ensure(sink, key, lookup, child, idle_ms)
      when is_function(lookup, 0) and is_function(child, 0) do
    with {:ok, pid} <- find_or_start(sink, lookup, child) do
      :ok = Reaper.touch(reaper(sink), key, pid, idle_ms)
      {:ok, pid}
    end
  end

  # The lookup first: the common case must not serialize every delivery
  # through the DynamicSupervisor.
  defp find_or_start(sink, lookup, child) do
    case lookup.() do
      pid when is_pid(pid) ->
        {:ok, pid}

      nil ->
        case DynamicSupervisor.start_child(supervisor(sink), child.()) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, reason} -> {:error, reason}
        end
    end
  end
end
