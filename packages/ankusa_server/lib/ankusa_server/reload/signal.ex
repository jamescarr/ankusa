defmodule AnkusaServer.Reload.Signal do
  @moduledoc """
  `SIGHUP` reloads the config (`AnkusaServer.Reload`).

  The VM ignores `SIGHUP` unless told to hand it over; this process does that
  (`:os.set_signal(:sighup, :handle)`) and listens on `:erl_signal_server`, the
  event manager the VM reports signals to (its own handler keeps answering
  `SIGTERM`). Each signal runs one reload in its own task, as the system: the
  reload logs what it did or refused, and a reload that is already running makes
  the next one wait for it. The handler is linked to this process
  (`:gen_event.add_sup_handler/3`), so it goes when this process does.
  """

  use GenServer

  require Logger

  defmodule Handler do
    @moduledoc false
    # The `:erl_signal_server` handler: forwards `:sighup` to the owner and
    # nothing else.
    @behaviour :gen_event

    @impl true
    def init(owner), do: {:ok, owner}

    @impl true
    def handle_event(:sighup, owner) do
      send(owner, :sighup)
      {:ok, owner}
    end

    def handle_event(_signal, owner), do: {:ok, owner}

    @impl true
    def handle_call(_request, owner), do: {:ok, :ok, owner}

    @impl true
    def handle_info(_message, owner), do: {:ok, owner}
  end

  @doc """
  Start listening for `SIGHUP`. Options: `:instance` (required) and `:reload`, the
  options each reload passes to `AnkusaServer.Reload.reload/2` (`:path`, `:env`;
  none by default: the file and environment the node booted from).
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(
      __MODULE__,
      {Keyword.fetch!(opts, :instance), Keyword.get(opts, :reload, [])}
    )
  end

  @impl true
  def init({instance, reload}) do
    handler = {Handler, make_ref()}
    :os.set_signal(:sighup, :handle)
    :ok = :gen_event.add_sup_handler(:erl_signal_server, handler, self())
    {:ok, %{instance: instance, reload: reload, handler: handler}}
  end

  @impl true
  def handle_info(:sighup, %{instance: instance, reload: reload} = state) do
    Logger.info("[ankusa] SIGHUP received: reloading the config")
    options = Keyword.put(reload, :audit, Ankusa.Audit.Context.system())

    # The reload logs its own outcome; an unexpected exit is logged by the task.
    {:ok, _pid} = Task.start(fn -> AnkusaServer.Reload.reload(instance, options) end)

    {:noreply, state}
  end

  def handle_info({:gen_event_EXIT, handler, reason}, %{handler: handler} = state),
    do: {:stop, {:signal_handler_exit, reason}, state}

  def handle_info(_message, state), do: {:noreply, state}
end
