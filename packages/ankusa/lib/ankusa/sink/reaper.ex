defmodule Ankusa.Sink.Reaper do
  @moduledoc """
  Closes the connections a sink adapter opened and then stopped using.

  The broker adapters (`ankusa_rabbitmq`, `ankusa_kafka`, `ankusa_nats`,
  `ankusa_redis`) start one connection process per broker on the first
  delivery, under their own `DynamicSupervisor`. Without this, a source that is
  deleted, or re-pointed at another broker, leaves its connection open for the
  life of the node. Each adapter runs one reaper next to its supervisor and
  calls `touch/4` before every publish; a connection not touched for its
  `idle_ms` is stopped through that supervisor (`DynamicSupervisor.terminate_child/2`,
  so a `:permanent` child is removed, not restarted), and the next delivery
  that needs it starts a new one.

  `touch/4` is one `:ets.insert/2` into a public table, so the publish path
  never waits on this process. A sweep runs every `:tick_ms` (default 30 s); a
  row whose connection already died is dropped without a word.

  ## The race it accepts

  A delivery that looked its connection up just before the sweep stopped it
  fails that attempt (the call exits, which `Ankusa.Sink` turns into
  `{:error, _}`), and the retry policy's next attempt starts a fresh
  connection. A connection touched while the sweep looks at it is kept: the
  sweep deletes only the exact row it judged idle. Because the touch comes
  before the publish, a publish in flight is never older than `idle_ms` as long
  as `idle_ms` is longer than the publish can take; `idle_ms/2` enforces that
  floor for each adapter.

  `start_link/1` opts: `:name` (the reaper's registered name and its table's),
  `:supervisor` (the `DynamicSupervisor` whose children it stops), `:tick_ms`.
  """

  use GenServer

  require Logger

  @default_idle_ms 600_000
  @default_tick_ms 30_000

  @doc false
  def child_spec(opts) do
    %{id: Keyword.fetch!(opts, :name), start: {__MODULE__, :start_link, [opts]}}
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @doc """
  Record that the connection `pid`, known as `key`, is about to be used, and
  that it may be stopped once unused for `idle_ms`. `idle_ms` of `0` means
  never: the row is dropped. `key` is printed when the connection is closed,
  so it must not carry a credential.
  """
  @spec touch(atom(), term(), pid(), non_neg_integer()) :: :ok
  def touch(name, key, _pid, 0) do
    :ets.delete(name, key)
    :ok
  end

  def touch(name, key, pid, idle_ms) when is_pid(pid) and is_integer(idle_ms) and idle_ms > 0 do
    :ets.insert(name, {key, pid, now_ms(), idle_ms})
    :ok
  end

  @doc """
  An adapter's idle timeout from its sink opts: `:idle_timeout_ms` (default 10
  minutes), `0` for never, else at least `floor_ms` — the longest one publish
  can take, plus a margin — so a sweep never stops a connection under a
  publish. Raises `ArgumentError` on anything but a non-negative integer.
  """
  @spec idle_ms(keyword(), pos_integer()) :: non_neg_integer()
  def idle_ms(opts, floor_ms) do
    case Keyword.get(opts, :idle_timeout_ms, @default_idle_ms) do
      0 ->
        0

      ms when is_integer(ms) and ms > 0 ->
        max(ms, floor_ms)

      other ->
        raise ArgumentError,
              ":idle_timeout_ms must be a non-negative integer, got #{inspect(other)}"
    end
  end

  # ── GenServer ─────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    :ets.new(name, [:named_table, :public, :set, write_concurrency: true])
    tick_ms = Keyword.get(opts, :tick_ms, @default_tick_ms)
    schedule(tick_ms)

    {:ok, %{table: name, supervisor: Keyword.fetch!(opts, :supervisor), tick_ms: tick_ms}}
  end

  @impl true
  def handle_info(:tick, state) do
    sweep(state)
    schedule(state.tick_ms)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp sweep(%{table: table, supervisor: supervisor}) do
    now = now_ms()

    :ets.foldl(
      fn {key, pid, last, idle_ms}, :ok ->
        cond do
          not Process.alive?(pid) ->
            take(table, key, pid, last)

          now - last >= idle_ms and take(table, key, pid, last) ->
            _ = DynamicSupervisor.terminate_child(supervisor, pid)
            Logger.info("[ankusa] closed idle #{inspect(key)} after #{idle_ms} ms")

          true ->
            :ok
        end

        :ok
      end,
      :ok,
      table
    )
  end

  # Deletes the row only if it is still the one judged: a touch since then
  # (a new `last`, or a new pid) keeps it.
  defp take(table, key, pid, last) do
    spec = [
      {{:"$1", :"$2", :"$3", :_},
       [{:"=:=", :"$1", {:const, key}}, {:"=:=", :"$2", pid}, {:"=:=", :"$3", last}], [true]}
    ]

    :ets.select_delete(table, spec) == 1
  end

  defp schedule(tick_ms), do: Process.send_after(self(), :tick, tick_ms)

  defp now_ms, do: System.monotonic_time(:millisecond)
end
