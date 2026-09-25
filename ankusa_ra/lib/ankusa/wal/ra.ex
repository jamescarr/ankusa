defmodule Ankusa.WAL.Ra do
  @moduledoc """
  Shared, multi-node `Ankusa.WAL` backed by a [Ra](https://github.com/rabbitmq/ra)
  (Raft) log. Every node that runs the `:wal` role hosts one Raft member; every
  node that uses the WAL — an edge acking a hook, a dispatcher advancing a
  cursor — talks to whichever member is currently leader.

  This is the adapter for a *fleet*: unlike `Ankusa.WAL.Postgres` there is no
  single database box to lose, no fleet-wide lock serializing every append, and
  no split-brain window where two nodes both believe they own a cursor.

  ## What Raft buys, and where it shows up

    * **An ack means a majority has the bytes.** `append/2` returns only after
      the leader has replicated the entry to a majority (Ra's `ra_log_wal`
      fsyncs before that). Losing any minority of members loses nothing.
    * **One serialization point, not one lock.** The leader is the only writer,
      so commit order *is* seq order with no advisory lock, no per-instance
      lock table, and no cross-node contention.
    * **Cursors and leases are replicated state.** `Ankusa.WAL`'s leases
      (`:dispatch`, `:storage`) live in the state machine (see
      `Ankusa.WAL.Ra.Machine`), so a cursor write carrying a stale token is
      refused by the *cluster*, not by a lock a zombie might still hold.
    * **The log is compacted, not truncated.** `truncate_through/3` removes the
      records from the machine state and `live_indexes/1` tells Ra which log
      entries to keep. Everything else — segments, snapshots — Ra reclaims on
      its own, and a member joining later catches up by snapshot install.

  ## Two shapes

  **Dedicated WAL cluster.** Three (or five) `:wal`-only nodes, each with its
  own volume, and clients (edges, dispatchers, storage) reaching them over
  Erlang distribution. The client nodes do *not* need a `:ra` member; they only
  need the cookie and the member names:

      roles: [:edge]
      wal: {Ankusa.WAL.Ra,
            members: [
              {:"ankusa_wal_default", :"ankusa@ankusa-wal-0"},
              {:"ankusa_wal_default", :"ankusa@ankusa-wal-1"},
              {:"ankusa_wal_default", :"ankusa@ankusa-wal-2"}
            ]}

  **Single node.** All roles on one machine — the laptop shape:

      roles: [:edge, :dispatch, :storage, :wal]
      wal: {Ankusa.WAL.Ra, members: [{:"ankusa_wal_default", node()}]}

  A one-member cluster is a valid Raft cluster: it elects itself and commits
  immediately. It is *not* fault tolerant — it is the same promise as
  `Ankusa.WAL.DiskLog`, with the same API as the fleet shape, which is what
  makes local development honest.

  ## Config

  All options come from the `%Ankusa.Config{}`'s `wal` entry:

  | Option | Default | Meaning |
  | --- | --- | --- |
  | `:members` | required | non-empty list of `{cluster_name, node}`; the cluster name is fixed to `:"ankusa_wal_<instance>"` and only the node varies |
  | `:append_timeout_ms` | `10_000` | how long a command may retry before it is reported as failed |
  | `:read_timeout_ms` | `5_000` | one Ra query's timeout |
  | `:max_command_bytes` | `16 * 1024 * 1024` | largest single Raft command; a bigger append is split into several, and must still fit `max_body_bytes` plus 64 KiB of overhead |

  `:next_seq` and `:time_offset_ms` are also honoured (machine init config) —
  they exist for the migration task and the clock-skew fault tests.

  ## Rules this adapter keeps

    * `read/3` is **leader-consistent** (`:ra.consistent_aux/3`), so a reader
      never sees a shorter log than one it already observed, and a new cursor
      holder never resumes from a stale cursor.
    * A command that could not be answered returns `{:error, reason}` — an
      append that fails after some of its chunks committed returns an error and
      fabricates no results. The caller (the edge batcher) turns that into a
      `503`: never ack what was not committed.
    * `put_cursor/4` and `truncate_through/3` map a timeout to
      `{:error, :fenced}`, because the caller cannot know whether the write
      applied and "re-acquire the lease" is the only safe reading.
  """

  @behaviour Ankusa.WAL

  use GenServer

  require Logger

  alias Ankusa.{Config, Envelope}
  alias Ankusa.WAL.Ra.Machine

  @default_append_timeout_ms 10_000
  @default_read_timeout_ms 5_000
  @default_max_command_bytes 16 * 1024 * 1024
  # Room for Ra's own framing and the machine's per-record bookkeeping inside
  # one command.
  @command_overhead_bytes 65_536
  # A GenServer.call into this adapter can legitimately block for one append
  # timeout plus one read timeout (rediscovering a leader after a change), so
  # the call timeout allows both, plus this slack.
  @call_slack_ms 5_000

  # Retry pacing while a leader is being elected. Short enough that failover is
  # not the bottleneck, long enough not to spin a scheduler.
  @retry_sleep_ms 20

  # ── lifecycle ─────────────────────────────────────────────────────────────

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :wal))
  end

  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :instance)},
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    %Config{} = config = Keyword.fetch!(opts, :config)
    wal_opts = elem(config.wal, 1)

    validate_config!(config, wal_opts)

    system = :"ankusa_ra_#{instance}"
    cluster = :"ankusa_wal_#{instance}"
    server_id = {cluster, node()}
    data_dir = Config.path(config, "ra")
    hosts_member = hosts_member?(config, wal_opts)

    with :ok <- ensure_ra(),
         :ok <- start_system_if_member(hosts_member, system, data_dir),
         :ok <- bootstrap(hosts_member, system, cluster, server_id, wal_opts) do
      {:ok,
       %{
         instance: instance,
         system: system,
         cluster: cluster,
         server_id: server_id,
         members: Keyword.fetch!(wal_opts, :members),
         has_member: hosts_member,
         leader: nil,
         append_timeout_ms: Keyword.get(wal_opts, :append_timeout_ms, @default_append_timeout_ms),
         read_timeout_ms: Keyword.get(wal_opts, :read_timeout_ms, @default_read_timeout_ms),
         max_command_bytes: Keyword.get(wal_opts, :max_command_bytes, @default_max_command_bytes)
       }}
    else
      {:error, reason} -> {:stop, {:ankusa_wal_ra_bootstrap_failed, reason}}
    end
  end

  # `:ra` the application, which every role needs: the cluster calls
  # (`:ra.process_command/3`, `:ra.consistent_aux/3`) go through it.
  defp ensure_ra do
    {:ok, _} = Application.ensure_all_started(:ra)
    :ok
  end

  # The *node-local* Ra system, and nothing else — only a node that hosts a
  # member has a log to keep. Starting one on a client node means the node keeps
  # a Ra log on its own disk for no reason, and that a failure to open it takes
  # the whole application down with it: an edge with no member of its own is then
  # unavailable because it could not write a log nobody reads. The system is what
  # this adapter wants where it is wanted, started in-process so two instances —
  # and a test run — do not trample each other.
  defp start_system_if_member(hosts_member?, system, data_dir) do
    if hosts_member? do
      File.mkdir_p!(data_dir)
      start_system(system, data_dir)
    else
      :ok
    end
  end

  defp start_system(system, data_dir) do
    # Ra hands `data_dir` straight to `:dets`, which rejects a binary path — a
    # charlist is what it accepts (and what `ra_env:data_dir/0` returns).
    data_dir = String.to_charlist(data_dir)

    config =
      :ra_system.default_config()
      |> Map.put(:name, system)
      |> Map.put(:data_dir, data_dir)
      |> Map.put(:wal_data_dir, data_dir)
      # Derived from the system name, so two instances in one VM (and a test
      # run) each get their own registered processes instead of fighting over
      # the global defaults.
      |> Map.put(:names, :ra_system.derive_names(system))

    case :ra_system.start(config) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  # A node hosts a member when it is told to (`:wal` in its roles) or when it
  # names *itself* in `:members` — the two ways of saying the same thing. The
  # second matters for the single-node shape: `roles: [:edge, :dispatch,
  # :storage, :wal]` with `members: [{cluster, node()}]` is the documented
  # laptop config, and a node that lists itself but never runs a member would
  # hang on "no leader" forever instead of saying so.
  #
  # This has to ask the *member list*. Asking "is this node's name its own name"
  # is true for every node that ever asks, which quietly turned every edge and
  # dispatcher into a Raft member: each kept a log on its own disk for a cluster
  # whose members list did not contain it, and a node that could not write that
  # log — an edge with a full or read-only volume — failed to boot at all,
  # taking its whole application down over a log nobody read.
  defp hosts_member?(%Config{} = config, wal_opts) do
    Config.role?(config, :wal) or
      Enum.any?(Keyword.fetch!(wal_opts, :members), fn {_cluster, member} -> member == node() end)
  end

  # A node that hosts a member recovers it (or starts it and joins); on any
  # other node the clients just talk to the cluster.
  defp bootstrap(hosts_member?, system, cluster, server_id, wal_opts) do
    if hosts_member? do
      case :ra.restart_server(system, server_id) do
        :ok ->
          campaign(server_id)
          :ok

        {:error, {:already_started, _pid}} ->
          # A race: the member is already up. Nothing to recover, just campaign.
          campaign(server_id)
          :ok

        {:error, reason} when reason in [:not_found, :name_not_registered] ->
          start_fresh(system, cluster, server_id, wal_opts)

        {:error, reason} ->
          # A member whose log cannot be recovered (a torn or corrupted WAL)
          # must not stay down: discard its local copy and re-initialize, then
          # catch up from the other members.
          Logger.warning(
            "[ankusa] Ra WAL #{inspect(server_id)} failed to recover: #{inspect(reason)}; re-initializing"
          )

          _ = :ra.force_delete_server(system, server_id)
          start_fresh(system, cluster, server_id, wal_opts)
      end
    else
      :ok
    end
  end

  defp start_fresh(system, cluster, server_id, wal_opts) do
    members = Keyword.fetch!(wal_opts, :members)

    # `:machine` lets a test (or an upgrade) boot a later machine version on a
    # fresh member; it defaults to the real machine.
    machine = {:module, Keyword.get(wal_opts, :machine, Machine), machine_config(wal_opts)}

    case :ra.start_server(system, cluster, server_id, machine, members) do
      :ok ->
        campaign(server_id)
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Ra does not start an election on a cluster that has never written anything —
  # it expects the caller to say when the cluster is complete, which is what
  # `:ra.start_cluster/4` does internally. Members here boot independently (one
  # per node, at their own pace), so the trigger has to be retried until one of
  # them wins: the first node up usually cannot gather a majority yet.
  #
  # Unlinked and best effort: a node that never sees a leader still serves reads
  # (they will simply fail) and its own member keeps retrying through Ra's own
  # timeout once it is no longer "new".
  defp campaign(server_id) do
    _ = spawn(fn -> campaign(server_id, 60) end)
    :ok
  end

  defp campaign(_server_id, 0), do: :ok

  defp campaign(server_id, attempts) do
    Process.sleep(250)

    case :ra.members(server_id, 1_000) do
      {:ok, _members, leader} when leader != nil ->
        :ok

      _ ->
        _ = :ra.trigger_election(server_id, 1_000)
        campaign(server_id, attempts - 1)
    end
  catch
    _kind, _reason -> campaign(server_id, attempts - 1)
  end

  defp machine_config(wal_opts) do
    wal_opts
    |> Keyword.take([:next_seq, :time_offset_ms])
    |> Map.new()
  end

  @doc """
  Reject a config that cannot work, at boot rather than at the first append.

  Raises `ArgumentError` when `:members` is missing or empty, when a member's
  node is not an atom, or when a single hook's body could not fit in one Raft
  command even after the append is split (`max_body_bytes` must leave
  #{@command_overhead_bytes} bytes of room under `max_command_bytes`).
  """
  @spec validate_config!(Config.t(), keyword()) :: :ok
  def validate_config!(%Config{} = config, wal_opts) do
    members = Keyword.get(wal_opts, :members, [])
    max_command_bytes = Keyword.get(wal_opts, :max_command_bytes, @default_max_command_bytes)

    cond do
      members == [] ->
        raise ArgumentError,
              "Ankusa.WAL.Ra needs a non-empty :members list of {cluster, node} tuples"

      Enum.any?(members, fn
        {_cluster, node} when is_atom(node) -> false
        _ -> true
      end) ->
        raise ArgumentError,
              "Ankusa.WAL.Ra :members must be {cluster_name, node} tuples with an atom node, " <>
                "got #{inspect(members)}"

      config.max_body_bytes > max_command_bytes - @command_overhead_bytes ->
        raise ArgumentError,
              "max_body_bytes (#{config.max_body_bytes}) must fit one Ra command " <>
                "(max_command_bytes #{max_command_bytes} minus 64 KiB of overhead)"

      true ->
        :ok
    end
  end

  # ── behaviour ─────────────────────────────────────────────────────────────

  @impl Ankusa.WAL
  def append(server, records), do: GenServer.call(server, {:append, records}, :infinity)

  @impl Ankusa.WAL
  def read(server, after_seq, limit),
    do: GenServer.call(server, {:read, after_seq, limit}, call_timeout(server))

  @impl Ankusa.WAL
  def get_cursor(server, name) do
    case GenServer.call(server, {:get_cursor, name}, call_timeout(server)) do
      {:ok, v} -> v
      {:error, r} -> raise RuntimeError, "Ankusa.WAL.Ra get_cursor failed: #{inspect(r)}"
    end
  end

  @impl Ankusa.WAL
  def put_cursor(server, name, seq, token),
    do: GenServer.call(server, {:put_cursor, name, seq, token}, call_timeout(server))

  @impl Ankusa.WAL
  def truncate_through(server, seq, token),
    do: GenServer.call(server, {:truncate_through, seq, token}, call_timeout(server))

  @impl Ankusa.WAL
  def stats(server) do
    case GenServer.call(server, :stats, call_timeout(server)) do
      {:ok, v} -> v
      {:error, r} -> raise RuntimeError, "Ankusa.WAL.Ra stats failed: #{inspect(r)}"
    end
  end

  @impl Ankusa.WAL
  def acquire_lease(server, name, holder, ttl_ms),
    do: GenServer.call(server, {:acquire_lease, name, holder, ttl_ms}, call_timeout(server))

  @impl Ankusa.WAL
  def renew_lease(server, lease),
    do: GenServer.call(server, {:renew_lease, lease}, call_timeout(server))

  @impl Ankusa.WAL
  def release_lease(server, lease),
    do: GenServer.call(server, {:release_lease, lease}, call_timeout(server))

  # The adapter's own calls can block for one append timeout plus one read
  # timeout, so `call_timeout/1` allows both (plus slack), reading the
  # instance's configured values when they are set.
  defp call_timeout({:via, Registry, {_, {instance, _}}}) do
    configured(instance, :append_timeout_ms, @default_append_timeout_ms) +
      configured(instance, :read_timeout_ms, @default_read_timeout_ms) + @call_slack_ms
  end

  defp configured(instance, key, default) do
    %Config{wal: {_mod, wal_opts}} = Ankusa.config(instance)
    Keyword.get(wal_opts, key, default)
  rescue
    _ -> default
  end

  # ── server ────────────────────────────────────────────────────────────────

  @impl true
  def handle_call({:append, records}, _from, state) do
    {reply, state} = do_append(records, state)
    {:reply, reply, state}
  end

  def handle_call({:read, after_seq, limit}, _from, state) do
    {reply, state} = do_read(after_seq, limit, state)
    {:reply, reply, state}
  end

  def handle_call({:get_cursor, name}, _from, state) do
    {reply, state} = aux_reply(state, {:cursor, name})
    {:reply, reply, state}
  end

  def handle_call({:put_cursor, name, seq, token}, _from, state) do
    {reply, state} = cursor_command({:put_cursor, name, seq, token}, state)
    {:reply, reply, state}
  end

  def handle_call({:truncate_through, seq, token}, _from, state) do
    {reply, state} = cursor_command({:truncate_through, seq, token}, state)
    {:reply, reply, state}
  end

  def handle_call(:stats, _from, state) do
    {reply, state} = aux_reply(state, :overview)

    reply =
      case reply do
        {:ok, overview} ->
          {:ok,
           Map.take(overview, [
             :records,
             :bytes,
             :next_seq,
             :min_seq,
             :max_seq,
             :cursors,
             :floor,
             :leases
           ])}

        {:error, reason} ->
          {:error, reason}
      end

    {:reply, reply, state}
  end

  def handle_call({:acquire_lease, name, holder, ttl_ms}, _from, state) do
    # "Could not reach the cluster" must not read as "you hold the lease": the
    # safe answer is the one that sends the caller back to standby.
    case command(state, {:acquire_lease, name, holder, ttl_ms}) do
      {:ok, reply, state} -> {:reply, reply, state}
      {:error, _reason, state} -> {:reply, {:error, {:held, "unavailable"}}, state}
    end
  end

  def handle_call({:renew_lease, %{name: n, holder: h, token: t, ttl_ms: ttl}}, _from, state) do
    case command(state, {:renew_lease, n, h, t, ttl}) do
      {:ok, reply, state} -> {:reply, reply, state}
      # A renewal that cannot be confirmed is a lease we may have lost.
      {:error, _reason, state} -> {:reply, {:error, :lost}, state}
    end
  end

  def handle_call({:release_lease, %{name: n, holder: h, token: t}}, _from, state) do
    # Releasing is best effort by design: the lease expires on its own, and a
    # holder that cannot reach the cluster must still be able to shut down.
    state =
      case command(state, {:release_lease, n, h, t}) do
        {_status, _reply, state} -> state
      end

    {:reply, :ok, state}
  end

  @impl true
  def handle_info(_message, state), do: {:noreply, state}

  # ── commands ──────────────────────────────────────────────────────────────

  # `put_cursor`/`truncate_through` have no "unknown" reply: a timeout means the
  # caller cannot know whether it applied, and the only safe reading is
  # "re-acquire the lease".
  defp cursor_command(command_tuple, state) do
    case command(state, command_tuple) do
      {:ok, :ok, state} -> {:ok, state}
      {:ok, {:error, :fenced}, state} -> {{:error, :fenced}, state}
      {:error, _reason, state} -> {{:error, :fenced}, state}
      {:ok, other, state} -> {other, state}
    end
  end

  defp do_append(records, state) do
    # One id for the whole call, and a distinct command id per chunk: a retry of
    # a chunk after a timeout or a leader change returns the results it already
    # committed instead of allocating again. Random, not a counter: a counter
    # restarts at 0 on boot, so a retry after a node restart could collide with
    # an unrelated earlier append.
    batch_id = :crypto.strong_rand_bytes(16)
    rows = Enum.map(records, &record(&1.envelope))

    {results, state} =
      rows
      |> chunks(state.max_command_bytes)
      |> Enum.with_index()
      |> Enum.reduce_while({[], state}, fn {chunk, n}, {acc, st} ->
        case command(st, {:append, {batch_id, n}, chunk}) do
          {:ok, {:ok, chunk_results}, st} ->
            {:cont, {acc ++ chunk_results, st}}

          {:ok, other, st} ->
            # Never expected: the machine only ever answers {:ok, results} here.
            {:halt, {{:error, {:unexpected_reply, other}}, st}}

          {:error, reason, st} ->
            # A later chunk failed after earlier ones committed. Report the
            # failure and fabricate no results: the caller must retry, and the
            # retry is idempotent per chunk.
            {:halt, {{:error, reason}, st}}
        end
      end)

    case results do
      {:error, _reason} = error ->
        {error, state}

      replies ->
        paired =
          records
          |> Enum.zip(replies)
          |> Enum.map(fn {%{envelope: env}, {:committed, seq}} ->
            {:committed, %{env | seq: seq}}
          end)

        {{:ok, paired}, state}
    end
  end

  # The command carries the encoded envelopes and nothing else: the machine
  # records *where* each one sits in the log, so every other field would be
  # state it has to carry and never reads.
  @doc false
  def record(%Envelope{} = env) do
    env = Ankusa.WAL.stamp_commit(env)

    Envelope.to_binary(%{env | seq: nil})
  end

  # Consecutive chunks whose summed payload stays within one command; always at
  # least one record per chunk, so a single record larger than the budget still
  # makes progress (validate_config!/2 is what stops that being possible).
  defp chunks([], _max), do: []
  defp chunks(rows, max), do: chunk(rows, max, [])

  defp chunk([], _max, acc), do: Enum.reverse(acc)

  defp chunk([row | rest], max, acc) do
    # Seed `take` with the first record, so a chunk always makes progress even
    # when that record alone is bigger than the budget.
    {taken, remaining} = take(rest, max, [row], size(row))
    chunk(remaining, max, [Enum.reverse(taken) | acc])
  end

  defp take([], _max, acc, _size), do: {acc, []}

  defp take([row | rest], max, acc, size) do
    next = size + size(row)

    if next > max do
      {acc, [row | rest]}
    else
      take(rest, max, [row | acc], next)
    end
  end

  defp size(envelope), do: byte_size(envelope)

  # ── reads ─────────────────────────────────────────────────────────────────

  defp do_read(after_seq, limit, state) do
    case aux(state, {:read_plan, after_seq, limit}) do
      {:ok, [], state} ->
        {[], state}

      {:ok, plan, state} ->
        indexes = plan |> Enum.map(&elem(&1, 1)) |> Enum.uniq()

        case read_entries(indexes, state) do
          {:ok, entries, state} ->
            {Enum.map(plan, fn {seq, idx, pos} -> envelope_at(entries, idx, pos, seq) end), state}

          {:error, reason, state} ->
            # A read has no error channel, and a reader that is told "nothing"
            # simply tries again on its next tick. Raising here would take the
            # dispatch pipeline down on every leader election.
            Logger.warning("[ankusa] Ra WAL read failed: #{inspect(reason)}")
            {[], state}
        end

      {:error, _reason, state} ->
        {[], state}
    end
  end

  # Prefer this node's own copy when it is caught up: it costs no distribution
  # hop. Otherwise read from the leader, which is guaranteed to have applied the
  # entries the plan was built from.
  defp read_entries(indexes, state) do
    last = Enum.max(indexes)

    cond do
      state.has_member and last_applied(state) >= last ->
        ra_read_entries(state.server_id, indexes, state)

      state.leader ->
        ra_read_entries(state.leader, indexes, state)

      true ->
        {:error, :no_leader, state}
    end
  end

  defp ra_read_entries(server_id, indexes, state) do
    result =
      if elem(server_id, 1) == node() do
        :ra_server_proc.read_entries(server_id, indexes, :undefined, state.read_timeout_ms)
      else
        :erpc.call(
          elem(server_id, 1),
          :ra_server_proc,
          :read_entries,
          [server_id, indexes, :undefined, state.read_timeout_ms]
        )
      end

    case result do
      {:ok, {entries, _flru}} -> {:ok, entries, state}
      {:error, reason} -> {:error, reason, state}
      other -> {:error, {:unexpected_read_reply, other}, state}
    end
  catch
    kind, reason -> {:error, {kind, reason}, state}
  end

  defp last_applied(state) do
    case :ra_counters.counters(state.server_id, [:last_applied]) do
      %{last_applied: n} -> n
      _ -> 0
    end
  end

  # The log entry holds the whole command, so the record is fetched out of it by
  # position. Anything else in the log at that index is not something this
  # adapter knows how to read, and silently skipping it would lose a record.
  defp envelope_at(entries, index, pos, seq) do
    case Map.get(entries, index) do
      {^index, _term, {:"$usr", _meta, {:append, _batch_id, records}, _reply_mode}} ->
        %{Envelope.from_binary(Enum.at(records, pos - 1)) | seq: seq}

      other ->
        raise "unexpected Ra log entry at index #{index}: #{inspect(other)}"
    end
  end

  # ── commands and queries ──────────────────────────────────────────────────

  # Retries until `append_timeout_ms` is spent: a leader change, a lost reply or
  # an election all look the same from here, and the caller's deadline is the
  # only thing that should end the attempt.
  defp command(state, command) do
    deadline = mono_ms() + state.append_timeout_ms
    do_command(command, state, deadline)
  end

  defp do_command(command, state, deadline) do
    state = resolve_leader(state)

    case state.leader do
      nil ->
        retry(command, state, deadline, :no_leader)

      leader ->
        case :ra.process_command(leader, command, remaining(state.append_timeout_ms, deadline)) do
          {:ok, reply, _leader} ->
            {:ok, reply, state}

          {:error, reason} ->
            retry(command, forget_leader(state), deadline, reason)

          {:timeout, _server} ->
            retry(command, state, deadline, :timeout)
        end
    end
  end

  defp retry(command, state, deadline, reason) do
    if mono_ms() >= deadline do
      {:error, reason, state}
    else
      # Drop the cached leader: it either moved or is unreachable, and the next
      # attempt should rediscover rather than hammer the same dead server id.
      Process.sleep(@retry_sleep_ms)
      do_command(command, forget_leader(state), deadline)
    end
  end

  # A leader-consistent query. Used for reads, cursors and stats — everything a
  # caller must not see a stale answer for.
  defp aux(state, command) do
    state = resolve_leader(state)

    case state.leader do
      nil ->
        {:error, :no_leader, state}

      leader ->
        case :ra.consistent_aux(leader, command, state.read_timeout_ms) do
          {:ok, reply, _leader} -> {:ok, reply, state}
          {:error, reason} -> {:error, reason, forget_leader(state)}
          {:timeout, _server} -> {:error, :timeout, forget_leader(state)}
        end
    end
  catch
    kind, reason -> {:error, {kind, reason}, forget_leader(state)}
  end

  # Convert `aux/2`'s reply into the `{:ok, reply} | {:error, reason}` shape the
  # client functions expect, without raising inside the server: the raise is
  # the *client's* job (a wrong cursor is worse than no cursor, but a query
  # error must not take the whole WAL process down).
  defp aux_reply(state, command) do
    case aux(state, command) do
      {:ok, reply, state} -> {{:ok, reply}, state}
      {:error, reason, state} -> {{:error, reason}, state}
    end
  end

  defp resolve_leader(%{leader: leader} = state) when leader != nil, do: state

  defp resolve_leader(state) do
    case find_leader(state.members, state.read_timeout_ms) do
      {:ok, leader} -> %{state | leader: leader}
      :error -> state
    end
  end

  defp find_leader([], _timeout), do: :error

  defp find_leader([member | rest], timeout) do
    case :ra.members(member, timeout) do
      {:ok, _members, leader} when leader != nil -> {:ok, leader}
      _ -> find_leader(rest, timeout)
    end
  catch
    _kind, _reason -> find_leader(rest, timeout)
  end

  defp forget_leader(state), do: %{state | leader: nil}

  @doc """
  Send one raw machine command to a cluster addressed by its `members`, without
  starting a node-local Ra system or member.

  This is the same leader discovery and retry the adapter's own commands use,
  exposed for one-shot tooling — the `mix ankusa.wal.migrate` task, which has to
  drive a real cluster from a node that is not part of it.
  """
  @spec remote_command([{atom(), atom()}], term(), keyword()) :: {:ok, term()} | {:error, term()}
  def remote_command(members, command, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_append_timeout_ms)
    read_timeout = Keyword.get(opts, :read_timeout_ms, @default_read_timeout_ms)
    remote_command(members, command, read_timeout, mono_ms() + timeout)
  end

  defp remote_command(members, command, read_timeout, deadline) do
    case find_remote_leader(members, read_timeout) do
      {:ok, leader} ->
        case :ra.process_command(leader, command, remaining(read_timeout, deadline)) do
          {:ok, reply, leader} ->
            remember_leader(members, leader)
            {:ok, reply}

          {:error, reason} ->
            forget_leader_cache(members)
            remote_retry(members, command, read_timeout, deadline, reason)

          {:timeout, _server} ->
            remote_retry(members, command, read_timeout, deadline, :timeout)
        end

      :error ->
        remote_retry(members, command, read_timeout, deadline, :no_leader)
    end
  catch
    kind, reason -> remote_retry(members, command, read_timeout, deadline, {kind, reason})
  end

  # A per-members cached leader: `Ankusa.DedupStore.Ra` sends one command per
  # record, so rediscovering the leader on every call is a wasted round trip.
  defp find_remote_leader(members, read_timeout) do
    case :persistent_term.get({__MODULE__, :leader, members}, nil) do
      nil -> find_leader(members, read_timeout)
      leader -> {:ok, leader}
    end
  end

  defp remember_leader(members, leader) do
    case :persistent_term.get({__MODULE__, :leader, members}, nil) do
      ^leader -> :ok
      _ -> :persistent_term.put({__MODULE__, :leader, members}, leader)
    end
  end

  defp forget_leader_cache(members) do
    :persistent_term.erase({__MODULE__, :leader, members})
    :ok
  end

  defp remote_retry(members, command, read_timeout, deadline, reason) do
    if mono_ms() >= deadline do
      {:error, reason}
    else
      Process.sleep(@retry_sleep_ms)
      remote_command(members, command, read_timeout, deadline)
    end
  end

  @doc """
  Run one leader-consistent aux query against a cluster addressed by its
  `members`, without starting a node-local Ra system or member.

  The read-side companion of `remote_command/3`: `:overview` is the machine's
  `stats/1` map, `{:cursor, name}` a cursor, `{:read_plan, after_seq, limit}` a
  read plan. Used by operator tooling and the cluster test harness.
  """
  @spec remote_aux([{atom(), atom()}], term(), keyword()) :: {:ok, term()} | {:error, term()}
  def remote_aux(members, command, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_read_timeout_ms)

    case find_leader(members, timeout) do
      {:ok, leader} ->
        case :ra.consistent_aux(leader, command, timeout) do
          {:ok, reply, _leader} -> {:ok, reply}
          {:error, reason} -> {:error, reason}
          {:timeout, _server} -> {:error, :timeout}
        end

      :error ->
        {:error, :no_leader}
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp mono_ms, do: System.monotonic_time(:millisecond)

  # A Ra call's timeout, capped at `timeout` but never longer than what is left
  # before the caller's deadline (and never zero: a zero timeout is not a
  # meaningful bound for Ra).
  defp remaining(timeout, deadline) do
    min(timeout, max(deadline - mono_ms(), 1))
  end
end
