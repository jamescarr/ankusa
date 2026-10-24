defmodule Ankusa.Intake.Recovery do
  @moduledoc """
  Boot recovery of `wal: :dual` (ADR 0002, section 8), and the same code offline
  as `mix ankusa.recover`.

  A replacement node starts with the **same `intake.node_id`** as the lost one,
  restores from the backup (`Ankusa.Store.Backup`), then `run/2`:

    1. **Identity.** `m:intake_node` is stamped on first use; a different id
       refuses (`{:intake_node_mismatch, stamped, configured}`): restoring node a's
       backup into node b would interleave two seq spaces.
    2. **Topology.** `Ankusa.Intake.verify/1` (skipped with
       `intake.verify_topic: false`); the node waits until the log answers.
    3. **Replay the tail** from `m:intake_offset + 1` (`:earliest` when the store
       has no offset), in chunks of 500 in one synced batch each, the offset
       advancing in the same batch. A record is applied through **one rule, keyed
       by seq and hook id**: no row at `hooks/<seq>` writes the hook under its
       original seq (delivery rows from the source's *current* config, dedupe
       keys, counters, `m:next_seq` raised); the same id is skipped; a different id
       is a seq collision, written under a fresh seq, never over the other hook.
       A duplicate, reordered or twice-replayed record changes nothing.
    4. **How far back.** If the log's earliest position is above `offset + 1` and
       the store was restored, `{:intake_truncated, wanted, earliest}` refuses
       (`allow_gap: true` goes on and says so); on an existing store it is a
       warning, because every hook in the gap is in the local store.
    5. **Republish the local tail**: every `hooks` row above `m:intake_seq`
       (a crash can leave local hooks the log never confirmed), then the
       watermark moves to `next_seq - 1`.

  It runs as an `Ankusa.Instance` child after the source store (replay resolves
  sinks through it) and before the writer: the node serves nothing until it is
  done, and `Health.ready/1` says `intake: "recovering"` meanwhile.
  """

  use GenServer

  require Logger

  alias Ankusa.{Envelope, Intake, Queue, Store}
  alias Ankusa.Intake.Pending
  alias Ankusa.Queue.Deliveries
  alias Ankusa.Queue.Deliveries.Counters
  alias Ankusa.Queue.Tenants
  alias Ankusa.Store.Keys

  @chunk 500
  @republish_chunk 256
  @warn_every_ms 60_000

  @type summary :: %{
          records: non_neg_integer(),
          applied: non_neg_integer(),
          present: non_neg_integer(),
          collided: non_neg_integer(),
          unroutable: non_neg_integer(),
          skipped: non_neg_integer(),
          republished: non_neg_integer()
        }

  # ── the instance child ────────────────────────────────────────────────────

  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :instance)},
      start: {__MODULE__, :start_link, [opts]},
      restart: :transient
    }
  end

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)

    origin = Pending.meta(Pending.table(instance), :origin, :existing)

    case boot(instance, Keyword.get(opts, :origin, origin)) do
      :ok -> :ignore
      {:error, reason} -> {:stop, {:intake_recovery_failed, reason}}
    end
  end

  @doc """
  The boot sequence: identity, then (`intake.recover_on_boot`) topology, replay
  and republish. A log that cannot be reached is waited for, never a crash.
  """
  @spec boot(atom(), :existing | :restored | :fresh) :: :ok | {:error, term()}
  def boot(instance, origin) do
    config = Ankusa.config(instance)
    t = Pending.table(instance)
    Pending.put_meta(t, :state, :recovering)

    result =
      if config.intake.recover_on_boot do
        wait_for_log(instance, config, 0)
        run(instance, origin: origin, boot: true)
      else
        with :ok <- identity(instance, config, []),
             {:ok, _partition} <- partition(instance, false) do
          {:ok, :skipped}
        end
      end

    Pending.put_meta(t, :state, :up)

    case result do
      {:ok, _summary} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp wait_for_log(instance, config, last_warned) do
    verified =
      if config.intake.verify_topic,
        do: Intake.verify(instance),
        else: if(Intake.ready?(instance), do: :ok, else: {:error, :not_ready})

    case verified do
      :ok ->
        :ok

      {:error, reason} ->
        now = System.monotonic_time(:millisecond)

        warned =
          if now - last_warned >= @warn_every_ms do
            Logger.warning(
              "[ankusa] intake log #{Intake.topic(config)} is not usable yet (#{inspect(reason)}); " <>
                "the node does not serve until it is: waiting"
            )

            now
          else
            last_warned
          end

        Process.sleep(config.intake.probe_ms)
        wait_for_log(instance, config, warned)
    end
  end

  # ── run ───────────────────────────────────────────────────────────────────

  @doc """
  Steps 1-5 against an open store. Options: `origin` (`:existing` | `:restored` |
  `:fresh`), `node` (asserts the id), `from` (`:backup`, the default, `:earliest`
  or a position), `dry_run`, `allow_gap`. Returns the summary.
  """
  @spec run(atom(), keyword()) :: {:ok, summary()} | {:error, term()}
  def run(instance, opts \\ []) do
    config = Ankusa.config(instance)
    dry? = Keyword.get(opts, :dry_run, false)
    started = System.monotonic_time()

    with :ok <- identity(instance, config, opts),
         :ok <- verify(instance, config, opts),
         {:ok, _partition} <- partition(instance, dry?),
         {:ok, replay} <- replay_tail(instance, config, opts),
         {:ok, republished, last_published} <- republish(instance, config, opts) do
      finish_offsets(instance, dry?, replay.last_position, last_published)

      summary = %{
        records: replay.records,
        applied: replay.applied,
        present: replay.present,
        collided: replay.collided,
        unroutable: replay.unroutable,
        skipped: replay.skipped,
        republished: republished
      }

      emit_summary(instance, summary, System.monotonic_time() - started)
      {:ok, summary}
    end
  end

  # 1. identity ---------------------------------------------------------------

  defp identity(instance, config, opts) do
    configured = config.intake.node_id
    asserted = Keyword.get(opts, :node)

    if asserted != nil and asserted != configured do
      {:error, {:intake_node_mismatch, asserted, configured}}
    else
      case Store.get(instance, :default, Keys.meta("intake_node")) do
        {:ok, ^configured} ->
          :ok

        {:ok, stamped} ->
          {:error, {:intake_node_mismatch, stamped, configured}}

        :not_found ->
          if Keyword.get(opts, :dry_run, false),
            do: :ok,
            else:
              Store.write(instance, [{:put, :default, Keys.meta("intake_node"), configured}],
                sync: true
              )

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  # 2. topology ---------------------------------------------------------------

  defp verify(instance, config, opts) do
    cond do
      Keyword.get(opts, :boot, false) ->
        :ok

      not config.intake.verify_topic ->
        :ok

      true ->
        with {:error, reason} <- Intake.verify(instance),
             do: {:error, {:intake_unverified, reason}}
    end
  end

  # The node's partition: stamped once, by the adapter that has the notion.
  defp partition(instance, dry?) do
    t = Pending.table(instance)

    case Store.get(instance, :default, Keys.meta("intake_partition")) do
      {:ok, <<p::32>>} ->
        if Pending.exists?(instance), do: Pending.put_meta(t, :partition, p)
        {:ok, p}

      :not_found ->
        case Intake.assign_partition(instance) do
          {:ok, p} ->
            unless dry?,
              do:
                Store.write(
                  instance,
                  [{:put, :default, Keys.meta("intake_partition"), <<p::32>>}],
                  sync: true
                )

            if Pending.exists?(instance), do: Pending.put_meta(t, :partition, p)
            {:ok, p}

          :none ->
            {:ok, nil}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # 3. + 4. replay ------------------------------------------------------------

  defp replay_tail(instance, config, opts) do
    offset = read_u64(instance, Keys.meta("intake_offset"))
    origin = Keyword.get(opts, :origin, :existing)

    from =
      case Keyword.get(opts, :from, :backup) do
        :backup -> if offset == nil, do: :earliest, else: offset + 1
        other -> other
      end

    with :ok <- check_gap(instance, offset, origin, opts),
         {:ok, next_seq} <- Queue.Writer.next_seq(instance),
         {:ok, next_ord} <- Tenants.next_free_ord(instance) do
      ctx = %{
        instance: instance,
        config: config,
        dry?: Keyword.get(opts, :dry_run, false),
        archive?: Ankusa.Config.role?(config, :storage),
        now: System.system_time(:millisecond)
      }

      state = %{
        buf: [],
        records: 0,
        applied: 0,
        present: 0,
        collided: 0,
        unroutable: 0,
        skipped: 0,
        next_seq: next_seq,
        next_ord: next_ord,
        offset: offset,
        last_position: nil,
        error: nil
      }

      {:ok, agent} = Agent.start_link(fn -> state end)

      try do
        fun = fn record ->
          chunk =
            Agent.get_and_update(agent, fn s ->
              s = %{s | buf: [record | s.buf], records: s.records + 1}

              if length(s.buf) >= @chunk,
                do: {Enum.reverse(s.buf), %{s | buf: []}},
                else: {nil, s}
            end)

          if chunk == nil, do: :ok, else: flush(ctx, agent, chunk)
        end

        case Intake.replay(instance, from, fun) do
          {:ok, last} ->
            rest = Agent.get_and_update(agent, fn s -> {Enum.reverse(s.buf), %{s | buf: []}} end)

            with :ok <- if(rest == [], do: :ok, else: flush(ctx, agent, rest)) do
              final = Agent.get(agent, & &1)
              {:ok, Map.put(final, :last_position, last || final.last_position)}
            end

          {:error, class} ->
            {:error, {:intake_replay_failed, class}}
        end
      after
        Agent.stop(agent)
      end
    end
  end

  defp check_gap(instance, offset, origin, opts) do
    with true <- offset != nil,
         {:ok, earliest} when is_integer(earliest) <- Intake.earliest(instance),
         true <- earliest > offset + 1 do
      wanted = offset + 1

      cond do
        origin == :restored and not Keyword.get(opts, :allow_gap, false) ->
          {:error, {:intake_truncated, wanted, earliest}}

        origin == :restored ->
          accept_gap(instance, offset, earliest, opts)

        true ->
          Logger.warning(
            "[ankusa] intake log starts at #{earliest}, above #{wanted}: the gap holds only " <>
              "hooks already in this store"
          )

          :ok
      end
    else
      _ -> :ok
    end
  end

  # `--allow-gap`: the operator accepts a loss the log can no longer cover. The
  # decision is a row in the node's own audit trail, written before the replay
  # goes on (a dry run only says so, and writes nothing).
  defp accept_gap(instance, offset, earliest, opts) do
    wanted = offset + 1

    Logger.warning(
      "[ankusa] intake log starts at #{earliest} but the restored store holds up to " <>
        "#{offset}: hooks acked in positions #{wanted}..#{earliest - 1} are NOT recovered " <>
        "(--allow-gap)"
    )

    if Keyword.get(opts, :dry_run, false) do
      :ok
    else
      node = Ankusa.config(instance).intake.node_id

      attrs = %{
        action: "intake.recover.allow_gap",
        resource: node,
        detail: %{
          node: node,
          wanted: wanted,
          earliest: earliest,
          lost_positions: earliest - wanted
        }
      }

      context = %Ankusa.Audit.Context{actor: Keyword.get(opts, :actor, "ankusa.recover")}

      case Ankusa.Audit.commit(instance, context, attrs, []) do
        :ok -> :ok
        {:error, reason} -> {:error, {:audit_failed, reason}}
      end
    end
  end

  # One chunk: decide per record, write the chunk (and the offset) in one batch.
  defp flush(ctx, agent, records) do
    state = Agent.get(agent, & &1)

    case apply_chunk(ctx, state, records) do
      {:ok, state} ->
        Agent.update(agent, fn s -> %{state | buf: s.buf, records: s.records} end)
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp apply_chunk(ctx, state, records) do
    seqs = Enum.map(records, & &1.seq)

    with {:ok, stored} <- Store.multi_get(ctx.instance, :hooks, Enum.map(seqs, &Keys.hook/1)),
         {:ok, ords} <- resolve_tenants(ctx, state, records) do
      existing = Map.new(Enum.zip(seqs, stored))

      {ops, state, _seen} =
        Enum.reduce(records, {[], state, %{}}, fn record, {ops, state, seen} ->
          case decide(record, existing, seen) do
            :present ->
              {ops, bump(state, :present, record), seen}

            {:write, seq, collided?} ->
              {new_ops, state} = write_ops(ctx, state, record, seq, collided?, ords)
              {new_ops ++ ops, bump(state, nil, record), Map.put(seen, record.seq, record.id)}
          end
        end)

      position = records |> Enum.map(& &1.position) |> Enum.max()
      offset = max(state.offset || -1, position)

      meta_ops =
        [
          {:put, :default, Keys.meta("next_seq"), <<state.next_seq::64>>},
          {:put, :default, Keys.meta("intake_offset"), <<offset::64>>}
        ] ++ ords.ops

      state = %{state | offset: offset, next_ord: ords.next_ord}
      all = Deliveries.coalesce(ops ++ meta_ops)

      if ctx.dry? do
        {:ok, state}
      else
        case Store.write(ctx.instance, all, sync: true) do
          :ok -> {:ok, state}
          {:error, reason} -> {:error, reason}
        end
      end
    end
  end

  defp decide(record, existing, seen) do
    case Map.get(seen, record.seq) do
      nil -> decide_stored(record, Map.fetch!(existing, record.seq))
      id when id == record.id -> :present
      _other -> {:write, :fresh, true}
    end
  end

  defp decide_stored(_record, :not_found), do: {:write, :original, false}

  defp decide_stored(record, {:ok, bin}) do
    case Envelope.from_binary(bin) do
      {:ok, %Envelope{id: id}} when id == record.id -> :present
      _different_or_unreadable -> {:write, :fresh, true}
    end
  end

  defp bump(state, :present, record) do
    %{
      state
      | present: state.present + 1,
        last_position: max(state.last_position || -1, record.position)
    }
  end

  defp bump(state, nil, record),
    do: %{state | last_position: max(state.last_position || -1, record.position)}

  defp write_ops(ctx, state, record, mode, collided?, ords) do
    {seq, state} =
      case mode do
        :original -> {record.seq, state}
        :fresh -> {state.next_seq, %{state | next_seq: state.next_seq + 1}}
      end

    state = %{state | next_seq: max(state.next_seq, seq + 1)}

    if collided? do
      Logger.error(
        "[ankusa] intake replay: seq #{record.seq} is held by a different hook; " <>
          "#{record.id} is written under seq #{seq}"
      )
    end

    case Envelope.from_binary(record.bin) do
      {:ok, env} ->
        {ops, kind} = hook_ops(ctx, env, record.bin, seq, ords)

        state =
          state
          |> then(&if(collided?, do: %{&1 | collided: &1.collided + 1}, else: &1))
          |> count(kind, collided?)

        emit_result(ctx.instance, if(collided?, do: :collision, else: kind))
        {ops, state}

      {:error, _unsupported} ->
        emit_result(ctx.instance, :skipped_unknown)
        {[], %{state | skipped: state.skipped + 1}}
    end
  end

  defp count(state, _kind, true), do: state
  defp count(state, :applied, false), do: %{state | applied: state.applied + 1}
  defp count(state, :unroutable, false), do: %{state | unroutable: state.unroutable + 1}

  # The hook, its delivery rows (sinks from the source's current config), its
  # archive obligation, and its dedupe key. An unknown source keeps only its
  # archive obligation.
  defp hook_ops(ctx, env, bin, seq, ords) do
    size = byte_size(bin)
    ord = Map.fetch!(ords.ords, Tenants.normalize(env.tenant_id))
    now = ctx.now

    {source, sinks, kind} =
      case Ankusa.SourceStore.fetch(ctx.instance, env.source_id) do
        {:ok, source} -> {source, Enum.map(source.sinks, &elem(&1, 0)), :applied}
        _unknown -> {nil, [], :unroutable}
      end

    lanes = if source, do: Ankusa.Ordering.lanes(ctx.instance, source, env)

    rows =
      sinks
      |> Enum.with_index()
      |> Enum.flat_map(fn {mod, i} ->
        lane = lanes && Enum.at(lanes, i)
        row = Deliveries.encode_row(Deliveries.new_row(mod, now, size, env.source_id, lane: lane))

        Deliveries.new_ops(ord, now, seq, i, size, row, lane && elem(lane, 0))
      end)

    archive = if ctx.archive?, do: Deliveries.archive_ops(ord, seq, size), else: []

    hook =
      if rows == [] and archive == [],
        do: [],
        else: [{:put, :hooks, Keys.hook(seq), bin}]

    {hook ++ rows ++ archive ++ dedupe_ops(ctx, source, env, ord, hook != []), kind}
  end

  # The key the hook was admitted under, so a provider retry after recovery is
  # still a duplicate. Only a key that has not expired and is not already there.
  defp dedupe_ops(_ctx, nil, _env, _ord, _written?), do: []
  defp dedupe_ops(_ctx, _source, _env, _ord, false), do: []

  defp dedupe_ops(ctx, %{dedupe: %{ttl_ms: ttl}}, %Envelope{dedupe_key: key} = env, ord, true)
       when is_binary(key) and is_integer(ttl) do
    expires = env.received_at + ttl
    tenant = env.tenant_id || "default"
    u_key = Keys.dedupe(tenant, env.source_id, key)

    cond do
      expires <= ctx.now ->
        []

      Store.get(ctx.instance, :index, u_key) != :not_found ->
        []

      true ->
        [
          {:put, :index, u_key, <<expires::64, env.id::binary>>},
          {:put, :index, Keys.dedupe_expiry(expires, tenant, env.source_id, key), <<>>}
        ] ++ Counters.ops([{ord, :dedupe, 1}])
    end
  end

  defp dedupe_ops(_ctx, _source, _env, _ord, _written?), do: []

  # Ordinals of the chunk's tenants: the stored ones, and new ones for the rest.
  defp resolve_tenants(ctx, state, records) do
    tenants =
      records
      |> Enum.map(&Tenants.normalize(tenant_of(&1)))
      |> Enum.uniq()

    with {:ok, known} <- Tenants.lookup(ctx.instance, tenants) do
      {new, ops, next_ord, refused} =
        Tenants.allocate(
          state.next_ord,
          Enum.reject(tenants, &Map.has_key?(known, &1)),
          ctx.config.tenants.max
        )

      if refused != [] do
        Logger.error(
          "[ankusa] intake replay: tenants over tenants.max not replayed: #{inspect(refused)}"
        )
      end

      {:ok, %{ords: Map.merge(known, new), ops: ops, next_ord: next_ord}}
    end
  end

  # The record header's tenant is for diagnostics; the envelope is authoritative.
  defp tenant_of(%{bin: bin}) do
    case Envelope.from_binary(bin) do
      {:ok, env} -> env.tenant_id
      _ -> nil
    end
  end

  # 5. republish --------------------------------------------------------------

  defp republish(instance, config, opts) do
    dry? = Keyword.get(opts, :dry_run, false)
    watermark = read_u64(instance, Keys.meta("intake_seq")) || 0
    incarnation = incarnation(instance)

    case republish_from(instance, config, watermark + 1, incarnation, dry?, 0, nil) do
      {:ok, count, last} ->
        with {:ok, next_seq} <- Queue.Writer.next_seq(instance),
             :ok <- write_watermark(instance, next_seq - 1, dry?) do
          {:ok, count, last}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp republish_from(instance, config, from_seq, incarnation, dry?, count, last) do
    %{hi: hi} = Keys.family(:hooks)

    with {:ok, rows} <-
           Store.fold(
             instance,
             :hooks,
             {Keys.hook(from_seq), hi},
             [],
             fn key, value, acc ->
               acc = [{:binary.decode_unsigned(key), value} | acc]
               if length(acc) >= @republish_chunk, do: {:halt, acc}, else: {:cont, acc}
             end
           ) do
      case Enum.reverse(rows) do
        [] ->
          {:ok, count, last}

        rows ->
          records = records_of(rows, incarnation)

          with {:ok, position} <- publish(instance, config, records, dry?) do
            {max_seq, _} = List.last(rows)

            republish_from(
              instance,
              config,
              max_seq + 1,
              incarnation,
              dry?,
              count + length(records),
              position || last
            )
          end
      end
    end
  end

  defp records_of(rows, incarnation) do
    Enum.flat_map(rows, fn {seq, bin} ->
      case Envelope.from_binary(bin) do
        {:ok, env} ->
          [
            {seq, bin,
             %{
               id: env.id,
               tenant: Tenants.normalize(env.tenant_id),
               source: env.source_id,
               incarnation: incarnation
             }}
          ]

        {:error, _} ->
          []
      end
    end)
  end

  # Boot waits for the log (the node does not serve without it); a dry run
  # counts and publishes nothing.
  defp publish(_instance, _config, _records, true), do: {:ok, nil}
  defp publish(_instance, _config, [], _dry?), do: {:ok, nil}

  defp publish(instance, config, records, false) do
    case Intake.publish_batch(instance, records) do
      {:ok, position} ->
        {:ok, position}

      {:error, class, retry_after} ->
        Logger.warning("[ankusa] intake republish of local hooks failed (#{class}); retrying")
        Process.sleep(retry_after || config.intake.probe_ms)
        publish(instance, config, records, false)
    end
  end

  defp write_watermark(_instance, _seq, true), do: :ok

  defp write_watermark(instance, seq, false) do
    Store.write(instance, [{:put, :default, Keys.meta("intake_seq"), <<seq::64>>}], sync: true)
  end

  defp finish_offsets(_instance, true, _replayed, _published), do: :ok

  defp finish_offsets(instance, false, replayed, published) do
    t = Pending.table(instance)

    stored = read_u64(instance, Keys.meta("intake_offset"))
    best = Enum.max([stored, replayed, published] |> Enum.reject(&is_nil/1), fn -> nil end)

    if best != nil do
      if best != stored,
        do:
          Store.write(instance, [{:put, :default, Keys.meta("intake_offset"), <<best::64>>}],
            sync: true
          )

      if Pending.exists?(instance), do: Pending.bump_confirmed(t, best)
    end

    :ok
  end

  # ── offline ───────────────────────────────────────────────────────────────

  @doc """
  `mix ankusa.recover`: steps 1-5 against a **stopped** store, by the same code
  as boot. Opens the store (restoring it from the backup first when its
  directory is empty, as boot does), the intake adapter and the source store,
  runs `run/2`, and closes them. `opts` are `run/2`'s; `origin` is derived.
  """
  @spec run_offline(Ankusa.Config.t(), keyword()) :: {:ok, summary()} | {:error, term()}
  def run_offline(config, opts \\ [])

  def run_offline(%Ankusa.Config{wal: :dual} = config, opts) do
    instance = config.instance
    Application.put_env(:ankusa, :autostart, false)
    {:ok, _} = Application.ensure_all_started(:ankusa)
    Ankusa.put_config(config)
    Intake.validate_config!(config)
    start_adapter_app(config)

    store_path = Ankusa.Config.path(config, "store")
    origin = if File.exists?(Path.join(store_path, "CURRENT")), do: :existing, else: :restored
    Process.flag(:trap_exit, true)
    Pending.create(instance)
    Pending.put_meta(Pending.table(instance), :origin, origin)

    {mod, _opts} = config.source_store
    child_opts = [instance: instance]

    adapter =
      Supervisor.start_link([Intake.module(config).child_spec(child_opts)],
        strategy: :one_for_one
      )

    with {:ok, adapter} <- adapter,
         {:ok, store} <- Store.start_link(child_opts),
         {:ok, audit} <- Ankusa.Audit.start_link(child_opts),
         {:ok, sources} <- start_source_store(mod, child_opts) do
      try do
        run(instance, Keyword.put_new(opts, :origin, origin))
      after
        for pid <- [sources, audit, store, adapter], is_pid(pid), do: stop(pid)
      end
    else
      {:error, reason} -> {:error, {:start_failed, reason}}
    end
  end

  def run_offline(%Ankusa.Config{}, _opts), do: {:error, :not_dual}

  # The adapter's own OTP application (brod and its dependencies for Kafka): an
  # offline run starts the instance's processes by hand, not its release.
  defp start_adapter_app(config) do
    with {:ok, app} <- :application.get_application(Intake.module(config)),
         do: Application.ensure_all_started(app)

    :ok
  end

  @doc """
  The options `run_offline/2` takes, from the command line of `mix ankusa.recover`
  and of the server image's `recover` command: `--node`, `--from`
  (`backup` | `earliest` | a position), `--dry-run`, `--allow-gap`. `{:ok, opts}`
  or `{:error, message}`.
  """
  @spec parse_args([String.t()]) :: {:ok, keyword()} | {:error, String.t()}
  def parse_args(args) do
    {parsed, argv, invalid} =
      OptionParser.parse(args,
        strict: [
          instance: :string,
          node: :string,
          from: :string,
          dry_run: :boolean,
          allow_gap: :boolean
        ]
      )

    cond do
      invalid != [] ->
        {:error, "invalid options: #{inspect(invalid)}"}

      argv != [] ->
        {:error, "unexpected arguments: #{inspect(argv)}"}

      true ->
        with {:ok, from} <- parse_from(parsed[:from]) do
          {:ok,
           [from: from]
           |> put_if(:instance, parsed[:instance])
           |> put_if(:node, parsed[:node])
           |> put_if(:dry_run, parsed[:dry_run])
           |> put_if(:allow_gap, parsed[:allow_gap])}
        end
    end
  end

  defp parse_from(nil), do: {:ok, :backup}
  defp parse_from("backup"), do: {:ok, :backup}
  defp parse_from("earliest"), do: {:ok, :earliest}

  defp parse_from(value) do
    case Integer.parse(value) do
      {position, ""} when position >= 0 -> {:ok, position}
      _ -> {:error, "--from must be backup, earliest or a position, got #{inspect(value)}"}
    end
  end

  defp put_if(opts, _key, nil), do: opts
  defp put_if(opts, key, value), do: Keyword.put(opts, key, value)

  @doc "The one line `mix ankusa.recover` and the image's `recover` command print."
  @spec format_summary(summary()) :: String.t()
  def format_summary(summary) do
    "records=#{summary.records} applied=#{summary.applied} present=#{summary.present} " <>
      "collided=#{summary.collided} unroutable=#{summary.unroutable} " <>
      "skipped=#{summary.skipped} republished=#{summary.republished}"
  end

  defp start_source_store(mod, opts) do
    if Code.ensure_loaded?(mod) and function_exported?(mod, :start_link, 1),
      do: mod.start_link(opts),
      else: {:ok, nil}
  end

  defp stop(pid) do
    GenServer.stop(pid)
  catch
    :exit, _ -> :ok
  end

  # ── helpers ───────────────────────────────────────────────────────────────

  defp read_u64(instance, key) do
    case Store.get(instance, :default, key) do
      {:ok, <<n::64>>} -> n
      _ -> nil
    end
  end

  @doc "The current `m:incarnation` id, `\"0\"` for a store that never restored."
  @spec incarnation(atom()) :: String.t()
  def incarnation(instance) do
    case Store.get(instance, :default, Keys.meta("incarnation")) do
      {:ok, json} ->
        with {:ok, [id | _]} <- JSON.decode(json), true <- is_binary(id), do: id, else: (_ -> "0")

      _ ->
        "0"
    end
  end

  defp emit_result(instance, result),
    do:
      Ankusa.Telemetry.emit([:intake, :replay], %{count: 1}, %{instance: instance, result: result})

  defp emit_summary(instance, summary, duration) do
    Ankusa.Telemetry.emit(
      [:intake, :replay_done],
      %{duration: duration, records: summary.records},
      %{
        instance: instance
      }
    )
  end
end
