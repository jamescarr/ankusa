defmodule Ankusa.ClaimCheck.Sweeper do
  @moduledoc """
  Retention for `LocalFS`-backed claims, and the upkeep of the claim leases
  (rows `?l` of the node's store) that decide what retention may delete.
  Runs on a node with the `:dispatch` role (the role that writes leases), and
  on a `:storage` node when `claim_check.retention_days` is set;
  `Ankusa.ClaimCheck.validate_config!/1` already rejects that setting against
  any other blob store at boot, so the retention pass only ever runs against
  `BlobStore.LocalFS`.

  **Leases.** A claim pack is leased to every hook whose claim points into it,
  from check-in until the hook is reclaimed (delivered everywhere, or
  dead-lettered and evicted from the DLQ). Reclaiming does not clear the lease
  at once: it runs on for `claim_check.redeem_grace_ms`, because "delivered" to
  a queue sink only means the broker took a message that carries the ref, and
  its consumer redeems it later. Each tick the lease pass reads
  `claim_check.sweep_max_objects` lease rows after a rotating cursor and deletes
  the ones that are over (grace passed) and the ones whose hook no longer
  names that pack (a hook that checked its body in twice, or whose claim row is
  gone), taking `claim_leases` off the counters in the same batch.

  **Retention.** Claim objects live in day partitions,
  `claims/tenant=<t>/dt=<yyyy-mm-dd>/` (or `claims/tenant=<t>/shard=<c>/dt=<yyyy-mm-dd>/`
  under the `:sharded` key layout — both are swept, whatever
  `claim_check.key_layout` says now), in every store of
  `claim_check.blob_stores`. A day strictly before the cutoff
  (`retention_days` ago) holds only packs older than the cutoff by creation,
  but not necessarily unneeded ones, so the sweeper lists such a partition and
  deletes a pack only when no lease names it that is still held or inside its
  grace. A leased pack outlives its date cutoff and is deleted by the first
  tick after its last lease ends. A partition's directory goes once it is
  empty. A name in a partition that is not a pack id (a leftover temporary
  file) is deleted with it. Packs written by another node's store are not
  leased in this one: a `LocalFS` root shared between nodes is only as safe as
  `retention_days` is long.

  **Bounds.** A tick deletes at most `claim_check.sweep_max_objects` packs and at
  most as many lease rows. What it left is the `backlog` of the
  `[:ankusa, :claim_check, :sweep]` event (objects it did not examine in the
  partition it stopped in, `ankusa_claim_check_sweep_backlog`) and
  `partitions_left` (expired partitions it did not reach); while either is
  non-zero the next tick follows in a few seconds instead of after
  `claim_check.sweep_interval_ms`. Listing one partition is a single `readdir`
  of that directory: only deletes, lease reads and store writes are capped.
  A store that cannot be read deletes nothing, so a pack is never swept on a
  lease lookup that failed.

  S3/GCS-backed claims are never covered here; the bucket's lifecycle rule is
  their only cutoff and cannot see leases (`claim_check.bucket_expiry_days`
  declares it, see `docs/claim-check.md`).
  """

  use GenServer

  require Logger

  alias Ankusa.ClaimCheck.{Ref, Stores}
  alias Ankusa.{Config, Store, Telemetry}
  alias Ankusa.Queue.Deliveries
  alias Ankusa.Queue.Deliveries.Counters
  alias Ankusa.Queue.Tenants
  alias Ankusa.Store.Keys

  # Rows or names handled per store call.
  @chunk 1_000
  # While a tick left work behind, the next one follows this soon (ms).
  @catchup_ms 5_000
  @day_ms 86_400_000

  @typedoc "What one tick did, as the `[:ankusa, :claim_check, :sweep]` event reports it."
  @type report :: %{
          deleted: non_neg_integer(),
          scanned: non_neg_integer(),
          packs: non_neg_integer(),
          kept: non_neg_integer(),
          leases_pruned: non_neg_integer(),
          backlog: non_neg_integer(),
          partitions_left: non_neg_integer()
        }

  # ── lifecycle ─────────────────────────────────────────────────────────────

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :claim_check_sweeper))
  end

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc """
  Whether a node with this config runs a sweeper: the `:dispatch` role writes
  the leases it prunes, and `claim_check.retention_days` is what it sweeps.
  """
  @spec run?(Config.t()) :: boolean()
  def run?(%Config{} = config),
    do: config.claim_check.retention_days != nil or Config.role?(config, :dispatch)

  @doc """
  Run one sweep synchronously; returns `{deleted, scanned}` day partitions
  (`deleted` is the partitions whose directory it removed). `now:` (Unix ms)
  replaces the clock, for tests.
  """
  @spec sweep(atom(), keyword()) :: {non_neg_integer(), non_neg_integer()}
  def sweep(instance, opts \\ []) do
    %{deleted: deleted, scanned: scanned} = report(instance, opts)
    {deleted, scanned}
  end

  @doc "Run one sweep synchronously and return everything it did."
  @spec report(atom(), keyword()) :: report()
  def report(instance, opts \\ []),
    do: GenServer.call(Ankusa.via(instance, :claim_check_sweeper), {:sweep, opts}, :infinity)

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    %Config{} = config = Ankusa.config(instance)
    interval = config.claim_check.sweep_interval_ms
    schedule(interval)
    {:ok, %{instance: instance, config: config, interval: interval, cursor: nil}}
  end

  # Crash reports print the state; the config carries the object store's
  # credentials.
  @impl true
  def format_status(%{state: %{config: _} = state} = status),
    do: %{status | state: %{state | config: :redacted}}

  def format_status(status), do: status

  # ── ticks ─────────────────────────────────────────────────────────────────

  @impl true
  def handle_call({:sweep, opts}, _from, state) do
    {report, state} = run_sweep(state, opts)
    {:reply, report, state}
  end

  @impl true
  def handle_info(:tick, state) do
    {report, state} = run_sweep(state, [])

    if report.backlog > 0 or report.partitions_left > 0,
      do: schedule(min(state.interval, @catchup_ms)),
      else: schedule(state.interval)

    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # ── sweep ─────────────────────────────────────────────────────────────────

  defp run_sweep(%{config: config, instance: instance} = state, opts) do
    started = System.monotonic_time()
    now = Keyword.get_lazy(opts, :now, fn -> System.system_time(:millisecond) end)
    cap = config.claim_check.sweep_max_objects

    {leases_pruned, cursor} = prune_leases(instance, now, cap, state.cursor)

    report =
      config
      |> retention_pass(instance, now, cap)
      |> Map.put(:leases_pruned, leases_pruned)

    Telemetry.emit(
      [:claim_check, :sweep],
      Map.put(report, :duration, System.monotonic_time() - started),
      %{instance: instance, deleted: deleted?(report)}
    )

    {report, %{state | cursor: cursor}}
  end

  # Whether the tick removed anything at all: a partition directory, a pack
  # object, or a lease row.
  defp deleted?(report), do: report.deleted + report.packs + report.leases_pruned > 0

  # ── leases ────────────────────────────────────────────────────────────────

  # One bounded pass over the lease rows after `cursor`: `{deleted, next cursor}`
  # (`nil` once the end of the family was reached, so the next pass starts over).
  defp prune_leases(instance, now, cap, cursor) do
    %{lo: lo, hi: hi} = Keys.family(:claim_leases)
    from = if is_nil(cursor), do: lo, else: cursor <> <<0>>

    with {:ok, {rows, more?}} <- lease_rows(instance, {from, hi}, cap),
         {:ok, stale} <- stale_leases(instance, rows, now),
         :ok <- delete_leases(instance, stale) do
      {length(stale), if(more?, do: rows |> List.last() |> elem(0), else: nil)}
    else
      {:error, reason} ->
        Logger.warning("[ankusa] claim lease pass failed: #{inspect(reason)}; retrying next tick")
        {0, cursor}
    end
  end

  defp lease_rows(instance, range, cap) do
    result =
      Store.fold(instance, :claim_leases, range, {0, []}, fn key, value, {n, acc} ->
        acc = [{key, value} | acc]
        if n + 1 >= cap, do: {:halt, {n + 1, acc}}, else: {:cont, {n + 1, acc}}
      end)

    with {:ok, {n, acc}} <- result, do: {:ok, {Enum.reverse(acc), n >= cap}}
  end

  # The lease keys that are over: the grace ones past their expiry, and the held
  # ones whose hook is gone or names another pack.
  defp stale_leases(instance, rows, now) do
    {held, graced} = Enum.split_with(rows, fn {_key, <<expires::64>>} -> expires == 0 end)
    expired = for {key, <<expires::64>>} <- graced, expires <= now, do: key

    with {:ok, stale_held} <- stale_held(instance, Enum.map(held, &elem(&1, 0))) do
      {:ok, expired ++ stale_held}
    end
  end

  defp stale_held(_instance, []), do: {:ok, []}

  defp stale_held(instance, keys) do
    held =
      for key <- keys, {_tenant, _pack, seq} <- [Keys.decode_claim_lease(key)], do: {key, seq}

    with {:ok, claims} <-
           Store.multi_get(instance, :index, Enum.map(held, &Keys.claim(elem(&1, 1)))) do
      verdicts =
        for {{key, seq}, claim} <- Enum.zip(held, claims), do: {key, verdict(key, seq, claim)}

      gone = for {key, :gone} <- verdicts, do: key
      other = for {key, :other_pack} <- verdicts, do: key

      with {:ok, orphans} <- still_held(instance, gone), do: {:ok, other ++ orphans}
    end
  end

  defp verdict(_key, _seq, :not_found), do: :gone

  defp verdict(key, seq, {:ok, bin}) do
    case decode_claim(bin) do
      %{ref: %Ref{}} = claim ->
        if Deliveries.claim_lease_key(claim, seq) == key, do: :live, else: :other_pack

      _ ->
        :other_pack
    end
  end

  # A reclaim deletes the claim row and turns its lease into a grace lease in
  # one batch, so a lease re-read after its claim row was seen missing already
  # shows the grace expiry; only one still held is an orphan.
  defp still_held(_instance, []), do: {:ok, []}

  defp still_held(instance, keys) do
    with {:ok, values} <- Store.multi_get(instance, :index, keys) do
      {:ok, for({key, {:ok, <<0::64>>}} <- Enum.zip(keys, values), do: key)}
    end
  end

  # Delete lease rows and take them off the tenants' and the node's counters,
  # in one batch per chunk.
  defp delete_leases(_instance, []), do: :ok

  defp delete_leases(instance, keys) do
    keys
    |> Enum.chunk_every(@chunk)
    |> Enum.reduce_while(:ok, fn chunk, :ok ->
      tenants =
        for key <- chunk, {tenant, _pack, _seq} <- [Keys.decode_claim_lease(key)], do: tenant

      with {:ok, ords} <- Tenants.lookup(instance, Enum.uniq(tenants)),
           :ok <- Store.write(instance, lease_delete_ops(chunk, tenants, ords), sync: false) do
        {:cont, :ok}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp lease_delete_ops(keys, tenants, ords) do
    {known, unknown} =
      tenants |> Enum.map(&Map.get(ords, &1)) |> Enum.split_with(&is_integer/1)

    deletes = Enum.map(keys, &{:delete, :index, &1})
    deltas = Enum.map(known, &{&1, :claim_leases, -1})

    deletes ++
      Deliveries.coalesce(
        Counters.ops(deltas) ++ Counters.node_ops(claim_leases: -length(unknown))
      )
  end

  # ── retention ─────────────────────────────────────────────────────────────

  defp retention_pass(%{claim_check: %{retention_days: nil}}, _instance, _now, _cap),
    do: %{deleted: 0, scanned: 0, packs: 0, kept: 0, backlog: 0, partitions_left: 0}

  defp retention_pass(config, instance, now, cap) do
    # A day strictly before this date holds only packs created before the cutoff.
    cutoff =
      (now - config.claim_check.retention_days * @day_ms)
      |> DateTime.from_unix!(:millisecond)
      |> DateTime.to_date()

    partitions = partitions(config)

    expired =
      partitions
      |> Enum.filter(fn {_dir, date, _tenant} -> Date.compare(date, cutoff) == :lt end)
      |> Enum.sort_by(fn {dir, date, _tenant} -> {Date.to_erl(date), dir} end)

    acc = %{
      deleted: 0,
      scanned: length(partitions),
      packs: 0,
      kept: 0,
      backlog: 0,
      partitions_left: 0
    }

    sweep_partitions(expired, instance, now, cap, acc)
  end

  defp sweep_partitions([], _instance, _now, _budget, acc), do: acc

  defp sweep_partitions(partitions, _instance, _now, budget, acc) when budget <= 0,
    do: %{acc | partitions_left: length(partitions)}

  defp sweep_partitions([{dir, _date, tenant} | rest], instance, now, budget, acc) do
    case sweep_partition(instance, dir, tenant, now, budget) do
      {:ok, %{packs: packs, kept: kept, left: left, removed?: removed?}} ->
        acc = %{
          acc
          | packs: acc.packs + packs,
            kept: acc.kept + kept,
            deleted: acc.deleted + if(removed?, do: 1, else: 0),
            backlog: acc.backlog + left
        }

        # A partition cut short by the budget ends the tick, the rest is next's.
        if left > 0,
          do: %{acc | partitions_left: length(rest)},
          else: sweep_partitions(rest, instance, now, budget - packs, acc)

      :error ->
        sweep_partitions(rest, instance, now, budget, acc)
    end
  end

  # List the partition and delete its packs that no lease still holds, oldest
  # first, until `budget` are gone. `left` is the names it did not get to.
  defp sweep_partition(instance, dir, tenant, now, budget) do
    case File.ls(dir) do
      {:ok, names} ->
        names |> Enum.sort() |> sweep_names(instance, dir, tenant, now, budget, 0, 0)

      {:error, reason} ->
        Logger.warning(
          "[ankusa] claim retention could not list #{dir} (#{inspect(reason)}); " <>
            "retrying on the next sweep"
        )

        :error
    end
  end

  defp sweep_names([], _instance, dir, _tenant, _now, _budget, packs, kept) do
    {:ok, %{packs: packs, kept: kept, left: 0, removed?: kept == 0 and remove_dir(dir)}}
  end

  defp sweep_names(names, _instance, _dir, _tenant, _now, budget, packs, kept)
       when budget <= 0 do
    {:ok, %{packs: packs, kept: kept, left: length(names), removed?: false}}
  end

  defp sweep_names(names, instance, dir, tenant, now, budget, packs, kept) do
    {chunk, rest} = Enum.split(names, min(@chunk, budget))

    case sweep_chunk(instance, dir, tenant, now, chunk) do
      {:ok, %{packs: n, kept: k}} ->
        sweep_names(rest, instance, dir, tenant, now, budget - n, packs + n, kept + k)

      :error ->
        # Nothing in an unreadable chunk is deleted; the partition stays.
        {:ok, %{packs: packs, kept: kept + length(names), left: 0, removed?: false}}
    end
  end

  defp sweep_chunk(instance, dir, tenant, now, names) do
    {ids, foreign} = Enum.split_with(names, &(Ref.validate_pack_id(&1) == :ok))
    prefixes = Enum.map(ids, &Keys.claim_lease(tenant, &1))

    with {:ok, leased?} <- Store.prefixes_present(instance, :claim_leases, prefixes),
         {:ok, verdicts} <- lease_verdicts(instance, Enum.zip(ids, prefixes), leased?, now) do
      removable = foreign ++ for({id, {:free, _keys}} <- verdicts, do: id)
      gone = Enum.filter(removable, &remove_file(Path.join(dir, &1)))
      gone_set = MapSet.new(gone)

      over =
        for {id, {:free, keys}} <- verdicts, MapSet.member?(gone_set, id), key <- keys, do: key

      with {:error, reason} <- delete_leases(instance, over),
           do: Logger.warning("[ankusa] claim lease delete failed: #{inspect(reason)}")

      {:ok, %{packs: length(gone), kept: length(names) - length(gone)}}
    else
      {:error, reason} ->
        Logger.warning(
          "[ankusa] claim retention could not read the leases of #{dir} " <>
            "(#{inspect(reason)}); deleting nothing, retrying on the next sweep"
        )

        :error
    end
  end

  # `{id, {:free, over_lease_keys}}` for a pack nothing holds any more,
  # `{id, :held}` for one a lease keeps.
  defp lease_verdicts(instance, packs, leased?, now) do
    packs
    |> Enum.zip(leased?)
    |> Enum.reduce_while({:ok, []}, fn {{id, prefix}, present?}, {:ok, acc} ->
      if present? do
        case lease_state(instance, prefix, now) do
          {:ok, verdict} -> {:cont, {:ok, [{id, verdict} | acc]}}
          {:error, _reason} = error -> {:halt, error}
        end
      else
        {:cont, {:ok, [{id, {:free, []}} | acc]}}
      end
    end)
  end

  defp lease_state(instance, prefix, now) do
    result =
      Store.fold(instance, :claim_leases, Keys.range(prefix), [], fn key, value, acc ->
        {:cont, [{key, value} | acc]}
      end)

    with {:ok, rows} <- result do
      if Enum.any?(rows, fn {_key, <<expires::64>>} -> expires == 0 or expires > now end),
        do: {:ok, :held},
        else: {:ok, {:free, Enum.map(rows, &elem(&1, 0))}}
    end
  end

  # One name that cannot be removed (a permission, a busy mount) is logged and
  # skipped; the sweep goes on, and the next tick tries it again.
  # The path comes from operator config or this node's own index, never request input.
  # sobelow_skip ["Traversal.FileModule"]
  defp remove_file(path) do
    case File.rm_rf(path) do
      {:ok, _removed} ->
        true

      {:error, reason, path} ->
        Logger.warning(
          "[ankusa] claim retention could not remove #{path} (#{inspect(reason)}); " <>
            "retrying on the next sweep"
        )

        false
    end
  end

  # The directory of a swept partition, once it holds nothing. `false` when it
  # could not go.
  # The path comes from operator config or this node's own index, never request input.
  # sobelow_skip ["Traversal.FileModule"]
  defp remove_dir(dir) do
    case File.rmdir(dir) do
      :ok ->
        true

      {:error, reason} ->
        Logger.warning(
          "[ankusa] claim retention could not remove #{dir} (#{inspect(reason)}); " <>
            "retrying on the next sweep"
        )

        false
    end
  end

  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp decode_claim(bin), do: :erlang.binary_to_term(bin)

  # Every `claims/tenant=*/dt=*` and `claims/tenant=*/shard=*/dt=*` directory
  # under each claim store's root, with its date and tenant. `validate_config!/1`
  # makes every store LocalFS whenever the retention pass runs. Two stores on one
  # root (or one on the default root spelled out) are one root here, so nothing
  # is scanned or counted twice.
  defp partitions(config) do
    roots =
      for {_index, Ankusa.BlobStore.LocalFS, opts} <- Stores.list(config),
          uniq: true,
          do: Keyword.get(opts, :root) || Config.path(config, "segments")

    for root <- roots,
        glob <- [
          [root, Ref.claims_prefix(), "tenant=*", "dt=*"],
          [root, Ref.claims_prefix(), "tenant=*", "shard=*", "dt=*"]
        ],
        dir <- glob |> Path.join() |> Path.wildcard(),
        File.dir?(dir),
        "dt=" <> date <- [Path.basename(dir)],
        {:ok, date} <- [Date.from_iso8601(date)],
        tenant <- [tenant_of(dir)],
        tenant != nil do
      {dir, date, tenant}
    end
  end

  defp tenant_of(dir) do
    dir
    |> Path.split()
    |> Enum.find_value(fn
      "tenant=" <> tenant -> tenant
      _ -> nil
    end)
  end

  defp schedule(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :tick, interval)
  end

  defp schedule(_), do: :ok
end
