defmodule Ankusa.Store.Migrations.TenantKeys do
  @moduledoc false

  # Store format 2 -> 3 (TEN-1.b, ADR 0001): due and inflight rows lead with the
  # tenant ordinal, the dead index gains its by-tenant companion and the
  # ordinal, archive obligations carry the ordinal, and the per-tenant counters
  # are seeded.
  #
  # Five phases, each a bounded number of rows per step. The cursor is
  # `<<phase::8, last_old_key::binary>>`, and every step seeks to the key after
  # `last_old_key`, never to the family's start: the keys already moved are
  # tombstones, and reseeking from the start would make the migration quadratic.
  #
  #   1 due (`?d`)       delete it; `?D` row (a retry when its time is after the
  #                      migration began, else a first attempt) and its marker;
  #                      `pending`/`scheduled` and `bytes`
  #   2 inflight (`?f`)  delete it; `?F` row; `inflight`, `bytes`
  #   3 dead (`?x`)      value gains the ordinal; `?X` row; `dead`, `bytes`
  #   4 archive (`?a`)   value gains the ordinal; `archive_pending`
  #   5 finish           range-delete the old `?d` and `?f` families
  #
  # The counters are the merge deltas of the batch that moves the rows, the same
  # code path as runtime. `Ankusa.Store.Migrate` commits a step's ops and its
  # cursor in one synced batch, so a step either landed (and is never replayed)
  # or did not (nothing of it applied): resuming cannot count a row twice. Rows
  # already in the new shape (a dead or archive row of a store that was migrated
  # before and has since imported more from a 0.3 data dir) are skipped.
  #
  # The tenant of a row is its hook's (`nil` is `"default"`); a hook that is
  # gone or does not decode files its rows under `"default"`, and the Pipeline
  # drops or dead-letters them when they come due, through the counted path.

  @behaviour Ankusa.Store.Migration

  require Logger

  alias Ankusa.Envelope
  alias Ankusa.Queue.Deliveries
  alias Ankusa.Queue.Deliveries.Counters
  alias Ankusa.Queue.Tenants
  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @batch 500

  @impl true
  def describe, do: "re-key due/inflight/dead by tenant ordinal, seed per-tenant counters"

  @impl true
  def step(instance, cursor) do
    {phase, last} =
      case cursor do
        nil -> {1, <<>>}
        <<phase::8, last::binary>> -> {phase, last}
      end

    # The scanned families of format 2 need their end-of-range sentinels; the
    # last step deletes them with the families.
    with :ok <- put_sentinels(instance) do
      run(phase, last, instance)
    end
  end

  defp put_sentinels(instance) do
    Store.write(instance, for({cf, key} <- Keys.legacy_sentinels(), do: {:put, cf, key, <<>>}))
  end

  defp run(5, _last, instance) do
    report_overage(instance)

    {:ok,
     [
       {:delete_range, :index, elem(Keys.range(<<?d>>), 0), elem(Keys.range(<<?d>>), 1)},
       {:delete_range, :index, elem(Keys.range(<<?f>>), 0), elem(Keys.range(<<?f>>), 1)},
       {:delete, :default, Keys.meta("tenant_migration_at")}
     ], :done}
  end

  defp run(phase, last, instance) when phase in 1..4 do
    with {:ok, rows} <- read_rows(instance, family(phase), last),
         {:ok, started, started_ops} <- started_at(instance),
         {:ok, deltas_and_ops, alloc} <- convert(phase, instance, rows, started) do
      {ops, deltas} = deltas_and_ops
      ops = ops ++ alloc ++ started_ops ++ Counters.ops(deltas)

      if length(rows) < @batch do
        {:ok, ops, {:more, <<phase + 1::8>>}}
      else
        {key, _value} = List.last(rows)
        {:ok, ops, {:more, <<phase::8, key::binary>>}}
      end
    end
  end

  defp family(1), do: :due_v2
  defp family(2), do: :inflight_v2
  defp family(3), do: :dead
  defp family(4), do: :archive_pending

  defp read_rows(instance, family, last) do
    %{lo: lo, hi: hi} = Keys.family(family)
    from = if last == <<>>, do: lo, else: last <> <<0>>

    with {:ok, {_n, rows}} <-
           Store.fold(instance, family, {from, hi}, {0, []}, fn key, value, {n, acc} ->
             acc = [{key, value} | acc]
             if n + 1 >= @batch, do: {:halt, {n + 1, acc}}, else: {:cont, {n + 1, acc}}
           end) do
      {:ok, Enum.reverse(rows)}
    end
  end

  # When the migration began, kept so a resumed one classifies the same way.
  defp started_at(instance) do
    case Store.get(instance, :default, Keys.meta("tenant_migration_at")) do
      {:ok, <<at::64>>} ->
        {:ok, at, []}

      :not_found ->
        at = System.system_time(:millisecond)
        {:ok, at, [{:put, :default, Keys.meta("tenant_migration_at"), <<at::64>>}]}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ── phases ────────────────────────────────────────────────────────────────

  defp convert(1, instance, rows, started) do
    keyed = for {key, value} <- rows, {at, seq, sink} = Keys.decode_due_v2(key), do: {key, value, seq, sink, at}

    with {:ok, ords, alloc} <- ordinals(instance, Enum.map(keyed, &elem(&1, 2))) do
      {ops, deltas} =
        Enum.reduce(keyed, {[], []}, fn {key, value, seq, sink, at}, {ops, deltas} ->
          size = size_of(value)
          ord = Map.fetch!(ords, seq)
          kind = if at > started, do: 1, else: 0
          field = if kind == 1, do: :scheduled, else: :pending

          {[{:delete, :index, key} | Deliveries.due_ops(ord, at, seq, sink, size, kind)] ++ ops,
           [{ord, field, 1}, {ord, :bytes, size} | deltas]}
        end)

      {:ok, {ops, deltas}, alloc}
    end
  end

  defp convert(2, instance, rows, _started) do
    keyed = for {key, value} <- rows, {seq, sink} = Keys.decode_inflight_v2(key), do: {key, value, seq, sink}

    with {:ok, ords, alloc} <- ordinals(instance, Enum.map(keyed, &elem(&1, 2))) do
      {ops, deltas} =
        Enum.reduce(keyed, {[], []}, fn {key, value, seq, sink}, {ops, deltas} ->
          size = size_of(value)
          ord = Map.fetch!(ords, seq)

          {[
             {:delete, :index, key},
             Deliveries.inflight_put(ord, seq, sink, size) | ops
           ], [{ord, :inflight, 1}, {ord, :bytes, size} | deltas]}
        end)

      {:ok, {ops, deltas}, alloc}
    end
  end

  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp convert(3, instance, rows, _started) do
    old =
      for {key, value} <- rows,
          {source_id, id} <- [dead_owner(:erlang.binary_to_term(value))],
          {at, seq, sink} = Keys.decode_dead(key),
          do: {key, source_id, id, at, seq, sink}

    with {:ok, ords, alloc} <- ordinals(instance, Enum.map(old, &elem(&1, 4))),
         {:ok, sizes} <- dead_sizes(instance, old) do
      {ops, deltas} =
        old
        |> Enum.zip(sizes)
        |> Enum.reduce({[], []}, fn {{key, source_id, id, at, seq, sink}, size}, {ops, deltas} ->
          ord = Map.fetch!(ords, seq)

          {[
             {:put, :index, key, :erlang.term_to_binary({source_id, id, ord})},
             Deliveries.dead_by_tenant_put(ord, at, seq, sink) | ops
           ], [{ord, :dead, 1}, {ord, :bytes, size} | deltas]}
        end)

      {:ok, {ops, deltas}, alloc}
    end
  end

  defp convert(4, instance, rows, _started) do
    old = for {<<?a, seq::64>> = key, <<size::32>>} <- rows, do: {key, seq, size}

    with {:ok, ords, alloc} <- ordinals(instance, Enum.map(old, &elem(&1, 1))) do
      {ops, deltas} =
        Enum.reduce(old, {[], []}, fn {key, seq, size}, {ops, deltas} ->
          ord = Map.fetch!(ords, seq)
          {[{:put, :index, key, <<size::32, ord::32>>} | ops], [{ord, :archive_pending, 1} | deltas]}
        end)

      {:ok, {ops, deltas}, alloc}
    end
  end

  # A dead row of format 2 holds `{source_id, id}`; one already carrying its
  # ordinal is in the new shape and is left alone.
  defp dead_owner({source_id, id}), do: {source_id, id}
  defp dead_owner(_converted), do: nil

  defp size_of(<<size::32>>), do: size
  defp size_of(_other), do: 0

  # The size of each dead row's delivery row; a row that is gone or does not
  # decode counts nothing.
  defp dead_sizes(instance, old) do
    with {:ok, rows} <-
           Store.multi_get(
             instance,
             :deliveries,
             Enum.map(old, fn {_key, _s, _id, _at, seq, sink} -> Keys.delivery(seq, sink) end)
           ) do
      {:ok,
       Enum.map(rows, fn
         {:ok, bin} ->
           case Deliveries.decode_row(bin) do
             {:ok, %{size: size}} when is_integer(size) -> size
             _ -> 0
           end

         :not_found ->
           0
       end)}
    end
  end

  # ── ordinals ──────────────────────────────────────────────────────────────

  # `seq => ordinal` for the hooks of these rows, and the puts of the ordinals
  # first met here. Allocation is single-threaded (nothing serves yet) and reads
  # `m:next_ord` from the committed state, which is what the step's own ops
  # advance.
  defp ordinals(_instance, []), do: {:ok, %{}, []}

  defp ordinals(instance, seqs) do
    seqs = Enum.uniq(seqs)

    with {:ok, hooks} <- Store.multi_get(instance, :hooks, Enum.map(seqs, &Keys.hook/1)),
         tenant_of = seqs |> Enum.zip(hooks) |> Map.new(fn {seq, hook} -> {seq, tenant(hook)} end),
         tenants = tenant_of |> Map.values() |> Enum.uniq() |> Enum.sort(),
         {:ok, stored} <- Tenants.lookup(instance, tenants),
         {:ok, next} <- Tenants.next_ord(instance) do
      {new, alloc, _next, []} =
        Tenants.allocate(next, Enum.reject(tenants, &Map.has_key?(stored, &1)), nil)

      ords = Map.merge(stored, new)
      {:ok, Map.new(tenant_of, fn {seq, tenant} -> {seq, Map.fetch!(ords, tenant)} end), alloc}
    end
  end

  defp tenant({:ok, bin}) do
    case Envelope.from_binary(bin) do
      {:ok, env} -> Tenants.normalize(env.tenant_id)
      {:error, _reason} -> "default"
    end
  end

  defp tenant(:not_found), do: "default"

  defp report_overage(instance) do
    max = Ankusa.config(instance).tenants.max

    with {:ok, count} <- Tenants.count(instance), true <- count > max do
      Logger.warning(
        "[ankusa] the store holds #{count} tenants, over tenants.max (#{max}): existing tenants " <>
          "keep their rows, new ones are refused"
      )
    end

    :ok
  rescue
    _ -> :ok
  end
end
