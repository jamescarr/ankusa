defmodule Ankusa.WAL.RaPropertyTest do
  @moduledoc """
  Level 3: model-based property testing of the Ra WAL.

  Random command scripts are applied to `Ankusa.WAL.Ra.Machine.apply/3` with
  synthetic metas, and after *every* command the whole machine state is compared
  against an independent reference model. This is where every seq, lease, cursor
  and dedup rule lives, and it runs in-process in milliseconds — so it is the
  property that runs everywhere, including at `MAX_RUNS=5000` nightly.

  A failing case prints the actions and ExUnit prints the seed; because the
  interpreter takes every choice (stale token, cursor direction, truncation
  point) from the generated action rather than from `Enum.random/1`, replaying
  the seed reproduces the same script.

  Run count is `MAX_RUNS` (default 200).
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ankusa.Envelope
  alias Ankusa.WAL.Ra.Machine

  @max_runs (System.get_env("MAX_RUNS") || "200") |> String.to_integer()

  @tenants ["acme", "beta", "gamma"]
  @sources ["github", "stripe", "shopify"]
  # Two holders fight over each lease, which is what a standby does to an active
  # node: one acquires, the other steals it once the first stops renewing.
  @holders [:holder_a, :holder_b]
  @ttl 5_000
  # Matches `Ankusa.WAL.Ra.Machine`'s `@batch_retention_ms`: the reply cache
  # drops batches older than this, and the model must prune the same way or a
  # script whose synthetic time advances past it will disagree.
  @batch_retention_ms 600_000
  # Records repeated within and across batches, which is what a provider's
  # retries look like to the log: it appends every copy.

  property "the machine agrees with a reference model after every command" do
    check all(actions <- StreamData.list_of(action(), max_length: 150), max_runs: @max_runs) do
      run(actions)
    end
  end

  property "no seq is handed out twice, whatever the history" do
    check all(actions <- StreamData.list_of(action(), max_length: 150), max_runs: @max_runs) do
      %{machine: state, model: model, by_seq: by_seq} = run(actions)

      # Every seq the machine reported is distinct (enforced as the replies came
      # back), the live set is a subset of it, and the machine's bookkeeping
      # agrees with the model's.
      assert :gb_trees.size(state.entries) == map_size(model.committed)
      assert Enum.all?(:gb_trees.keys(state.entries), &Map.has_key?(by_seq, &1))
      assert state.next_seq > state.floor
      assert state.bytes == Enum.sum(for {_seq, {_pos, size}} <- model.committed, do: size)
    end
  end

  # ── the interpreter ───────────────────────────────────────────────────────

  defp run(actions) do
    {machine, model, by_seq, _meta} =
      Enum.reduce_while(actions, {new_machine(), new_model(), %{}, meta(0, 0)}, fn {action, dt},
                                                                                   {machine,
                                                                                    model, by_seq,
                                                                                    meta} ->
        # Time advances by a generated step — 0 to 2× the lease TTL — so leases
        # expire between commands and the fence checks see both sides of the
        # boundary, not just a world where nothing ever lapses.
        meta = meta(meta.index + 1, meta.system_time + dt)
        {:cont, step(action, {machine, model, by_seq, meta})}
      end)

    assert agree(machine, model)
    %{machine: machine, model: model, by_seq: by_seq}
  end

  defp step({:append, tenant, count, batch_id, size}, {machine, model, by_seq, meta}) do
    records = records(tenant, count, size, meta.index)
    # A retry of the same `batch_id` returns the stored results by design, so
    # its seqs are expected to be ones the machine already reported. A *reused*
    # id with a different record count is refused outright.
    fresh? = not Map.has_key?(model.batches, batch_id)

    {machine, reply, []} = Machine.apply(meta, {:append, batch_id, records}, machine)

    case model_append(model, batch_id, records, by_seq, meta.system_time) do
      {:ok, results, model, by_seq} ->
        assert reply == {:ok, results}, "append: #{inspect(reply)} vs #{inspect(results)}"
        assert agree(machine, model)
        by_seq = record_seqs(records, results, by_seq, fresh?)
        {machine, model, by_seq, meta}

      {:conflict, model, by_seq} ->
        assert reply == {:error, :batch_id_conflict}
        assert agree(machine, model)
        {machine, model, by_seq, meta}
    end
  end

  defp step({:put_cursor, name, seq_choice, stale?}, {machine, model, by_seq, meta}) do
    {seq, token} = cursor_args(model, name, seq_choice, stale?)
    {expected, model} = model_put_cursor(model, name, seq, token, meta)

    {machine, reply, []} = Machine.apply(meta, {:put_cursor, name, seq, token}, machine)

    assert reply == expected,
           "put_cursor #{inspect(name)} #{seq} token #{inspect(token)}: " <>
             "#{inspect(reply)} vs #{inspect(expected)}"

    assert agree(machine, model)
    {machine, model, by_seq, meta}
  end

  defp step({:truncate, fraction, stale?}, {machine, model, by_seq, meta}) do
    max_seq = model.committed |> Map.keys() |> Enum.max(fn -> 0 end)
    seq = round(max_seq * fraction)
    token = if stale?, do: stale_token(model, :storage), else: live_token(model, :storage)

    {expected, model} = model_truncate(model, seq, token, meta)
    {machine, reply, _effects} = Machine.apply(meta, {:truncate_through, seq, token}, machine)

    assert reply == expected, "truncate #{seq} token #{token}: #{inspect(reply)}"
    assert agree(machine, model)
    {machine, model, by_seq, meta}
  end

  defp step({:acquire, name, holder}, {machine, model, by_seq, meta}) do
    {expected, model} = model_acquire(model, name, holder, @ttl, meta)

    {machine, reply, []} = Machine.apply(meta, {:acquire_lease, name, holder, @ttl}, machine)

    assert reply == expected,
           "acquire #{inspect(name)}: #{inspect(reply)} vs #{inspect(expected)}"

    assert agree(machine, model)
    {machine, model, by_seq, meta}
  end

  defp step({:renew, name}, {machine, model, by_seq, meta}) do
    {holder, token} = live_holder_token(model, name)
    {expected, model} = model_renew(model, name, holder, token, @ttl, meta)

    {machine, reply, []} = Machine.apply(meta, {:renew_lease, name, holder, token, @ttl}, machine)

    assert reply == expected, "renew #{inspect(name)}: #{inspect(reply)}"
    assert agree(machine, model)
    {machine, model, by_seq, meta}
  end

  defp step({:release, name}, {machine, model, by_seq, meta}) do
    {holder, token} = live_holder_token(model, name)
    {expected, model} = model_release(model, name, holder, token)

    {machine, reply, []} = Machine.apply(meta, {:release_lease, name, holder, token}, machine)

    assert reply == expected, "release #{inspect(name)}: #{inspect(reply)}"
    assert agree(machine, model)
    {machine, model, by_seq, meta}
  end

  # Every seq the machine reports must be either brand new (a commit) or one it
  # has already handed out (a memozied retry, or a duplicate). That is the "no
  # seq is reused" rule checked against the machine's *replies*, independently of
  # the model.
  defp record_seqs(records, results, by_seq, fresh?) do
    records
    |> Enum.zip(results)
    |> Enum.reduce(by_seq, fn {envelope, result}, acc ->
      id = Envelope.from_binary(envelope).id

      case result do
        {:committed, seq} when fresh? ->
          refute Map.has_key?(acc, seq), "seq #{seq} was handed out twice (id #{id})"
          Map.put(acc, seq, id)

        {:committed, seq} ->
          assert Map.has_key?(acc, seq),
                 "a retry allocated seq #{seq}, which had never been committed"

          acc
      end
    end)
  end

  # ── the reference model ───────────────────────────────────────────────────

  defp new_model do
    %{
      next_seq: 1,
      committed: %{},
      cursors: %{},
      leases: %{},
      floor: 0,
      bytes: 0,
      # `batch_id` memoisation: a retry of a command whose reply was lost must
      # return the stored results and allocate nothing. The machine prunes this
      # cache by age (`@batch_retention_ms`), so the model does too, with the
      # same FIFO of `{now, batch_id}` timestamps.
      batches: %{},
      batch_order: :queue.new()
    }
  end

  defp new_machine, do: Machine.init(%{})

  defp model_append(model, batch_id, records, by_seq, now) do
    case Map.fetch(model.batches, batch_id) do
      {:ok, results} when length(results) == length(records) ->
        {:ok, results, model, by_seq}

      {:ok, _results} ->
        {:conflict, model, by_seq}

      :error ->
        {results, model, by_seq} = model_commit(model, records, by_seq)

        model = %{
          model
          | batches: Map.put(model.batches, batch_id, results),
            batch_order: :queue.in({now, batch_id}, model.batch_order)
        }

        {:ok, results, prune_batches(model, now), by_seq}
    end
  end

  # The same age-only pruning as the machine: drop the oldest cached replies
  # once they are old enough that a client can no longer be retrying them.
  defp prune_batches(model, now) do
    case :queue.peek(model.batch_order) do
      {:value, {ts, _batch_id}} when now - ts > @batch_retention_ms ->
        case :queue.out(model.batch_order) do
          {{:value, {_ts, batch_id}}, order} ->
            prune_batches(
              %{model | batches: Map.delete(model.batches, batch_id), batch_order: order},
              now
            )

          {:empty, _order} ->
            model
        end

      _ ->
        model
    end
  end

  # The model tracks seqs, cursors, leases and bytes. The *set of seqs the
  # machine has ever reported* is not the model's business — that is checked
  # against the machine's replies (`record_seqs/3`), so the model leaves it
  # alone. Nothing here dedups: the log appends every record it is given, so
  # the model's only job per record is to allocate the next seq.
  defp model_commit(model, records, by_seq) do
    {results, model} =
      records
      |> Enum.with_index(1)
      |> Enum.reduce({[], model}, fn {envelope, pos}, {acc, model} ->
        seq = model.next_seq

        model = %{
          model
          | next_seq: seq + 1,
            committed: Map.put(model.committed, seq, {pos, byte_size(envelope)}),
            bytes: model.bytes + byte_size(envelope)
        }

        {[{:committed, seq} | acc], model}
      end)

    {Enum.reverse(results), model, by_seq}
  end

  defp model_put_cursor(model, name, seq, token, meta) do
    if fenced?(model, Ankusa.WAL.lease_for_cursor(name), token, meta) do
      {{:error, :fenced}, model}
    else
      {:ok, %{model | cursors: Map.update(model.cursors, name, seq, &max(&1, seq))}}
    end
  end

  defp model_truncate(model, seq, token, meta) do
    if fenced?(model, :storage, token, meta) do
      {{:error, :fenced}, model}
    else
      {kept, dropped} = Enum.split_with(model.committed, fn {s, _} -> s > seq end)
      bytes = model.bytes - Enum.sum(Enum.map(dropped, fn {_s, {_pos, size}} -> size end))

      # Same clamp as the machine: the floor is the highest seq ever handed out,
      # so `next_seq` can never be at or below it.
      floor = min(max(model.floor, seq), model.next_seq - 1)

      {:ok, %{model | committed: Map.new(kept), bytes: bytes, floor: floor}}
    end
  end

  defp model_acquire(model, name, holder, ttl, meta) do
    now = meta.system_time

    case Map.get(model.leases, name) do
      %{expires_at: expires_at, holder: existing}
      when is_integer(expires_at) and expires_at >= now and existing != holder ->
        {{:error, {:held, existing}}, model}

      existing ->
        token = ((existing && existing.token) || 0) + 1
        lease = %{holder: holder, token: token, expires_at: now + ttl}
        reply = %{name: name, holder: holder, token: token, ttl_ms: ttl, expires_at: now + ttl}

        {{:ok, reply}, %{model | leases: Map.put(model.leases, name, lease)}}
    end
  end

  defp model_renew(model, name, holder, token, ttl, meta) do
    now = meta.system_time

    case Map.get(model.leases, name) do
      %{holder: ^holder, token: ^token, expires_at: expires_at}
      when is_integer(expires_at) and expires_at >= now ->
        lease = %{holder: holder, token: token, expires_at: now + ttl}
        reply = %{name: name, holder: holder, token: token, ttl_ms: ttl, expires_at: now + ttl}

        {{:ok, reply}, %{model | leases: Map.put(model.leases, name, lease)}}

      _ ->
        {{:error, :lost}, model}
    end
  end

  defp model_release(model, name, holder, token) do
    case Map.get(model.leases, name) do
      %{holder: ^holder, token: ^token} ->
        lease = %{holder: holder, token: token, expires_at: nil}
        {:ok, %{model | leases: Map.put(model.leases, name, lease)}}

      _ ->
        {:ok, model}
    end
  end

  defp fenced?(model, lease_name, token, meta) do
    now = meta.system_time

    case Map.get(model.leases, lease_name) do
      %{token: ^token, expires_at: expires_at}
      when is_integer(expires_at) and expires_at >= now ->
        false

      _ ->
        true
    end
  end

  # ── state comparison ──────────────────────────────────────────────────────

  defp agree(machine, model) do
    machine.next_seq == model.next_seq and
      machine.floor == model.floor and
      machine.bytes == model.bytes and
      machine.cursors == model.cursors and
      machine.leases == model.leases and
      machine_entries(machine) == model.committed and
      machine.batches == model.batches and
      live_consistent?(machine)
  end

  # The machine stores `{raft_index, pos, size}` per live seq and a live count
  # per raft index. `pos` and `size` are what a reader needs to find the record
  # again, so the model tracks those; the raft index is what `live_indexes/1`
  # needs to tell Ra which entries still hold live records, so it is checked by
  # deriving `live` from `entries` and comparing.
  defp machine_entries(machine) do
    machine.entries
    |> :gb_trees.to_list()
    |> Map.new(fn {seq, {_index, pos, size}} -> {seq, {pos, size}} end)
  end

  defp live_consistent?(machine) do
    expected =
      machine.entries
      |> :gb_trees.to_list()
      |> Enum.reduce(%{}, fn {_seq, {index, _pos, _size}}, acc ->
        Map.update(acc, index, 1, &(&1 + 1))
      end)

    Map.new(machine.live) == expected
  end

  # ── generators ────────────────────────────────────────────────────────────

  defp action do
    StreamData.tuple({
      StreamData.one_of([
        append(),
        cursor_write(),
        truncate(),
        acquire(),
        lease_verb(:renew),
        lease_verb(:release)
      ]),
      time_step()
    })
  end

  # 0 to 2× the lease TTL: a lease may lapse between commands, or not, so the
  # fence checks see both sides of the expiry boundary.
  defp time_step, do: StreamData.integer(0..(2 * @ttl))

  defp lease_verb(verb) do
    StreamData.map(StreamData.member_of([:dispatch, :storage, :other]), &{verb, &1})
  end

  defp append do
    StreamData.map(
      {
        StreamData.member_of(@tenants),
        StreamData.integer(1..24),
        # Deliberately repeated payloads: a provider's retry looks exactly like
        # a repeat to the log, and it appends every copy.
        StreamData.integer(0..4),
        # A tiny pool on purpose: reusing a batch id is exactly the retry an
        # interrupted client sends, and it must return the stored results.
        StreamData.integer(1..3)
      },
      fn {tenant, count, size, batch} ->
        {:append, tenant, count, {:batch, batch}, size}
      end
    )
  end

  defp cursor_write do
    StreamData.map(
      {
        StreamData.member_of([:dispatch, :compactor]),
        StreamData.member_of([:above, :at, :below]),
        StreamData.boolean()
      },
      fn {name, choice, stale?} -> {:put_cursor, name, choice, stale?} end
    )
  end

  defp truncate do
    StreamData.map(
      {StreamData.member_of([0.0, 0.25, 0.5, 0.75, 1.0, 1.5]), StreamData.boolean()},
      fn {fraction, stale?} -> {:truncate, fraction, stale?} end
    )
  end

  defp acquire do
    StreamData.map(
      StreamData.tuple(
        {StreamData.member_of([:dispatch, :storage, :other]), StreamData.member_of(@holders)}
      ),
      fn {name, holder} -> {:acquire, name, holder} end
    )
  end

  # ── helpers ───────────────────────────────────────────────────────────────

  defp records(_tenant, count, _size, _index) when count <= 0, do: []

  defp records(tenant, count, size, index) do
    for i <- 1..count do
      # Real envelopes, encoded the way the adapter encodes them: the machine
      # never decodes a payload, but the property has to (to name a record in a
      # failure). `size` repeats across actions, which is what makes two records
      # identical.
      body = :binary.copy("x", size)

      Envelope.to_binary(%Envelope{
        id: "e#{index}-#{i}",
        source_id: Enum.at(@sources, rem(index + i, length(@sources))),
        tenant_id: tenant,
        received_at: 0,
        method: "POST",
        path: "/hooks/x",
        headers: [],
        body: body,
        size: byte_size(body)
      })
    end
  end

  defp meta(index, system_time) do
    %{index: index, term: 1, system_time: system_time, machine_version: 1}
  end

  defp live_token(model, name) do
    case Map.get(model.leases, name) do
      %{token: token, expires_at: expires_at} when is_integer(expires_at) -> token
      _ -> 999
    end
  end

  # The holder the lease is currently *live* under, plus its token. A renew or
  # release targets whichever holder owns the lease at run time; with no live
  # lease it targets a default holder and a stale token, so the machine answers
  # `:lost` / a no-op and the model says the same.
  defp live_holder_token(model, name) do
    case Map.get(model.leases, name) do
      %{holder: holder, token: token, expires_at: expires_at} when is_integer(expires_at) ->
        {holder, token}

      _ ->
        {:holder_a, 999}
    end
  end

  defp stale_token(model, name) do
    case Map.get(model.leases, name) do
      %{token: token} -> token + 7
      _ -> 7
    end
  end

  defp cursor_args(model, name, choice, stale?) do
    max_seq = model.committed |> Map.keys() |> Enum.max(fn -> 0 end)
    live = Map.get(model.leases, Ankusa.WAL.lease_for_cursor(name))

    seq =
      case choice do
        :above -> max_seq + 5
        :at -> max_seq
        :below -> max(max_seq - 3, 0)
      end

    token = if stale?, do: ((live && live.token) || 0) + 13, else: live && live.token

    {seq, token}
  end
end
