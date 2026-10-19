defmodule Ankusa.ClaimCheck.SweeperTest do
  use ExUnit.Case, async: false

  import Ankusa.TestHelpers, only: [folded_counters: 1, stored_counters: 1]

  alias Ankusa.{ClaimCheck, Config, Store, UUIDv7}
  alias Ankusa.ClaimCheck.{Ref, Sweeper}
  alias Ankusa.Queue.{Deliveries, Reclaim, Tenants}
  alias Ankusa.Store.Keys

  setup do
    inst = :"sw#{System.unique_integer([:positive])}"
    dir = Ankusa.TestHelpers.unique_data_dir(inst)
    on_exit(fn -> File.rm_rf(dir) end)

    config =
      Config.new(
        instance: inst,
        data_dir: dir,
        roles: [:storage],
        claim_check: %{retention_days: 7}
      )

    Ankusa.put_config(config)
    start_supervised!({Store, instance: inst})

    # Two tenants with ordinals 1 (acme) and 2 (globex), as the writer hands them out.
    {_ords, tenant_ops, _next, []} = Tenants.allocate(1, ["acme", "globex"], nil)
    :ok = Store.write(inst, tenant_ops, sync: true)
    start_supervised!({Sweeper, instance: inst})

    %{inst: inst}
  end

  # A one-claim pack whose id — and so its dt partition — dates from `ms`.
  defp claim_at(inst, ms, body) do
    pack_id = Ref.pack_id(ms, :crypto.strong_rand_bytes(8))
    id = UUIDv7.generate()
    {:ok, claims} = ClaimCheck.check_in(inst, "acme", [%{id: id, body: body}], pack_id: pack_id)
    claims[id]
  end

  defp redeem(inst, %{ref: ref, sha256: sha256}), do: ClaimCheck.redeem(inst, ref, sha256)

  defp days_ago(n), do: System.system_time(:millisecond) - n * 86_400_000

  test "deletes day partitions past retention_days and keeps newer ones", %{inst: inst} do
    old = claim_at(inst, days_ago(10), "old")
    fresh = claim_at(inst, days_ago(1), "fresh")

    assert {1, 2} = Sweeper.sweep(inst)

    assert {:error, :not_found} = redeem(inst, old)
    assert {:ok, "fresh"} = redeem(inst, fresh)
  end

  test "sweeps shard=<c>/dt= partitions of the sharded key layout too", %{inst: inst} do
    config = Ankusa.config(inst)
    Ankusa.put_config(%{config | claim_check: %{config.claim_check | key_layout: :sharded}})

    old = claim_at(inst, days_ago(10), "old")
    fresh = claim_at(inst, days_ago(1), "fresh")

    root = Config.path(config, "segments")
    assert [_, _] = Path.wildcard(Path.join(root, "claims/tenant=acme/shard=*/dt=*"))

    assert {1, 2} = Sweeper.sweep(inst)

    assert {:error, :not_found} = redeem(inst, old)
    assert {:ok, "fresh"} = redeem(inst, fresh)
  end

  test "sweeps every LocalFS claim store's root, and a root two stores share once", %{inst: inst} do
    config = Ankusa.config(inst)
    default_root = Config.path(config, "segments")

    other_root =
      Path.join(System.tmp_dir!(), "ankusa_claims_#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf(other_root) end)
    stores = fn list -> %{config | claim_check: %{config.claim_check | blob_stores: list}} end

    # :ordered writes to the first store: one old and one fresh pack per root
    Ankusa.put_config(stores.([{Ankusa.BlobStore.LocalFS, []}]))
    old_default = claim_at(inst, days_ago(10), "old default")
    fresh_default = claim_at(inst, days_ago(1), "fresh default")
    Ankusa.put_config(stores.([{Ankusa.BlobStore.LocalFS, root: other_root}]))
    old_other = claim_at(inst, days_ago(10), "old other")
    fresh_other = claim_at(inst, days_ago(1), "fresh other")

    # three stores, two roots: the default root is named once and left
    # implicit once. The sweeper reads its config when it starts.
    Ankusa.put_config(
      stores.([
        {Ankusa.BlobStore.LocalFS, []},
        {Ankusa.BlobStore.LocalFS, root: other_root},
        {Ankusa.BlobStore.LocalFS, root: default_root}
      ])
    )

    :ok = stop_supervised!({Sweeper, inst})
    start_supervised!({Sweeper, instance: inst})

    assert {2, 4} = Sweeper.sweep(inst)

    assert {:error, :not_found} = redeem(inst, old_default)
    assert {:error, :not_found} = redeem(inst, old_other)
    assert {:ok, "fresh default"} = redeem(inst, fresh_default)
    assert {:ok, "fresh other"} = redeem(inst, fresh_other)
  end

  test "keeps a claim for at least retention_days: the partition exactly on the cutoff day stays",
       %{inst: inst} do
    boundary = claim_at(inst, days_ago(7), "boundary")

    assert {0, 1} = Sweeper.sweep(inst)
    assert {:ok, "boundary"} = redeem(inst, boundary)
  end

  test "never touches compaction segments under seg/", %{inst: inst} do
    key = "seg/00000000000000000001-00000000000000000002.seg"
    :ok = Ankusa.BlobStore.put(inst, :segments, key, "segment bytes")
    _old = claim_at(inst, days_ago(30), "old claim")

    assert {1, 1} = Sweeper.sweep(inst)
    assert {:ok, "segment bytes"} = Ankusa.BlobStore.get(inst, :segments, key)
  end

  test "a second sweep with nothing expired deletes nothing", %{inst: inst} do
    _fresh = claim_at(inst, days_ago(0), "x")
    assert {0, 1} = Sweeper.sweep(inst)
    assert {0, 1} = Sweeper.sweep(inst)
  end

  @tag :capture_log
  test "a partition that cannot be removed is skipped, and the sweep goes on", %{inst: inst} do
    {uid, 0} = System.cmd("id", ["-u"])

    if String.trim(uid) == "0" do
      # root ignores directory permissions: nothing to prove here.
      :ok
    else
      stuck = claim_at(inst, days_ago(20), "stuck")
      gone = claim_at(inst, days_ago(10), "gone")

      root = Ankusa.Config.path(Ankusa.config(inst), "segments")

      [stuck_dir] =
        Path.wildcard(Path.join(root, "claims/tenant=acme/dt=*")) |> Enum.sort() |> Enum.take(1)

      # Its contents cannot be listed, so `rm -rf` fails on it.
      File.chmod!(stuck_dir, 0o000)
      on_exit(fn -> File.chmod(stuck_dir, 0o755) end)

      assert {1, 2} = Sweeper.sweep(inst)
      File.chmod!(stuck_dir, 0o755)

      assert {:ok, "stuck"} = redeem(inst, stuck)
      assert {:error, :not_found} = redeem(inst, gone)
    end
  end

  # ── claim leases (DUR-2.c) ───────────────────────────────────────────────

  # A claim whose pack dates from `ms`, leased to the hook `seq` of acme (ordinal
  # 1) the way the dispatch pipeline writes it: claim row, held lease and count.
  defp leased_claim(inst, ms, body, seq) do
    claim = claim_at(inst, ms, body)
    :ok = Store.write(inst, Deliveries.claim_ops(seq, 1, claim), sync: true)
    claim
  end

  defp assert_counters_equal_fold(inst),
    do: assert(stored_counters(inst) == folded_counters(inst))

  defp leases(inst) do
    %{lo: lo, hi: hi} = Keys.family(:claim_leases)

    {:ok, rows} =
      Store.fold(inst, :claim_leases, {lo, hi}, [], fn key, value, acc ->
        {:cont, [{Keys.decode_claim_lease(key), value} | acc]}
      end)

    Enum.reverse(rows)
  end

  describe "claim leases" do
    test "lease blocks sweep: a leased pack outlives its cutoff, and goes after the lease clears",
         %{inst: inst} do
      leased = leased_claim(inst, days_ago(10), "leased", 1)
      free = claim_at(inst, days_ago(10), "free")
      assert {:ok, %{claim_leases: 1}} = Deliveries.Counters.read(inst, 1)

      # Both packs are past the date cutoff; only the unleased one goes.
      assert %{packs: 1, kept: 1, deleted: 0} = Sweeper.report(inst)
      assert {:ok, "leased"} = redeem(inst, leased)
      assert {:error, :not_found} = redeem(inst, free)
      assert [{{"acme", _pack, 1}, <<0::64>>}] = leases(inst)

      # The hook is delivered: reclaiming it turns the lease into a grace lease.
      assert :ok = Reclaim.run(inst, [{1, Keys.cleared(1, 0, 0)}])
      assert :not_found = Store.get(inst, :index, Keys.claim(1))
      assert [{{"acme", _pack, 1}, <<expires::64>>}] = leases(inst)
      assert expires > 0

      # Inside the grace window (24 h by default) the pack is still redeemable...
      now = System.system_time(:millisecond)
      assert %{packs: 0, kept: 1, leases_pruned: 0} = Sweeper.report(inst, now: now)
      assert {:ok, "leased"} = redeem(inst, leased)

      # ...and goes once the grace is over, with its lease and the lease's count.
      later = now + 86_400_000 + 60_000
      assert %{packs: 1, kept: 0, deleted: 1, leases_pruned: 1} = Sweeper.report(inst, now: later)
      assert {:error, :not_found} = redeem(inst, leased)
      assert leases(inst) == []
      assert {:ok, %{claim_leases: 0}} = Deliveries.Counters.read(inst, 0)
      assert_counters_equal_fold(inst)
    end

    test "a lease row is only ever deleted with its count, in one batch", %{inst: inst} do
      for seq <- 1..3, do: leased_claim(inst, days_ago(1), "b#{seq}", seq)
      assert {:ok, %{claim_leases: 3}} = Deliveries.Counters.read(inst, 0)

      assert :ok = Reclaim.run(inst, for(seq <- 1..3, do: {seq, Keys.cleared(seq, 0, 0)}))
      later = System.system_time(:millisecond) + 2 * 86_400_000
      assert %{leases_pruned: 3, packs: 0} = Sweeper.report(inst, now: later)

      assert {:ok, %{claim_leases: 0}} = Deliveries.Counters.read(inst, 0)
      assert {:ok, %{claim_leases: 0}} = Deliveries.Counters.read(inst, 1)
      assert_counters_equal_fold(inst)
    end

    test "a held lease whose hook is gone is pruned, and its pack goes", %{inst: inst} do
      claim = claim_at(inst, days_ago(10), "orphan")

      # A lease with no claim row behind it: nothing reclaimed it.
      ops =
        [{:put, :index, Deliveries.claim_lease_key(claim, 7), <<0::64>>}] ++
          Deliveries.Counters.ops([{1, :claim_leases, 1}])

      :ok = Store.write(inst, ops, sync: true)

      assert %{leases_pruned: 1, packs: 1, deleted: 1} = Sweeper.report(inst)
      assert {:error, :not_found} = redeem(inst, claim)
      assert leases(inst) == []
      assert_counters_equal_fold(inst)
    end

    test "a lease the hook no longer names is pruned; the pack it does name stays", %{inst: inst} do
      first = leased_claim(inst, days_ago(10), "first", 3)
      # The same hook checked its body in again (two sinks, a failed pack): its
      # claim row now names the second pack and the first lease is left behind.
      second = leased_claim(inst, days_ago(10), "second", 3)
      assert {:ok, %{claim_leases: 2}} = Deliveries.Counters.read(inst, 0)

      assert %{leases_pruned: 1, packs: 1, kept: 1} = Sweeper.report(inst)
      assert {:error, :not_found} = redeem(inst, first)
      assert {:ok, "second"} = redeem(inst, second)
      assert [{{"acme", _pack, 3}, <<0::64>>}] = leases(inst)
      assert_counters_equal_fold(inst)
    end

    test "sweep_max_objects caps the deletes of a tick and the rest is its backlog",
         %{inst: inst} do
      config = Ankusa.config(inst)
      Ankusa.put_config(%{config | claim_check: %{config.claim_check | sweep_max_objects: 2}})
      :ok = stop_supervised!({Sweeper, inst})
      start_supervised!({Sweeper, instance: inst})

      packs = for n <- 1..5, do: claim_at(inst, days_ago(10), "p#{n}")

      assert %{packs: 2, backlog: 3, deleted: 0, partitions_left: 0} = Sweeper.report(inst)
      assert %{packs: 2, backlog: 1, deleted: 0} = Sweeper.report(inst)
      assert %{packs: 1, backlog: 0, deleted: 1} = Sweeper.report(inst)

      for claim <- packs, do: assert({:error, :not_found} = redeem(inst, claim))
    end

    @tag :capture_log
    test "a store that cannot be read deletes nothing", %{inst: inst} do
      claim = leased_claim(inst, days_ago(10), "kept", 1)
      :ok = stop_supervised!({Store, inst})

      assert %{packs: 0, deleted: 0} = Sweeper.report(inst)
      assert {:ok, "kept"} = redeem(inst, claim)
    end

    test "run?/1: the dispatch role writes leases, retention_days is what is swept" do
      node = &Config.new(Keyword.merge([instance: :run_p, data_dir: "/tmp/x"], &1))

      assert Sweeper.run?(node.(roles: [:dispatch]))
      assert Sweeper.run?(node.(roles: [:storage], claim_check: %{retention_days: 3}))
      refute Sweeper.run?(node.(roles: [:edge]))
      refute Sweeper.run?(node.(roles: [:storage]))
    end
  end
end
