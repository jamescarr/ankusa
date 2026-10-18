defmodule Ankusa.ClaimCheck.SweeperTest do
  use ExUnit.Case, async: false

  alias Ankusa.{ClaimCheck, Config, UUIDv7}
  alias Ankusa.ClaimCheck.{Ref, Sweeper}

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
end
