defmodule Ankusa.ClaimCheck.SweeperTest do
  use ExUnit.Case, async: false

  alias Ankusa.{ClaimCheck, Config, UUIDv7}
  alias Ankusa.ClaimCheck.Sweeper

  setup do
    inst = :"sw#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "ankusa_#{inst}")
    on_exit(fn -> File.rm_rf(dir) end)

    config =
      Config.new(
        instance: inst,
        data_dir: dir,
        roles: [:storage],
        claim_check: %{retention_days: 7}
      )

    Ankusa.put_config(config)
    start_supervised!({Sweeper, instance: inst, config: config})

    %{inst: inst}
  end

  defp id_at(ms), do: UUIDv7.generate(ms)

  test "sweep deletes claims older than retention_days and keeps newer ones", %{inst: inst} do
    now = System.system_time(:millisecond)
    old_ms = now - 10 * 86_400_000
    fresh_ms = now - 1 * 86_400_000

    old_id = id_at(old_ms)
    fresh_id = id_at(fresh_ms)

    {:ok, old_ticket} = ClaimCheck.check_in(inst, "old", %{tenant_id: "acme", id: old_id})
    {:ok, fresh_ticket} = ClaimCheck.check_in(inst, "fresh", %{tenant_id: "acme", id: fresh_id})

    assert {1, 2} = Sweeper.sweep(inst)

    assert {:error, :not_found} = ClaimCheck.redeem(inst, old_ticket)
    assert {:ok, "fresh"} = ClaimCheck.redeem(inst, fresh_ticket)
  end

  test "sweep never touches compaction segments under seg/", %{inst: inst} do
    now = System.system_time(:millisecond)
    old_id = id_at(now - 30 * 86_400_000)

    :ok =
      Ankusa.BlobStore.put(
        inst,
        "seg/00000000000000000001-00000000000000000002.seg",
        "segment bytes"
      )

    {:ok, _ticket} = ClaimCheck.check_in(inst, "old claim", %{tenant_id: "acme", id: old_id})

    assert {1, 1} = Sweeper.sweep(inst)

    assert {:ok, "segment bytes"} =
             Ankusa.BlobStore.get(inst, "seg/00000000000000000001-00000000000000000002.seg")
  end

  test "a second sweep with nothing expired deletes nothing", %{inst: inst} do
    fresh_id = id_at(System.system_time(:millisecond))
    {:ok, _ticket} = ClaimCheck.check_in(inst, "x", %{tenant_id: "acme", id: fresh_id})

    assert {0, 1} = Sweeper.sweep(inst)
  end
end
