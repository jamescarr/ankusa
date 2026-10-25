defmodule Ankusa.ClaimCheck.BatchTest do
  # `check_in_batch/2` runs in the dispatch packing task: whatever one pack's
  # upload does, the caller gets a result per item.
  use ExUnit.Case, async: false

  alias Ankusa.{ClaimCheck, Config, UUIDv7}
  alias Ankusa.Test.CountingBlobStore

  setup do
    inst = :"cb#{System.unique_integer([:positive])}"
    dir = Ankusa.TestHelpers.unique_data_dir(inst)
    on_exit(fn -> File.rm_rf(dir) end)

    %Config{} = config = Config.new(instance: inst, data_dir: dir, roles: [])
    Ankusa.put_config(config)
    %{inst: inst, config: config}
  end

  defp use_store(%{config: config}, store_opts) do
    blob_store = {CountingBlobStore, [pid: self()] ++ store_opts}
    Ankusa.put_config(%{config | storage: %{config.storage | blob_store: blob_store}})
  end

  defp item(tenant, body), do: %{id: UUIDv7.generate(), tenant_id: tenant, body: body}

  defp redeem(inst, %{ref: ref, sha256: sha256}), do: ClaimCheck.redeem(inst, ref, sha256)

  test "batch exit handled: an upload that dies fails only its own claims", ctx do
    use_store(ctx, kill_tenant: "globex")
    acme = item("acme", "survives")
    globex = item("globex", "lost")

    # The upload process is killed past every `rescue`; the caller (this process,
    # linked to nothing it could take down) still gets a result per item.
    results = ClaimCheck.check_in_batch(ctx.inst, [acme, globex])

    assert {:ok, claim} = results[acme.id]
    assert {:ok, "survives"} = redeem(ctx.inst, claim)
    assert {:error, {:unavailable, :killed}} = results[globex.id]
  end

  test "batch exit handled: every item of a pack that dies is unavailable", ctx do
    use_store(ctx, kill_tenant: "globex")
    items = for n <- 1..3, do: item("globex", "g#{n}")

    results = ClaimCheck.check_in_batch(ctx.inst, items)

    assert map_size(results) == 3
    for it <- items, do: assert({:error, {:unavailable, :killed}} = results[it.id])
  end

  test "batch exit handled: a store that raises is an unavailable pack, not a crash", ctx do
    use_store(ctx, raise_tenant: "globex")
    acme = item("acme", "survives")
    globex = item("globex", "lost")

    results = ClaimCheck.check_in_batch(ctx.inst, [acme, globex])

    assert {:ok, _claim} = results[acme.id]

    assert {:error, {:unavailable, %RuntimeError{message: "injected raise"}}} =
             results[globex.id]
  end

  describe "limits" do
    test "split/3 returns an item no pack can hold apart, and packs the rest" do
      small = item("acme", "a")
      huge = item("acme", String.duplicate("x", 5_000))

      assert {[[^small]], [^huge]} = ClaimCheck.split([small, huge], 1_000_000, 2_000)
    end

    test "split/3 starts a pack at the cap and keeps order" do
      items = for n <- 1..5, do: item("acme", String.duplicate("x", 1_000 + n))

      # Each claim costs its body plus a few hundred bytes: two fit under 3 KB.
      assert {[[a, b], [c, d], [e]], []} = ClaimCheck.split(items, 3_000)
      assert [a, b, c, d, e] == items
    end

    test "check_in/4 refuses more claims than a pack holds before anything is built", ctx do
      use_store(ctx, [])
      items = for n <- 1..65_001, do: %{id: "claim-#{n}", body: "x"}

      assert {:error, :too_large} = ClaimCheck.check_in(ctx.inst, "acme", items)
      refute_received {:blob_put, _key}
    end
  end
end
