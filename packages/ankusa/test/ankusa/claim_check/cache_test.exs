defmodule Ankusa.ClaimCheck.CacheTest do
  @moduledoc """
  `Ankusa.ClaimCheck.Cache`'s memory bound: a small budget, bodies of a known
  weight, and what survives each turnover of the generations.
  """

  use ExUnit.Case, async: false

  alias Ankusa.ClaimCheck.Cache
  alias Ankusa.Config

  # The cache counts an entry as its bytes plus this.
  @overhead 256
  # Four bodies fill one generation (half the budget) exactly.
  @budget 4_000
  @body :crypto.strong_rand_bytes(500 - @overhead)

  setup do
    inst = :"ccc#{System.unique_integer([:positive])}"

    Ankusa.put_config(
      Config.new(instance: inst, roles: [], claim_check: %{read_cache_bytes: @budget})
    )

    pid = start_supervised!({Cache, instance: inst})
    %{inst: inst, pid: pid}
  end

  # A turnover is a cast from the insert that filled the generation: wait for
  # it, so every assertion sees the generations as they are after it.
  defp put(inst, pid, indexes) do
    for i <- indexes do
      :ok = Cache.put_body(inst, "t", "p", i, @body)
      :sys.get_state(pid)
    end
  end

  defp held?(inst, i), do: Cache.body(inst, "t", "p", i) != :error

  # Reads promote (and count), so nothing is read before the drop.
  test "a generation is dropped when the next one fills", ctx do
    put(ctx.inst, ctx.pid, 1..7)
    assert Cache.bytes(ctx.inst) == 3_500

    put(ctx.inst, ctx.pid, [8])
    assert Cache.bytes(ctx.inst) == 2_000
    refute Enum.any?(1..4, &held?(ctx.inst, &1))
    assert Enum.all?(5..8, &held?(ctx.inst, &1))
  end

  test "a body read again moves into the newer generation and outlives its old one", ctx do
    put(ctx.inst, ctx.pid, 1..4)
    assert {:ok, @body} = Cache.body(ctx.inst, "t", "p", 2)

    # 2 took one of the newer generation's four slots, so 7 fills it
    put(ctx.inst, ctx.pid, 5..7)

    refute held?(ctx.inst, 1)
    assert held?(ctx.inst, 2)
    assert held?(ctx.inst, 7)
  end

  test "a full generation takes nothing more until it is turned over", ctx do
    :sys.suspend(ctx.pid)
    for i <- 1..4, do: :ok = Cache.put_body(ctx.inst, "t", "p", i, @body)
    assert Cache.bytes(ctx.inst) == 2_000

    # the turnover is queued behind the suspension: this insert is dropped
    :ok = Cache.put_body(ctx.inst, "t", "p", 5, @body)
    assert Cache.bytes(ctx.inst) == 2_000
    refute held?(ctx.inst, 5)

    :sys.resume(ctx.pid)
    :sys.get_state(ctx.pid)
    :ok = Cache.put_body(ctx.inst, "t", "p", 5, @body)
    assert held?(ctx.inst, 5)
    assert Cache.bytes(ctx.inst) == 2_500
  end

  test "what is held never passes the budget, whether or not a turnover is pending", ctx do
    for i <- 1..200 do
      :ok = Cache.put_body(ctx.inst, "t", "p", i, @body)
      assert Cache.bytes(ctx.inst) <= @budget
    end

    # two more turnovers, waited for, so 1 is gone whatever the race above did
    put(ctx.inst, ctx.pid, 201..208)
    assert Cache.bytes(ctx.inst) in 500..@budget
    refute held?(ctx.inst, 1)
  end

  test "an entry larger than half the budget is never cached", ctx do
    :ok = Cache.put_body(ctx.inst, "t", "p", 0, :crypto.strong_rand_bytes(2_000))
    assert Cache.bytes(ctx.inst) == 0
    refute held?(ctx.inst, 0)
  end

  # The insert that fills a generation asks for the turnover; if its process
  # died first, the owner's own check must notice. Stand in for that death by
  # filling the counter without an insert.
  test "a generation filled without a turnover request is turned over by the check", ctx do
    [{:gens, {_table, bytes}, nil, 2_000}] = :ets.lookup(:"ankusa_claim_cache_#{ctx.inst}", :gens)
    :atomics.put(bytes, 1, 2_000)

    :ok = Cache.put_body(ctx.inst, "t", "p", 1, @body)
    refute held?(ctx.inst, 1)

    send(ctx.pid, :check)
    :sys.get_state(ctx.pid)
    :ok = Cache.put_body(ctx.inst, "t", "p", 2, @body)
    assert held?(ctx.inst, 2)
  end

  test "a pack forgotten while a read was promoting it stays forgotten", ctx do
    pack = %{store: 0, key: "k", prefix: "p", read: 8_231}
    :ok = Cache.put_pack(ctx.inst, "t", "swept", pack)
    put(ctx.inst, ctx.pid, 1..4)

    # The forget lands between the reader's lookup in the older generation and
    # its re-check: the reader's promoted copy must not survive. Interleave by
    # hand: look the pack up (promoting it), forget, then look again.
    assert {:ok, ^pack} = Cache.pack(ctx.inst, "t", "swept")
    :ok = Cache.forget_pack(ctx.inst, "t", "swept")
    assert Cache.pack(ctx.inst, "t", "swept") == :error
  end

  test "forget_pack removes a pack from either generation", ctx do
    pack = %{store: 0, key: "k", prefix: "p", read: 8_231}
    :ok = Cache.put_pack(ctx.inst, "t", "old", pack)
    put(ctx.inst, ctx.pid, 1..4)
    :ok = Cache.put_pack(ctx.inst, "t", "new", pack)

    :ok = Cache.forget_pack(ctx.inst, "t", "old")
    :ok = Cache.forget_pack(ctx.inst, "t", "new")

    assert Cache.pack(ctx.inst, "t", "old") == :error
    assert Cache.pack(ctx.inst, "t", "new") == :error
  end

  test "a cache that isn't running misses, and takes nothing" do
    assert Cache.body(:nope, "t", "p", 0) == :error
    assert Cache.pack(:nope, "t", "p") == :error
    assert Cache.put_body(:nope, "t", "p", 0, "x") == :ok
    assert Cache.forget_pack(:nope, "t", "p") == :ok
    assert Cache.bytes(:nope) == 0
  end
end
