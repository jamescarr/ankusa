defmodule Ankusa.ClaimCheck.ReaderTest do
  @moduledoc """
  The claim gateway's read path under load: `Ankusa.ClaimCheck.Cache` and
  `Ankusa.ClaimCheck.Reader` started as a `:claim_check` node starts them,
  over one `CountingBlobStore` claim store that reports every ranged read.
  """

  use ExUnit.Case, async: false

  alias Ankusa.{ClaimCheck, Config, UUIDv7}
  alias Ankusa.ClaimCheck.{Cache, Reader, Ref, Stores}
  alias Ankusa.Test.CountingBlobStore

  setup do
    inst = :"ccrd#{System.unique_integer([:positive])}"
    dir = Ankusa.TestHelpers.unique_data_dir(inst)
    on_exit(fn -> File.rm_rf(dir) end)

    Ankusa.put_config(Config.new(instance: inst, data_dir: dir, roles: []))
    %{inst: inst}
  end

  # One claim store on a tmp root, reporting each ranged read as
  # `{:blob_range, 0}`, under `claim_check` merged into the config; then the
  # processes a `:claim_check` node reads through.
  defp start_reader(inst, store_opts \\ [], claim_check \\ %{}) do
    root = Path.join(System.tmp_dir!(), "ankusa_claims_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf(root) end)

    store = {CountingBlobStore, [root: root, notify_range: {self(), 0}] ++ store_opts}
    config = Ankusa.config(inst)

    cc =
      config.claim_check
      |> Map.merge(claim_check)
      |> Map.put(:blob_stores, [store])

    Ankusa.put_config(%{config | claim_check: cc})
    start_supervised!({Stores, instance: inst})
    if cc.read_cache_bytes > 0, do: start_supervised!({Cache, instance: inst})
    start_supervised!({Reader, instance: inst})
    :ok
  end

  # A pack of `bodies` for "acme"; its claim ids in order.
  defp pack(inst, bodies) do
    pack_id = Ref.new_pack_id()
    items = Enum.map(bodies, &%{id: UUIDv7.generate(), body: &1})
    {:ok, _claims} = ClaimCheck.check_in(inst, "acme", items, pack_id: pack_id)
    for index <- 0..(length(bodies) - 1), do: Ref.claim_id(pack_id, index)
  end

  # A claim id of a pack no store has.
  defp missing, do: Ref.claim_id(Ref.new_pack_id(), 0)

  # The store reads since the last call.
  defp ranges(acc \\ []) do
    receive do
      {:blob_range, store} -> ranges([store | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp attach(inst, events) do
    test = self()
    handler = "reader-test-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler,
      events,
      fn event, measurements, meta, _ ->
        if meta.instance == inst, do: send(test, {:telemetry, event, measurements, meta})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp timed(fun) do
    {us, result} = :timer.tc(fun)
    {result, div(us, 1000)}
  end

  test "a pack's index is read once for all its claims, and a claim once", %{inst: inst} do
    start_reader(inst)
    [a, b, c] = pack(inst, ["a", "b", "c"])

    assert {:ok, _location} = ClaimCheck.locate(inst, "acme", a)
    assert ranges() == [0]
    assert {:ok, _location} = ClaimCheck.locate(inst, "acme", c)
    assert ranges() == []

    assert {:ok, "b"} = ClaimCheck.read(inst, "acme", b)
    assert ranges() == [0]
    assert {:ok, "b"} = ClaimCheck.read(inst, "acme", b)
    assert ranges() == []
  end

  test "a claim over read_cache_max_entry_bytes is served, never cached", %{inst: inst} do
    start_reader(inst, [], %{read_cache_max_entry_bytes: 100})
    body = :crypto.strong_rand_bytes(200)
    [id] = pack(inst, [body])

    assert {:ok, ^body} = ClaimCheck.read(inst, "acme", id)
    assert ranges() == [0, 0]
    assert {:ok, ^body} = ClaimCheck.read(inst, "acme", id)
    assert ranges() == [0]
  end

  test "read_cache_bytes: 0 caches nothing", %{inst: inst} do
    start_reader(inst, [], %{read_cache_bytes: 0})
    [id] = pack(inst, ["a"])

    assert {:ok, _location} = ClaimCheck.locate(inst, "acme", id)
    assert ranges() == [0]
    assert {:ok, _location} = ClaimCheck.locate(inst, "acme", id)
    assert ranges() == [0]
  end

  test "concurrent reads of one claim share one index read and one bytes read", %{inst: inst} do
    start_reader(inst, range_delay_ms: 150)
    [id] = pack(inst, ["shared"])

    results =
      1..8
      |> Enum.map(fn _ -> Task.async(fn -> ClaimCheck.read(inst, "acme", id) end) end)
      |> Task.await_many()

    assert results == List.duplicate({:ok, "shared"}, 8)
    assert ranges() == [0, 0]
  end

  test "past the limit reads queue, and past the queue they are shed at once", %{inst: inst} do
    start_reader(inst, [range_delay_ms: 300], %{read_max_inflight: 2, read_queue_max: 1})
    ids = for _ <- 1..4, do: hd(pack(inst, ["x"]))

    results =
      ids
      |> Enum.map(fn id ->
        Task.async(fn -> timed(fn -> ClaimCheck.locate(inst, "acme", id) end) end)
      end)
      |> Task.await_many()

    {shed, served} = Enum.split_with(results, &match?({{:error, _}, _ms}, &1))
    assert [{{:error, {:unavailable, :overloaded}}, ms}] = shed
    assert ms < 100
    assert [{:ok, _}, {:ok, _}, {:ok, _}] = Enum.map(served, &elem(&1, 0))
    assert Reader.stats(inst)[0] == %{limit: 2, inflight: 0, queued: 0}
  end

  test "a throttle halves the limit and never opens the breaker; successes grow it back", %{
    inst: inst
  } do
    {:ok, failures} = Agent.start_link(fn -> 1 end)
    start_reader(inst, [range_failures: failures], %{read_max_inflight: 8})
    attach(inst, [[:ankusa, :claim_check, :store_breaker], [:ankusa, :claim_check, :read_limit]])

    # The store answers 503 SlowDown, and is not asked again at the other
    # key layout within the same read.
    assert {:error, {:unavailable, {:status, 503, "SlowDown"}}} =
             ClaimCheck.locate(inst, "acme", missing())

    assert ranges() == [0]
    assert_received {:telemetry, [:ankusa, :claim_check, :read_limit], %{limit: 4}, %{store: 0}}
    refute_received {:telemetry, [:ankusa, :claim_check, :store_breaker], _, _}

    # a missing pack is an answer: four of them at limit 4 grow it by one
    for _ <- 1..4, do: assert({:error, :not_found} = ClaimCheck.locate(inst, "acme", missing()))
    assert Reader.stats(inst)[0].limit == 5
    assert_received {:telemetry, [:ankusa, :claim_check, :read_limit], %{limit: 5}, %{store: 0}}
  end

  test "a read slower than the deadline answers at it, and still fills the cache", %{inst: inst} do
    start_reader(inst, [range_delay_ms: 400], %{read_deadline_ms: 100})
    [id] = pack(inst, ["late"])

    {result, ms} = timed(fn -> ClaimCheck.locate(inst, "acme", id) end)
    assert result == {:error, {:unavailable, :deadline}}
    assert ms < 250

    Process.sleep(500)
    assert ranges() == [0]
    assert {:ok, %{length: 4}} = ClaimCheck.locate(inst, "acme", id)
    assert ranges() == []
  end

  @tag :capture_log
  test "a fetch that crashes is unavailable to its caller, and the reader carries on", %{
    inst: inst
  } do
    start_reader(inst)
    key = {:bytes, "acme", Ref.new_pack_id(), 0}
    deadline = ClaimCheck.deadline(inst)

    assert {:error, {:unavailable, {:exit, _reason}}} =
             Reader.fetch(inst, key, 0, fn -> raise "boom" end, deadline)

    assert {:ok, 1} = Reader.fetch(inst, key, 0, fn -> {:ok, 1} end, deadline)
  end

  test "every read is counted by where it was answered from", %{inst: inst} do
    start_reader(inst)
    attach(inst, [[:ankusa, :claim_check, :read]])
    [id] = pack(inst, ["counted"])

    assert {:ok, _location} = ClaimCheck.locate(inst, "acme", id)
    assert {:ok, "counted"} = ClaimCheck.read(inst, "acme", id)
    assert {:ok, "counted"} = ClaimCheck.read(inst, "acme", id)

    events =
      for _ <- 1..5 do
        assert_received {:telemetry, [:ankusa, :claim_check, :read], %{duration: _}, meta}
        assert %{result: :ok, store: 0} = meta
        {meta.kind, meta.source}
      end

    assert events == [
             {:index, :store},
             {:index, :cache},
             {:bytes, :store},
             {:index, :cache},
             {:bytes, :cache}
           ]

    refute_received {:telemetry, _event, _measurements, _meta}
  end
end
