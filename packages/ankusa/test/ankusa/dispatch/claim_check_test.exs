defmodule Ankusa.Dispatch.ClaimCheckTest do
  @moduledoc """
  Dispatch's side of the claim check: bodies checked in once per envelope,
  packed per tenant per WAL read batch, and read-order preserved while packs
  upload.
  """

  use ExUnit.Case, async: false

  alias Ankusa.{ClaimCheck, Config, Envelope, UUIDv7, WAL}
  alias Ankusa.Dispatch.Pipeline
  alias Ankusa.Test.CountingBlobStore

  # A queue-style sink: claims bodies over `:threshold`, reports the ref it was
  # handed, and fails its first `:fail_times` attempts (counted in `:agent`).
  defmodule ClaimSink do
    @behaviour Ankusa.Sink

    @impl true
    def inline_max_bytes(opts), do: Keyword.fetch!(opts, :threshold)

    @impl true
    def deliver(env, ctx, opts) do
      failures_left =
        case Keyword.get(opts, :agent) do
          nil -> 0
          agent -> Agent.get_and_update(agent, fn n -> {n, max(n - 1, 0)} end)
        end

      if failures_left > 0 do
        {:error, :transient}
      else
        send(
          Keyword.fetch!(opts, :pid),
          {:delivered, Keyword.fetch!(opts, :name), env.id, ctx[:claim]}
        )

        :ok
      end
    end
  end

  defp start(sinks, store_opts, dispatch \\ %{}) do
    inst = :"dcc#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "ankusa_#{inst}")
    on_exit(fn -> File.rm_rf(dir) end)

    config =
      Config.new(
        instance: inst,
        data_dir: dir,
        roles: [:dispatch],
        storage: %{blob_store: {CountingBlobStore, [pid: self()] ++ store_opts}},
        dispatch: dispatch,
        source_store: {Ankusa.SourceStore.Static, sources: %{"src1" => %{sinks: sinks}}}
      )

    Ankusa.put_config(config)
    start_supervised!({Ankusa.WAL.DiskLog, instance: inst, config: config})
    start_supervised!({Pipeline, instance: inst, config: config, max_sleep_ms: 1})
    inst
  end

  defp append(inst, tenant, body) do
    env = %Envelope{
      id: UUIDv7.generate(),
      source_id: "src1",
      tenant_id: tenant,
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/hooks",
      headers: [],
      content_type: "application/json",
      body: body,
      size: byte_size(body)
    }

    {:ok, [{:committed, _}]} = WAL.append(inst, [%{envelope: env}])
    env
  end

  defp puts do
    receive do
      {:blob_put, key} -> [key | puts()]
    after
      0 -> []
    end
  end

  defp fat, do: :crypto.strong_rand_bytes(1_000)

  test "a fat hook on two claim sinks is written once, even when a sink fails before succeeding" do
    {:ok, agent} = Agent.start_link(fn -> 2 end)

    inst =
      start(
        [
          {ClaimSink, name: :a, pid: self(), threshold: 100, agent: agent},
          {ClaimSink, name: :b, pid: self(), threshold: 100}
        ],
        []
      )

    env = append(inst, "acme", fat())
    assert {:ok, 1} = Pipeline.tick(inst)

    assert_receive {:delivered, :a, id, ref_a}
    assert_receive {:delivered, :b, ^id, ref_b}
    assert id == env.id
    assert ref_a == ref_b
    assert length(puts()) == 1
    assert {:ok, env.body} == ClaimCheck.redeem(inst, ref_a)
  end

  test "a batch packs per tenant: three fat hooks across two tenants take two writes" do
    inst = start([{ClaimSink, name: :a, pid: self(), threshold: 100}], [])

    envs = [
      append(inst, "acme", fat()),
      append(inst, "globex", fat()),
      append(inst, "acme", fat())
    ]

    assert {:ok, 3} = Pipeline.tick(inst)

    assert length(puts()) == 2

    for env <- envs do
      assert_receive {:delivered, :a, id, ref} when id == env.id
      assert {:ok, env.body} == ClaimCheck.redeem(inst, ref)
    end
  end

  test "a body under every sink's threshold is never written" do
    inst = start([{ClaimSink, name: :a, pid: self(), threshold: 10_000}], [])

    _env = append(inst, "acme", fat())
    assert {:ok, 1} = Pipeline.tick(inst)

    assert_receive {:delivered, :a, _id, nil}
    assert puts() == []
  end

  test "when a pack fails, its hooks still deliver: each checks its body in on its own" do
    {:ok, failures} = Agent.start_link(fn -> 1 end)
    inst = start([{ClaimSink, name: :a, pid: self(), threshold: 100}], failures: failures)

    envs = [append(inst, "acme", fat()), append(inst, "acme", fat())]
    assert {:ok, 2} = Pipeline.tick(inst)

    for env <- envs do
      assert_receive {:delivered, :a, id, ref} when id == env.id
      assert {:ok, env.body} == ClaimCheck.redeem(inst, ref)
      assert ref.object_id == env.id
    end

    # The failed pack, then one write per hook.
    assert length(puts()) == 3
  end

  test "a later hook in the same lane never overtakes one whose pack is still uploading" do
    inst =
      start([{ClaimSink, name: :a, pid: self(), threshold: 100}], [put_delay_ms: 200], %{batch: 1})

    first = append(inst, "acme", fat())
    second = append(inst, "acme", "small")
    assert {:ok, 2} = Pipeline.tick(inst)

    assert_receive {:delivered, :a, id1, %Ankusa.ClaimCheck.Ref{}}
    assert_receive {:delivered, :a, id2, nil}
    assert [id1, id2] == [first.id, second.id]
  end
end
