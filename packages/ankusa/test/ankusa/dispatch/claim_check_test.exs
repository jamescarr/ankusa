defmodule Ankusa.Dispatch.ClaimCheckTest do
  @moduledoc """
  Dispatch's side of the claim check: bodies checked in once per hook, packs
  shared by every sink of a hook and every retry, and a failed pack falling
  back to a per-hook check-in.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.{ClaimCheck, Envelope, UUIDv7}
  alias Ankusa.Dispatch.Pipeline
  alias Ankusa.Test.CountingBlobStore

  @moduletag capture_log: true

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
    config =
      test_config(
        roles: [:edge, :dispatch],
        storage: %{blob_store: {CountingBlobStore, [pid: self()] ++ store_opts}},
        dispatch:
          Map.merge(
            %{
              retry: {Ankusa.RetryPolicy.Exponential, base_ms: 0, max_attempts: 20, jitter: false}
            },
            dispatch
          ),
        source_store: {Ankusa.SourceStore.Static, sources: %{"src1" => %{sinks: sinks}}}
      )

    start_supervised!({Ankusa.Instance, config})
    config.instance
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

    enqueue!(inst, env)
  end

  defp puts do
    receive do
      {:blob_put, key} -> [key | puts()]
    after
      0 -> []
    end
  end

  defp fat, do: :crypto.strong_rand_bytes(1_000)

  defp redeem(inst, %{ref: ref, sha256: sha256}), do: ClaimCheck.redeem(inst, ref, sha256)

  # Holds dispatch so a batch of enqueues is committed before anything is
  # claimed: the pipeline packs per store scan, and a wake would otherwise let
  # it claim the first hook before the rest are committed.
  defp with_held_dispatch(inst, fun) do
    pid = Ankusa.whereis(inst, :dispatch)
    :ok = :sys.suspend(pid)

    try do
      fun.()
    after
      :ok = :sys.resume(pid)
    end
  end

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
    assert {:ok, _} = Pipeline.tick(inst)

    assert_receive {:delivered, :a, id, claim_a}
    assert_receive {:delivered, :b, ^id, claim_b}
    assert id == env.id
    assert claim_a == claim_b
    assert length(puts()) == 1
    assert {:ok, env.body} == redeem(inst, claim_a)
  end

  test "a batch packs per tenant: three fat hooks across two tenants take two writes" do
    inst = start([{ClaimSink, name: :a, pid: self(), threshold: 100}], [])

    envs =
      with_held_dispatch(inst, fn ->
        [
          append(inst, "acme", fat()),
          append(inst, "globex", fat()),
          append(inst, "acme", fat())
        ]
      end)

    assert {:ok, _} = Pipeline.tick(inst)

    assert length(puts()) == 2

    for env <- envs do
      assert_receive {:delivered, :a, id, claim} when id == env.id
      assert {:ok, env.body} == redeem(inst, claim)
    end
  end

  test "a body under every sink's threshold is never written" do
    inst = start([{ClaimSink, name: :a, pid: self(), threshold: 10_000}], [])

    _env = append(inst, "acme", fat())
    assert {:ok, _} = Pipeline.tick(inst)

    assert_receive {:delivered, :a, _id, nil}
    assert puts() == []
  end

  test "when a pack fails, its hooks still deliver: each checks its body in on its own" do
    {:ok, failures} = Agent.start_link(fn -> 1 end)
    inst = start([{ClaimSink, name: :a, pid: self(), threshold: 100}], failures: failures)

    envs =
      with_held_dispatch(inst, fn ->
        [append(inst, "acme", fat()), append(inst, "acme", fat())]
      end)

    assert {:ok, _} = Pipeline.tick(inst)

    for env <- envs do
      assert_receive {:delivered, :a, id, claim} when id == env.id
      assert {:ok, env.body} == redeem(inst, claim)
    end

    # The failed pack, then one write per hook.
    assert length(puts()) == 3
  end
end
