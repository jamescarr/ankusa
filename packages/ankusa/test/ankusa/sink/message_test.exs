defmodule Ankusa.Sink.MessageTest do
  use ExUnit.Case, async: true

  alias Ankusa.{ClaimCheck, Envelope, UUIDv7}
  alias Ankusa.ClaimCheck.Ref
  alias Ankusa.Sink.Message

  setup do
    instance = :"msg_#{System.unique_integer([:positive])}"
    dir = Ankusa.TestHelpers.unique_data_dir(instance)
    Ankusa.put_config(Ankusa.Config.new(instance: instance, data_dir: dir))
    on_exit(fn -> File.rm_rf(dir) end)
    %{ctx: %{instance: instance, source_id: "src", tenant_id: "t1", attempt: 1}}
  end

  defp envelope(body, overrides \\ %{}) do
    struct(
      %Envelope{
        id: UUIDv7.generate(),
        source_id: "src",
        tenant_id: "t1",
        received_at: 1_737_500_000_000,
        method: "POST",
        path: "/hooks/src",
        headers: [],
        content_type: "application/octet-stream",
        body: body,
        size: byte_size(body)
      },
      overrides
    )
  end

  test "a body of exactly inline_max_bytes rides inline", %{ctx: ctx} do
    env = envelope(:binary.copy("a", 100))

    assert {:ok, json} = Message.encode(env, ctx, 100)
    decoded = JSON.decode!(json)

    assert decoded["v"] == 1
    assert decoded["id"] == env.id
    assert decoded["received_at"] == env.received_at
    assert Base.decode64!(decoded["body_base64"]) == env.body
    refute Map.has_key?(decoded, "claim")
  end

  test "one byte over inline_max_bytes carries a claim ref string that redeems to the body",
       %{ctx: ctx} do
    body = :crypto.strong_rand_bytes(101)
    env = envelope(body)

    assert {:ok, json} = Message.encode(env, ctx, 100)
    decoded = JSON.decode!(json)

    assert decoded["v"] == 1
    assert decoded["size"] == 101
    refute Map.has_key?(decoded, "body_base64")

    assert "urn:ankusa:claim:v1:t1:" <> _ = decoded["claim"]
    assert decoded["sha256"] == Base.encode16(:crypto.hash(:sha256, body), case: :lower)
    assert {:ok, ^body} = ClaimCheck.redeem(ctx.instance, decoded["claim"], decoded["sha256"])
  end

  test "each check-in gets a fresh, random pack id dated by check-in time, not receive time",
       %{ctx: ctx} do
    month_ago = System.system_time(:millisecond) - 30 * 86_400_000
    env = %{envelope(:crypto.strong_rand_bytes(101)) | received_at: month_ago}

    assert {:ok, first} = Message.check_in(ctx.instance, env)
    assert {:ok, second} = Message.check_in(ctx.instance, env)
    assert first.ref != second.ref

    today = Date.utc_today() |> Date.to_iso8601()
    assert {:ok, [_, _] = keys} = Ankusa.BlobStore.list(ctx.instance, :claims, "claims/")
    assert Enum.all?(keys, &String.contains?(&1, "/dt=#{today}/"))
  end

  test "a claim dispatch already checked in is used as-is, with no second write", %{ctx: ctx} do
    claim = %{
      ref: %Ref{tenant_id: "t1", claim_id: Ref.claim_id(Ref.new_pack_id(), 3)},
      sha256: String.duplicate("0", 64)
    }

    env = envelope(:crypto.strong_rand_bytes(101))

    assert {:ok, json} = Message.encode(env, Map.put(ctx, :claim, claim), 100)
    decoded = JSON.decode!(json)
    assert {decoded["claim"], decoded["sha256"]} == {Ref.to_string(claim.ref), claim.sha256}
    assert Ankusa.BlobStore.list(ctx.instance, :claims, "claims/") == {:ok, []}
  end

  test "a failed check-in is tagged :claim_check", %{ctx: ctx} do
    env = envelope(:crypto.strong_rand_bytes(101), %{tenant_id: ""})

    assert {:error, {:claim_check, :invalid_tenant}} = Message.encode(env, ctx, 100)
  end

  # ── G5 fields ─────────────────────────────────────────────────────────────

  test "an inline body carries its sha256, dedupe_key, replay_id and headers", %{ctx: ctx} do
    env =
      envelope("hello", %{
        dedupe_key: "evt_1",
        headers: [{"x-github-event", "push"}, {"X-Custom", "v"}]
      })

    assert {:ok, json} =
             Message.encode(
               env,
               Map.merge(ctx, %{replay_id: "rid", forward_headers: :default}),
               100
             )

    decoded = JSON.decode!(json)

    assert decoded["sha256"] ==
             "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

    assert decoded["dedupe_key"] == "evt_1"
    assert decoded["idempotency_key"] == "t1:src:evt_1"
    assert decoded["replay_id"] == "rid"
    assert decoded["headers"] == %{"x-github-event" => "push", "x-custom" => "v"}
  end

  test "absent dedupe_key/replay_id encode as null and headers as {}", %{ctx: ctx} do
    env = envelope("hello")
    assert {:ok, json} = Message.encode(env, ctx, 100)
    decoded = JSON.decode!(json)
    assert decoded["dedupe_key"] == nil
    assert decoded["replay_id"] == nil
    assert decoded["idempotency_key"] == env.id
    assert decoded["headers"] == %{}
  end

  test "forwarded_headers honors default, allowlist and []" do
    env =
      envelope("x", %{
        headers: [
          {"X-GitHub-Event", "push"},
          {"X-Custom", "v"},
          {"Authorization", "Bearer secret"},
          {"X-Ankusa-Whatever", "no"}
        ]
      })

    assert Message.forwarded_headers(env, :default) == %{
             "x-github-event" => "push",
             "x-custom" => "v"
           }

    assert Message.forwarded_headers(env, ["x-github-event"]) == %{"x-github-event" => "push"}
    assert Message.forwarded_headers(env, []) == %{}
  end

  test "repeated headers are joined with \", \" in arrival order" do
    env =
      envelope("x", %{
        headers: [{"x-multi", "a"}, {"x-other", "z"}, {"x-multi", "b"}]
      })

    assert Message.forwarded_headers(env) == %{"x-multi" => "a, b", "x-other" => "z"}
  end

  test "a claim message also carries dedupe_key/replay_id/headers", %{ctx: ctx} do
    env =
      envelope(:crypto.strong_rand_bytes(101), %{
        dedupe_key: "evt_9",
        headers: [{"x-a", "1"}]
      })

    assert {:ok, json} = Message.encode(env, Map.put(ctx, :replay_id, "rid"), 100)
    decoded = JSON.decode!(json)
    assert decoded["dedupe_key"] == "evt_9"
    assert decoded["replay_id"] == "rid"
    assert decoded["idempotency_key"] == "t1:src:evt_9"
    assert decoded["headers"] == %{"x-a" => "1"}
    assert decoded["sha256"] == Base.encode16(:crypto.hash(:sha256, env.body), case: :lower)
  end
end
