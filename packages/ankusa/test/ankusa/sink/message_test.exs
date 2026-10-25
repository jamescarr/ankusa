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

  defp list_claims(instance) do
    [{0, mod, opts}] = ClaimCheck.Stores.list(Ankusa.config(instance))
    mod.list(instance, "claims/", opts)
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
    assert {:ok, [_, _] = keys} = list_claims(ctx.instance)
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
    assert list_claims(ctx.instance) == {:ok, []}
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
        verification: %Ankusa.Verification{status: :ok},
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
    assert decoded["verification"] == %{"status" => "ok", "flagged" => false}
  end

  test "a hook accepted after a failed check says so, without the reason", %{ctx: ctx} do
    env =
      envelope("hello", %{
        verification: %Ankusa.Verification{status: :failed, flagged: true, reason: :no_match}
      })

    assert {:ok, json} = Message.encode(env, ctx, 100)
    assert JSON.decode!(json)["verification"] == %{"status" => "failed", "flagged" => true}
    refute json =~ "reason"
    refute json =~ "no_match"
  end

  test "a hook nothing checked encodes as skipped: no verifier, or no ingest", %{ctx: ctx} do
    # `Verifier.None` accepts everything, so its `:ok` says nothing about the sender.
    none = %Ankusa.Verification{status: :ok, provider: Ankusa.Verifier.None}

    for verification <- [none, nil] do
      assert {:ok, json} =
               Message.encode(envelope("hello", %{verification: verification}), ctx, 100)

      assert JSON.decode!(json)["verification"] == %{"status" => "skipped", "flagged" => false}
    end
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

  # ── transport metadata ────────────────────────────────────────────────────

  test "headers/2 always carries the tenant header, empty without a tenant", %{ctx: ctx} do
    env = envelope("x", %{tenant_id: nil})

    assert Message.headers(env, ctx) == [
             {"ankusa_id", env.id},
             {"ankusa_source_id", "src"},
             {"ankusa_tenant_id", ""},
             {"ankusa_message_version", "1"},
             {"ankusa_idempotency_key", Ankusa.Envelope.idempotency_key(env)},
             {"content_type", "application/json"}
           ]
  end

  test "headers/2 appends the dedupe key and the replay id when the hook has them",
       %{ctx: ctx} do
    env = envelope("x", %{dedupe_key: "evt_1"})

    assert Message.headers(env, Map.put(ctx, :replay_id, "rid")) == [
             {"ankusa_id", env.id},
             {"ankusa_source_id", "src"},
             {"ankusa_tenant_id", "t1"},
             {"ankusa_message_version", "1"},
             {"ankusa_idempotency_key", "t1:src:evt_1"},
             {"content_type", "application/json"},
             {"ankusa_dedupe_key", "evt_1"},
             {"ankusa_replay_id", "rid"}
           ]
  end

  test "a hook of an ordered sink carries its lane's key in the message, headers and attributes",
       %{ctx: ctx} do
    env = envelope("x", %{dedupe_key: "evt_1"})
    ordered = Map.merge(ctx, %{ordering_key: "cus_1", lane: "abc"})

    assert {:ok, json} = Message.encode(env, ordered, 100)
    assert JSON.decode!(json)["ordering_key"] == "cus_1"

    assert {:ok, plain} = Message.encode(env, ctx, 100)
    refute Map.has_key?(JSON.decode!(plain), "ordering_key")

    assert List.last(Message.headers(env, ordered)) == {"ankusa_ordering_key", "cus_1"}
    assert {"ankusa_ordering_key", "cus_1"} in Message.attributes(env, ordered)
    refute List.keymember?(Message.headers(env, ctx), "ankusa_ordering_key", 0)
    refute List.keymember?(Message.attributes(env, ctx), "ankusa_ordering_key", 0)
  end

  describe "origin" do
    test "a message names its instance and, with none configured, a null region", %{ctx: ctx} do
      assert {:ok, json} = Message.encode(envelope("x"), ctx, 100)
      decoded = JSON.decode!(json)

      assert decoded["instance"] == Atom.to_string(ctx.instance)
      assert Map.fetch!(decoded, "region") == nil
    end

    test "the region of the instance's config rides in every message form", %{ctx: ctx} do
      dir = Ankusa.TestHelpers.unique_data_dir(ctx.instance)

      Ankusa.put_config(
        Ankusa.Config.new(
          instance: ctx.instance,
          data_dir: dir,
          dispatch: %{region: "eu-west-1"}
        )
      )

      on_exit(fn -> File.rm_rf(dir) end)

      for inline_max_bytes <- [100, 10] do
        assert {:ok, json} =
                 Message.encode(envelope(String.duplicate("x", 50)), ctx, inline_max_bytes)

        decoded = JSON.decode!(json)

        assert decoded["instance"] == Atom.to_string(ctx.instance)
        assert decoded["region"] == "eu-west-1"
      end
    end

    test "a ctx with no instance (a sink called outside dispatch) has neither field set" do
      assert {:ok, json} = Message.encode(envelope("x"), %{attempt: 1}, 100)
      decoded = JSON.decode!(json)

      assert Map.fetch!(decoded, "instance") == nil
      assert Map.fetch!(decoded, "region") == nil
    end

    test "an instance that never started has no region" do
      ctx = %{instance: :never_started_for_message_test, attempt: 1}
      assert {:ok, json} = Message.encode(envelope("x"), ctx, 100)
      decoded = JSON.decode!(json)

      assert decoded["instance"] == "never_started_for_message_test"
      assert Map.fetch!(decoded, "region") == nil
    end
  end

  describe "trace context" do
    @traceparent "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

    test "an inline message carries traceparent and tracestate, null when the hook has none",
         %{ctx: ctx} do
      traced = envelope("x", %{trace: %{traceparent: @traceparent, tracestate: "a=1"}})
      assert {:ok, json} = Message.encode(traced, ctx, 100)
      assert %{"traceparent" => @traceparent, "tracestate" => "a=1"} = JSON.decode!(json)

      assert {:ok, json} = Message.encode(envelope("x"), ctx, 100)
      decoded = JSON.decode!(json)
      assert Map.fetch!(decoded, "traceparent") == nil
      assert Map.fetch!(decoded, "tracestate") == nil
    end

    test "a claim message carries them too", %{ctx: ctx} do
      env =
        envelope(String.duplicate("x", 50), %{
          trace: %{traceparent: @traceparent, tracestate: nil}
        })

      assert {:ok, json} = Message.encode(env, ctx, 10)
      decoded = JSON.decode!(json)
      assert is_binary(decoded["claim"])
      assert decoded["traceparent"] == @traceparent
      assert Map.fetch!(decoded, "tracestate") == nil
    end

    test "headers/2 and attributes/2 end with the pairs, tracestate only when there is one",
         %{ctx: ctx} do
      with_state = envelope("x", %{trace: %{traceparent: @traceparent, tracestate: "a=1"}})
      without = envelope("x", %{trace: %{traceparent: @traceparent, tracestate: nil}})

      assert Enum.take(Message.headers(with_state, ctx), -2) ==
               [{"traceparent", @traceparent}, {"tracestate", "a=1"}]

      assert Enum.take(Message.attributes(with_state, ctx), -2) ==
               [{"traceparent", @traceparent}, {"tracestate", "a=1"}]

      assert List.last(Message.headers(without, ctx)) == {"traceparent", @traceparent}
      refute List.keymember?(Message.headers(without, ctx), "tracestate", 0)
      refute List.keymember?(Message.attributes(without, ctx), "tracestate", 0)

      plain = envelope("x")
      refute List.keymember?(Message.headers(plain, ctx), "traceparent", 0)
      refute List.keymember?(Message.attributes(plain, ctx), "traceparent", 0)
    end
  end

  test "delivery_id/2 is the hook's id, made distinct per replay", %{ctx: ctx} do
    env = envelope("x")

    assert Message.delivery_id(env, ctx) == env.id
    assert Message.delivery_id(env, Map.put(ctx, :replay_id, "rid")) == env.id <> ":replay:rid"
  end

  test "fits/2 takes exactly max bytes and refuses one more as permanent" do
    assert Message.fits(100, 100) == :ok
    assert Message.fits(101, 100) == {:error, {:permanent, {:message_too_large, 101, 100}}}
  end

  # ── DEL-5: size preflight and forwarded-header caps ───────────────────────

  @doc false
  def report(_event, measurements, meta, pid), do: send(pid, {:dropped, measurements, meta})

  defp attach_dropped do
    handler = "message-dropped-#{System.unique_integer([:positive])}"
    :telemetry.attach(handler, [:ankusa, :sink, :headers_dropped], &__MODULE__.report/4, self())
    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp numbered_headers(count), do: for(n <- 0..(count - 1), do: {"x-h#{n}", "v"})

  test "max_bytes/1 is the sink's :max_message_bytes, 1 MiB by default" do
    assert Message.max_bytes([]) == 1_048_576
    assert Message.max_bytes(max_message_bytes: 5) == 5
    assert Message.default_max_message_bytes() == 1_048_576
  end

  test "a 1 MiB header value is dropped whole and counted; the message stays under 1 MiB",
       %{ctx: ctx} do
    attach_dropped()

    env =
      envelope("hello", %{
        headers: [{"x-small", "ok"}, {"x-huge", String.duplicate("a", 1_048_576)}]
      })

    assert {:ok, json} = Message.encode(env, ctx, 100)
    assert JSON.decode!(json)["headers"] == %{"x-small" => "ok"}
    assert byte_size(json) < 1_048_576

    assert_receive {:dropped, %{count: 1},
                    %{instance: instance, source_id: "src", tenant_id: "t1", reason: :bytes}}

    assert instance == ctx.instance
    refute_receive {:dropped, _, %{instance: ^instance}}
  end

  test "more than max_headers names: the first 64 by arrival are kept, the rest counted" do
    attach_dropped()
    env = envelope("x", %{headers: numbered_headers(70)})

    kept = Message.forwarded_headers(env, %{instance: :cap_test, attempt: 1})

    assert map_size(kept) == 64
    assert Map.has_key?(kept, "x-h0") and Map.has_key?(kept, "x-h63")
    refute Map.has_key?(kept, "x-h64")
    assert_receive {:dropped, %{count: 6}, %{instance: :cap_test, reason: :count}}

    # The bare-mode call is capped by the same defaults, without telemetry.
    assert map_size(Message.forwarded_headers(env)) == 64
    assert map_size(Message.forwarded_headers(env, :default)) == 64
    refute_receive {:dropped, _, %{instance: :cap_test}}
  end

  test "a source's own max_headers and forward_headers_max_bytes apply" do
    attach_dropped()
    env = envelope("x", %{headers: numbered_headers(5)})
    ctx = %{instance: :cap_test, attempt: 1, max_headers: 2}

    assert Message.forwarded_headers(env, ctx) == %{"x-h0" => "v", "x-h1" => "v"}
    assert_receive {:dropped, %{count: 3}, %{reason: :count}}

    # 13 bytes fit in 20, 33 do not, and the 4 after it still do: dropped whole,
    # later smaller headers still considered.
    env =
      envelope("x", %{
        headers: [{"x-a", "1234567890"}, {"x-b", String.duplicate("b", 30)}, {"x-c", "z"}]
      })

    ctx = %{instance: :cap_test, attempt: 1, forward_headers_max_bytes: 20}

    assert Message.forwarded_headers(env, ctx) == %{"x-a" => "1234567890", "x-c" => "z"}
    assert_receive {:dropped, %{count: 1}, %{reason: :bytes}}
  end

  test "headers within both caps emit nothing", %{ctx: ctx} do
    attach_dropped()
    env = envelope("x", %{headers: numbered_headers(64)})

    assert map_size(Message.forwarded_headers(env, ctx)) == 64
    instance = ctx.instance
    refute_receive {:dropped, _, %{instance: ^instance}}
  end

  test "a body over the limit by itself is permanent and writes no claim", %{ctx: ctx} do
    env = envelope(:binary.copy("a", 200))

    assert Message.encode(env, ctx, 1_000, 100) ==
             {:error, {:permanent, {:message_too_large, 200, 100}}}

    assert list_claims(ctx.instance) == {:ok, []}
  end

  test "an inline message over the limit whose body fits is sent as a claim", %{ctx: ctx} do
    body = :crypto.strong_rand_bytes(300)
    env = envelope(body)
    assert {:ok, inline} = Message.encode(env, ctx, 1_000, 1_000_000)
    assert JSON.decode!(inline)["body_base64"]
    max = byte_size(inline) - 1

    assert {:ok, json} = Message.encode(env, ctx, 1_000, max)
    decoded = JSON.decode!(json)

    assert byte_size(json) <= max
    refute Map.has_key?(decoded, "body_base64")
    assert {:ok, ^body} = ClaimCheck.redeem(ctx.instance, decoded["claim"], decoded["sha256"])
  end

  test "a message whose metadata alone is over the limit is permanent, with no claim written",
       %{ctx: ctx} do
    env = envelope(:crypto.strong_rand_bytes(101))

    assert {:error, {:permanent, {:message_too_large, size, 50}}} =
             Message.encode(env, ctx, 100, 50)

    assert size > 50
    assert list_claims(ctx.instance) == {:ok, []}
  end
end
