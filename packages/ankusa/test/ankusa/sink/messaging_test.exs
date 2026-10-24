defmodule Ankusa.Sink.MessagingTest do
  use ExUnit.Case, async: true

  alias Ankusa.ClaimCheck.Ref
  alias Ankusa.{Envelope, UUIDv7}
  alias Ankusa.Sink.Messaging

  defmodule Echo do
    use Ankusa.Sink.Messaging

    @impl true
    def publish(payload, env, ctx, opts) do
      send(opts[:to], {:published, payload, env, ctx})
      opts[:reply] || :ok
    end
  end

  setup do
    instance = :"messaging_#{System.unique_integer([:positive])}"
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

  describe "deliver/3" do
    test "hands publish/4 the encoded message with the hook and ctx, and returns its answer",
         %{ctx: ctx} do
      env = envelope("hello")

      assert Echo.deliver(env, ctx, to: self()) == :ok
      assert_receive {:published, payload, ^env, ^ctx}

      decoded = JSON.decode!(payload)
      assert decoded["v"] == 1
      assert decoded["id"] == env.id
      assert Base.decode64!(decoded["body_base64"]) == env.body

      assert Echo.deliver(env, ctx, to: self(), reply: {:error, :nope}) == {:error, :nope}
    end

    test "a body over the sink's :inline_max_bytes is published as its claim", %{ctx: ctx} do
      claim = %{
        ref: %Ref{tenant_id: "t1", claim_id: Ref.claim_id(Ref.new_pack_id(), 3)},
        sha256: String.duplicate("0", 64)
      }

      ctx = Map.put(ctx, :claim, claim)

      assert Echo.deliver(envelope(:crypto.strong_rand_bytes(101)), ctx,
               to: self(),
               inline_max_bytes: 100
             ) == :ok

      assert_receive {:published, payload, _env, _ctx}
      decoded = JSON.decode!(payload)
      assert decoded["claim"] == Ref.to_string(claim.ref)
      refute Map.has_key?(decoded, "body_base64")
    end

    test "a failed claim check-in is the answer, and publish/4 never runs", %{ctx: ctx} do
      env = envelope(:crypto.strong_rand_bytes(101), %{tenant_id: ""})

      assert Echo.deliver(env, ctx, to: self(), inline_max_bytes: 100) ==
               {:error, {:claim_check, :invalid_tenant}}

      refute_receive {:published, _, _, _}
    end
  end

  test "dispatch reads the sink's inline threshold, so it checks large bodies in first" do
    assert Ankusa.Sink.inline_max_bytes(Echo, []) == {:ok, 65_536}
    assert Ankusa.Sink.inline_max_bytes(Echo, inline_max_bytes: 10) == {:ok, 10}
  end

  test "a per-hook option resolves for a hook, and only a string is static" do
    env = envelope("x")
    fun = fn env -> env.source_id end

    assert Messaging.resolve(nil, env) == nil
    assert Messaging.resolve("a", env) == "a"
    assert Messaging.resolve(fun, env) == "src"

    assert Messaging.static("a") == "a"
    assert Messaging.static(fun) == nil
    assert Messaging.static(nil) == nil
  end

  describe "endpoints/1" do
    test "reads \"host:port\" strings and {host, port} tuples, charlist hosts too" do
      assert Messaging.endpoints(["a:1", {"b", 2}, {~c"c", 3}]) == [{"a", 1}, {"b", 2}, {"c", 3}]
    end

    test "reads one comma-separated string, ignoring blanks" do
      assert Messaging.endpoints("a:1, b:2,") == [{"a", 1}, {"b", 2}]
    end

    test "an entry with no port, or no host, raises" do
      assert_raise ArgumentError, ~r/nohost/, fn -> Messaging.endpoints(["nohost"]) end
      assert_raise ArgumentError, fn -> Messaging.endpoints([":9092"]) end
    end
  end
end
