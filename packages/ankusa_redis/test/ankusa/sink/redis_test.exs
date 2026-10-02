defmodule Ankusa.Sink.RedisTest do
  @moduledoc """
  Requires a live Redis: `docker compose up -d --wait` in this directory.
  `REDIS_URL` (default `redis://localhost:6399`) points elsewhere.
  """

  use ExUnit.Case, async: false

  alias Ankusa.{ClaimCheck, Envelope, UUIDv7}
  alias Ankusa.Sink.Redis

  @url System.get_env("REDIS_URL", "redis://localhost:6399")

  setup do
    instance = :"redis_sink_#{System.unique_integer([:positive])}"
    suffix = System.unique_integer([:positive])

    %{instance: instance, suffix: suffix, channel: "ankusa.test.#{suffix}.hooks"}
  end

  defp envelope(overrides \\ %{}) do
    struct(
      %Envelope{
        id: UUIDv7.generate(),
        source_id: "src",
        tenant_id: "t1",
        received_at: 1_737_500_000_000,
        method: "POST",
        path: "/hooks/src",
        headers: [],
        content_type: "application/json",
        body: ~s({"hello":"world"}),
        size: 17
      },
      overrides
    )
  end

  defp ctx(instance), do: %{instance: instance, source_id: "src", tenant_id: "t1", attempt: 1}

  # Subscribes and waits for the server to confirm, so a publish made after
  # this returns is counted by `PUBLISH` — which is the whole point of the
  # sink's zero-subscriber error.
  defp subscribe!(channels) do
    {:ok, ps} = Redix.PubSub.start_link(@url)
    {:ok, ref} = Redix.PubSub.subscribe(ps, channels, self())

    for _ <- List.wrap(channels) do
      assert_receive {:redix_pubsub, ^ps, ^ref, :subscribed, _}, 2_000
    end

    {ps, ref}
  end

  test "an inline message carries the payload on the channel", %{
    instance: inst,
    channel: channel
  } do
    {ps, ref} = subscribe!(channel)

    env = envelope()
    assert :ok = Redis.deliver(env, ctx(inst), url: @url, channel: channel)

    assert_receive {:redix_pubsub, ^ps, ^ref, :message, %{channel: ^channel, payload: payload}},
                   2_000

    decoded = JSON.decode!(payload)
    assert decoded["v"] == 1
    assert decoded["id"] == env.id
    assert decoded["source_id"] == "src"
    assert decoded["tenant_id"] == "t1"
    assert Base.decode64!(decoded["body_base64"]) == env.body
  end

  test "a publish nobody is subscribed to is an error, not a delivered hook", %{
    instance: inst,
    channel: channel
  } do
    assert {:error, :no_subscribers} =
             Redis.deliver(envelope(), ctx(inst), url: @url, channel: channel)
  end

  test "a fat payload is checked in through ClaimCheck and the message carries a ticket", %{
    instance: inst,
    channel: channel
  } do
    {ps, ref} = subscribe!(channel)

    dir = Path.join(System.tmp_dir!(), "ankusa_#{inst}_#{System.os_time(:microsecond)}")
    on_exit(fn -> File.rm_rf(dir) end)
    Ankusa.put_config(Ankusa.Config.new(instance: inst, data_dir: dir))

    body = :crypto.strong_rand_bytes(20_000)
    env = envelope(%{body: body, size: byte_size(body)})

    assert :ok =
             Redis.deliver(env, ctx(inst), url: @url, channel: channel, inline_max_bytes: 1_000)

    assert_receive {:redix_pubsub, ^ps, ^ref, :message, %{payload: payload}}, 2_000
    decoded = JSON.decode!(payload)
    refute Map.has_key?(decoded, "body_base64")

    assert {:ok, ^body} = ClaimCheck.redeem(inst, decoded["claim"], decoded["sha256"])
  end

  test "channel accepts a static string or a 1-arity function", %{
    instance: inst,
    suffix: suffix
  } do
    fixed = "ankusa.test.#{suffix}.fixed"
    dynamic = "ankusa.test.#{suffix}.dyn.other"

    {ps, ref} = subscribe!([fixed, dynamic])

    assert :ok = Redis.deliver(envelope(), ctx(inst), url: @url, channel: fixed)

    assert :ok =
             Redis.deliver(
               envelope(%{source_id: "other"}),
               ctx(inst),
               url: @url,
               channel: &"ankusa.test.#{suffix}.dyn.#{&1.source_id}"
             )

    channels = for _ <- 1..2, do: receive_message(ps, ref)

    assert channels == [fixed, dynamic]
  end

  test "the sink is not durable: pub/sub keeps no copy" do
    assert Redis.durable?([]) == false
    assert Ankusa.Sink.durable?(Redis, url: @url, channel: "any") == false

    # What that buys: `wal: none` can't ack on this sink's confirm.
    config =
      Ankusa.Config.new(
        instance: :"redis_sink_#{System.unique_integer([:positive])}",
        wal: :none,
        source_store:
          {Ankusa.SourceStore.Static,
           sources: %{"s" => [sinks: [{Redis, url: @url, channel: "any"}]]}}
      )

    assert_raise ArgumentError, ~r/none of its sinks is durable/, fn ->
      Ankusa.Queue.validate_config!(config)
    end
  end

  test "an unreachable server fails fast instead of hanging" do
    {micros, result} =
      :timer.tc(fn ->
        Redis.deliver(
          envelope(),
          ctx(:"redis_sink_#{System.unique_integer([:positive])}"),
          url: "redis://127.0.0.1:1",
          channel: "ankusa.test.unreachable"
        )
      end)

    assert result == {:error, {:connection, :econnrefused}}
    assert micros < 2_000_000
  end

  defp receive_message(ps, ref) do
    assert_receive {:redix_pubsub, ^ps, ^ref, :message, %{channel: channel}}, 2_000
    channel
  end
end
