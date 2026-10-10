defmodule Ankusa.Sink.NATSTest do
  @moduledoc """
  Requires a live NATS server with JetStream enabled:
  `docker compose up -d --wait` in this directory.
  `NATS_SERVERS` (default `localhost:4223`) points elsewhere.

  Each test creates its own stream (and deletes it in `on_exit`), so the
  suite never depends on a stream that happens to exist — which is also the
  point of the "no stream covers this subject" test below: the sink never
  creates one.
  """

  use ExUnit.Case, async: false

  alias Ankusa.{ClaimCheck, Envelope, UUIDv7}
  alias Ankusa.Sink.NATS
  alias Gnat.Jetstream.API.Stream

  @servers System.get_env("NATS_SERVERS", "localhost:4223") |> String.split(",")

  setup do
    instance = :"nats_#{System.unique_integer([:positive])}"
    suffix = System.unique_integer([:positive])
    stream = "ankusa_test_#{suffix}"
    subjects = "ankusa.test.#{suffix}.>"

    # A second connection for the test's own admin work (creating the stream,
    # reading it back, subscribing). The sink makes its own.
    {:ok, admin} = Gnat.start_link(admin_settings())

    {:ok, _info} =
      Stream.create(admin, %Stream{name: stream, subjects: [subjects], storage: :memory})

    # Not in the test process: it exits before `on_exit` runs, taking any
    # linked connection with it, and the stream would outlive the test.
    on_exit(fn ->
      {:ok, conn} = Gnat.start_link(admin_settings())
      Stream.delete(conn, stream)
      Gnat.stop(conn)
    end)

    %{
      instance: instance,
      stream: stream,
      subject: "ankusa.test.#{suffix}.hooks",
      suffix: suffix,
      admin: admin
    }
  end

  defp admin_settings do
    [host_port | _] = @servers
    [host, port] = String.split(host_port, ":")

    %{host: to_charlist(host), port: String.to_integer(port)}
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

  defp opts(subject, extra \\ []), do: [servers: @servers, subject: subject] ++ extra

  # The sink connects in the background: the first delivery of an instance
  # waits for the connection, the way a retry would.
  defp deliver(env, ctx, opts, tries \\ 100) do
    case NATS.deliver(env, ctx, opts) do
      {:error, :not_connected} when tries > 0 ->
        Process.sleep(50)
        deliver(env, ctx, opts, tries - 1)

      result ->
        result
    end
  end

  test "an inline message carries the payload, subject, and headers, and is stored", %{
    instance: inst,
    subject: subject,
    stream: stream,
    admin: admin
  } do
    {:ok, _sid} = Gnat.sub(admin, self(), subject)

    env = envelope(%{dedupe_key: "evt_1"})
    assert :ok = deliver(env, Map.put(ctx(inst), :replay_id, "rid"), opts(subject))

    assert_receive {:msg, %{topic: ^subject, body: body, headers: headers}}, 2_000

    assert Map.new(headers) == %{
             "nats-msg-id" => env.id <> ":replay:rid",
             "ankusa_id" => env.id,
             "ankusa_source_id" => "src",
             "ankusa_tenant_id" => "t1",
             "ankusa_idempotency_key" => "t1:src:evt_1",
             "ankusa_message_version" => "1",
             "content_type" => "application/json",
             "ankusa_dedupe_key" => "evt_1",
             "ankusa_replay_id" => "rid"
           }

    decoded = JSON.decode!(body)
    assert decoded["v"] == 1
    assert decoded["id"] == env.id
    assert Base.decode64!(decoded["body_base64"]) == env.body

    # The `:ok` above was JetStream's publish ack, so the message is in the
    # stream — not merely on the wire in front of the subscriber.
    assert {:ok, info} = Stream.info(admin, stream)
    assert info.state.messages == 1
    assert info.state.last_seq == 1
  end

  test "a fat payload is checked in through ClaimCheck and the message carries a ticket", %{
    instance: inst,
    subject: subject,
    admin: admin
  } do
    {:ok, _sid} = Gnat.sub(admin, self(), subject)

    dir = Path.join(System.tmp_dir!(), "ankusa_#{inst}_#{System.os_time(:microsecond)}")
    on_exit(fn -> File.rm_rf(dir) end)
    Ankusa.put_config(Ankusa.Config.new(instance: inst, data_dir: dir))

    body = :crypto.strong_rand_bytes(20_000)
    env = envelope(%{body: body, size: byte_size(body)})

    assert :ok = deliver(env, ctx(inst), opts(subject, inline_max_bytes: 1_000))

    assert_receive {:msg, %{body: payload}}, 2_000
    decoded = JSON.decode!(payload)
    refute Map.has_key?(decoded, "body_base64")

    assert {:ok, ^body} = ClaimCheck.redeem(inst, decoded["claim"], decoded["sha256"])
  end

  test "subject accepts a static string or a 1-arity function", %{
    instance: inst,
    suffix: suffix,
    admin: admin
  } do
    {:ok, _sid} = Gnat.sub(admin, self(), "ankusa.test.#{suffix}.>")

    assert :ok = deliver(envelope(), ctx(inst), opts("ankusa.test.#{suffix}.fixed"))

    assert :ok =
             deliver(
               envelope(%{source_id: "other"}),
               ctx(inst),
               opts(&"ankusa.test.#{suffix}.dyn.#{&1.source_id}")
             )

    assert_receive {:msg, %{topic: first}}, 2_000
    assert_receive {:msg, %{topic: second}}, 2_000
    assert [first, second] == ["ankusa.test.#{suffix}.fixed", "ankusa.test.#{suffix}.dyn.other"]
  end

  test "a subject no stream covers is an error, and no stream is created for it", %{
    instance: inst,
    suffix: suffix,
    admin: admin
  } do
    subject = "ankusa.missing.#{suffix}.hooks"

    assert {:error, :no_stream} = deliver(envelope(), ctx(inst), opts(subject))
    assert {:ok, %{streams: []}} = Stream.list(admin, subject: subject)
  end

  test "a publish the stream refuses is an error, not a stored hook", %{
    instance: inst,
    suffix: suffix,
    admin: admin
  } do
    stream = "ankusa_small_#{suffix}"
    subject = "ankusa.small.#{suffix}.hooks"

    {:ok, _info} =
      Stream.create(admin, %Stream{
        name: stream,
        subjects: [subject],
        max_msg_size: 64,
        storage: :memory
      })

    on_exit(fn ->
      {:ok, conn} = Gnat.start_link(admin_settings())
      Stream.delete(conn, stream)
      Gnat.stop(conn)
    end)

    # JetStream answers a rejected publish with `"seq": 0` *and* an `"error"`
    # in the same ack, so a sink that only looks for a stream name and a
    # sequence reads this as a success.
    assert {:error, {:jetstream, %{"code" => 400, "description" => description}}} =
             deliver(envelope(), ctx(inst), opts(subject))

    assert description =~ "maximum"

    assert {:ok, info} = Stream.info(admin, stream)
    assert info.state.messages == 0
  end

  test "an unreachable server is :not_connected at once; the handshake never blocks the caller" do
    inst = :"nats_down_#{System.unique_integer([:positive])}"
    opts = [servers: ["127.0.0.1:1"], subject: "whatever", connection_timeout: 1_000]

    {elapsed_us, result} = :timer.tc(fn -> NATS.deliver(envelope(), ctx(inst), opts) end)

    assert {:error, :not_connected} = result
    assert elapsed_us < 100_000

    # Its supervisor keeps trying in the background; asking again stays fast.
    {elapsed_us, result} = :timer.tc(fn -> NATS.deliver(envelope(), ctx(inst), opts) end)
    assert {:error, :not_connected} = result
    assert elapsed_us < 100_000
  end

  test "the connection is registered in the instance's registry, not as a global atom", %{
    instance: inst,
    subject: subject
  } do
    assert :ok = deliver(envelope(), ctx(inst), opts(subject))
    assert is_pid(Ankusa.whereis(inst, {:nats_conn, :default}))
    refute Process.whereis(:"ankusa_nats.#{inst}.default")
  end

  test "an idle connection is closed, and the next delivery opens a new one", %{
    instance: inst,
    subject: subject,
    stream: stream,
    admin: admin
  } do
    opts = opts(subject, idle_timeout_ms: 1_000, publish_timeout_ms: 5_000)
    assert :ok = deliver(envelope(), ctx(inst), opts)

    sup = Ankusa.whereis(inst, {:nats_sup, :default})
    conn = Ankusa.whereis(inst, {:nats_conn, :default})
    conn_ref = Process.monitor(conn)
    key = {inst, {:nats_sup, :default}}
    reaper = Ankusa.Sink.NATS.Reaper

    # Floored at the publish timeout plus a second.
    assert [{^key, ^sup, _last, 6_000}] = :ets.lookup(reaper, key)

    true = :ets.update_element(reaper, key, {3, System.monotonic_time(:millisecond) - 6_000})
    send(reaper, :tick)
    :sys.get_state(reaper)

    assert_receive {:DOWN, ^conn_ref, :process, ^conn, _reason}, 5_000
    assert Ankusa.whereis(inst, {:nats_sup, :default}) == nil
    assert Ankusa.whereis(inst, {:nats_conn, :default}) == nil

    assert :ok = deliver(envelope(), ctx(inst), opts)
    assert Ankusa.whereis(inst, {:nats_conn, :default}) not in [nil, conn]
    assert {:ok, %{state: %{messages: 2}}} = Stream.info(admin, stream)
  end
end
