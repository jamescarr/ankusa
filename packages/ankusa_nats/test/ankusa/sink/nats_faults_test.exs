defmodule Ankusa.Sink.NATSFaultsTest do
  @moduledoc """
  Broker faults (DEL-7): `Ankusa.Sink.NATS` publishes through toxiproxy
  (`docker compose up -d --wait` in this directory starts both), and the server
  is killed, slowed and cut under it. The test's own reads go straight to the
  server (`NATS_SERVERS`, default `localhost:4223`).

  The contract under test: `deliver/3` answers `:ok` only for a message
  JetStream stored, and every other answer is an error dispatch retries; a
  retried publish of a message the first attempt already stored is collapsed by
  `Nats-Msg-Id`, so the stream holds it once. `TOXIPROXY_URL` (default
  `http://127.0.0.1:8477`) and `NATS_PROXY_SERVERS` (default `127.0.0.1:4224`)
  point elsewhere.
  """

  use ExUnit.Case, async: false

  import Ankusa.BrokerFaults, only: [envelope: 0, ctx: 1]

  alias Ankusa.{BrokerFaults, Toxiproxy}
  alias Ankusa.Sink.NATS
  alias Gnat.Jetstream.API.Stream

  @moduletag capture_log: true
  @moduletag timeout: 300_000

  @servers System.get_env("NATS_SERVERS", "localhost:4223") |> String.split(",")
  @proxy_servers System.get_env("NATS_PROXY_SERVERS", "127.0.0.1:4224") |> String.split(",")
  @api System.get_env("TOXIPROXY_URL", "http://127.0.0.1:8477")

  setup_all do
    Toxiproxy.populate!(@api, [%{name: "nats", listen: "0.0.0.0:4224", upstream: "nats:4222"}])
    :ok
  end

  setup do
    Toxiproxy.reset!(@api)
    on_exit(fn -> Toxiproxy.reset!(@api) end)

    suffix = System.unique_integer([:positive])
    stream = "ankusa_faults_#{suffix}"

    # File storage: the kill test needs a stream that outlives the server.
    with_admin(fn admin ->
      {:ok, _info} =
        Stream.create(admin, %Stream{
          name: stream,
          subjects: ["ankusa.faults.#{suffix}.>"],
          storage: :file
        })
    end)

    on_exit(fn -> with_admin(&Stream.delete(&1, stream)) end)

    %{
      instance: :"nats_faults_#{suffix}",
      stream: stream,
      subject: "ankusa.faults.#{suffix}.hooks"
    }
  end

  # The test's own connection, direct to the server and unlinked: it must not
  # take the test down when the server is killed under it.
  defp with_admin(fun) do
    [host_port | _] = @servers
    [host, port] = String.split(host_port, ":")
    {:ok, admin} = Gnat.start_link(%{host: to_charlist(host), port: String.to_integer(port)})
    Process.unlink(admin)

    try do
      fun.(admin)
    after
      Gnat.stop(admin)
    end
  end

  # How many messages the stream holds, read from the server directly.
  defp stored(stream) do
    BrokerFaults.eventually(
      fn ->
        with_admin(fn admin ->
          case Stream.info(admin, stream) do
            {:ok, %{state: %{messages: messages}}} -> messages
            _other -> nil
          end
        end)
      end,
      30_000,
      "the stream to be readable"
    )
  end

  # The stream holds exactly `count` messages, and keeps holding them.
  defp stored?(stream, count) do
    BrokerFaults.eventually(fn -> stored(stream) == count end, 10_000, "#{count} stored messages")
  end

  defp opts(subject, extra \\ []), do: [servers: @proxy_servers, subject: subject] ++ extra

  defp deliver_until_ok!(env, ctx, opts),
    do: BrokerFaults.deliver_until_ok!(fn -> NATS.deliver(env, ctx, opts) end)

  test "a server killed mid-publish fails the delivery retryably, and the retry stores the message once",
       %{instance: inst, stream: stream, subject: subject} do
    opts = opts(subject, publish_timeout_ms: 15_000)
    deliver_until_ok!(envelope(), ctx(inst), opts)
    assert stored(stream) == 1

    # The server stores the message; its publish ack never gets back.
    Toxiproxy.toxic!(@api, "nats", "latency", "downstream", %{latency: 60_000})

    env = envelope()
    task = Task.async(fn -> NATS.deliver(env, ctx(inst), opts) end)
    Process.sleep(500)
    BrokerFaults.kill_and_restart!("nats")

    assert {:error, reason} = Task.await(task, 60_000)
    assert BrokerFaults.transient?(reason), "not retryable: #{inspect(reason)}"

    Toxiproxy.reset!(@api)
    deliver_until_ok!(env, ctx(inst), opts)

    # The warm message survived the kill; the retried one is there once, whether
    # or not the first attempt had been stored before the kill.
    assert stored?(stream, 2)
  end

  test "publish_timeout_ms expiring mid-flight is an error, and the retry is collapsed by Nats-Msg-Id",
       %{instance: inst, stream: stream, subject: subject} do
    opts = opts(subject, publish_timeout_ms: 500)
    deliver_until_ok!(envelope(), ctx(inst), opts)
    assert stored(stream) == 1

    Toxiproxy.toxic!(@api, "nats", "latency", "downstream", %{latency: 2_000})

    env = envelope()
    assert {:error, :publish_timeout} = NATS.deliver(env, ctx(inst), opts)

    # The caller gave up, the server did not: an error never means "not stored".
    assert stored?(stream, 2)

    Toxiproxy.reset!(@api)
    deliver_until_ok!(env, ctx(inst), opts)
    assert stored?(stream, 2)
  end

  test "a JetStream ack lost with the connection is an error, and the retry is collapsed by Nats-Msg-Id",
       %{instance: inst, stream: stream, subject: subject} do
    opts = opts(subject, publish_timeout_ms: 2_000)
    deliver_until_ok!(envelope(), ctx(inst), opts)
    assert stored(stream) == 1

    # The server stores the message; its ack is dropped and the proxy cuts the
    # connection after 500 ms.
    Toxiproxy.toxic!(@api, "nats", "timeout", "downstream", %{timeout: 500})

    env = envelope()
    assert {:error, reason} = NATS.deliver(env, ctx(inst), opts)
    assert BrokerFaults.transient?(reason), "not retryable: #{inspect(reason)}"
    assert stored?(stream, 2)

    Toxiproxy.reset!(@api)
    deliver_until_ok!(env, ctx(inst), opts)
    assert stored?(stream, 2)
  end

  test "latency above the attempt deadline kills the attempt, keeps the row, and delivers once it clears",
       %{stream: stream, subject: subject} do
    inst = BrokerFaults.start_dispatch!({NATS, opts(subject)}, 1_000)

    BrokerFaults.enqueue!(inst, envelope())
    BrokerFaults.await_delivered!(inst, 30_000)
    assert stored(stream) == 1

    Toxiproxy.toxic!(@api, "nats", "latency", "downstream", %{latency: 3_000})

    BrokerFaults.enqueue!(inst, envelope())
    row = BrokerFaults.await_attempt_timeout!(inst, 15_000)
    assert row.row.attempts >= 1

    Toxiproxy.reset!(@api)
    BrokerFaults.await_delivered!(inst, 30_000)

    # Every attempt carried the same Nats-Msg-Id: one message, however many tries.
    assert stored?(stream, 2)
  end
end
