defmodule Ankusa.Sink.RabbitMQ.ConnectionCallbacksTest do
  @moduledoc """
  The connection's GenServer callbacks driven directly with a hand-built state:
  the paths that need a broker to misbehave (a dropped connection, a confirm
  that never comes, a full window) without one. `rabbitmq_test.exs` covers the
  same module against a live RabbitMQ.
  """

  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Ankusa.Sink.RabbitMQ.Connection

  # `init/1` traps exits on the calling process; a task keeps that off the test.
  defp init_state(opts) do
    opts = Keyword.merge([instance: :cb, exchange: "ex"], opts)

    {:ok, state, {:continue, :connect}} =
      Task.async(fn -> Connection.init(opts) end) |> Task.await()

    state
  end

  defp pending_entry(ref) do
    %{
      from: {self(), ref},
      routing_key: "rk",
      payload: "body",
      returned?: false,
      timer: Process.send_after(self(), :unused_timer, 60_000)
    }
  end

  describe "init/1" do
    test "uses the default URL, a configured string, or the result of a fun" do
      assert init_state([]).url == Connection.default_url()
      assert init_state(url: "amqp://h:1").url == "amqp://h:1"
      assert init_state(url: fn -> "amqp://from-fun:2" end).url == "amqp://from-fun:2"
    end

    test "starts disconnected with nothing pending" do
      state = init_state(retry_ms: 7, max_inflight: 3)

      assert %{conn: nil, chan: nil, pending: pending, retry_ms: 7, max_inflight: 3} = state
      assert pending == %{}
    end
  end

  describe "format_status/1" do
    test "drops the password from the URL and the bodies from the pending map" do
      state = %{init_state(url: "amqp://user:secret@host:5672") | pending: %{1 => :a, 2 => :b}}

      assert %{state: shown} = Connection.format_status(%{state: state})
      refute inspect(shown) =~ "secret"
      assert shown.url == "amqp://host:5672"
      assert shown.pending == 2
    end

    test "a URL that is not a string is redacted outright" do
      state = %{init_state([]) | url: :not_a_string}

      assert %{state: %{url: "[redacted]"}} = Connection.format_status(%{state: state})
    end

    test "a status without the connection state passes through" do
      assert Connection.format_status(%{other: 1}) == %{other: 1}
    end
  end

  describe "handle_info/2" do
    test "a DOWN for a process this connection does not watch changes nothing" do
      state = init_state([])

      assert {:noreply, ^state} =
               Connection.handle_info({:DOWN, make_ref(), :process, self(), :boom}, state)
    end

    test "a normal exit is ignored; any other exit stops the connection" do
      state = init_state([])

      assert {:noreply, ^state} = Connection.handle_info({:EXIT, self(), :normal}, state)
      assert {:stop, :killed, ^state} = Connection.handle_info({:EXIT, self(), :killed}, state)
    end

    test "losing the connection answers every waiting publish and schedules a reconnect" do
      ref = make_ref()
      conn_ref = make_ref()
      state = %{init_state(retry_ms: 5) | conn_ref: conn_ref, pending: %{1 => pending_entry(ref)}}

      log =
        capture_log(fn ->
          assert {:noreply, next} =
                   Connection.handle_info({:DOWN, conn_ref, :process, self(), :boom}, state)

          assert %{conn: nil, conn_ref: nil, chan: nil, chan_ref: nil, pending: pending} = next
          assert pending == %{}
        end)

      assert log =~ "connection lost"
      assert_receive {^ref, {:error, :not_connected}}
      assert_receive :connect
    end

    test "a confirm timeout for an unknown publish changes nothing" do
      state = init_state([])

      assert {:noreply, ^state} = Connection.handle_info({:confirm_timeout, 99}, state)
    end

    test "a confirm timeout answers that publish and leaves the others waiting" do
      ref = make_ref()
      other = make_ref()

      state = %{
        init_state([])
        | pending: %{1 => pending_entry(ref), 2 => pending_entry(other)}
      }

      assert {:noreply, next} = Connection.handle_info({:confirm_timeout, 1}, state)

      assert_receive {^ref, {:error, :confirm_timeout}}
      refute_receive {^other, _}
      assert Map.keys(next.pending) == [2]
    end
  end

  describe "handle_call/3 publish" do
    test "a publish whose caller already gave up is expired, never sent" do
      state = init_state([])
      expired = System.monotonic_time(:millisecond) - 1

      assert {:reply, {:error, :expired}, ^state} =
               Connection.handle_call(
                 {:publish, "rk", "body", [], expired},
                 {self(), make_ref()},
                 state
               )
    end

    test "without a channel the publish is not connected" do
      state = init_state([])
      deadline = System.monotonic_time(:millisecond) + 5_000

      assert {:reply, {:error, :not_connected}, ^state} =
               Connection.handle_call(
                 {:publish, "rk", "body", [], deadline},
                 {self(), make_ref()},
                 state
               )
    end

    test "a full window of unconfirmed publishes is busy" do
      state = %{
        init_state(max_inflight: 1)
        | chan: :channel,
          pending: %{1 => pending_entry(make_ref())}
      }

      deadline = System.monotonic_time(:millisecond) + 5_000

      assert {:reply, {:error, :busy}, ^state} =
               Connection.handle_call(
                 {:publish, "rk", "body", [], deadline},
                 {self(), make_ref()},
                 state
               )
    end
  end

  describe "terminate/2" do
    test "answers every waiting publish as not connected" do
      ref = make_ref()
      state = %{init_state([]) | pending: %{1 => pending_entry(ref)}}

      assert :ok = Connection.terminate(:shutdown, state)
      assert_receive {^ref, {:error, :not_connected}}
    end
  end
end
