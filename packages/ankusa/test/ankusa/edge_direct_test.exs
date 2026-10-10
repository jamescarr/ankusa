defmodule Ankusa.Edge.DirectTest do
  @moduledoc """
  `wal: :none`: the direct ack path. Ingest publishes to the source's sinks in
  the request and answers `201` only once every sink has confirmed; a sink that
  refuses or raises is a `503`, with no internal retry — the provider is the
  retry. The node keeps no local log and runs no batcher.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Edge.Router
  alias Ankusa.Sink

  defmodule CaptureSink do
    @moduledoc "A sink that reports every delivery to a pid, then follows `:outcome`."

    @behaviour Sink

    @impl true
    def deliver(env, ctx, opts) do
      send(Keyword.fetch!(opts, :to), {:delivered, env, ctx})

      case Keyword.get(opts, :sleep_ms) do
        ms when is_integer(ms) and ms > 0 -> Process.sleep(ms)
        _ -> :ok
      end

      case Keyword.get(opts, :outcome, :ok) do
        :ok -> :ok
        :error -> {:error, :nope}
        :bad_return -> :error
        :raise -> raise "sink exploded"
      end
    end

    # Opt-in threshold, so a test can make a body require the claim check.
    @impl true
    def inline_max_bytes(opts), do: Keyword.get(opts, :inline_max_bytes)
  end

  # The threshold callback is user code and raises; `deliver/3` itself is fine.
  defmodule RaisingThresholdSink do
    @moduledoc false
    @behaviour Sink

    @impl true
    def inline_max_bytes(_opts), do: raise("threshold")

    @impl true
    def deliver(_env, _ctx, _opts), do: :ok
  end

  # A blob store whose write misbehaves the way user code can: a
  # `:token_provider` that exits (a `GenServer.call` into a dead process), or a
  # return the behaviour does not allow.
  defmodule MisbehavingBlobStore do
    @moduledoc false
    @behaviour Ankusa.BlobStore

    @impl true
    def put(_instance, _key, _data, opts) do
      case Keyword.fetch!(opts, :mode) do
        :exit -> exit(:token_provider_down)
        :bad_return -> :stored
      end
    end

    @impl true
    def get(_, _, _), do: {:error, :not_found}
    @impl true
    def get_range(_, _, _, _, _), do: {:error, :not_found}
    @impl true
    def delete(_, _, _), do: :ok
    @impl true
    def list(_, _, _), do: {:ok, []}
  end

  defp start_direct(sinks, overrides \\ []) do
    config =
      test_config(
        Keyword.merge(
          [
            roles: [:edge],
            wal: :none,
            source_store: {Ankusa.SourceStore.Static, sources: %{"demo" => [sinks: sinks]}}
          ],
          overrides
        )
      )

    start_supervised!({Ankusa.Instance, config})
    config
  end

  defp route(config, source_id, body) do
    conn = Plug.Test.conn(:post, "/webhooks/#{source_id}", body)
    Router.call(conn, Router.init(instance: config.instance))
  end

  test "answers 201 only after the sink confirmed, with the same envelope the sink saw" do
    config = start_direct([{CaptureSink, [to: self(), outcome: :ok]}])

    conn = route(config, "demo", ~s({"hello":"world"}))

    assert conn.status == 201
    assert %{"status" => "accepted", "id" => id} = JSON.decode!(conn.resp_body)
    # Exactly these two keys on this path too: no seq, no node-local state.
    assert Map.keys(JSON.decode!(conn.resp_body)) == ["id", "status"]

    # `assert_received`, not `assert_receive`: the delivery was already in the
    # mailbox when the response came back, so the sink ran before the ack.
    assert_received {:delivered, env, ctx}
    assert env.id == id
    assert env.body == ~s({"hello":"world"})
    assert ctx.instance == config.instance
    assert ctx.source_id == "demo"
    assert ctx.tenant_id == "default"
    assert ctx.attempt == 1
    refute Map.has_key?(ctx, :claim)
  end

  test "a refusing sink is a 503 with Retry-After, called exactly once" do
    config = start_direct([{CaptureSink, [to: self(), outcome: :error]}])

    conn = route(config, "demo", "{}")

    assert conn.status == 503
    assert %{"error" => "store_unavailable"} = JSON.decode!(conn.resp_body)
    assert Plug.Conn.get_resp_header(conn, "retry-after") == ["1"]

    assert_received {:delivered, _, _}
    refute_received {:delivered, _, _}
  end

  test "a sink that raises is the same 503 — safe_deliver/4 is on this path" do
    config = start_direct([{CaptureSink, [to: self(), outcome: :raise]}])

    conn = route(config, "demo", "{}")

    assert conn.status == 503
    assert %{"error" => "store_unavailable"} = JSON.decode!(conn.resp_body)
  end

  test "a sink that returns something other than :ok or {:error, _} is the same 503" do
    config = start_direct([{CaptureSink, [to: self(), outcome: :bad_return]}])

    conn = route(config, "demo", "{}")

    assert conn.status == 503
    assert %{"error" => "store_unavailable"} = JSON.decode!(conn.resp_body)
    assert_received {:delivered, _, _}
  end

  test "a sink whose inline_max_bytes/1 raises is a 503, and no sink runs" do
    config =
      start_direct([
        {RaisingThresholdSink, []},
        {CaptureSink, [to: self(), outcome: :ok]}
      ])

    conn = route(config, "demo", "{}")

    assert conn.status == 503
    assert %{"error" => "store_unavailable"} = JSON.decode!(conn.resp_body)
    assert Plug.Conn.get_resp_header(conn, "retry-after") == ["1"]
    refute_received {:delivered, _, _}
  end

  test "publishes to every sink concurrently, even when one refuses" do
    config =
      start_direct([
        {CaptureSink, [to: self(), outcome: :ok]},
        {CaptureSink, [to: self(), outcome: :error]},
        {CaptureSink, [to: self(), outcome: :ok]}
      ])

    conn = route(config, "demo", "{}")

    assert conn.status == 503
    # All three ran: publishing is concurrent, the refusal does not stop the rest.
    assert_received {:delivered, _, _}
    assert_received {:delivered, _, _}
    assert_received {:delivered, _, _}
  end

  test "a source with no sinks cannot ack" do
    # Boot validation already refuses a statically configured sinkless source
    # under `wal: :none`; this pins the runtime guard the publish path keeps for
    # a source that arrives some other way (the admin API).
    config = start_direct([{CaptureSink, [to: self(), outcome: :ok]}])

    assert route(config, "demo", "{}").status == 201
    assert_received {:delivered, env, _ctx}

    assert {:error, :store_unavailable} =
             Ankusa.Edge.Publish.publish(
               config.instance,
               %Ankusa.Source{id: "demo", sinks: []},
               env
             )
  end

  test "nothing is queued on this node: no queue writer, no stored hook" do
    config = start_direct([{CaptureSink, [to: self(), outcome: :ok]}])

    assert route(config, "demo", "{}").status == 201

    assert Ankusa.whereis(config.instance, :queue_writer) == nil
    assert Ankusa.Queue.hooks(config.instance, 0, 10) == {:ok, []}
  end

  test "a body over a sink's inline_max_bytes is checked in once, and the ctx carries the claim" do
    config = start_direct([{CaptureSink, [to: self(), inline_max_bytes: 16]}])

    conn = route(config, "demo", String.duplicate("x", 64))

    assert conn.status == 201
    assert_received {:delivered, env, ctx}
    assert %Ankusa.ClaimCheck.Ref{} = ctx.claim.ref
    assert is_binary(ctx.claim.sha256)
    assert ctx.claim.ref.tenant_id == env.tenant_id
  end

  for mode <- [:exit, :bad_return] do
    @tag :capture_log
    test "a claim check whose blob store #{mode}s is a 503, and the sink never runs (#{mode})" do
      config =
        start_direct([{CaptureSink, [to: self(), inline_max_bytes: 16]}],
          storage: %{blob_store: {MisbehavingBlobStore, [mode: unquote(mode)]}}
        )

      conn = route(config, "demo", String.duplicate("x", 64))

      assert conn.status == 503
      assert %{"error" => "store_unavailable"} = JSON.decode!(conn.resp_body)
      refute_received {:delivered, _, _}
    end
  end

  # ── E7: concurrent publish under one deadline ─────────────────────────────

  test "two slow sinks run concurrently: the ack waits for the slowest, not the sum" do
    config =
      start_direct([
        {CaptureSink, [to: self(), sleep_ms: 300]},
        {CaptureSink, [to: self(), sleep_ms: 300]}
      ])

    start = System.monotonic_time(:millisecond)

    conn = route(config, "demo", "{}")

    elapsed = System.monotonic_time(:millisecond) - start

    assert conn.status == 201
    # Sequentially this would be ~600 ms; concurrently it is ~300 ms.
    assert elapsed < 550
    assert_received {:delivered, _, _}
    assert_received {:delivered, _, _}
  end

  @tag :capture_log
  test "a sink that misses direct_publish_timeout_ms is a 503 within the deadline" do
    config =
      start_direct([{CaptureSink, [to: self(), sleep_ms: 1_000]}],
        direct_publish_timeout_ms: 200
      )

    start = System.monotonic_time(:millisecond)

    conn = route(config, "demo", "{}")

    elapsed = System.monotonic_time(:millisecond) - start

    assert conn.status == 503
    assert %{"error" => "store_unavailable"} = JSON.decode!(conn.resp_body)
    assert elapsed >= 150 and elapsed < 500
  end
end
