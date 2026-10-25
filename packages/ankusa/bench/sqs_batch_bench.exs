# What batching does to the requests a sink makes, and to its throughput, at
# dispatch's default configuration (DEL-4):
#
#     MIX_ENV=test mix run bench/sqs_batch_bench.exs
#     N=10000 LATENCY_MS=20 MIX_ENV=test mix run bench/sqs_batch_bench.exs
#
# `N` hooks (default 10 000) go through a real instance — the store, the
# dispatch pipeline and the sink — to a local HTTP server that answers each
# request after `LATENCY_MS` (default 20), the way SQS and Pub/Sub answer from
# the same region. Each sink runs twice, once with `batch: false` (one request
# per message, `dispatch.sink_concurrency` 8 of them at a time) and once as it
# ships. The table is requests per 10 000 messages and messages per second per
# sink.
#
# It exits non-zero if batching stops saving requests: SQS (standard queue)
# must take at most a fifth of the requests, Pub/Sub at most a hundredth.

Logger.configure(level: :warning)

defmodule Bench.Stub do
  @moduledoc false
  # An SQS/Pub/Sub that answers after `latency_ms` and counts what it was asked.
  @behaviour Plug

  def init(opts), do: opts

  def call(conn, opts) do
    {:ok, body, conn} = Plug.Conn.read_body(conn, length: 64_000_000)
    Process.sleep(opts[:latency_ms])
    :counters.add(opts[:counters], 1, 1)

    reply =
      case Plug.Conn.get_req_header(conn, "x-amz-target") do
        ["AmazonSQS.SendMessageBatch"] ->
          %{"Entries" => entries} = JSON.decode!(body)

          %{
            "Successful" =>
              for %{"Id" => id, "MessageBody" => message} <- entries do
                %{"Id" => id, "MessageId" => "m", "MD5OfMessageBody" => md5(message)}
              end,
            "Failed" => []
          }

        ["AmazonSQS.SendMessage"] ->
          %{"MessageId" => "m", "MD5OfMessageBody" => md5(JSON.decode!(body)["MessageBody"])}

        _pubsub ->
          %{"messages" => messages} = JSON.decode!(body)
          %{"messageIds" => Enum.map(1..length(messages), &Integer.to_string/1)}
      end

    Plug.Conn.send_resp(conn, 200, JSON.encode!(reply))
  end

  defp md5(message), do: Base.encode16(:crypto.hash(:md5, message), case: :lower)
end

defmodule Bench.SQSBatch do
  alias Ankusa.{Envelope, Queue}

  @sources %{
    sqs: "sqs",
    sqs_fifo: "sqs (FIFO, 64 groups)",
    pubsub: "pubsub"
  }

  def sinks(port) do
    base = "http://127.0.0.1:#{port}"

    sqs = [
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s"
    ]

    %{
      sqs: fn extra ->
        {Ankusa.Sink.SQS, [queue_url: "#{base}/000000000000/hooks"] ++ sqs ++ extra}
      end,
      sqs_fifo: fn extra ->
        {Ankusa.Sink.SQS,
         [
           queue_url: "#{base}/000000000000/hooks.fifo",
           message_group_id: fn env -> "g#{:erlang.phash2(env.id, 64)}" end
         ] ++ sqs ++ extra}
      end,
      pubsub: fn extra ->
        {Ankusa.Sink.GooglePubSub, [project: "p", topic: "t", endpoint: base] ++ extra}
      end
    }
  end

  def label(kind), do: Map.fetch!(@sources, kind)

  def run(kind, batch?, n, latency_ms) do
    counters = :counters.new(1, [])
    delivered = :counters.new(1, [])

    {:ok, listener} =
      Bandit.start_link(
        plug: {Bench.Stub, latency_ms: latency_ms, counters: counters},
        port: 0,
        startup_log: false
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(listener)
    sink = sinks(port) |> Map.fetch!(kind) |> then(& &1.(if batch?, do: [], else: [batch: false]))

    dir =
      Path.join(System.tmp_dir!(), "ankusa_sqs_batch_bench_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    instance = :"bench_batch_#{System.unique_integer([:positive])}"

    config =
      Ankusa.Config.new(
        instance: instance,
        data_dir: dir,
        port: 0,
        roles: [:edge, :dispatch],
        source_store: {Ankusa.SourceStore.Static, sources: %{"s" => %{sinks: [sink]}}}
      )

    {:ok, sup} = Ankusa.Instance.start_link(config)
    handler = "sqs-batch-bench-#{instance}"

    :telemetry.attach(
      handler,
      [:ankusa, :dispatch, :stop],
      fn _event, _measurements, %{result: result}, _ ->
        if result == :ok, do: :counters.add(delivered, 1, 1)
      end,
      nil
    )

    {:ok, source} = Ankusa.SourceStore.fetch(instance, "s")
    started = System.monotonic_time(:millisecond)

    1..n
    |> Enum.chunk_every(256)
    |> Enum.each(fn chunk ->
      items = for _ <- chunk, do: %{envelope: envelope(), sinks: source.sinks}
      {:ok, _} = Queue.enqueue(instance, items)
    end)

    wait(delivered, n, 600_000)
    elapsed_ms = System.monotonic_time(:millisecond) - started

    :telemetry.detach(handler)
    Supervisor.stop(sup)
    Supervisor.stop(listener)
    File.rm_rf(dir)

    requests = :counters.get(counters, 1)

    %{
      kind: kind,
      batch?: batch?,
      requests: requests,
      per_10k: requests * 10_000 / n,
      msgs_per_s: n * 1000 / elapsed_ms,
      elapsed_s: elapsed_ms / 1000
    }
  end

  defp wait(counter, n, budget_ms) do
    cond do
      :counters.get(counter, 1) >= n ->
        :ok

      budget_ms <= 0 ->
        raise "timed out: #{:counters.get(counter, 1)} of #{n} delivered"

      true ->
        Process.sleep(25)
        wait(counter, n, budget_ms - 25)
    end
  end

  defp envelope do
    %Envelope{
      id: "evt_" <> Integer.to_string(System.unique_integer([:positive])),
      source_id: "s",
      tenant_id: "acme",
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/hooks",
      headers: [],
      content_type: "application/json",
      body: ~s({"hello":"world"}),
      size: 17
    }
  end
end

n = String.to_integer(System.get_env("N", "10000"))
latency_ms = String.to_integer(System.get_env("LATENCY_MS", "20"))

IO.puts(
  "N=#{n} hooks per run, #{latency_ms} ms per request, dispatch defaults (sink_concurrency 8)\n"
)

results =
  for kind <- [:sqs, :sqs_fifo, :pubsub], batch? <- [false, true] do
    result = Bench.SQSBatch.run(kind, batch?, n, latency_ms)

    IO.puts(
      :io_lib.format("~-24s ~-9s ~8w requests  ~10.1f per 10k  ~9w msgs/s  ~7.1f s~n", [
        Bench.SQSBatch.label(kind),
        if(batch?, do: "batched", else: "single"),
        result.requests,
        result.per_10k,
        round(result.msgs_per_s),
        result.elapsed_s
      ])
    )

    result
  end

by = fn kind, batch? -> Enum.find(results, &(&1.kind == kind and &1.batch? == batch?)) end

IO.puts("")

for {kind, factor} <- [sqs: 5, pubsub: 100] do
  single = by.(kind, false)
  batched = by.(kind, true)
  saved = single.requests / max(batched.requests, 1)

  IO.puts(
    :io_lib.format("~-24s ~.1fx fewer requests, ~.1fx the messages per second~n", [
      Bench.SQSBatch.label(kind),
      saved,
      batched.msgs_per_s / single.msgs_per_s
    ])
  )

  if saved < factor do
    IO.puts(
      "FAIL #{Bench.SQSBatch.label(kind)}: saved #{Float.round(saved, 1)}x, want at least #{factor}x"
    )

    System.halt(1)
  end
end
