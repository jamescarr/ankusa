defmodule Ankusa.LossTest do
  @moduledoc """
  The loss checker: the project's credibility. Every acked id must be readable
  after a crash. Zero tolerance.
  """
  use ExUnit.Case, async: false

  import Ankusa.TestHelpers
  alias Ankusa.{Edge.Ingest, WAL}

  @count 500

  test "every acked hook survives a hard crash of the whole instance" do
    config =
      test_config(
        roles: [:edge],
        source_store:
          {Ankusa.SourceStore.Static, sources: %{"load" => [verifier: {Ankusa.Verifier.None, []}]}},
        batcher: %{partitions: 4, max_batch: 64, max_delay_ms: 5, max_queue: 100_000}
      )

    pid = start_supervised!({Ankusa.Instance, config})

    # Concurrent ingest, exactly like the load generator: record every acked id.
    acked =
      1..@count
      |> Task.async_stream(
        fn n ->
          case Ingest.ingest(config.instance, request("load", ~s({"n":#{n}}))) do
            {:ok, env} -> env.id
            other -> flunk("ingest did not ack: #{inspect(other)}")
          end
        end,
        max_concurrency: 32,
        ordered: false
      )
      |> Enum.map(fn {:ok, id} -> id end)
      |> MapSet.new()

    assert MapSet.size(acked) == @count

    # Hard kill: no clean shutdown, no fd flush beyond what fsync already durably
    # wrote. This is crash-after-commit.
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, _, _}, 5_000

    # Recover with a fresh WAL over the same data dir and replay.
    start_supervised!({Ankusa.WAL.DiskLog, instance: config.instance, config: config})

    recovered =
      WAL.read(config.instance, -1, @count * 2)
      |> Enum.map(& &1.id)
      |> MapSet.new()

    missing = MapSet.difference(acked, recovered)

    assert MapSet.size(missing) == 0,
           "lost #{MapSet.size(missing)} acked hooks: #{inspect(Enum.take(missing, 5))}"
  end
end
