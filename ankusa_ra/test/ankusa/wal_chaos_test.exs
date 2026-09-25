defmodule Ankusa.WAL.ChaosTest do
  @moduledoc """
  `Ankusa.WAL.Chaos.scan/1` is what the chaos harness takes its final scan with,
  and what an operator asks after an incident: *what does the log still hold?*

  Both of those rest on the answer being complete. A read that cannot be
  answered comes back as `[]` from `Ankusa.WAL` — which is right for the dispatch
  pipeline, whose reader retries on its next tick, and wrong for a scan, which
  would report a short log as the whole log. These cases pin the scan against a
  real one-member cluster: that it returns what is live, that it tracks
  truncation, and that the completeness check which catches a failed read does
  not fire on a healthy log.
  """

  use ExUnit.Case, async: false

  import Ankusa.WAL.ConformanceCase, only: [envelope: 1, hold!: 3]

  alias Ankusa.WAL.Chaos
  alias Ankusa.WAL.Conformance.Ra, as: SingleNode

  setup do
    instance = :"chaos_#{System.unique_integer([:positive])}"

    dir =
      Path.join(
        System.tmp_dir!(),
        "ankusa_chaos_#{instance}_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)

    config =
      Ankusa.Config.new(
        instance: instance,
        data_dir: dir,
        roles: [:edge, :dispatch, :storage, :wal],
        wal: {Ankusa.WAL.Ra, members: [{:"ankusa_wal_#{instance}", node()}]}
      )

    Ankusa.put_config(config)
    :ok = SingleNode.start(instance, config)

    on_exit(fn ->
      SingleNode.stop(instance)
      File.rm_rf(dir)
    end)

    %{instance: instance}
  end

  defp append(instance, bodies) do
    {:ok, committed} =
      Ankusa.WAL.append(instance, for(b <- bodies, do: %{envelope: envelope(%{body: b})}))

    Enum.map(committed, fn {:committed, env} -> env.seq end)
  end

  test "an empty log scans to nothing rather than raising", %{instance: instance} do
    assert Chaos.scan(instance) == []
  end

  test "every live record comes back, with the digest the load generator records", %{
    instance: instance
  } do
    bodies = for i <- 1..5, do: "body-#{i}"
    seqs = append(instance, bodies)

    rows = Chaos.scan(instance)

    assert Enum.map(rows, & &1["seq"]) == seqs
    assert Enum.map(rows, & &1["sha256"]) == Enum.map(bodies, &Chaos.sha256/1)
    assert rows |> Enum.map(& &1["id"]) |> Enum.uniq() |> length() == 5
  end

  # Truncation moves the live set, and the scan must follow it — while the
  # completeness check stays quiet, because the log read the same both times.
  test "a truncated record leaves the scan, and the scan does not raise about it", %{
    instance: instance
  } do
    seqs = append(instance, for(i <- 1..4, do: "b#{i}"))

    lease = hold!(instance, :storage, [])
    assert :ok = Ankusa.WAL.truncate_through(instance, Enum.at(seqs, 1), lease.token)

    assert Enum.map(Chaos.scan(instance), & &1["seq"]) == Enum.drop(seqs, 2)
  end

  test "dump/1 hands back JSON, which is what the harness captures to a file", %{
    instance: instance
  } do
    assert [_] = append(instance, ["one"])

    assert [row] = JSON.decode!(Chaos.dump(instance))
    assert row["seq"] == 1
    assert row["sha256"] == Chaos.sha256("one")
  end
end
