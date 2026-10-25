defmodule Ankusa.ConformanceGenTest do
  @moduledoc """
  `conformance/cases/producer.json` is what `mix ankusa.conformance.gen` writes
  from `Ankusa.Sink.Message.encode/3`. A field added to the encoder without
  regenerating the file fails here (and in `mise run check:conformance`, which
  runs the same comparison for the whole repository).
  """

  use ExUnit.Case, async: false

  alias Mix.Tasks.Ankusa.Conformance.Gen

  @vectors Path.expand("../../../../conformance/cases/producer.json", __DIR__)

  test "the checked-in producer vectors are what the task generates" do
    assert File.read!(@vectors) == Gen.render()
  end

  test "the task writes the same bytes to --out" do
    out = Path.join(System.tmp_dir!(), "producer-#{System.unique_integer([:positive])}.json")
    on_exit(fn -> File.rm(out) end)

    Mix.Task.rerun("ankusa.conformance.gen", ["--out", out])

    assert File.read!(out) == File.read!(@vectors)
  end

  test "every vector is a decode_message case whose message the encoder wrote" do
    %{"cases" => cases} = JSON.decode!(File.read!(@vectors))

    assert Enum.map(cases, & &1["id"]) ==
             ~w(producer.inline_full producer.claim producer.replay_ordered producer.flagged)

    for %{"feature" => "message.producer", "operation" => "decode_message"} = test_case <- cases do
      message = JSON.decode!(test_case["input"]["message"])
      expect = test_case["expect"]["ok"]

      assert message["v"] == 1
      assert expect["id"] == message["id"]
      assert expect["instance"] == message["instance"]
      assert expect["region"] == message["region"]
    end
  end

  test "the instance and region in the vectors are the publishing instance's" do
    %{"cases" => cases} = JSON.decode!(File.read!(@vectors))
    by_id = Map.new(cases, &{&1["id"], &1["expect"]["ok"]})

    assert %{"instance" => "conformance_eu", "region" => "eu-west-1"} =
             by_id["producer.inline_full"]

    assert %{"instance" => "conformance", "region" => nil} = by_id["producer.flagged"]
  end
end
