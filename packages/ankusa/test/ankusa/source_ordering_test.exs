defmodule Ankusa.SourceOrderingTest do
  @moduledoc """
  ADR 0004, sections 1 and 2: the `ordered` / `ordering_key` grammar of a sink
  entry (every form that is accepted and every one that is rejected), what the
  instance's config adds (`wal.type`, the stored-header policy), and how a hook's
  lane is resolved from the complete header list and the body.
  """

  use ExUnit.Case, async: false

  alias Ankusa.{Config, Envelope, Ordering, Source}
  alias Ankusa.Sink.{GooglePubSub, Http, Log, SQS}

  @fifo "https://sqs.us-east-1.amazonaws.com/1/h.fifo"
  @standard "https://sqs.us-east-1.amazonaws.com/1/h"

  defmodule Keys do
    @moduledoc false
    def entity(env), do: Envelope.header(env, "x-fn")
    def raises(_env), do: raise("no key for you")
    def not_binary(_env), do: 42
    def from_arg(_env, key), do: key
  end

  defp new(sinks, extra \\ []), do: Source.new("s", [sinks: sinks] ++ extra)

  defp env(headers \\ [], body \\ "{}") do
    %Envelope{
      id: Ankusa.UUIDv7.generate(),
      source_id: "s",
      tenant_id: "acme",
      received_at: 1,
      method: "POST",
      path: "/",
      headers: headers,
      body: body,
      size: byte_size(body)
    }
  end

  defp config(overrides \\ []), do: Config.new([roles: [:edge]] ++ overrides)

  describe "the option grammar" do
    test "an unordered source has no ordering and its sink options are untouched" do
      source = new([{Http, url: "https://x.test/"}, {Log, []}])
      assert source.ordering == nil
      assert source.sinks == [{Http, url: "https://x.test/"}, {Log, []}]
    end

    test "ordered: true with no key is one keyless lane per sink" do
      source = new([{Log, ordered: true}, {Log, []}])
      assert source.ordering == [{:ordered, nil}, nil]
      assert source.sinks == [{Log, []}, {Log, []}]
    end

    test "{:header, name} is lowercased, {:json, path} is split, {m, f, a} is kept" do
      source =
        new([
          {Log, ordered: true, ordering_key: {:header, "X-Entity-Id"}},
          {Log, ordered: true, ordering_key: {:json, "data.object.customer"}},
          {Log, ordered: true, ordering_key: {Keys, :from_arg, ["k"]}}
        ])

      assert source.ordering == [
               {:ordered, {:header, "x-entity-id"}},
               {:ordered, {:json, ["data", "object", "customer"]}},
               {:ordered, {Keys, :from_arg, ["k"]}}
             ]

      # The pipeline hands the sink neither option.
      assert source.sinks == [{Log, []}, {Log, []}, {Log, []}]
    end

    test "a normalized json path is accepted again" do
      source = new([{Log, ordered: true, ordering_key: {:json, ["a", "b"]}}])
      assert source.ordering == [{:ordered, {:json, ["a", "b"]}}]
    end

    test "ordered: false is the same as no option, and is dropped" do
      source = new([{Log, ordered: false}])
      assert source.ordering == nil
      assert source.sinks == [{Log, []}]
    end

    test "every rejected form raises, naming the source and the sink" do
      for {sinks, message} <- [
            {[{Log, ordered: "yes"}], "ordered must be a boolean"},
            {[{Log, ordered: true, ordering_key: "x"}], "ordering_key must be"},
            {[{Log, ordered: true, ordering_key: fn _ -> "x" end}], "ordering_key must be"},
            {[{Log, ordered: true, ordering_key: {:header, ""}}], "ordering_key must be"},
            {[{Log, ordered: true, ordering_key: {:json, "a..b"}}], "non-empty"},
            {[{Log, ordered: true, ordering_key: {:json, ""}}], "non-empty"},
            {[{Log, ordered: true, ordering_key: {:json, ["a", ""]}}], "non-empty"},
            {[{Log, ordered: true, ordering_key: {:other, "x"}}], "ordering_key must be"},
            {[{Log, ordering_key: {:header, "x"}}], "needs ordered: true"},
            {[{Http, url: "https://x.test/", ordering_key: "x"}], "needs ordered: true"}
          ] do
        error = assert_raise ArgumentError, fn -> new(sinks) end
        assert error.message =~ ~s(source "s": sinks[0]), inspect(sinks)
        assert error.message =~ message, inspect(sinks)
      end
    end

    test "an SQS standard queue cannot be ordered, a FIFO queue can" do
      error =
        assert_raise ArgumentError, fn ->
          new([{SQS, queue_url: @standard, region: "r", ordered: true}])
        end

      assert error.message =~ "FIFO queue"

      source = new([{SQS, queue_url: @fifo, region: "r", ordered: true}])
      assert source.ordering == [{:ordered, nil}]
      refute Keyword.has_key?(elem(hd(source.sinks), 1), :ordered)
    end

    test "Pub/Sub keeps its own ordering_key when unordered, and takes the lane's when ordered" do
      for key <- ["tenant-key", fn env -> env.source_id end] do
        source = new([{GooglePubSub, project: "p", topic: "t", ordering_key: key}])
        assert source.ordering == nil
        assert Keyword.fetch!(elem(hd(source.sinks), 1), :ordering_key) == key
      end

      source =
        new([
          {GooglePubSub,
           project: "p", topic: "t", ordered: true, ordering_key: {:header, "x-entity"}}
        ])

      assert source.ordering == [{:ordered, {:header, "x-entity"}}]
      refute Keyword.has_key?(elem(hd(source.sinks), 1), :ordering_key)

      # With `ordered: true` the legacy string form is refused.
      assert_raise ArgumentError, fn ->
        new([{GooglePubSub, project: "p", topic: "t", ordered: true, ordering_key: "k"}])
      end
    end
  end

  describe "what the instance's config adds" do
    test "lanes need the store-backed queue" do
      source = new([{Log, ordered: true}])
      assert Ordering.check(config(), source) == :ok

      assert {:error, message} = Ordering.check(config(wal: :none), source)
      assert message =~ "wal.type disk or dual"

      assert_raise ArgumentError, ~r/wal.type disk or dual/, fn ->
        Ordering.validate_config!(
          config(
            wal: :none,
            source_store:
              {Ankusa.SourceStore.Static,
               sources: %{
                 "s" => [
                   verifier: {Ankusa.Verifier.None, []},
                   sinks: [{Log, ordered: true}]
                 ]
               }}
          )
        )
      end
    end

    test "a header key must survive the stored-header policy" do
      ordered = fn name, extra ->
        new([{Log, ordered: true, ordering_key: {:header, name}}], extra)
      end

      assert Ordering.check(config(), ordered.("x-entity", [])) == :ok

      # The instance's default policy drops credential-bearing headers.
      assert {:error, message} = Ordering.check(config(), ordered.("Authorization", []))
      assert message =~ "drops or hashes"
      assert message =~ ~s("authorization")

      # A source's own policy, per key.
      assert {:error, _} =
               Ordering.check(config(), ordered.("x-entity", store_headers: %{drop: ["x-entity"]}))

      assert {:error, _} =
               Ordering.check(config(), ordered.("x-entity", store_headers: %{hash: ["X-Entity"]}))

      # Dropping something else is fine.
      assert Ordering.check(config(), ordered.("x-entity", store_headers: %{drop: ["x-other"]})) ==
               :ok

      # JSON and MFA keys read the body, which no policy touches.
      json = new([{Log, ordered: true, ordering_key: {:json, "id"}}], store_headers: %{drop: ["id"]})
      assert Ordering.check(config(), json) == :ok
    end
  end

  describe "resolving a lane" do
    test "the lane id is 16 bytes, hashed over the sink, the source and the key" do
      id = Ordering.lane_id(0, "s", "k")
      assert byte_size(id) == 16
      assert id == Ordering.lane_id(0, "s", "k")

      assert Enum.uniq([
               id,
               Ordering.lane_id(1, "s", "k"),
               Ordering.lane_id(0, "t", "k"),
               Ordering.lane_id(0, "s", "j"),
               Ordering.lane_id(0, "s", nil)
             ]) |> length() == 5

      # The keyless lane is its own: an empty key is not the same lane.
      assert Ordering.lane_id(0, "s", nil) != Ordering.lane_id(0, "s", "")

      # No delimiter can be forged: the boundary between source and key is hashed.
      assert Ordering.lane_id(0, "a", "bc") != Ordering.lane_id(0, "ab", "c")
    end

    test "a header key is read from the complete header list, case-insensitively" do
      source = new([{Log, ordered: true, ordering_key: {:header, "x-entity"}}])

      assert {nil, [{lane, "e1"}]} =
               Ordering.resolve(:none, source, env([{"X-Entity", "e1"}]))

      assert lane == Ordering.lane_id(0, "s", "e1")
      assert {nil, [{^lane, "e1"}]} = Ordering.resolve(:none, source, env([{"x-entity", "e1"}]))
      assert {nil, [{other, "e2"}]} = Ordering.resolve(:none, source, env([{"x-entity", "e2"}]))
      assert other != lane
    end

    test "the lane is per sink: two sinks with one key are two lanes" do
      source =
        new([
          {Log, ordered: true, ordering_key: {:header, "x-entity"}},
          {Log, []},
          {Log, ordered: true, ordering_key: {:header, "x-entity"}}
        ])

      assert {nil, [{first, "e"}, nil, {third, "e"}]} =
               Ordering.resolve(:none, source, env([{"x-entity", "e"}]))

      assert first != third
    end

    test "a key over 128 bytes becomes sha256:hex" do
      source = new([{Log, ordered: true, ordering_key: {:header, "x-entity"}}])
      long = String.duplicate("k", 129)
      assert {nil, [{_lane, key}]} = Ordering.resolve(:none, source, env([{"x-entity", long}]))

      assert key == "sha256:" <> Base.encode16(:crypto.hash(:sha256, long), case: :lower)
      assert byte_size(key) <= 128

      exactly = String.duplicate("k", 128)

      assert {nil, [{_lane, ^exactly}]} =
               Ordering.resolve(:none, source, env([{"x-entity", exactly}]))
    end

    test "a json key walks the body; a missing path, a non-string and a non-JSON body are no key" do
      source = new([{Log, ordered: true, ordering_key: {:json, "data.customer"}}])
      body = ~s({"data":{"customer":"cus_1"}})

      assert {nil, [{lane, "cus_1"}]} = Ordering.resolve(:none, source, env([], body))
      assert lane == Ordering.lane_id(0, "s", "cus_1")

      keyless = Ordering.lane_id(0, "s", nil)

      for body <- [~s({"data":{}}), ~s({"data":{"customer":{"a":1}}}), "not json", ~s({"data":{"customer":""}})] do
        assert {nil, [{^keyless, ""}]} = Ordering.resolve(:none, source, env([], body)), body
      end
    end

    test "a json body past 1 MiB has no key" do
      source = new([{Log, ordered: true, ordering_key: {:json, "id"}}])
      pad = String.duplicate("x", 1_048_576)
      body = ~s({"id":"a","pad":"#{pad}"})
      keyless = Ordering.lane_id(0, "s", nil)

      assert {nil, [{^keyless, ""}]} = Ordering.resolve(:none, source, env([], body))
    end

    test "one decode serves an ordering key and a json dedupe on the same source" do
      source =
        new([{Log, ordered: true, ordering_key: {:json, "customer"}}],
          dedupe: %{json: "id", ttl_ms: 1_000}
        )

      body = ~s({"id":"evt_1","customer":"cus_9"})
      assert {"evt_1", [{_lane, "cus_9"}]} = Ordering.resolve(:none, source, env([], body))
    end

    test "a function key runs under rescue: a raise or a non-binary is no key" do
      keyless = Ordering.lane_id(0, "s", nil)

      for mfa <- [{Keys, :raises, []}, {Keys, :not_binary, []}, {Keys, :entity, []}] do
        source = new([{Log, ordered: true, ordering_key: mfa}])
        assert {nil, [{^keyless, ""}]} = Ordering.resolve(:none, source, env()), inspect(mfa)
      end

      source = new([{Log, ordered: true, ordering_key: {Keys, :entity, []}}])
      assert {nil, [{lane, "v"}]} = Ordering.resolve(:none, source, env([{"x-fn", "v"}]))
      assert lane == Ordering.lane_id(0, "s", "v")
    end

    test "no key: the keyless lane, counted only when a key was configured" do
      handler = "source-ordering-#{System.unique_integer([:positive])}"
      test = self()

      :telemetry.attach(
        handler,
        [:ankusa, :dispatch, :lane_key_missing],
        fn _event, _measurements, metadata, _config -> send(test, {:missing, metadata}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      keyed = new([{Log, ordered: true, ordering_key: {:header, "x-entity"}}])
      keyless = Ordering.lane_id(0, "s", nil)

      assert {nil, [{^keyless, ""}]} = Ordering.resolve(:inst, keyed, env())
      assert_receive {:missing, %{instance: :inst, sink: Log}}

      assert {nil, [{_lane, "e"}]} = Ordering.resolve(:inst, keyed, env([{"x-entity", "e"}]))
      refute_receive {:missing, _}

      # `ordered: true` with no key at all is one lane by choice, not a missing key.
      plain = new([{Log, ordered: true}])
      assert {nil, [{^keyless, ""}]} = Ordering.resolve(:inst, plain, env())
      refute_receive {:missing, _}
    end

    test "an unordered source resolves no lanes and only the dedupe key" do
      source = new([{Log, []}], dedupe: %{header: "x-id", ttl_ms: 1_000})
      assert {"abc", nil} = Ordering.resolve(:none, source, env([{"x-id", "abc"}]))
      assert Ordering.lanes(:none, source, env()) == nil
    end

    test "the sink's key: the lane's, or tenant/source for the keyless lane" do
      assert Ordering.sink_key("cus_1", "acme", "s") == "cus_1"
      assert Ordering.sink_key("", "acme", "s") == "acme/s"
      assert Ordering.sink_key("", nil, "s") == "/s"
    end
  end
end
