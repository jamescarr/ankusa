defmodule Ankusa.FilterTest do
  @moduledoc """
  `Ankusa.Filter`: the grammar of a sink's `filter` (every accepted and every
  rejected form), what a filter reads from a hook, what the source and the
  instance's config add, and how `Ankusa.Routing` turns it into the sinks that
  get a delivery row.
  """

  use ExUnit.Case, async: false

  alias Ankusa.{Config, Envelope, Filter, Routing, Source}
  alias Ankusa.Sink.{Http, Log}

  defp env(opts \\ []) do
    body = Keyword.get(opts, :body, "{}")

    %Envelope{
      id: Ankusa.UUIDv7.generate(),
      source_id: "s",
      tenant_id: Keyword.get(opts, :tenant, "acme"),
      received_at: 1,
      method: "POST",
      path: "/",
      headers: Keyword.get(opts, :headers, []),
      content_type: Keyword.get(opts, :content_type),
      event_type: Keyword.get(opts, :event_type),
      body: body,
      size: byte_size(body)
    }
  end

  defp filter!(term), do: Filter.new!("test", term)
  defp decoded(body), do: {:ok, JSON.decode!(body)}

  # The verdict of `term` on `env` with `decoded` as the shared decode.
  defp judge(term, env, decoded \\ :skip), do: Filter.evaluate(filter!(term), env, decoded)

  defp error(term) do
    assert {:error, reason} = Filter.parse(term)
    reason
  end

  describe "filter: the grammar" do
    test "filter: a literal, a list, every operator and every combinator parse" do
      for term <- [
            %{"event_type" => "invoice.paid"},
            %{event_type: "invoice.paid"},
            %{"event_type" => ["a", "b"]},
            %{"event_type" => %{"eq" => "a"}},
            %{"event_type" => %{neq: "a"}},
            %{"event_type" => %{"in" => ["a"]}},
            %{"event_type" => %{"not_in" => ["a"]}},
            %{"event_type" => %{"prefix" => "invoice."}},
            %{"event_type" => %{"suffix" => ".paid"}},
            %{"header.x-github-event" => %{"exists" => true}},
            %{"header.X-Github-Event" => %{"exists" => false}},
            %{"json.data.object.id" => "x"},
            %{"tenant" => "acme"},
            %{"content_type" => "application/json"},
            %{"all" => [%{"event_type" => "a"}, %{"tenant" => "acme"}]},
            %{"any" => [%{"event_type" => "a"}, %{"event_type" => "b"}]},
            %{"not" => %{"event_type" => "a"}},
            %{all: [%{any: [%{not: %{event_type: "a"}}]}]}
          ] do
        assert {:ok, %Filter{}} = Filter.parse(term), inspect(term)
      end
    end

    test "filter: header names are lowercased, a json path is split, ops are atoms" do
      assert %Filter{tree: {:leaf, {:header, "x-github-event"}, {:eq, "push"}}} =
               filter!(%{"header.X-GitHub-Event" => "push"})

      assert %Filter{tree: {:leaf, {:json, ["data", "object"]}, {:in, ["a", "b"]}}, json?: true} =
               filter!(%{"json.data.object" => ["a", "b"]})

      assert %Filter{tree: {:not, {:leaf, :event_type, {:prefix, "x"}}}, json?: false} =
               filter!(%{"not" => %{"event_type" => %{"prefix" => "x"}}})
    end

    test "filter: every rejected form says why" do
      for {term, fragment} <- [
            {"event_type", "node must be a map"},
            {nil, "node must be a map"},
            {%{}, "exactly one key, got 0"},
            {%{"event_type" => "a", "tenant" => "b"}, "exactly one key, got 2"},
            {%{"nope" => "a"}, ~s(field "nope" is not one of)},
            {%{"header." => "a"}, "needs a header name"},
            {%{"json." => "a"}, "needs non-empty segments"},
            {%{"json.a..b" => "a"}, "needs non-empty segments"},
            {%{"event_type" => 5}, "takes a string, a list of strings or one operator"},
            {%{"event_type" => %{}}, "takes exactly one operator"},
            {%{"event_type" => %{"eq" => "a", "neq" => "b"}}, "takes exactly one operator"},
            {%{"event_type" => %{"like" => "a"}}, ~s(unknown operator "like")},
            {%{"event_type" => %{"eq" => 5}}, "must be a string (quote a number)"},
            {%{"event_type" => %{"in" => "a"}}, "in takes a list of strings"},
            {%{"event_type" => %{"in" => []}}, "needs at least one value"},
            {%{"event_type" => %{"exists" => "yes"}}, "exists takes true or false"},
            {%{"event_type" => [1]}, "must be a string (quote a number)"},
            {%{"all" => []}, "all needs a non-empty list of nodes"},
            {%{"any" => %{"event_type" => "a"}}, "any needs a non-empty list of nodes"},
            {%{"not" => [%{"event_type" => "a"}]}, "node must be a map"},
            {%{1 => "a"}, "node key must be a string"}
          ] do
        assert error(term) =~ fragment, inspect(term)
      end
    end

    test "filter: every ceiling is enforced at its edge" do
      leaf = %{"event_type" => "a"}

      # 32 conditions, not 33.
      assert {:ok, _} = Filter.parse(%{"all" => List.duplicate(leaf, 32)})
      assert error(%{"all" => List.duplicate(leaf, 33)}) =~ "more than 32 conditions"

      # 4 levels of all/any/not, not 5.
      nest = fn node -> %{"all" => [node]} end
      four = leaf |> nest.() |> nest.() |> nest.() |> nest.()
      assert {:ok, _} = Filter.parse(four)
      assert error(nest.(four)) =~ "more than 4 deep"
      assert error(%{"not" => %{"not" => %{"not" => %{"not" => %{"not" => leaf}}}}}) =~ "4 deep"

      # A literal of 256 bytes, not 257.
      assert {:ok, _} = Filter.parse(%{"event_type" => String.duplicate("e", 256)})
      assert error(%{"event_type" => String.duplicate("e", 257)}) =~ "over 256 bytes"

      # 64 values in an in / not_in, not 65.
      values = &Enum.map(1..&1, fn i -> "v#{i}" end)
      assert {:ok, _} = Filter.parse(%{"event_type" => values.(64)})
      assert error(%{"event_type" => values.(65)}) =~ "at most 64 values"
      assert {:ok, _} = Filter.parse(%{"event_type" => %{"not_in" => values.(64)}})
      assert error(%{"event_type" => %{"not_in" => values.(65)}}) =~ "at most 64 values"

      # A header name of 128 bytes, not 129.
      assert {:ok, _} = Filter.parse(%{("header." <> String.duplicate("h", 128)) => "x"})
      assert error(%{("header." <> String.duplicate("h", 129)) => "x"}) =~ "over 128 bytes"

      # A JSON path of 16 segments, not 17.
      path = &Enum.join(List.duplicate("p", &1), ".")
      assert {:ok, _} = Filter.parse(%{("json." <> path.(16)) => "x"})
      assert error(%{("json." <> path.(17)) => "x"}) =~ "more than 16 segments"
    end

    test "filter: new!/2 raises ArgumentError with the label" do
      error =
        assert_raise ArgumentError, fn -> Filter.new!("source \"s\": sinks[1]", %{"x" => "a"}) end

      assert error.message =~ ~s(source "s": sinks[1]: filter field "x")
    end
  end

  describe "filter: what it reads" do
    test "filter: event_type is the hook's, and a hook without one is absent" do
      assert judge(%{"event_type" => "invoice.paid"}, env(event_type: "invoice.paid")) == :match
      assert judge(%{"event_type" => "invoice.paid"}, env(event_type: "other")) == :no_match
      assert judge(%{"event_type" => "invoice.paid"}, env()) == :no_match
      assert judge(%{"event_type" => %{"exists" => false}}, env()) == :match
    end

    test "filter: tenant is the hook's tenant, \"default\" without one" do
      assert judge(%{"tenant" => "acme"}, env()) == :match
      assert judge(%{"tenant" => "default"}, env(tenant: nil)) == :match
      assert judge(%{"tenant" => %{"neq" => "acme"}}, env()) == :no_match
    end

    test "filter: content_type is the lowercased media type without parameters" do
      json = env(content_type: " Application/JSON ; charset=utf-8")
      assert judge(%{"content_type" => "application/json"}, json) == :match
      assert judge(%{"content_type" => %{"prefix" => "application/"}}, json) == :match
      assert judge(%{"content_type" => "application/json"}, env()) == :no_match
      assert judge(%{"content_type" => %{"exists" => false}}, env()) == :match
    end

    test "filter: a header is read case-insensitively, first value wins, byte-exact" do
      hook = env(headers: [{"x-github-event", "push"}, {"x-github-event", "ping"}])
      assert judge(%{"header.X-GitHub-Event" => "push"}, hook) == :match
      assert judge(%{"header.x-github-event" => "Push"}, hook) == :no_match
      assert judge(%{"header.x-missing" => "push"}, hook) == :no_match
      assert judge(%{"header.x-github-event" => %{"exists" => true}}, hook) == :match
    end

    test "filter: a json path reads strings, integers and booleans, nothing else" do
      body = ~s({"a":{"s":"x","n":42,"t":true,"f":false,"nil":null,"o":{},"l":[1]}})
      d = decoded(body)

      for {path, expected} <- [
            {"a.s", "x"},
            {"a.n", "42"},
            {"a.t", "true"},
            {"a.f", "false"}
          ] do
        assert judge(%{"json.#{path}" => expected}, env(body: body), d) == :match, path
        assert judge(%{"json.#{path}" => "zzz"}, env(body: body), d) == :no_match, path
      end

      for absent <- ["a.nil", "a.o", "a.l", "a.missing", "a.s.deeper", "b"] do
        assert judge(%{"json.#{absent}" => %{"exists" => true}}, env(body: body), d) == :no_match,
               absent

        assert judge(%{"json.#{absent}" => %{"exists" => false}}, env(body: body), d) == :match,
               absent
      end
    end
  end

  describe "filter: operators on a present and an absent value" do
    test "filter: each operator against a present value" do
      hook = env(event_type: "invoice.paid")

      for {matcher, expected} <- [
            {"invoice.paid", :match},
            {"invoice", :no_match},
            {["x", "invoice.paid"], :match},
            {["x", "y"], :no_match},
            {%{"eq" => "invoice.paid"}, :match},
            {%{"neq" => "invoice.paid"}, :no_match},
            {%{"neq" => "x"}, :match},
            {%{"in" => ["invoice.paid"]}, :match},
            {%{"not_in" => ["invoice.paid"]}, :no_match},
            {%{"not_in" => ["x"]}, :match},
            {%{"prefix" => "invoice."}, :match},
            {%{"prefix" => "paid"}, :no_match},
            {%{"suffix" => ".paid"}, :match},
            {%{"suffix" => "invoice"}, :no_match},
            {%{"exists" => true}, :match},
            {%{"exists" => false}, :no_match}
          ] do
        assert judge(%{"event_type" => matcher}, hook) == expected, inspect(matcher)
      end
    end

    test "filter: an absent value fails eq, in, prefix, suffix, exists: true; passes neq, not_in, exists: false" do
      hook = env()

      for {matcher, expected} <- [
            {"x", :no_match},
            {["x"], :no_match},
            {%{"eq" => "x"}, :no_match},
            {%{"in" => ["x"]}, :no_match},
            {%{"prefix" => "x"}, :no_match},
            {%{"suffix" => "x"}, :no_match},
            {%{"exists" => true}, :no_match},
            {%{"neq" => "x"}, :match},
            {%{"not_in" => ["x"]}, :match},
            {%{"exists" => false}, :match}
          ] do
        assert judge(%{"event_type" => matcher}, hook) == expected, inspect(matcher)
      end
    end

    test "filter: all, any and not combine" do
      hook = env(event_type: "invoice.paid", tenant: "acme")
      yes = %{"event_type" => "invoice.paid"}
      no = %{"event_type" => "x"}

      assert judge(%{"all" => [yes, %{"tenant" => "acme"}]}, hook) == :match
      assert judge(%{"all" => [yes, no]}, hook) == :no_match
      assert judge(%{"any" => [no, yes]}, hook) == :match
      assert judge(%{"any" => [no, no]}, hook) == :no_match
      assert judge(%{"not" => no}, hook) == :match
      assert judge(%{"not" => yes}, hook) == :no_match
      assert judge(%{"all" => [%{"any" => [no, %{"not" => no}]}, yes]}, hook) == :match
    end
  end

  describe "filter: unevaluable" do
    test "filter: a json condition on a body that did not decode is unevaluable, any other is not" do
      json = filter!(%{"json.type" => "a"})
      header = filter!(%{"header.x" => "a"})
      mixed = filter!(%{"all" => [%{"event_type" => "zzz"}, %{"json.type" => "a"}]})

      for decoded <- [:skip, {:error, :bad}] do
        assert Filter.evaluate(json, env(), decoded) == :unevaluable
        # Even when another condition already fails: a filter never loses a hook.
        assert Filter.evaluate(mixed, env(), decoded) == :unevaluable
        assert Filter.evaluate(header, env(), decoded) == :no_match
      end

      assert Filter.evaluate(json, env(), decoded(~s({"type":"a"}))) == :match
      assert Filter.evaluate(json, env(), decoded(~s({"type":"b"}))) == :no_match
      # Decoded, but the path is missing: absent, not unevaluable.
      assert Filter.evaluate(json, env(), decoded("{}")) == :no_match
    end

    test "filter: unevaluable/2 names the sinks a body leaves unjudged" do
      filters = [nil, filter!(%{"json.a" => "x"}), filter!(%{"event_type" => "x"})]
      assert Filter.unevaluable(filters, :skip) == [1]
      assert Filter.unevaluable(filters, {:error, :bad}) == [1]
      assert Filter.unevaluable(filters, decoded("{}")) == []
      assert Filter.unevaluable(nil, :skip) == []
    end
  end

  describe "filter: a source's sink entries" do
    test "filter: normalize_sinks!/2 strips the option and aligns the filters" do
      sinks = [
        {Log, []},
        {Http, url: "https://x.test/", filter: %{"event_type" => "a"}},
        {Log, filter: nil}
      ]

      assert {[{Log, []}, {Http, [url: "https://x.test/"]}, {Log, []}], [nil, %Filter{}, nil]} =
               Filter.normalize_sinks!("s", sinks)

      assert {[{Log, []}], nil} = Filter.normalize_sinks!("s", [{Log, []}])
    end

    test "filter: Source.new/2 builds the filters and the sink never sees the option" do
      source =
        Source.new("billing",
          sinks: [{Log, filter: %{"event_type" => ["a", "b"]}}, {Log, []}],
          event_type: :stripe
        )

      assert [%Filter{}, nil] = source.filters
      assert source.sinks == [{Log, []}, {Log, []}]
      assert Source.new("billing", sinks: [{Log, []}]).filters == nil
    end

    test "filter: Source.new/2 names the source and the sink of a bad filter" do
      error =
        assert_raise ArgumentError, fn ->
          Source.new("billing", sinks: [{Log, []}, {Log, filter: %{"nope" => "x"}}])
        end

      assert error.message =~ ~s(source "billing": sinks[1]: filter field "nope")
    end

    test "filter: describe/1 is one line" do
      filter =
        filter!(%{
          "all" => [
            %{"event_type" => ["invoice.paid", "invoice.failed"]},
            %{"header.x-github-event" => "push"},
            %{"any" => [%{"tenant" => %{"neq" => "t"}}, %{"not" => %{"json.a.b" => "c"}}]},
            %{"content_type" => %{"prefix" => "application/"}},
            %{"event_type" => %{"suffix" => ".paid"}},
            %{"event_type" => %{"not_in" => ["x"]}},
            %{"header.x" => %{"exists" => true}},
            %{"header.y" => %{"exists" => false}}
          ]
        })

      assert Filter.describe(filter) ==
               "all(event_type in [invoice.paid, invoice.failed], header.x-github-event = push, " <>
                 "any(tenant != t, not(json.a.b = c)), content_type starts with application/, " <>
                 "event_type ends with .paid, event_type not in [x], header.x exists, " <>
                 "header.y is absent)"

      assert Filter.describe(filter!(%{"event_type" => "a"})) == "event_type = a"
    end
  end

  describe "filter: what the instance's config adds" do
    defp config(overrides \\ []), do: Config.new([roles: [:edge]] ++ overrides)

    defp with_filter(term, extra \\ []) do
      Source.new("s", [sinks: [{Log, filter: term}]] ++ extra)
    end

    test "filter: an event_type condition needs a source that reads one" do
      assert Filter.check(config(), with_filter(%{"event_type" => "a"}, event_type: :github)) ==
               :ok

      assert {:error, message} = Filter.check(config(), with_filter(%{"event_type" => "a"}))
      assert message =~ ~s(source "s": sinks[0] filters on event_type)

      # Any depth.
      assert {:error, _} =
               Filter.check(config(), with_filter(%{"not" => %{"event_type" => "a"}}))

      assert Filter.check(config(), with_filter(%{"tenant" => "a"})) == :ok
      assert Filter.check(config(), Source.new("s", sinks: [{Log, []}])) == :ok
    end

    test "filter: a header condition must survive the stored-header policy" do
      header = fn name, extra -> with_filter(%{"header.#{name}" => "x"}, extra) end

      assert Filter.check(config(), header.("x-entity", [])) == :ok

      # The instance's default policy drops credential-bearing headers.
      assert {:error, message} = Filter.check(config(), header.("Authorization", []))
      assert message =~ "drops or hashes"
      assert message =~ ~s(sinks[0] filters on header "authorization")

      assert {:error, _} =
               Filter.check(config(), header.("x-entity", store_headers: %{drop: ["x-entity"]}))

      assert {:error, _} =
               Filter.check(config(), header.("x-entity", store_headers: %{hash: ["X-Entity"]}))

      assert Filter.check(config(), header.("x-entity", store_headers: %{drop: ["x-other"]})) ==
               :ok

      # A JSON path reads the body, which no policy touches.
      json = with_filter(%{"json.id" => "x"}, store_headers: %{drop: ["id"]})
      assert Filter.check(config(), json) == :ok
    end

    test "filter: a token verifier's credential header cannot be filtered on" do
      source =
        with_filter(%{"header.x-api-token" => "x"},
          verifier: {Ankusa.Verifier.Token, secret: "s3cret", location: {:header, "x-api-token"}}
        )

      assert {:error, message} = Filter.check(config(), source)
      assert message =~ "drops or hashes"
    end

    test "filter: validate_config!/1 raises for a static source, and passes the rest" do
      static = fn sources ->
        config(source_store: {Ankusa.SourceStore.Static, sources: sources})
      end

      assert :ok = Filter.validate_config!(static.(%{"ok" => [sinks: [{Log, []}]]}))

      assert :ok =
               Filter.validate_config!(
                 static.(%{
                   "ok" => [sinks: [{Log, filter: %{"event_type" => "a"}}], event_type: :github]
                 })
               )

      assert_raise ArgumentError, ~r/filters on event_type/, fn ->
        Filter.validate_config!(
          static.(%{"bad" => [sinks: [{Log, filter: %{"event_type" => "a"}}]]})
        )
      end
    end
  end

  describe "filter: Routing.resolve/4" do
    setup do
      handler = "filter-#{System.unique_integer([:positive])}"
      test = self()

      :telemetry.attach_many(
        handler,
        [[:ankusa, :dispatch, :filtered], [:ankusa, :dispatch, :filter_unevaluable]],
        fn event, _measurements, metadata, _config ->
          send(test, {List.last(event), metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)
    end

    defp billing do
      Source.new("s",
        event_type: %{json: "type"},
        sinks: [
          {Log, filter: %{"event_type" => ["invoice.paid"]}},
          {Http, url: "https://x.test/", filter: %{"header.x-github-event" => "push"}},
          {Log, []}
        ]
      )
    end

    test "filter: a hook selects the sinks its filters pass, and the others are counted" do
      r = Routing.resolve(:inst, billing(), env(body: ~s({"type":"invoice.paid"})), dedupe: true)

      assert %Routing{event_type: "invoice.paid", selected: [0, 2]} = r

      assert_receive {:filtered,
                      %{instance: :inst, source_id: "s", tenant_id: "acme", sink: Http}}

      refute_receive {:filtered, _}
      refute_receive {:filter_unevaluable, _}
    end

    test "filter: the filter is judged on the event type the source reads" do
      pushed =
        env(body: ~s({"type":"other"}), headers: [{"x-github-event", "push"}])

      assert %Routing{event_type: "other", selected: [1, 2]} =
               Routing.resolve(:inst, billing(), pushed, dedupe: true)

      assert_receive {:filtered, %{sink: Log}}
    end

    test "filter: a hook no filtered sink selects still reaches the sink without a filter" do
      assert %Routing{selected: [2]} =
               Routing.resolve(:inst, billing(), env(body: ~s({"type":"x"})), dedupe: true)
    end

    test "filter: every sink filtered out is an empty selection" do
      source =
        Source.new("s", event_type: :github, sinks: [{Log, filter: %{"event_type" => "push"}}])

      assert %Routing{selected: []} = Routing.resolve(:inst, source, env(), dedupe: true)
      assert_receive {:filtered, %{sink: Log}}
    end

    test "filter: a source without filters selects :all and counts nothing" do
      assert %Routing{selected: :all} =
               Routing.resolve(:inst, Source.new("s", sinks: [{Log, []}]), env(), dedupe: true)

      refute_receive {:filtered, _}
    end

    test "filter: a body past the cap, or not JSON, delivers and counts filter_unevaluable" do
      source = Source.new("s", sinks: [{Log, filter: %{"json.type" => "a"}}, {Log, []}])

      assert %Routing{selected: [0, 1]} =
               Routing.resolve(:inst, source, env(body: "not json"), dedupe: true)

      assert_receive {:filter_unevaluable, %{sink: Log, source_id: "s", tenant_id: "acme"}}

      big = ~s({"type":"a","pad":"#{String.duplicate("x", 1_048_576)}"})

      assert %Routing{selected: [0, 1]} =
               Routing.resolve(:inst, source, env(body: big), dedupe: true)

      assert_receive {:filter_unevaluable, %{sink: Log}}
      refute_receive {:filter_unevaluable, _}

      assert %Routing{selected: [1]} =
               Routing.resolve(:inst, source, env(body: ~s({"type":"b"})), dedupe: true)

      refute_receive {:filter_unevaluable, _}
    end

    test "filter: telemetry: false counts nothing, and a stored event type is kept" do
      source = billing()
      stored = env(body: ~s({"type":"invoice.paid"}), event_type: "invoice.paid")

      assert %Routing{selected: [0, 2]} =
               Routing.resolve(:inst, source, stored, dedupe: false, telemetry: false)

      refute_receive {:filtered, _}

      # A body that no longer yields an event type: the stored one is judged.
      gone = env(body: "{}", event_type: "invoice.paid")

      assert %Routing{event_type: "invoice.paid", selected: [0, 2]} =
               Routing.resolve(:inst, source, gone, dedupe: false, telemetry: false)
    end

    test "filter: a sink a filter dropped reads no ordering key" do
      handler = "lane-#{System.unique_integer([:positive])}"
      test = self()

      :telemetry.attach(
        handler,
        [:ankusa, :dispatch, :lane_key_missing],
        fn _event, _measurements, metadata, _config -> send(test, {:lane_missing, metadata}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      source =
        Source.new("s",
          event_type: :github,
          sinks: [
            {Log,
             ordered: true, ordering_key: {:header, "x-entity"}, filter: %{"event_type" => "push"}},
            {Log, ordered: true, ordering_key: {:header, "x-entity"}}
          ]
        )

      r = Routing.resolve(:inst, source, env(headers: [{"x-github-event", "ping"}]), dedupe: true)
      assert %Routing{selected: [1], lanes: [nil, {_lane, ""}]} = r
      assert_receive {:lane_missing, %{sink: Log}}
      refute_receive {:lane_missing, _}
    end
  end
end
