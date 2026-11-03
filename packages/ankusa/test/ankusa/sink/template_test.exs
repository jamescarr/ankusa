defmodule Ankusa.Sink.TemplateTest do
  @moduledoc """
  `{{ var }}` templates on a sink's per-hook address options: the syntax (every
  form accepted, every form refused), what a variable reads, which keys take a
  template, the stored-header check, and `Messaging.resolve/2` and
  `Sink.safe_deliver/4` on a compiled one.
  """

  use ExUnit.Case, async: true

  alias Ankusa.{Config, Envelope, Source}
  alias Ankusa.Sink

  alias Ankusa.Sink.{
    GooglePubSub,
    Http,
    Kafka,
    Log,
    Messaging,
    NATS,
    RabbitMQ,
    Redis,
    SQS,
    Template
  }

  defp env(overrides \\ %{}) do
    struct(
      %Envelope{
        id: "evt_1",
        source_id: "billing",
        tenant_id: "acme",
        received_at: 1,
        method: "POST",
        path: "/",
        headers: [{"X-GitHub-Event", "push"}, {"x-empty", ""}],
        body: "{}",
        event_type: "invoice.paid",
        size: 2
      },
      overrides
    )
  end

  defp render!(string, env \\ env()) do
    {:ok, template} = Template.compile(string)
    Template.render(template, env)
  end

  describe "template syntax" do
    test "template?/1 is a `{{` anywhere in a string" do
      assert Template.template?("a.{{ tenant }}")
      assert Template.template?("{{")
      refute Template.template?("a}}b")
      refute Template.template?(nil)
      refute Template.template?({:template, []})
    end

    test "a string is literal text and variables, in order" do
      assert {:ok, {:template, ["hooks.", {:var, "tenant", nil}, ".", {:var, "source", nil}]}} =
               Template.compile("hooks.{{ tenant }}.{{source}}")

      assert {:ok, {:template, [{:var, "event_type", "other"}, "/x"]}} =
               Template.compile("{{event_type??other}}/x")

      assert {:ok, {:template, [{:var, "header.x-github-event", nil}]}} =
               Template.compile("{{ header.X-GitHub-Event }}")
    end

    test "`}}` outside a template is literal" do
      assert {:ok, {:template, ["a}}b", {:var, "id", nil}]}} = Template.compile("a}}b{{ id }}")
    end

    test "every malformed template is refused with its reason" do
      for {string, reason} <- [
            {"{{ nope }}", ~s(unknown variable "nope")},
            {"{{ tenant", "unclosed {{"},
            {"a {{", "unclosed {{"},
            {"{{ }}", "empty {{ }}"},
            {"{{}}", "empty {{ }}"},
            {"{{ tenant ?? }}", "fallback of {{ tenant }}"},
            {"{{ tenant ?? a b }}", "fallback of {{ tenant }}"},
            {"{{ tenant ?? a?b }}", "fallback of {{ tenant }}"},
            {"{{ tenant ?? a ?? b }}", "more than one ??"},
            {"{{ header. }}", "header. needs a header name"},
            {"{{ header.x y }}", "not a valid header name"},
            {"{{ ?? a }}", ~s(unknown variable "")}
          ] do
        assert {:error, message} = Template.compile(string), string
        assert message =~ reason, "#{string}: #{message}"
      end
    end

    test "compile!/2 raises ArgumentError with the label" do
      assert_raise ArgumentError, ~r/\Atopic: unknown variable "x"/, fn ->
        Template.compile!("topic", "{{ x }}")
      end
    end

    test "a fallback may hold dots, colons, slashes, dashes and underscores" do
      assert {:ok, "events.a.b:c/d-e_f"} =
               render!("events.{{ event_type ?? a.b:c/d-e_f }}", env(%{event_type: nil}))
    end
  end

  describe "template rendering" do
    test "every variable reads its envelope field" do
      assert {:ok, "acme/billing/evt_1/invoice.paid/push"} =
               render!(
                 "{{ tenant }}/{{ source }}/{{ id }}/{{ event_type }}/{{ header.x-github-event }}"
               )
    end

    test "tenant falls back to \"default\" with no tenant" do
      assert {:ok, "default"} = render!("{{ tenant }}", env(%{tenant_id: nil}))
    end

    test "a missing value takes the fallback, else is an error naming the variable" do
      no_event = env(%{event_type: nil})
      assert {:ok, "events.other"} = render!("events.{{ event_type ?? other }}", no_event)

      assert {:error, {:template_missing, "event_type"}} =
               render!("events.{{ event_type }}", no_event)

      assert {:error, {:template_missing, "header.x-absent"}} =
               render!("a{{ header.X-Absent }}")

      assert {:ok, "a-b"} = render!("a-{{ header.x-absent ?? b }}")
    end

    test "an empty header value counts as no value" do
      assert {:error, {:template_missing, "header.x-empty"}} = render!("{{ header.x-empty }}")
      assert {:ok, "z"} = render!("{{ header.x-empty ?? z }}")
    end

    test "a value with a control character counts as no value" do
      for bad <- ["a\r\nb", "a\nb", <<"a", 0, "b">>, "a\x7Fb"] do
        evil = env(%{event_type: bad})
        assert {:error, {:template_missing, "event_type"}} = render!("{{ event_type }}", evil)
        assert {:ok, "fb"} = render!("{{ event_type ?? fb }}", evil)
      end
    end

    test "a value is rendered as it is: no encoding or sanitising" do
      assert {:ok, "/a b/../c?d=1"} =
               render!("{{ event_type }}", env(%{event_type: "/a b/../c?d=1"}))
    end

    test "static/1 is nil: no one address serves every hook" do
      {:ok, template} = Template.compile("a.{{ id }}")
      assert Template.static(template) == nil
      assert Messaging.static(template) == nil
    end
  end

  describe "Messaging.resolve/2 and safe_deliver/4" do
    test "resolve/2 renders a template, and raises MissingError for a missing variable" do
      {:ok, template} = Template.compile("ev.{{ event_type }}")
      assert Messaging.resolve(template, env()) == "ev.invoice.paid"

      assert_raise Template.MissingError, ~r/"event_type"/, fn ->
        Messaging.resolve(template, env(%{event_type: nil}))
      end

      assert %Template.MissingError{var: "event_type"} =
               catch_error(Messaging.resolve(template, env(%{event_type: nil})))
    end

    defmodule Resolving do
      @moduledoc false
      @behaviour Ankusa.Sink
      @impl true
      def deliver(env, _ctx, opts) do
        Messaging.resolve(Keyword.fetch!(opts, :address), env)
        :ok
      end
    end

    test "a template with no value is a permanent failure, any other raise stays transient" do
      {:ok, template} = Template.compile("ev.{{ event_type }}")

      assert :ok = Sink.safe_deliver(Resolving, env(), %{}, address: template)

      assert {:error, {:permanent, {:template_missing, "event_type"}} = reason} =
               Sink.safe_deliver(Resolving, env(%{event_type: nil}), %{}, address: template)

      assert Sink.classify(reason) == reason
      assert Sink.error_class(Sink.classify(reason)) == :permanent

      assert {:error, {:raised, %RuntimeError{}}} =
               Sink.safe_deliver(Resolving, env(), %{}, address: fn _env -> raise "boom" end)
    end
  end

  describe "normalize_sinks!/2" do
    defp normalize(sinks), do: Template.normalize_sinks!("s", sinks)

    test "the templatable keys of every sink are compiled, other options left alone" do
      assert [
               {Kafka, kafka},
               {RabbitMQ, rabbit},
               {NATS, nats},
               {Redis, redis},
               {SQS, sqs},
               {GooglePubSub, pubsub},
               {Http, http}
             ] =
               normalize([
                 {Kafka,
                  brokers: ["k:9092"], topic: "events.{{ event_type }}", key: "{{ tenant }}"},
                 {RabbitMQ, exchange: "x", routing_key: "{{ event_type }}"},
                 {NATS, servers: ["n:4222"], subject: "hooks.{{ tenant }}.{{ source }}"},
                 {Redis, url: "redis://r", channel: "c.{{ source }}"},
                 {SQS, queue_url: "https://sqs/q.fifo", message_group_id: "{{ tenant }}"},
                 {GooglePubSub, project: "p", topic: "t", ordering_key: "{{ id }}"},
                 {Http,
                  url: "https://h.test/t/{{ tenant }}",
                  headers: [{"x-event", "{{ event_type }}"}, {"x-static", "1"}],
                  allow_unsigned: true}
               ])

      assert {:template, ["events.", {:var, "event_type", nil}]} = kafka[:topic]
      assert {:template, [{:var, "tenant", nil}]} = kafka[:key]
      assert kafka[:brokers] == ["k:9092"]
      assert {:template, [{:var, "event_type", nil}]} = rabbit[:routing_key]
      assert rabbit[:exchange] == "x"

      assert {:template, ["hooks.", {:var, "tenant", nil}, ".", {:var, "source", nil}]} =
               nats[:subject]

      assert {:template, ["c.", {:var, "source", nil}]} = redis[:channel]
      assert redis[:url] == "redis://r"
      assert {:template, [{:var, "tenant", nil}]} = sqs[:message_group_id]
      assert {:template, [{:var, "id", nil}]} = pubsub[:ordering_key]
      assert {:template, ["https://h.test/t/", {:var, "tenant", nil}]} = http[:url]

      assert [{"x-event", {:template, [{:var, "event_type", nil}]}}, {"x-static", "1"}] =
               http[:headers]
    end

    test "a sink with no template, an Elixir function and a compiled template are untouched" do
      {:ok, compiled} = Template.compile("{{ id }}")
      fun = fn _env -> "x" end

      sinks = [
        {Kafka, brokers: ["k:9092"], topic: "plain"},
        {Kafka, brokers: ["k:9092"], topic: fun, key: compiled},
        {Http, url: fun, headers: fun, allow_unsigned: true},
        {Log, []},
        {MyApp.Custom, url: "{{ not ours }}"},
        :not_a_sink
      ]

      assert normalize(sinks) == sinks
    end

    test "a `{{` on any other option is refused, naming the key" do
      for {mod, opts, key} <- [
            {SQS, [queue_url: "https://sqs/{{ tenant }}"], "queue_url"},
            {GooglePubSub, [project: "p", topic: "{{ tenant }}"], "topic"},
            {GooglePubSub, [project: "{{ tenant }}", topic: "t"], "project"},
            {RabbitMQ, [exchange: "{{ tenant }}"], "exchange"},
            {RabbitMQ, [url: "amqp://{{ tenant }}"], "url"},
            {Redis, [url: "redis://{{ tenant }}", channel: "c"], "url"},
            {Kafka, [brokers: ["{{ tenant }}:9092"], topic: "t"], "brokers"},
            {NATS, [servers: ["{{ tenant }}:4222"], subject: "s"], "servers"},
            {Http, [url: "https://h/", secret: "{{ tenant }}"], "secret"},
            {Http, [url: "https://h/", cacertfile: "/{{ tenant }}.pem"], "cacertfile"}
          ] do
        assert_raise ArgumentError,
                     ~r/\Asource "s": sinks\[0\]: templates are not supported on #{key}\z/,
                     fn -> normalize([{mod, opts}]) end
      end
    end

    test "an Http header name or a header on a sink that takes none cannot be a template" do
      assert_raise ArgumentError, ~r/templates are not supported on header names/, fn ->
        normalize([{Http, url: "https://h/", headers: [{"x-{{ tenant }}", "1"}]}])
      end

      assert_raise ArgumentError, ~r/templates are not supported on headers/, fn ->
        normalize([{Kafka, brokers: ["k:9092"], topic: "t", headers: [{"a", "{{ tenant }}"}]}])
      end
    end

    test "credentials are never read for templates" do
      sinks = [
        {NATS, servers: ["n:4222"], subject: "s", password: "pa{{ss", token: "{{"},
        {Kafka, brokers: ["k:9092"], topic: "t", sasl: {:plain, "u", "p{{w"}}
      ]

      assert normalize(sinks) == sinks
    end

    test "a bad template names the source, the sink and the key" do
      assert_raise ArgumentError,
                   ~r/\Asource "s": sinks\[1\]\.topic: unknown variable "nope"/,
                   fn ->
                     normalize([{Log, []}, {Kafka, brokers: ["k:9092"], topic: "{{ nope }}"}])
                   end

      assert_raise ArgumentError, ~r/sinks\[0\]\.headers\.x-e: unclosed/, fn ->
        normalize([{Http, url: "https://h/", headers: [{"x-e", "{{ tenant"}]}])
      end
    end

    test "templated?/1 asks of the templatable keys only" do
      assert Template.templated?([{Kafka, topic: "a.{{ id }}"}])
      assert Template.templated?([{Http, url: "https://h/", headers: [{"x", "{{ id }}"}]}])
      refute Template.templated?([{Kafka, topic: "a"}, {Log, []}, {SQS, queue_url: "{{"}])
    end

    test "Source.new/2 compiles the sinks, and an invalid one raises naming the source" do
      source =
        Source.new("billing", sinks: [{Kafka, brokers: ["k:9092"], topic: "e.{{ event_type }}"}])

      assert [{Kafka, opts}] = source.sinks
      assert {:template, ["e.", {:var, "event_type", nil}]} = opts[:topic]

      assert_raise ArgumentError, ~r/source "billing": sinks\[0\]\.topic/, fn ->
        Source.new("billing", sinks: [{Kafka, brokers: ["k:9092"], topic: "{{ }}"}])
      end

      # a source built from an already-built source's sinks keeps them
      assert Source.new("again", sinks: source.sinks).sinks == source.sinks
    end
  end

  describe "check/2: header variables against the store_headers policy" do
    defp source(variable, extra \\ []) do
      Source.new(
        "s",
        [sinks: [{Kafka, brokers: ["k:9092"], topic: "t.{{ #{variable} }}"}]] ++ extra
      )
    end

    test "a header the policy keeps is accepted, one it drops or hashes is refused" do
      config = Config.new(roles: [:edge])

      assert Template.check(config, source("header.x-entity")) == :ok
      assert Template.check(config, source("event_type")) == :ok

      # credential headers are dropped by the instance's default policy
      assert {:error, message} = Template.check(config, source("header.Authorization"))
      assert message =~ ~s(source "s": a template reads header "authorization")
      assert message =~ "drops or hashes"

      assert {:error, _} =
               Template.check(
                 config,
                 source("header.x-entity", store_headers: %{drop: ["x-entity"]})
               )

      assert {:error, _} =
               Template.check(
                 config,
                 source("header.x-entity", store_headers: %{hash: ["x-entity"]})
               )

      assert Template.check(
               config,
               source("header.x-entity", store_headers: %{drop: ["x-other"]})
             ) ==
               :ok
    end

    test "a header read in an Http header value is checked too" do
      config = Config.new(roles: [:edge])

      http =
        Source.new("s",
          sinks: [
            {Http,
             url: "https://h/", headers: [{"x", "{{ header.cookie }}"}], allow_unsigned: true}
          ]
        )

      assert {:error, message} = Template.check(config, http)
      assert message =~ ~s("cookie")
    end

    test "validate_config!/1 raises for a static source with one" do
      config =
        Config.new(
          roles: [:edge],
          source_store: {Ankusa.SourceStore.Static, sources: %{"s" => source("header.cookie")}}
        )

      assert_raise ArgumentError, ~r/drops or hashes/, fn -> Template.validate_config!(config) end

      ok = %{config | source_store: {Ankusa.SourceStore.Static, sources: %{"s" => source("id")}}}
      assert Template.validate_config!(ok) == :ok
    end
  end
end
