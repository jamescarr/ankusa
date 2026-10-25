defmodule AnkusaServer.LogTest do
  # Moves the VM's log level, adds a global handler and swaps the default
  # handler's formatter.
  use ExUnit.Case, async: false

  require Logger

  alias Ankusa.Edge.Router
  alias AnkusaServer.{Config, LogFormatter}

  @moduletag :capture_log

  @traceparent "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
  @trace_id "0af7651916cd43dd8448eb211c80319c"

  defmodule RefusingSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, _opts), do: {:error, :refused}
  end

  # A `:logger` handler that hands every event to the test process. `log/2` runs
  # in the process that logged, so an event a request logged is in the test's
  # mailbox by the time the request has been answered.
  def log(event, %{config: %{pid: pid}}), do: send(pid, {:log_event, event})

  setup do
    level = Logger.level()
    Logger.configure(level: :debug)

    handler = :"log_test_#{System.unique_integer([:positive])}"
    :ok = :logger.add_handler(handler, __MODULE__, %{level: :all, config: %{pid: self()}})

    on_exit(fn ->
      :logger.remove_handler(handler)
      Logger.configure(level: level)
    end)
  end

  defp events(acc \\ []) do
    receive do
      {:log_event, event} -> events([event | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  # The line the formatter writes for `event`, parsed; one line, ending in a newline.
  defp line(event, config \\ %{instance: nil}) do
    raw = event |> LogFormatter.format(config) |> IO.iodata_to_binary()

    assert String.ends_with?(raw, "}\n")
    assert length(String.split(raw, "\n", trim: true)) == 1

    JSON.decode!(raw)
  end

  defp event(meta, msg \\ {:string, "a message"}, level \\ :warning),
    do: %{level: level, msg: msg, meta: Map.put_new(meta, :time, 1_761_300_000_123_456)}

  # An edge-only instance on `wal.type: none` whose one source, `hooks` of tenant
  # `acme`, has a sink that refuses every hook: each request is a failing commit.
  defp boot do
    instance = :"log_#{System.unique_integer([:positive])}"
    data_dir = Path.join(System.tmp_dir!(), "ankusa_#{instance}")
    path = Path.join(System.tmp_dir!(), "ankusa_#{instance}.yml")

    File.write!(path, """
    node: {roles: [edge], data_dir: "#{data_dir}"}
    admin: {enabled: false}
    wal: {type: none}
    sources:
      hooks:
        verify: {type: none}
        sinks: [{type: http, url: "https://hooks.example.com/in", allow_unsigned: true}]
    """)

    on_exit(fn ->
      File.rm_rf(data_dir)
      File.rm(path)
    end)

    sources = %{
      "hooks" => [
        tenant_id: "acme",
        verifier: {Ankusa.Verifier.None, []},
        sinks: [{RefusingSink, []}]
      ]
    }

    config = Config.load!(path: path, env: %{}).config

    config = %{
      config
      | instance: instance,
        port: 0,
        source_store: {Ankusa.SourceStore.Static, sources: sources}
    }

    start_supervised!({Ankusa.Instance, config})
    instance
  end

  defp post(instance, headers \\ []) do
    conn =
      Enum.reduce(headers, Plug.Test.conn(:post, "/webhooks/hooks", ~s({"n":1})), fn {k, v}, c ->
        Plug.Conn.put_req_header(c, k, v)
      end)

    Router.call(conn, Router.init(instance: instance))
  end

  # The line a sink's refusal wrote, parsed.
  defp refusal(instance) do
    instance |> events_of() |> Enum.find(&(&1["msg"] =~ "refused hook"))
  end

  defp events_of(instance), do: Enum.map(events(), &line(&1, %{instance: instance}))

  describe "the line a failing request writes" do
    test "json line: a failing commit parses as JSON with the hook's source, tenant and trace" do
      instance = boot()

      assert post(instance, [{"traceparent", @traceparent}]).status == 503

      assert %{
               "level" => "warning",
               "msg" => "[ankusa] sink AnkusaServer.LogTest.RefusingSink refused hook " <> _,
               "instance" => instance_name,
               "hook_id" => hook_id,
               "tenant_id" => "acme",
               "source_id" => "hooks",
               "sink" => "AnkusaServer.LogTest.RefusingSink",
               "trace_id" => @trace_id,
               "time" => time
             } = refusal(instance)

      assert instance_name == to_string(instance)
      assert is_binary(hook_id) and hook_id != ""
      assert {:ok, %DateTime{}, 0} = DateTime.from_iso8601(time)
    end

    test "a request without a traceparent has no trace_id, and the last request's ids are gone" do
      instance = boot()

      assert post(instance, [{"traceparent", @traceparent}]).status == 503
      first = refusal(instance)
      assert first["trace_id"] == @trace_id

      assert post(instance).status == 503
      second = refusal(instance)

      assert second["source_id"] == "hooks"
      refute Map.has_key?(second, "trace_id")
      refute second["hook_id"] == first["hook_id"]
    end

    test "a hook refused before it is ingested leaves the previous request's ids off its lines" do
      instance = boot()
      assert post(instance, [{"traceparent", @traceparent}]).status == 503
      _ = events()

      # An unknown source is answered by the router, before any hook exists.
      conn = Plug.Test.conn(:post, "/webhooks/nobody", "{}")
      assert Router.call(conn, Router.init(instance: instance)).status == 404

      assert Logger.metadata()[:instance] == instance

      for key <- [:hook_id, :source_id, :tenant_id, :trace_id],
          do: refute(Keyword.has_key?(Logger.metadata(), key))
    end
  end

  describe "correlation of the other stages" do
    test "a delivery attempt's lines carry the hook, the sink, the attempt and the trace" do
      env = %Ankusa.Envelope{
        id: "hook-1",
        source_id: "hooks",
        tenant_id: "acme",
        received_at: 1,
        method: "POST",
        path: "/webhooks/hooks",
        headers: [],
        body: "{}",
        trace: %{traceparent: @traceparent, tracestate: nil}
      }

      attempt =
        Task.async(fn ->
          Ankusa.Sink.attempt(RefusingSink, env, 3, "replay-9", fn ->
            Logger.warning("delivering")
            :ok
          end)
        end)

      assert Task.await(attempt) == :ok

      assert %{
               "hook_id" => "hook-1",
               "tenant_id" => "acme",
               "source_id" => "hooks",
               "sink" => "AnkusaServer.LogTest.RefusingSink",
               "trace_id" => @trace_id,
               "attempt" => 3,
               "replay_id" => "replay-9"
             } = events() |> Enum.map(&line/1) |> Enum.find(&(&1["msg"] == "delivering"))
    end
  end

  describe "the formatter" do
    test "json line: keys come in a fixed order, then the other metadata sorted, system metadata left out" do
      raw =
        %{
          instance: :edge_1,
          hook_id: "h",
          tenant_id: "acme",
          source_id: "s",
          sink: Ankusa.Sink.Http,
          trace_id: @trace_id,
          attempt: 2,
          alpha: true,
          pid: self(),
          file: "x.ex",
          line: 1,
          mfa: {Foo, :bar, 1},
          domain: [:elixir]
        }
        |> event({:string, "hello"}, :error)
        |> LogFormatter.format(%{instance: nil})
        |> IO.iodata_to_binary()

      assert raw ==
               ~s({"time":"2025-10-24T10:00:00.123456Z","level":"error","msg":"hello",) <>
                 ~s("instance":"edge_1","hook_id":"h","tenant_id":"acme","source_id":"s",) <>
                 ~s("sink":"Ankusa.Sink.Http","trace_id":"#{@trace_id}",) <>
                 ~s("alpha":true,"attempt":2}\n)
    end

    test "the instance is the line's own, else the one the node runs, else absent" do
      assert %{"instance" => "mine"} = line(event(%{instance: :mine}), %{instance: :default})
      assert %{"instance" => "default"} = line(event(%{}), %{instance: :default})
      refute Map.has_key?(line(event(%{})), "instance")
    end

    test "every level and message shape is one JSON line" do
      for level <- [:debug, :info, :notice, :warning, :error, :critical] do
        assert %{"level" => name} = line(event(%{}, {:string, "x"}, level))
        assert name == Atom.to_string(level)
      end

      assert %{"msg" => "a b 1"} = line(event(%{}, {~c"a ~s ~p", ["b", 1]}))
      assert %{"msg" => "multi\nline"} = line(event(%{}, {:string, ["multi", ?\n, "line"]}))
      assert %{"msg" => "boom"} = line(event(%{report_cb: &name_cb/1}, {:report, %{n: "boom"}}))
      assert %{"msg" => msg} = line(event(%{}, {:report, %{n: 1}}))
      assert msg =~ ~s("n" => 1)
    end

    test "the value of a secret key is redacted in the metadata, however deep and whatever the case" do
      assert %{
               "api_token" => "[REDACTED]",
               "Cookie" => "[REDACTED]",
               "note" => "kept",
               "nested" => %{"password" => "[REDACTED]", "ok" => 1},
               "headers" => [["authorization", "[REDACTED]"], ["accept", "json"]],
               "secret_key_base" => "[REDACTED]",
               "token" => "[REDACTED]"
             } =
               %{
                 api_token: "t-1",
                 Cookie: "c-1",
                 note: "kept",
                 nested: %{password: "p-1", ok: 1},
                 headers: [{"authorization", "Bearer z-1"}, {"accept", "json"}],
                 secret_key_base: %{deep: "s-1"},
                 token: "t-2"
               }
               |> event()
               |> line()
    end

    test "a structured report's secrets are redacted, in maps, pairs and structs" do
      report = %{
        user: "u",
        password: "hunter2",
        headers: [{"Authorization", "Bearer abc"}, {"accept", "json"}],
        source: %Ankusa.Verification{status: :failed, reason: "no", provider: Foo, scheme: :x},
        creds: [token: "t-3", id: 4]
      }

      assert %{"msg" => msg} = line(event(%{}, {:report, report}))

      for secret <- ["hunter2", "Bearer abc", "t-3"], do: refute(msg =~ secret)
      assert msg =~ "[REDACTED]"
      assert msg =~ "accept"

      # A report's own callback formats the redacted report, not the original.
      assert %{"msg" => dumped} =
               line(event(%{report_cb: &dump_cb/1}, {:report, %{n: "p", password: "hunter2"}}))

      refute dumped =~ "hunter2"
      assert dumped =~ "[REDACTED]"
    end

    test "it never raises: a term it cannot print is inspected" do
      ref = make_ref()

      assert %{
               "improper" => "[1 | 2]",
               "fun" => "&String.length/1",
               "ref" => ref_text,
               "bad" => "<<255, 254>>"
             } =
               line(
                 event(%{
                   improper: [1 | 2],
                   fun: &String.length/1,
                   ref: ref,
                   bad: <<255, 254>>
                 })
               )

      assert ref_text == inspect(ref)

      # A message that is not valid text, and an event that is not an event.
      assert %{"msg" => msg} = line(event(%{}, {:string, <<255>>}))
      assert msg =~ "<<255>>"

      assert %{"level" => "error", "msg" => "a log event could not be formatted: " <> _} =
               %{level: :error, msg: :nonsense, meta: %{}}
               |> LogFormatter.format(%{})
               |> IO.iodata_to_binary()
               |> JSON.decode!()
    end

    test "check_config accepts an instance name or none, and nothing else" do
      assert LogFormatter.check_config(%{instance: :default}) == :ok
      assert LogFormatter.check_config(%{}) == :ok
      assert {:error, _} = LogFormatter.check_config(%{instance: "x"})
      assert {:error, _} = LogFormatter.check_config(:nope)
    end
  end

  describe "install" do
    # `mix test` runs without the `:default` handler; the release has it. A silent
    # one stands in, and goes again afterwards.
    setup do
      added? = match?({:error, _}, :logger.get_handler_config(:default))

      if added? do
        :ok =
          :logger.add_handler(:default, :logger_std_h, %{
            level: :none,
            config: %{type: :standard_io}
          })
      end

      {:ok, %{formatter: formatter}} = :logger.get_handler_config(:default)

      on_exit(fn ->
        if added?,
          do: :logger.remove_handler(:default),
          else: :logger.update_handler_config(:default, :formatter, formatter)
      end)

      %{formatter: formatter}
    end

    test "json makes this the default handler's formatter, text leaves it alone", %{
      formatter: formatter
    } do
      assert LogFormatter.install(:text, :default) == :ok
      assert {:ok, %{formatter: ^formatter}} = :logger.get_handler_config(:default)

      assert LogFormatter.install(:json, :ankusa) == :ok

      assert {:ok, %{formatter: {LogFormatter, %{instance: :ankusa}}}} =
               :logger.get_handler_config(:default)
    end
  end

  defp name_cb(%{n: n}), do: {~c"~s", [n]}
  defp dump_cb(report), do: {~c"~p", [report]}
end
