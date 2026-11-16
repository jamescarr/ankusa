defmodule AnkusaGatewayServer.LogFormatterTest do
  @moduledoc """
  `log.format: json` (`AnkusaGatewayServer.LogFormatter`): one JSON object per
  line, with the shape the operator greps (time first, the line's instance,
  then the correlation keys, then the other metadata sorted) and every secret
  redacted however deep it sits. It never raises, and `install/2` makes it the
  default handler's formatter and puts the old one back.
  """

  # Swaps the default handler's formatter, which is global to the VM.
  use ExUnit.Case, async: false

  alias AnkusaGatewayServer.LogFormatter

  @time 1_761_300_000_123_456

  defp line(event, config \\ %{instance: nil}) do
    raw = event |> LogFormatter.format(config) |> IO.iodata_to_binary()

    assert String.ends_with?(raw, "}\n")
    assert [_one] = String.split(raw, "\n", trim: true)

    JSON.decode!(raw)
  end

  defp event(meta, msg \\ {:string, "a message"}, level \\ :warning),
    do: %{level: level, msg: msg, meta: Map.put_new(meta, :time, @time)}

  test "a formatted line is one JSON object, the values of secret keys redacted" do
    event = %{
      level: :warning,
      msg: {:string, "gateway refused a token"},
      meta: %{
        time: @time,
        instance: :default,
        authorization: "Bearer t-1",
        nested: %{client_secret: "s-1", issuer: "https://idp.test/"}
      }
    }

    raw = event |> LogFormatter.format(%{instance: nil}) |> IO.iodata_to_binary()

    assert String.ends_with?(raw, "}\n")
    assert [_one] = String.split(raw, "\n", trim: true)

    assert %{
             "level" => "warning",
             "msg" => "gateway refused a token",
             "instance" => "default",
             "authorization" => "[REDACTED]",
             "nested" => %{"client_secret" => "[REDACTED]", "issuer" => "https://idp.test/"}
           } = JSON.decode!(raw)

    refute raw =~ "t-1"
    refute raw =~ "s-1"
  end

  test "keys come in a fixed order, then the other metadata sorted, system metadata left out" do
    raw =
      %{
        instance: :edge_1,
        hook_id: "h",
        tenant_id: "acme",
        source_id: "s",
        sink: Ankusa.Sink.Http,
        trace_id: "0af7651916cd43dd8448eb211c80319c",
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
               ~s("sink":"Ankusa.Sink.Http","trace_id":"0af7651916cd43dd8448eb211c80319c",) <>
               ~s("alpha":true,"attempt":2}\n)
  end

  test "the instance is the line's own, else the one the node runs, else absent; a line without a time gets one" do
    assert %{"instance" => "mine"} = line(event(%{instance: :mine}), %{instance: :default})
    assert %{"instance" => "default"} = line(event(%{}), %{instance: :default})
    refute Map.has_key?(line(event(%{})), "instance")

    assert %{"time" => time} = line(%{level: :info, msg: {:string, "x"}, meta: %{}})
    assert {:ok, %DateTime{}, 0} = DateTime.from_iso8601(time)
  end

  test "every level and message shape is one JSON line" do
    for level <- [:debug, :info, :notice, :warning, :error, :critical] do
      assert %{"level" => name} = line(event(%{}, {:string, "x"}, level))
      assert name == Atom.to_string(level)
    end

    assert %{"msg" => "a b 1"} = line(event(%{}, {~c"a ~s ~p", ["b", 1]}))
    assert %{"msg" => "multi\nline"} = line(event(%{}, {:string, ["multi", ?\n, "line"]}))

    # A format string whose arguments do not fit is inspected, not raised over.
    assert %{"msg" => msg} = line(event(%{}, {~c"~s ~s", ["one", self()]}))
    assert msg =~ "one"

    assert %{"msg" => "boom"} = line(event(%{report_cb: &name_cb/1}, {:report, %{n: "boom"}}))

    assert %{"msg" => dumped} =
             line(event(%{report_cb: &dump_cb/2}, {:report, %{n: "two"}}))

    assert dumped =~ "two"

    # A report without a callback is printed as data.
    assert %{"msg" => msg} = line(event(%{}, {:report, %{n: 1}}))
    assert msg =~ ~s("n" => 1)

    # A callback that expects what redaction changed fails, and the report is
    # printed as redacted data instead.
    assert %{"msg" => msg} = line(event(%{report_cb: &strict_cb/1}, {:report, %{token: 7}}))
    assert msg =~ "[REDACTED]"
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
      source: %URI{scheme: "https", host: "hooks.example.com"},
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
             "bad" => "<<255, 254>>",
             "module" => "Ankusa.Sink.Http",
             "charlist" => "abc",
             "numbers" => [1, 2],
             "keyword" => %{"a" => 1},
             "counts" => %{"1" => "one"},
             "deep" => deep_text
           } =
             line(
               event(%{
                 improper: [1 | 2],
                 fun: &String.length/1,
                 ref: ref,
                 bad: <<255, 254>>,
                 module: Ankusa.Sink.Http,
                 charlist: ~c"abc",
                 numbers: [1, 2],
                 keyword: [a: 1],
                 counts: %{1 => "one"},
                 deep: deep(12)
               })
             )

    assert ref_text == inspect(ref)
    # @max_depth is 8: the term at that depth is inspected, not walked.
    truncated = Enum.reduce(1..8, deep_text, fn _, acc -> acc["n"] end)
    assert truncated == inspect(deep(4), limit: 20, printable_limit: 1_000)

    # A message that is not valid text, and a message that is not chardata at all.
    assert %{"msg" => msg} = line(event(%{}, {:string, <<255>>}))
    assert msg =~ "<<255>>"

    assert %{"msg" => msg} = line(event(%{}, {:string, [self()]}))
    assert msg =~ "PID"

    # An event that is not an event becomes a line that says so.
    assert %{"level" => "error", "msg" => "a log event could not be formatted: " <> _} =
             %{level: :error, msg: :nonsense, meta: %{}}
             |> LogFormatter.format(%{})
             |> IO.iodata_to_binary()
             |> JSON.decode!()

    assert %{"level" => "info", "msg" => "a log event could not be formatted: " <> _} =
             %{level: :info, msg: {:string, "x"}, meta: :nope}
             |> LogFormatter.format(%{})
             |> IO.iodata_to_binary()
             |> JSON.decode!()
  end

  test "check_config accepts an instance name or none, and nothing else" do
    assert LogFormatter.check_config(%{instance: :default}) == :ok
    assert LogFormatter.check_config(%{}) == :ok
    assert {:error, {:invalid_instance, "x"}} = LogFormatter.check_config(%{instance: "x"})
    assert {:error, {:invalid_config, :nope}} = LogFormatter.check_config(:nope)
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

    test "json makes this the default handler's formatter, text puts the old one back", %{
      formatter: formatter
    } do
      assert LogFormatter.install(:text, :default) == :ok
      assert {:ok, %{formatter: ^formatter}} = :logger.get_handler_config(:default)

      assert LogFormatter.install(:json, :ankusa) == :ok

      assert {:ok, %{formatter: {LogFormatter, %{instance: :ankusa}}}} =
               :logger.get_handler_config(:default)

      # A line formatted with the installed config carries the node's instance.
      assert {:ok, %{formatter: {LogFormatter, config}}} = :logger.get_handler_config(:default)

      assert %{"instance" => "ankusa", "msg" => "hello"} =
               line(event(%{}, {:string, "hello"}), config)

      assert LogFormatter.install(:text, :ankusa) == :ok
      assert {:ok, %{formatter: ^formatter}} = :logger.get_handler_config(:default)

      # Text with nothing of ours installed leaves the release's formatter alone.
      assert LogFormatter.install(:text, :ankusa) == :ok
      assert {:ok, %{formatter: ^formatter}} = :logger.get_handler_config(:default)

      # Installing twice keeps the formatter that was there before the first
      # install, not this module.
      assert LogFormatter.install(:json, :ankusa) == :ok
      assert LogFormatter.install(:json, :ankusa) == :ok
      assert LogFormatter.install(:text, :ankusa) == :ok
      assert {:ok, %{formatter: ^formatter}} = :logger.get_handler_config(:default)
    end
  end

  defp deep(0), do: "bottom"
  defp deep(n), do: %{n: deep(n - 1)}

  defp name_cb(%{n: n}), do: {~c"~s", [n]}
  defp dump_cb(report), do: {~c"~p", [report]}
  defp dump_cb(report, _opts), do: inspect(report)

  # Expects the integer it saw before redaction, so it fails afterwards.
  defp strict_cb(%{token: n}) when is_integer(n), do: {~c"~p", [n]}
end
