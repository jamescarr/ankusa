defmodule Mix.Tasks.Ankusa.Conformance.Gen do
  @shortdoc "Writes the producer conformance vectors from Sink.Message.encode/3"

  @moduledoc """
  Writes `conformance/cases/producer.json`: the queue messages
  `Ankusa.Sink.Message.encode/3` produces for a fixed set of hooks, each paired
  with what an SDK's `decode_message` must return for it (feature
  `message.producer`).

      mix ankusa.conformance.gen [--out PATH]

  The SDKs' own vectors are decoder-only: they prove an SDK reads the messages
  somebody wrote by hand. These prove it reads the messages core writes. The
  file is generated, never edited: `mise run check:conformance` (`check.mjs`)
  runs this task into a temporary file and fails when the result differs from
  the checked-in one, so a field added to, removed from or changed in `encode/3`
  fails the build until the vectors are regenerated and every SDK decodes them.

  `--out` defaults to `conformance/cases/producer.json` of the repository (run
  the task from `packages/ankusa`). The output is deterministic: fixed ids,
  times and bodies, keys in a fixed order.
  """

  use Mix.Task

  alias Ankusa.ClaimCheck.Ref
  alias Ankusa.{Config, Envelope, Verification}
  alias Ankusa.Sink.Message

  @default_out "../../conformance/cases/producer.json"

  # The keys of a `decode_message` result (conformance/README.md), in the order
  # the vectors list them. A key the message lacks decodes to `null`, `headers`
  # to `{}`; the message's other keys (`ordering_key`, …) are not decoded.
  @output_keys ~w(v id source_id tenant_id received_at content_type size body_base64 claim
                  sha256 dedupe_key replay_id idempotency_key headers verification
                  traceparent tracestate instance region)

  @traceparent "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

  @impl Mix.Task
  def run(args) do
    out =
      case OptionParser.parse(args, strict: [out: :string]) do
        {opts, [], []} -> Keyword.get(opts, :out, @default_out)
        _ -> Mix.raise("usage: mix ankusa.conformance.gen [--out PATH]")
      end

    Mix.Task.run("app.config")
    {:ok, _} = Application.ensure_all_started(:telemetry)

    out |> Path.expand() |> tap(&File.mkdir_p!(Path.dirname(&1))) |> File.write!(render())
    Mix.shell().info("wrote #{out}")
  end

  @doc """
  The file's text: the cases `{"cases": [...]}` as pretty JSON with a trailing
  newline.
  """
  @spec render() :: String.t()
  def render do
    # `put_config/1` is the only state: two instances, one in a region.
    dir = Path.join(System.tmp_dir!(), "ankusa-conformance-gen")

    Ankusa.put_config(
      Config.new(instance: :conformance_eu, data_dir: dir, dispatch: %{region: "eu-west-1"})
    )

    Ankusa.put_config(Config.new(instance: :conformance, data_dir: dir))

    cases = Enum.map(specs(), &to_case/1)
    format({:object, [{"cases", cases}]}, 0) <> "\n"
  end

  # One hook per row: the id, the instance publishing it, the envelope, the
  # sink's context beyond `instance`, and the inline threshold it is encoded
  # with (a body over it travels as a claim).
  defp specs do
    [
      %{
        id: "producer.inline_full",
        instance: :conformance_eu,
        inline_max_bytes: 65_536,
        ctx: %{},
        env: %Envelope{
          id: "01931e8a-7c2e-7000-8000-000000000001",
          source_id: "stripe",
          tenant_id: "acme",
          received_at: 1_737_500_000_000,
          method: "POST",
          path: "/hooks/stripe",
          headers: [
            {"X-GitHub-Event", "push"},
            {"X-Multi", "a"},
            {"x-multi", "b"},
            {"Authorization", "Bearer s3cret"},
            {"X-Ankusa-Id", "never-forwarded"},
            {"Content-Length", "14"}
          ],
          content_type: "application/json",
          body: ~s({"id":"evt_1"}),
          size: 14,
          dedupe_key: "evt_1",
          verification: %Verification{status: :ok, provider: Ankusa.Verifier.Hmac},
          trace: %{traceparent: @traceparent, tracestate: "vendor=1"}
        }
      },
      claim_spec(),
      %{
        id: "producer.replay_ordered",
        instance: :conformance,
        inline_max_bytes: 65_536,
        ctx: %{replay_id: "rpl_1", ordering_key: "cus_1", forward_headers: []},
        env: %Envelope{
          id: "01931e8a-7c2e-7000-8000-000000000003",
          source_id: "orders",
          tenant_id: nil,
          received_at: 1_737_500_000_001,
          method: "POST",
          path: "/hooks/orders",
          headers: [{"x-order", "7"}],
          content_type: "text/plain",
          body: "hello",
          size: 5,
          verification: nil
        }
      },
      %{
        id: "producer.flagged",
        instance: :conformance,
        inline_max_bytes: 65_536,
        ctx: %{},
        env: %Envelope{
          id: "01931e8a-7c2e-7000-8000-000000000004",
          source_id: "github",
          tenant_id: "globex",
          received_at: 1_737_500_000_002,
          method: "POST",
          path: "/hooks/github",
          headers: [],
          content_type: nil,
          body: <<0, 1, 2, 255>>,
          size: 4,
          dedupe_key: "d-1",
          verification: %Verification{
            status: :failed,
            provider: Ankusa.Verifier.Hmac,
            reason: :no_match,
            flagged: true
          }
        }
      }
    ]
  end

  # A body over the threshold: the message carries the claim dispatch checked
  # in (`ctx.claim`) instead of the bytes.
  defp claim_spec do
    body = String.duplicate("x", 100)
    sha256 = Base.encode16(:crypto.hash(:sha256, body), case: :lower)
    ref = %Ref{tenant_id: "acme", claim_id: "01M39VMD8RA3C5HR4RBV67Y002"}

    %{
      id: "producer.claim",
      instance: :conformance_eu,
      inline_max_bytes: 64,
      ctx: %{claim: %{ref: ref, sha256: sha256}},
      env: %Envelope{
        id: "01931e8a-7c2e-7000-8000-000000000002",
        source_id: "stripe",
        tenant_id: "acme",
        received_at: 1_737_500_000_000,
        method: "POST",
        path: "/hooks/stripe",
        headers: [{"x-github-event", "push"}],
        content_type: "application/octet-stream",
        body: body,
        size: byte_size(body),
        dedupe_key: "evt_2",
        verification: %Verification{status: :ok, provider: Ankusa.Verifier.Hmac}
      }
    }
  end

  defp to_case(%{id: id, instance: instance, env: env, ctx: ctx} = spec) do
    ctx =
      Map.merge(
        %{
          instance: instance,
          source_id: env.source_id,
          tenant_id: env.tenant_id,
          attempt: 1
        },
        ctx
      )

    {:ok, message} = Message.encode(env, ctx, spec.inline_max_bytes)

    decoded = JSON.decode!(message)

    {:object,
     [
       {"id", id},
       {"feature", "message.producer"},
       {"operation", "decode_message"},
       {"input", {:object, [{"message", canonical(decoded)}]}},
       {"expect", {:object, [{"ok", expected(decoded)}]}}
     ]}
  end

  # The message as compact JSON with every object's keys sorted. `encode/4`
  # builds a map whose key order is the VM's (atom keys sort by atom-table
  # index, which differs from one run to the next), so the bytes it returns are
  # not stable; the keys and values are, and a decoder does not see the order.
  defp canonical(map) when is_map(map) do
    fields =
      map
      |> Enum.sort()
      |> Enum.map_join(",", fn {key, value} -> JSON.encode!(key) <> ":" <> canonical(value) end)

    "{" <> fields <> "}"
  end

  defp canonical(list) when is_list(list),
    do: "[" <> Enum.map_join(list, ",", &canonical/1) <> "]"

  defp canonical(scalar), do: JSON.encode!(scalar)

  # What `decode_message` returns for `decoded`.
  defp expected(decoded) do
    {:object,
     for key <- @output_keys do
       {key, Map.get(decoded, key, if(key == "headers", do: %{}))}
     end}
  end

  # Pretty JSON, two-space indent. Maps print with sorted keys, `{:object,
  # pairs}` in the order given.
  defp format({:object, []}, _level), do: "{}"
  defp format({:object, pairs}, level), do: object(pairs, level)

  defp format(map, level) when is_map(map),
    do: map |> Enum.sort() |> then(&format({:object, &1}, level))

  defp format([], _level), do: "[]"

  defp format(list, level) when is_list(list) do
    pad = indent(level + 1)
    items = Enum.map_join(list, ",\n", &(pad <> format(&1, level + 1)))
    "[\n" <> items <> "\n" <> indent(level) <> "]"
  end

  defp format(scalar, _level), do: JSON.encode!(scalar)

  defp object(pairs, level) do
    pad = indent(level + 1)

    fields =
      Enum.map_join(pairs, ",\n", fn {key, value} ->
        pad <> JSON.encode!(key) <> ": " <> format(value, level + 1)
      end)

    "{\n" <> fields <> "\n" <> indent(level) <> "}"
  end

  defp indent(level), do: String.duplicate("  ", level)
end
