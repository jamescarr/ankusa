defmodule AnkusaServer.LogFormatter do
  @moduledoc """
  A `:logger` formatter that writes one JSON object per line: `log.format: json`.

  A line has these keys, in this order, and omits the ones with no value:

  | Key | Value |
  | --- | --- |
  | `time` | the event's time, RFC 3339 in UTC, to the microsecond |
  | `level` | `debug`, `info`, `notice`, `warning`, `error`, ... |
  | `msg` | the message, as one string |
  | `instance` | the instance's name: the line's `instance` metadata, else the instance this node runs |
  | `hook_id` | the hook the line is about |
  | `tenant_id`, `source_id` | the hook's tenant and source (a replay or a breaker line carries the source's) |
  | `sink` | the sink module a delivery, or a refusal by one, is about |
  | `trace_id` | the 32-hex W3C trace id of the hook's `traceparent` (`Ankusa.Trace.trace_id/1`) |

  Any other `Logger` metadata a line carries follows, sorted by key (`attempt`
  and `replay_id` on a delivery, for one). The correlation is `Logger` metadata
  core sets: `Ankusa.Edge.Router` clears it per request, `Ankusa.Edge.Ingest`
  sets the hook's, `Ankusa.Sink.attempt/5` sets it for a delivery attempt and
  `Ankusa.Dispatch.Replayer` sets `replay_id` for a replay step. The metadata
  the logger itself adds (`pid`, `mfa`, `file`, `line`, `domain`, ...) is not
  printed.

  ## Redaction

  The value of any key whose name contains `secret`, `token`, `password`,
  `authorization` or `cookie` (any case) is printed as `"[REDACTED]"`, whatever
  it holds: in the metadata, and in a structured report's maps, keyword lists,
  `{key, value}` pairs (a header list) and structs, however deep. The text of a
  message is **not** scanned: a line that interpolates a secret into its text
  would print it, which is why nothing in Ankusa does.

  The formatter never raises: a term it cannot print is `inspect`ed, and an
  event it cannot format at all becomes a line that says so.
  """

  @redacted "[REDACTED]"
  @secret_words ~w(secret token password authorization cookie)

  # Printed first and in this order, when set.
  @correlation [:hook_id, :tenant_id, :source_id, :sink, :trace_id]

  # What the logger itself puts on every event: not the operator's business.
  @system_metadata [
    :pid,
    :gl,
    :time,
    :mfa,
    :file,
    :line,
    :domain,
    :report_cb,
    :error_logger,
    :erl_level,
    :crash_reason,
    :instance
  ]

  # How deep `jsonable/2` follows a term before it falls back to `inspect/2`.
  @max_depth 8

  @typedoc "The formatter's config: the instance a line belongs to when it names none."
  @type config :: %{optional(:instance) => atom() | nil}

  @doc """
  Format one `:logger` event as a JSON line.
  """
  @spec format(:logger.log_event(), config()) :: iodata()
  def format(%{level: level, msg: msg, meta: meta}, config) do
    [encode(fields(level, msg, meta, config)), ?\n]
  rescue
    exception ->
      [
        ~s({"level":),
        JSON.encode!(to_string(level)),
        ~s(,"msg":),
        JSON.encode!("a log event could not be formatted: " <> Exception.message(exception)),
        "}\n"
      ]
  end

  @doc false
  @spec check_config(term()) :: :ok | {:error, term()}
  def check_config(%{} = config) do
    case Map.get(config, :instance) do
      instance when is_atom(instance) -> :ok
      other -> {:error, {:invalid_instance, other}}
    end
  end

  def check_config(other), do: {:error, {:invalid_config, other}}

  @doc """
  Make `format` the node's log format: `:json` replaces the default handler's
  formatter with this one (`instance` is the name lines carry when they name
  none); `:text` leaves the release's own text formatter in place.
  """
  @spec install(:json | :text, atom()) :: :ok | {:error, term()}
  def install(:text, _instance), do: :ok

  def install(:json, instance),
    do: :logger.update_handler_config(:default, :formatter, {__MODULE__, %{instance: instance}})

  # ── the object ────────────────────────────────────────────────────────────

  defp fields(level, msg, meta, config) do
    leading =
      [
        {"time", time(meta)},
        {"level", Atom.to_string(level)},
        {"msg", message(msg, meta)},
        {"instance", jsonable(Map.get(meta, :instance) || Map.get(config, :instance), 0)}
      ] ++ Enum.map(@correlation, &{Atom.to_string(&1), meta_value(&1, meta)})

    rest =
      meta
      |> Map.keys()
      |> Kernel.--(@system_metadata ++ @correlation)
      |> Enum.map(&{to_string(&1), meta_value(&1, meta)})
      |> Enum.sort()

    Enum.reject(leading ++ rest, fn {_key, value} -> is_nil(value) end)
  end

  defp meta_value(key, meta) do
    case Map.get(meta, key) do
      nil -> nil
      value -> if secret_key?(key), do: @redacted, else: value |> redact() |> jsonable(0)
    end
  end

  defp encode(fields) do
    body =
      fields
      |> Enum.map(fn {key, value} -> [JSON.encode!(key), ?:, JSON.encode!(value)] end)
      |> Enum.intersperse(?,)

    [?{, body, ?}]
  end

  defp time(%{time: microseconds}) when is_integer(microseconds),
    do: microseconds |> DateTime.from_unix!(:microsecond) |> DateTime.to_iso8601()

  defp time(_meta),
    do: DateTime.utc_now() |> DateTime.to_iso8601()

  # ── the message ───────────────────────────────────────────────────────────

  defp message({:string, chardata}, _meta), do: text(chardata)

  defp message({:report, report}, meta) do
    report = redact(report)

    case report_text(report, meta) do
      {:ok, chardata} -> text(chardata)
      :error -> inspect(jsonable(report, 0), limit: :infinity, printable_limit: :infinity)
    end
  end

  defp message({format, args}, _meta) do
    text(:io_lib.format(format, args))
  rescue
    _ -> inspect({format, args}, limit: 50, printable_limit: 1_000)
  end

  # The report's own callback, over the redacted report. A callback that expects
  # a shape the redaction changed (a redacted value where a number was) fails,
  # and the report is printed as data instead.
  defp report_text(report, %{report_cb: callback}) when is_function(callback, 1) do
    {format, args} = callback.(report)
    {:ok, :io_lib.format(format, args)}
  rescue
    _ -> :error
  end

  defp report_text(report, %{report_cb: callback}) when is_function(callback, 2) do
    {:ok, callback.(report, %{depth: :unlimited, chars_limit: :unlimited, single_line: false})}
  rescue
    _ -> :error
  end

  defp report_text(_report, _meta), do: :error

  defp text(chardata) do
    binary = IO.chardata_to_string(chardata)
    if String.valid?(binary), do: binary, else: inspect(binary, limit: 1_000)
  rescue
    _ -> inspect(chardata, limit: 50, printable_limit: 1_000)
  end

  # ── redaction ─────────────────────────────────────────────────────────────

  # Keeps the term's shape: the value of a secret key is replaced, nothing else
  # changes, so a report callback can still format it.
  defp redact(%_{} = struct) do
    fields =
      struct
      |> Map.from_struct()
      |> Map.new(fn {key, value} -> {key, redact_entry(key, value)} end)

    Map.merge(struct, fields)
  end

  defp redact(%{} = map), do: Map.new(map, fn {key, value} -> {key, redact_entry(key, value)} end)

  defp redact(list) when is_list(list), do: redact_list(list)

  defp redact({key, value}) when is_atom(key) or is_binary(key),
    do: {key, redact_entry(key, value)}

  defp redact(tuple) when is_tuple(tuple),
    do: tuple |> Tuple.to_list() |> Enum.map(&redact/1) |> List.to_tuple()

  defp redact(other), do: other

  defp redact_entry(key, value), do: if(secret_key?(key), do: @redacted, else: redact(value))

  # A list that is not proper (`[a | b]`) is walked as far as it goes.
  defp redact_list([]), do: []
  defp redact_list([head | tail]), do: [redact(head) | redact_list(tail)]
  defp redact_list(improper_tail), do: redact(improper_tail)

  defp secret_key?(key) when is_atom(key) or is_binary(key) do
    name = key |> to_string() |> String.downcase()
    Enum.any?(@secret_words, &String.contains?(name, &1))
  rescue
    _ -> false
  end

  defp secret_key?(_key), do: false

  # ── a JSON-encodable value ────────────────────────────────────────────────

  defp jsonable(nil, _depth), do: nil
  defp jsonable(value, _depth) when is_boolean(value) or is_number(value), do: value
  defp jsonable(value, _depth) when is_atom(value), do: atom(value)

  defp jsonable(value, _depth) when is_binary(value),
    do: if(String.valid?(value), do: value, else: inspect(value, limit: 1_000))

  defp jsonable(value, depth) when depth >= @max_depth, do: shown(value)

  defp jsonable(%_{} = struct, depth), do: struct |> Map.from_struct() |> jsonable(depth)

  defp jsonable(%{} = map, depth),
    do: Map.new(map, fn {key, value} -> {key_string(key), jsonable(value, depth + 1)} end)

  defp jsonable(list, depth) when is_list(list) do
    cond do
      list == [] -> []
      not proper?(list) -> shown(list)
      :io_lib.printable_unicode_list(list) -> List.to_string(list)
      keyword?(list) -> list |> Map.new() |> jsonable(depth)
      true -> Enum.map(list, &jsonable(&1, depth + 1))
    end
  end

  defp jsonable(tuple, depth) when is_tuple(tuple),
    do: tuple |> Tuple.to_list() |> jsonable(depth)

  defp jsonable(other, _depth), do: shown(other)

  defp atom(value) do
    case Atom.to_string(value) do
      "Elixir." <> _ -> inspect(value)
      name -> name
    end
  end

  defp key_string(key) when is_binary(key), do: jsonable(key, 0)
  defp key_string(key) when is_atom(key), do: atom(key)
  defp key_string(key), do: shown(key)

  defp keyword?(list), do: Enum.all?(list, &match?({key, _} when is_atom(key), &1))

  defp proper?([]), do: true
  defp proper?([_head | tail]), do: proper?(tail)
  defp proper?(_improper), do: false

  defp shown(term), do: inspect(term, limit: 20, printable_limit: 1_000)
end
