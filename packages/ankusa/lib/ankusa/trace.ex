defmodule Ankusa.Trace do
  @moduledoc """
  W3C trace context for a hook, and the spans every stage opens (ground rule 8).

  The edge reads `traceparent` / `tracestate` off the request (`from_headers/1`)
  and stores them on the hook as `Ankusa.Envelope.trace`, a map
  `%{traceparent: "00-<trace-id>-<parent-id>-<flags>", tracestate: String.t() | nil}`.
  Every sink then carries that value to the consumer: `traceparent` on
  `Ankusa.Sink.Http`, the `traceparent` / `tracestate` fields and the
  broker-native header pair on `Ankusa.Sink.Message`, the CloudEvents
  `traceparent` extension on lifecycle events.

  ## No SDK, no cost

  Core depends on `opentelemetry_api` only. Without an SDK started (the default
  for a library user; `ankusa_server` starts one when `tracing.enabled`), `span/3`
  calls its function directly and `current/1` returns the context the request
  carried, so a `traceparent` sent to the edge reaches the consumer **byte for
  byte** and nothing else happens.

  ## With an SDK

    * `ankusa.ingest` is a `:server` span, a child of the provider's span (the
      request's `traceparent`); `ankusa.verify` and `ankusa.commit` are its
      children. With an SDK the hook stores the *ingest span's* context
      (`current/1`): same trace id, our span as the parent id, `tracestate`
      carried through, the sampled flag as the sampler decided it. The value is
      fixed at ingest, so every retry, replay and sink carries the same one.
    * `ankusa.dispatch.attempt` (one per attempt, a `:producer` span) is a child
      of the stored context and **links** to it, so the whole life of a hook is
      one trace however long the retries take. `ankusa.claim_check` is the pack
      write of a large body; `ankusa.replay` is one step of a replay job;
      `ankusa.lifecycle.event` builds a lifecycle event.

  Only the sampled bit of the trace flags is kept (`00` or `01`): the other bits
  are reserved by the specification. A request whose `traceparent` is malformed,
  repeated, version `ff`, or all-zero in its trace or parent id carries no trace
  (the header stays an ordinary forwarded request header, as before). A
  `tracestate` over 512 bytes is dropped; the `traceparent` still travels.
  """

  @typedoc "The trace context stored on a hook (`Ankusa.Envelope.trace`)."
  @type t :: %{traceparent: String.t(), tracestate: String.t() | nil}

  @typedoc "Span attributes: string keys, scalar values (an OpenTelemetry attribute)."
  @type attributes :: %{optional(String.t()) => String.t() | integer() | boolean()}

  @type span_opt ::
          {:attributes, attributes() | (-> attributes())}
          | {:kind, :internal | :server | :client | :producer | :consumer}
          | {:parent, t() | nil}
          | {:link, t() | nil}

  @max_tracestate_bytes 512
  @zero_trace "00000000000000000000000000000000"
  @zero_span "0000000000000000"

  # ── the request's context ─────────────────────────────────────────────────

  @doc """
  The trace context of a request's headers, or `nil`. One pass over the list;
  header names are matched without case. A hook without a `traceparent` pays for
  nothing but that pass.
  """
  @spec from_headers([{String.t(), String.t()}]) :: t() | nil
  def from_headers(headers), do: scan(headers, nil, 0, [])

  @doc """
  The 32-hex trace id of a trace context, `nil` for `nil`. It is what a log line
  carries as `trace_id` (`Ankusa.Edge.Ingest`), so a line can be found from the
  trace and the other way round.
  """
  @spec trace_id(t() | nil) :: String.t() | nil
  def trace_id(%{traceparent: <<"00-", id::binary-size(32), ?-, _rest::binary>>}), do: id
  def trace_id(_other), do: nil

  # A malformed `traceparent` still counts, so `[valid, malformed]` is two
  # headers: no trace.
  defp scan([], nil, _count, _states), do: nil
  defp scan([], parent, 1, states), do: %{traceparent: parent, tracestate: tracestate(states)}
  defp scan([], _parent, _count, _states), do: nil

  defp scan([{name, value} | rest], parent, count, states) when byte_size(name) == 11 do
    if ci_eq?(name, "traceparent"),
      do: scan(rest, canonical(value) || parent, count + 1, states),
      else: scan(rest, parent, count, states)
  end

  defp scan([{name, value} | rest], parent, count, states) when byte_size(name) == 10 do
    if ci_eq?(name, "tracestate"),
      do: scan(rest, parent, count, [value | states]),
      else: scan(rest, parent, count, states)
  end

  defp scan([_other | rest], parent, count, states), do: scan(rest, parent, count, states)

  # Case-insensitive match of `name` against a lowercase literal, no allocation.
  defp ci_eq?(<<a, rest::binary>>, <<b, rest_b::binary>>),
    do: (a == b or (a in ?A..?Z and a + 32 == b)) and ci_eq?(rest, rest_b)

  defp ci_eq?(<<>>, <<>>), do: true
  defp ci_eq?(_, _), do: false

  defp tracestate([]), do: nil

  defp tracestate(states) do
    joined =
      states
      |> Enum.reverse()
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.join(",")

    if joined == "" or byte_size(joined) > @max_tracestate_bytes,
      do: nil,
      else: :binary.copy(joined)
  end

  # `00-<32 hex>-<16 hex>-<2 hex>` (lowercase), version `ff` and all-zero ids
  # refused; a future version's extra fields are cut. Returns the canonical v00
  # string with the flags reduced to the sampled bit, or `nil`.
  defp canonical(value) do
    case String.trim(value) do
      <<version::binary-2, ?-, trace::binary-32, ?-, span::binary-16, ?-, flags::binary-2,
        rest::binary>>
      when trace != @zero_trace and span != @zero_span and version != "ff" ->
        if hex?(version) and hex?(trace) and hex?(span) and hex?(flags) and
             tail_ok?(version, rest),
           do: <<"00-", trace::binary, ?-, span::binary, ?-, sampled(flags)::binary>>

      _ ->
        nil
    end
  end

  defp tail_ok?("00", <<>>), do: true
  defp tail_ok?("00", _), do: false
  defp tail_ok?(_version, <<>>), do: true
  defp tail_ok?(_version, <<?-, _::binary>>), do: true
  defp tail_ok?(_version, _), do: false

  defp hex?(<<>>), do: true
  defp hex?(<<c, rest::binary>>) when c in ?0..?9 or c in ?a..?f, do: hex?(rest)
  defp hex?(_), do: false

  defp sampled(flags) do
    {int, ""} = Integer.parse(flags, 16)
    if Bitwise.band(int, 1) == 1, do: "01", else: "00"
  end

  # ── spans ─────────────────────────────────────────────────────────────────

  @doc "Whether an SDK is running (spans are recorded); `false` is the no-op API."
  @spec active?() :: boolean()
  def active? do
    case :opentelemetry.get_application_tracer(__MODULE__) do
      {:otel_tracer_noop, _} -> false
      _ -> true
    end
  end

  @doc """
  Run `fun` inside a span named `name`; returns what `fun` returns.

  Options: `:attributes` (a map, or a function that builds it: it runs only when
  an SDK records the span, so the no-op path allocates nothing), `:kind`
  (default `:internal`), `:parent` (a stored context to continue; default the
  calling process's current span) and `:link` (a stored context to link to). An
  exception in `fun` is recorded on the span and re-raised. With no SDK started
  this is `fun.()`.
  """
  @spec span(String.t(), [span_opt()], (-> result)) :: result when result: term()
  def span(name, opts, fun) when is_binary(name) and is_function(fun, 0) do
    case :opentelemetry.get_application_tracer(__MODULE__) do
      {:otel_tracer_noop, _} -> fun.()
      tracer -> traced(tracer, name, opts, fun)
    end
  end

  defp traced(tracer, name, opts, fun) do
    ctx = parent_ctx(Keyword.get(opts, :parent))

    start = %{
      attributes: attributes(Keyword.get(opts, :attributes, %{})),
      kind: Keyword.get(opts, :kind, :internal),
      links: links(Keyword.get(opts, :link))
    }

    :otel_tracer.with_span(ctx, tracer, name, start, fn span_ctx ->
      try do
        fun.()
      catch
        kind, reason ->
          stack = __STACKTRACE__
          :otel_span.record_exception(span_ctx, kind, reason, stack, [])
          :otel_span.set_status(span_ctx, :error, "exception")
          :erlang.raise(kind, reason, stack)
      end
    end)
  end

  defp parent_ctx(nil), do: :otel_ctx.get_current()

  defp parent_ctx(trace) do
    case remote(trace) do
      nil -> :otel_ctx.get_current()
      span_ctx -> :otel_tracer.set_current_span(:otel_ctx.get_current(), span_ctx)
    end
  end

  defp links(nil), do: []

  defp links(trace) do
    case remote(trace) do
      nil -> []
      span_ctx -> [:opentelemetry.link(span_ctx)]
    end
  end

  # The stored context as the SDK's remote span context, through the SDK's own
  # W3C propagator (so a malformed value cannot be built by us).
  defp remote(%{traceparent: traceparent} = trace) do
    carrier = [{"traceparent", traceparent}, {"tracestate", Map.get(trace, :tracestate) || ""}]

    :otel_ctx.new()
    |> :otel_propagator_text_map.extract_to(:otel_propagator_trace_context, carrier)
    |> :otel_tracer.current_span_ctx()
    |> case do
      :undefined -> nil
      span_ctx -> span_ctx
    end
  end

  defp remote(_), do: nil

  @doc """
  The context to store on a hook being ingested: the current span's, when an SDK
  runs and the process is inside a valid span; otherwise `fallback` (the context
  the request carried, verbatim).
  """
  @spec current(t() | nil) :: t() | nil
  def current(fallback) do
    case :opentelemetry.get_application_tracer(__MODULE__) do
      {:otel_tracer_noop, _} -> fallback
      _ -> encode_current() || fallback
    end
  end

  defp encode_current do
    carrier =
      :otel_propagator_text_map.inject_from(
        :otel_ctx.get_current(),
        :otel_propagator_trace_context,
        []
      )

    case List.keyfind(carrier, "traceparent", 0) do
      {_, traceparent} ->
        tracestate =
          case List.keyfind(carrier, "tracestate", 0) do
            {_, state} -> state
            nil -> nil
          end

        %{traceparent: traceparent, tracestate: tracestate}

      nil ->
        nil
    end
  end

  @doc "Add attributes to the calling process's current span (a no-op without one)."
  @spec set_attributes(attributes()) :: :ok
  def set_attributes(attributes) do
    _ = :otel_span.set_attributes(:otel_tracer.current_span_ctx(), attributes)
    :ok
  end

  defp attributes(fun) when is_function(fun, 0), do: fun.()
  defp attributes(map) when is_map(map), do: map

  @doc """
  Mark the current span failed. `reason` is a short class (an atom or string),
  never a raw error term: those can carry URLs and credentials.
  """
  @spec error(atom() | String.t()) :: :ok
  def error(reason) do
    _ = :otel_span.set_status(:otel_tracer.current_span_ctx(), :error, to_string(reason))
    :ok
  end
end
