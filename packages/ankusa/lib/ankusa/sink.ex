defmodule Ankusa.Sink do
  @moduledoc """
  What happens to a delivered hook. The interesting extension point: it lets the
  framework be a library inside a Phoenix app, a standalone forwarding gateway,
  or the front door to someone else's pipeline — with the same ingest guarantees.

  Delivery is at-least-once. Return `:ok` on success; `{:error, reason}` triggers
  the source's `Ankusa.RetryPolicy`.

  ## Error classes

  Three shapes of `reason` change what dispatch does with a failure:

    * `{:permanent, term}` — retrying cannot help (a `410 Gone`, a record the
      broker will never take). The row is dead-lettered after this attempt,
      whatever attempts the retry policy had left, and the failure does not
      count towards the sink's circuit breaker.
    * `{:permanent, {:config, term}}` — a permanent failure that is the
      operator's to fix, not the hook's: a credential the destination refuses
      (`AccessDenied`, `PERMISSION_DENIED`), a queue or topic that does not
      exist. It dead-letters like any `{:permanent, _}` and also counts in
      `ankusa_dispatch_config_errors_total{sink}`, the alert that says a source
      needs a config fix and then a DLQ replay.
    * `{:retry_after, ms, term}` — the sink asked for a pause (`Retry-After`, or
      a throttle: AWS `Throttling`, Pub/Sub `RESOURCE_EXHAUSTED`). The next
      attempt is no sooner than `ms` (capped at `dispatch.max_retry_after_ms`,
      default one hour) or the policy's own backoff, whichever is later; the
      policy still decides when to give up.

  Every other `reason` is transient and follows the retry policy. Whatever the
  class, a delivery that is still failing `max_elapsed_ms` after its first
  attempt (default 24 hours, per source: `retry:`) is dead-lettered
  (`Ankusa.RetryPolicy`).

  Deliveries are **not ordered**: hooks for one sink may be delivered in any
  order, and a retry runs after whatever is due before it. A Kafka partition key
  or an AMQP routing key only keeps the order hooks were *published* in, so it
  does not restore one. A consumer that needs order has to rebuild it from data
  it receives (the provider's own event timestamp or sequence number in the
  body; `received_at` is on every message, but ties are possible within a
  millisecond) and tolerate redelivery.
  """

  alias Ankusa.{Envelope, Trace}

  @type ctx :: %{
          required(:instance) => atom(),
          required(:source_id) => String.t(),
          required(:tenant_id) => String.t() | nil,
          required(:attempt) => pos_integer(),
          # the envelope's claim-check ref, when dispatch already checked its
          # body in (see `c:inline_max_bytes/1`)
          optional(:claim) => Ankusa.ClaimCheck.claim(),
          # the replay job id, set on every delivery a replay job re-drives
          optional(:replay_id) => String.t(),
          # which provider request headers travel into the sink message
          # (`Ankusa.Sink.Message.forwarded_headers/2`); absent means `:default`
          optional(:forward_headers) => :default | [String.t()],
          # set by dispatch for a sink whose `c:batched?/1` is true: this attempt
          # may wait in the sink's batcher for its batch's response
          optional(:batch) => boolean(),
          # the source's caps on those headers (count; name plus value bytes);
          # absent means 64 and 16 KiB
          optional(:max_headers) => pos_integer(),
          optional(:forward_headers_max_bytes) => pos_integer(),
          optional(atom()) => term()
        }

  @typedoc "What `c:deliver/3` may return; see \"Error classes\"."
  @type failure :: {:permanent, term()} | {:retry_after, pos_integer(), term()} | term()

  @typedoc "What `classify/1` answers."
  @type class ::
          {:permanent, term()} | {:retry_after, pos_integer(), term()} | {:transient, term()}

  @callback deliver(Envelope.t(), ctx(), opts :: keyword()) :: :ok | {:error, failure()}

  @doc """
  The class of a sink's error `reason` (see "Error classes"): `{:permanent,
  term}`, `{:retry_after, ms, term}` with a positive integer `ms`, or
  `{:transient, reason}` for anything else.
  """
  @spec classify(term()) :: class()
  def classify({:permanent, _term} = reason), do: reason

  def classify({:retry_after, ms, _term} = reason) when is_integer(ms) and ms > 0, do: reason

  def classify(reason), do: {:transient, reason}

  @typedoc """
  Why a delivery was dead-lettered, as an operator filters on it
  (`error_class` of `GET /v1/dlq` and of a replay job): `:permanent` (the sink
  said it will never work, or the hook or its destination is gone), `:throttled`
  (the destination asked for a pause, `{:retry_after, ms, reason}`, and the
  retry horizon ended it) or `:transient` (anything else that kept failing until
  the retry policy gave up).
  """
  @type error_class :: :permanent | :throttled | :transient

  @error_classes [:permanent, :throttled, :transient]

  @doc "Every `t:error_class/0`."
  @spec error_classes() :: [error_class()]
  def error_classes, do: @error_classes

  @doc "The `t:error_class/0` of a `classify/1` answer."
  @spec error_class(class()) :: error_class()
  def error_class({:permanent, _term}), do: :permanent
  def error_class({:retry_after, _ms, _term}), do: :throttled
  def error_class({:transient, _term}), do: :transient

  @throttle_base_ms 500
  @throttle_cap_ms 30_000

  @doc """
  The pause a sink asks for when its destination throttles it and says nothing
  about how long (`{:retry_after, ms, reason}`): full jitter, a uniform draw
  from `1..min(30_000, 500 * 2^(attempt - 1))` milliseconds, so every throttled
  delivery of a source does not come back in the same instant. `attempt` is
  `ctx.attempt`.
  """
  @spec throttle_after_ms(pos_integer()) :: pos_integer()
  def throttle_after_ms(attempt) when is_integer(attempt) and attempt >= 1 do
    ceiling = min(@throttle_cap_ms, @throttle_base_ms * Integer.pow(2, min(attempt - 1, 20)))
    :rand.uniform(ceiling)
  end

  @doc """
  The largest body this sink sends inline, or `nil` if it never uses the claim
  check.

  Dispatch checks a body in **once**, before any sink runs, when it is larger
  than at least one of its source's sinks' thresholds, and hands the resulting
  ref to every sink and every retry in `ctx.claim`. A sink that declares a
  threshold must use that ref rather than checking the body in itself.
  """
  @callback inline_max_bytes(opts :: keyword()) :: pos_integer() | nil

  @doc """
  Does `:ok` from this sink mean the hook is durably accepted by something that
  outlives this node — a broker ack, a publisher confirm, an upstream `2xx`?

  Defaults to `true`: every shipped sink except `Ankusa.Sink.Log` confirms
  durably. `wal.type: none` acks the provider on this promise, so a sink that
  cannot make it must say so.
  """
  @callback durable?(opts :: keyword()) :: boolean()

  @doc """
  Where this sink publishes, for the AsyncAPI document (`Ankusa.AsyncApi`).

  `subject` names the hook the description is for: `source_id`, and `tenant_id`
  (`nil` when the tenant varies per hook, i.e. a resolver that reads it from the
  URL). Messaging sinks describe the channel they publish the envelope to;
  `Ankusa.Sink.Http` describes its destination with `payload: :body` and says
  which options it `computed` per hook, and `Ankusa.Sink.Log` has no channel to
  advertise. Only `payload: :message` descriptions reach the AsyncAPI document.

  The description must never contain credentials or URL userinfo.
  """
  @callback describe(
              subject :: %{source_id: String.t(), tenant_id: String.t() | nil},
              opts :: keyword()
            ) :: Ankusa.Sink.Description.t()

  @doc """
  Does this sink batch deliveries (`Ankusa.Sink.Batcher`)?

  Dispatch runs a batching sink's attempts without counting them against
  `dispatch.sink_concurrency`, a tenant's `concurrency` or the global
  `dispatch.concurrency`: an attempt waits in the batcher for its batch's
  response, so those limits cannot count messages and still let a batch fill.
  The sink's batcher bounds what it holds (a queue cap, a per-tenant share) and
  counts *requests* against `sink_concurrency`; the claimed-row windows
  (`dispatch.max_inflight`, `dispatch.tenants`) bound the attempts.

  Defaults to `false`. A sink that returns `true` must batch only for a delivery
  whose ctx carries `batch: true`, which dispatch sets and `Ankusa.Edge.Publish`
  and `Ankusa.Lifecycle.Publisher` do not.
  """
  @callback batched?(opts :: keyword()) :: boolean()

  @optional_callbacks inline_max_bytes: 1, durable?: 1, describe: 2, batched?: 1

  @doc """
  Resolve `c:batched?/1` for `mod` with `opts`; `false` for a sink that doesn't
  implement it, or whose callback raises or answers anything but `true`.
  """
  @spec batched?(module(), keyword()) :: boolean()
  def batched?(mod, opts) do
    Code.ensure_loaded?(mod) and function_exported?(mod, :batched?, 1) and
      mod.batched?(opts) == true
  rescue
    _error -> false
  catch
    _kind, _reason -> false
  end

  @doc """
  Resolve `c:describe/2` for `mod`; `nil` for a sink that doesn't implement it.
  """
  @spec describe(module(), %{source_id: String.t(), tenant_id: String.t() | nil}, keyword()) ::
          Ankusa.Sink.Description.t() | nil
  def describe(mod, subject, opts) do
    if Code.ensure_loaded?(mod) and function_exported?(mod, :describe, 2),
      do: mod.describe(subject, opts),
      else: nil
  end

  @doc """
  Resolve the inline threshold for `mod` with `opts`: `{:ok, nil}` for sinks
  that don't implement `c:inline_max_bytes/1`.

  The callback is user code. A raise, exit, throw or a value other than `nil`
  or a positive integer is an `{:error, reason}` — never a crash of the caller,
  which may be the dispatch pipeline or a request process.
  """
  @spec inline_max_bytes(module(), keyword()) :: {:ok, pos_integer() | nil} | {:error, term()}
  def inline_max_bytes(mod, opts) do
    Code.ensure_loaded(mod)

    if function_exported?(mod, :inline_max_bytes, 1) do
      case mod.inline_max_bytes(opts) do
        nil -> {:ok, nil}
        max when is_integer(max) and max > 0 -> {:ok, max}
        other -> {:error, {:bad_return, other}}
      end
    else
      {:ok, nil}
    end
  rescue
    error -> {:error, {:raised, error}}
  catch
    :exit, reason -> {:error, {:exit, reason}}
    :throw, value -> {:error, {:throw, value}}
  end

  @doc """
  Resolve `durable?/1` for `mod` with `opts`; `true` for sinks that don't
  implement it — a sink that has not said otherwise is assumed to confirm
  durably, which is the safe default for an ack path (`wal.type: none`).
  """
  @spec durable?(module(), keyword()) :: boolean()
  def durable?(mod, opts) do
    Code.ensure_loaded(mod)

    if function_exported?(mod, :durable?, 1), do: mod.durable?(opts), else: true
  end

  @doc """
  Call `c:deliver/3`, turning a raise, throw, or exit into `{:error, reason}`.

  A sink is user code: it may raise, throw, or exit (a `GenServer.call` into a
  dead process). Any of those is a delivery failure, not a caller crash. Any
  other return value is `{:error, {:bad_return, value}}`. Every ack path —
  `Ankusa.Dispatch.Pipeline`, `Ankusa.Lifecycle.Publisher` and
  `Ankusa.Edge.Publish` — delivers through here so they agree on what "the sink
  failed" means, and each can match on `:ok | {:error, _}` exhaustively.
  """
  @spec safe_deliver(module(), Envelope.t(), ctx(), keyword()) :: :ok | {:error, term()}
  def safe_deliver(mod, env, ctx, opts) do
    case mod.deliver(env, ctx, opts) do
      :ok -> :ok
      {:error, _reason} = error -> error
      other -> {:error, {:bad_return, other}}
    end
  rescue
    error -> {:error, {:raised, error}}
  catch
    :exit, reason -> {:error, {:exit, reason}}
    :throw, value -> {:error, {:throw, value}}
  end

  @doc """
  Run one delivery attempt of `env` inside an `ankusa.dispatch.attempt` span
  (`Ankusa.Trace`): a child of the hook's stored trace context and linked to it,
  so every attempt of a hook, however late, is in its trace. `fun` is the whole
  attempt, and may return `:ok`, `{:error, reason}`, or either of those paired
  with anything (`Ankusa.Dispatch.Pipeline` also reports a claim it checked in).
  A failure marks the span with its class (`permanent`, `retry_after`,
  `transient`), never the reason. With no OpenTelemetry SDK running this is
  `fun.()`.

  The attempt's process gets the hook's correlation as `Logger` metadata
  (`hook_id`, `source_id`, `tenant_id`, `sink`, `attempt`, `trace_id`, and
  `replay_id` for a replayed row), so whatever the sink logs while it delivers
  names the hook. Every caller runs it in a process of its own (a task per
  attempt).
  """
  @spec attempt(module(), Envelope.t(), pos_integer(), String.t() | nil, (-> result)) :: result
        when result: :ok | {:error, term()} | {:ok | {:error, term()}, term()}
  def attempt(mod, %Envelope{} = env, number, replay_id, fun) when is_function(fun, 0) do
    Logger.metadata(
      hook_id: env.id,
      source_id: env.source_id,
      tenant_id: env.tenant_id,
      sink: mod,
      attempt: number,
      trace_id: Trace.trace_id(env.trace),
      replay_id: replay_id
    )

    Trace.span(
      "ankusa.dispatch.attempt",
      [
        kind: :producer,
        parent: env.trace,
        link: env.trace,
        attributes: fn ->
          %{
            "ankusa.sink" => inspect(mod),
            "ankusa.attempt" => number,
            "ankusa.hook.id" => env.id,
            "ankusa.source_id" => env.source_id,
            "ankusa.tenant_id" => env.tenant_id || "default"
          }
          |> put_replay(replay_id)
        end
      ],
      fn ->
        result = fun.()
        mark_failed(result)
        result
      end
    )
  end

  defp put_replay(attributes, replay_id) when is_binary(replay_id),
    do: Map.put(attributes, "ankusa.replay_id", replay_id)

  defp put_replay(attributes, _none), do: attributes

  defp mark_failed(:ok), do: :ok
  defp mark_failed({:error, reason}), do: Trace.error(reason |> classify() |> elem(0))
  defp mark_failed({result, _extra}), do: mark_failed(result)
end
