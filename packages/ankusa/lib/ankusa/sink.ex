defmodule Ankusa.Sink do
  @moduledoc """
  What happens to a delivered hook. The interesting extension point: it lets the
  framework be a library inside a Phoenix app, a standalone forwarding gateway,
  or the front door to someone else's pipeline — with the same ingest guarantees.

  Delivery is at-least-once. Return `:ok` on success; `{:error, reason}` triggers
  the source's `Ankusa.RetryPolicy`.
  """

  alias Ankusa.Envelope

  @type ctx :: %{
          required(:instance) => atom(),
          required(:source_id) => String.t(),
          required(:tenant_id) => String.t() | nil,
          required(:attempt) => pos_integer(),
          # the envelope's claim-check ref, when dispatch already checked its
          # body in (see `c:inline_max_bytes/1`)
          optional(:claim) => Ankusa.ClaimCheck.claim(),
          optional(atom()) => term()
        }

  @callback deliver(Envelope.t(), ctx(), opts :: keyword()) :: :ok | {:error, term()}

  @doc """
  The ordering scope for this delivery, or `nil` for "no constraint".

  Deliveries to the same sink with an equal `ordering_key/2` are **never in
  flight at the same time**, and they run one at a time, in the order dispatch
  read them from the log. Different keys run concurrently, which is what lets
  dispatch fan out without giving up per-key ordering. `nil` opts the delivery
  out of ordering entirely.

  `wal.type: none` publishes inside the request and so imposes no ordering at
  all — the destination's own keying is the only ordering in that mode.

  A sink's ordering key must be at least as narrow as the ordering its
  destination actually guarantees — a Kafka topic partition key, an AMQP
  routing key, a downstream row id. Claiming a wider scope than the key
  guarantees (nothing) is a correctness bug, not a perf knob.
  """
  @callback ordering_key(Envelope.t(), opts :: keyword()) :: term() | nil

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

  @optional_callbacks ordering_key: 2, inline_max_bytes: 1, durable?: 1

  @doc """
  Resolve the inline threshold for `mod` with `opts`; `nil` for sinks that
  don't implement `c:inline_max_bytes/1`.
  """
  @spec inline_max_bytes(module(), keyword()) :: pos_integer() | nil
  def inline_max_bytes(mod, opts) do
    Code.ensure_loaded(mod)

    if function_exported?(mod, :inline_max_bytes, 1), do: mod.inline_max_bytes(opts), else: nil
  end

  @doc """
  Resolve the ordering key for `mod` with `opts`.

  Sinks that don't implement `ordering_key/2` get the conservative default
  `{tenant_id, source_id}`: a sink that has not reasoned about its own ordering
  guarantees gets serialization per source rather than silently interleaved
  deliveries.
  """
  @spec ordering_key(module(), Envelope.t(), keyword()) :: term() | nil
  def ordering_key(mod, env, opts) do
    Code.ensure_loaded(mod)

    if function_exported?(mod, :ordering_key, 2) do
      mod.ordering_key(env, opts)
    else
      {env.tenant_id, env.source_id}
    end
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
  dead process). Any of those is a delivery failure, not a caller crash. Both
  ack paths — `Ankusa.Dispatch.Pipeline` and `Ankusa.Edge.Publish` — deliver
  through here so they agree on what "the sink failed" means.
  """
  @spec safe_deliver(module(), Envelope.t(), ctx(), keyword()) :: :ok | {:error, term()}
  def safe_deliver(mod, env, ctx, opts) do
    mod.deliver(env, ctx, opts)
  rescue
    error -> {:error, {:raised, error}}
  catch
    :exit, reason -> {:error, {:exit, reason}}
    :throw, value -> {:error, {:throw, value}}
  end
end
