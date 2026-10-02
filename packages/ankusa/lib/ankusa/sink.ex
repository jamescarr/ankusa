defmodule Ankusa.Sink do
  @moduledoc """
  What happens to a delivered hook. The interesting extension point: it lets the
  framework be a library inside a Phoenix app, a standalone forwarding gateway,
  or the front door to someone else's pipeline — with the same ingest guarantees.

  Delivery is at-least-once. Return `:ok` on success; `{:error, reason}` triggers
  the source's `Ankusa.RetryPolicy`.

  Deliveries are **not ordered**: hooks for one sink may be delivered in any
  order, and a retry runs after whatever is due before it. A Kafka partition key
  or an AMQP routing key only keeps the order hooks were *published* in, so it
  does not restore one. A consumer that needs order has to rebuild it from data
  it receives (the provider's own event timestamp or sequence number in the
  body; `received_at` is on every message, but ties are possible within a
  millisecond) and tolerate redelivery.
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
  URL). Only messaging sinks implement this; `Ankusa.Sink.Log` and
  `Ankusa.Sink.Http` have no channel to advertise and are left out.

  The description must never contain credentials or URL userinfo.
  """
  @callback describe(
              subject :: %{source_id: String.t(), tenant_id: String.t() | nil},
              opts :: keyword()
            ) :: Ankusa.Sink.Description.t()

  @optional_callbacks inline_max_bytes: 1, durable?: 1, describe: 2

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
  Resolve the inline threshold for `mod` with `opts`; `nil` for sinks that
  don't implement `c:inline_max_bytes/1`.
  """
  @spec inline_max_bytes(module(), keyword()) :: pos_integer() | nil
  def inline_max_bytes(mod, opts) do
    Code.ensure_loaded(mod)

    if function_exported?(mod, :inline_max_bytes, 1), do: mod.inline_max_bytes(opts), else: nil
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
