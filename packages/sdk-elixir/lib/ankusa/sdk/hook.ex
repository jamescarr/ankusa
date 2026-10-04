defmodule Ankusa.SDK.Hook do
  @moduledoc """
  One delivered hook: the value a handler sees, whichever transport delivered
  it.

  * Over HTTP (`Ankusa.Sink.Http` → `Ankusa.SDK.Receiver`) the body was just
    read off the request; `received_at` is `nil`, because the HTTP sink does
    not send it.
  * From a queue (`Ankusa.Sink.RabbitMQ`/`Kafka`/`NATS`/`Redis` →
    `Ankusa.SDK.Message.to_hook/2`) `received_at` is the sink's receive time in
    unix milliseconds.

  ## Delivery is at-least-once

  A hook can arrive more than once: the dispatcher retries a sink that failed,
  a consumer requeues a message it could not handle, and neither is a bug. The
  fix is on the reader's side — dedupe on the idempotency key
  (`Ankusa.SDK.Idempotency.key/2`) rather than on the body or an arrival
  timestamp.

  `dedupe_key` is the provider's own event key when the source extracted one
  (`x-ankusa-dedupe-key` over HTTP, the message's `dedupe_key` from a queue);
  `idempotency_key` is the tenant-scoped key Ankusa computed for the hook
  (`x-ankusa-idempotency-key` over HTTP, the message's `idempotency_key` from a
  queue; `nil` from a node that predates it) — read it through
  `Ankusa.SDK.Idempotency.key/2`; `replay_id` names the replay job when this
  delivery is a replay; `headers` are the provider request headers — the
  request's over HTTP, the forwarded ones from a queue message.
  """

  @type t :: %__MODULE__{
          id: String.t(),
          source_id: String.t() | nil,
          tenant_id: String.t() | nil,
          content_type: String.t() | nil,
          body: binary(),
          received_at: non_neg_integer() | nil,
          size: non_neg_integer(),
          dedupe_key: String.t() | nil,
          replay_id: String.t() | nil,
          idempotency_key: String.t() | nil,
          headers: %{String.t() => String.t()}
        }

  defstruct [
    :id,
    :source_id,
    :tenant_id,
    :content_type,
    :body,
    :received_at,
    :size,
    :dedupe_key,
    :replay_id,
    :idempotency_key,
    headers: %{}
  ]
end
