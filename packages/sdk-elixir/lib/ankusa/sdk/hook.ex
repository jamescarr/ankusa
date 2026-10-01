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
  fix is on the reader's side — dedupe on `id` (a ULID assigned once, at
  ingest) rather than on the body or an arrival timestamp.
  """

  @type t :: %__MODULE__{
          id: String.t(),
          source_id: String.t() | nil,
          tenant_id: String.t() | nil,
          content_type: String.t() | nil,
          body: binary(),
          received_at: non_neg_integer() | nil,
          size: non_neg_integer()
        }

  defstruct [:id, :source_id, :tenant_id, :content_type, :body, :received_at, :size]
end
