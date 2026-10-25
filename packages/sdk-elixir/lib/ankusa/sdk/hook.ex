defmodule Ankusa.SDK.Hook do
  @moduledoc """
  One delivered hook: the value a handler sees, whichever transport delivered
  it.

  * Over HTTP (`Ankusa.Sink.Http` → `Ankusa.SDK.Receiver`) the body was just
    read off the request; `received_at` is the `x-ankusa-received-at` header
    (unix milliseconds), or `nil` from a sink that predates it.
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

  ## Verification

  `verification` says how the *node* verified the hook at ingest: `%{status:
  "ok" | "failed" | "skipped", flagged: boolean()}`, or `nil` from a node that
  predates the field. A source with `on_verify_failure: accept_flag` commits a
  hook whose signature check failed, with `flagged: true`; nothing about that
  hook proves the provider sent it, and neither does the tenant it names. Ask
  `trusted?/1` before acting on a hook whose origin matters.
  """

  @type verification :: %{status: String.t(), flagged: boolean()}

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
          verification: verification() | nil,
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
    :verification,
    headers: %{}
  ]

  @doc """
  Whether the node verified the hook at ingest and nothing flags it: `true`
  only for `status: "ok"` with `flagged: false`.

  A flagged hook (accepted despite a failed check), a `"failed"` or `"skipped"`
  one (a source with no verifier checked nothing), any other status, and a hook
  with no `verification` (a node older than the field) are all untrusted, so
  the answer fails closed. Takes a `Hook`, a decoded `Ankusa.SDK.Message` or the
  `Ankusa.SDK.Webhook.Headers` of a delivery.

  ```elixir
  if Ankusa.SDK.Hook.trusted?(hook), do: act_on(hook), else: quarantine(hook)
  ```
  """
  @spec trusted?(%{verification: verification() | nil}) :: boolean()
  def trusted?(%{verification: %{status: "ok", flagged: false}}), do: true
  def trusted?(%{verification: _other}), do: false
end
