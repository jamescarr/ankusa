defmodule Ankusa.SDK.Idempotency do
  @moduledoc """
  The idempotency key for one delivery, whichever transport delivered it.

  Delivery is at-least-once: a provider retry, a sink retry, or a requeued
  message can hand the same hook over more than once. Ankusa computes one
  tenant-scoped key per hook and ships it (the message's `idempotency_key`, the
  `x-ankusa-idempotency-key` header); this module returns that value. Read it,
  don't rebuild it.

  For a message or delivery from a node that predates the field, the key is
  computed from the hook's own fields, by the same rule core uses:

      tenant <> ":" <> source_id <> ":" <> dedupe_key   when dedupe_key is non-null and non-empty
      id                                                 otherwise

  where `tenant` is `"default"` when there is none. A replay of an older
  delivery carries a `replay_id`. The key ignores it by default, so a consumer
  that already processed the original drops the replay; a consumer that must
  reprocess replays passes `include_replay: true`, which appends
  `"#replay:" <> replay_id`.

  Accepts the decoded `Ankusa.SDK.Message`, the parsed
  `Ankusa.SDK.Webhook.Headers` from an HTTP delivery, or an
  `Ankusa.SDK.Hook` (where `source_id` already holds the message's
  `source_id`/the header's `source`).

  ```elixir
  {:ok, message} = Ankusa.SDK.Message.decode(body)
  key = Ankusa.SDK.Idempotency.key(message)
  key = Ankusa.SDK.Idempotency.key(message, include_replay: true)
  ```
  """

  alias Ankusa.SDK.{Hook, Message, Webhook}

  @doc """
  Compute the idempotency key for `message_or_headers`.

  Options: `:include_replay` (default `false`) appends the replay marker when
  the delivery carries a `replay_id`.
  """
  @spec key(Message.t() | Webhook.Headers.t() | Hook.t(), keyword()) :: String.t()
  def key(message_or_headers, opts \\ [])

  def key(%Message{} = message, opts) do
    build(
      message.idempotency_key,
      message.tenant_id,
      message.source_id,
      message.id,
      message.dedupe_key,
      message.replay_id,
      opts
    )
  end

  def key(%Webhook.Headers{} = headers, opts) do
    build(
      headers.idempotency_key,
      headers.tenant,
      headers.source,
      headers.id,
      headers.dedupe_key,
      headers.replay_id,
      opts
    )
  end

  def key(%Hook{} = hook, opts) do
    build(
      hook.idempotency_key,
      hook.tenant_id,
      hook.source_id,
      hook.id,
      hook.dedupe_key,
      hook.replay_id,
      opts
    )
  end

  defp build(shipped, tenant, source_id, id, dedupe_key, replay_id, opts) do
    base =
      cond do
        is_binary(shipped) and shipped != "" ->
          shipped

        is_binary(dedupe_key) and dedupe_key != "" ->
          "#{tenant || "default"}:#{source_id}:#{dedupe_key}"

        true ->
          id
      end

    if Keyword.get(opts, :include_replay, false) and not is_nil(replay_id) do
      base <> "#replay:" <> replay_id
    else
      base
    end
  end
end
