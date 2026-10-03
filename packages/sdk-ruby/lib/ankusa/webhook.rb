# frozen_string_literal: true

module Ankusa
  # The identity of one HTTP-sink delivery: the headers Ankusa's HTTP sink
  # attaches to every delivery.
  #
  # `content_type`, `tenant`, `dedupe_key` and `replay_id` are nil when the
  # delivery had no such header (a tenant only travels when the source has one;
  # `dedupe_key` only when the source declared a dedupe key; `replay_id` only
  # when the delivery is a replay).
  HookHeaders = Data.define(:id, :source, :tenant, :content_type, :dedupe_key, :replay_id) do
    # The idempotency key for this delivery: `source:dedupe_key` when a non-empty
    # `dedupe_key` is set, else `id`.
    #
    # A replay keeps the original `id` and `dedupe_key` and adds `replay_id`, so
    # by default a replay produces the same key as the delivery it replays and a
    # receiver that already processed it drops it. Pass `include_replay: true`
    # to reprocess replays instead.
    def idempotency_key(include_replay: false)
      key = (dedupe_key.nil? || dedupe_key.empty?) ? id : "#{source}:#{dedupe_key}"
      key += "#replay:#{replay_id}" if include_replay && !replay_id.nil?
      key
    end
  end

  # Raised by `Ankusa.parse_headers` when `x-ankusa-id` is absent or empty.
  #
  # Every other Ankusa header is optional; this one is the identity a receiver
  # dedupes on, so a delivery without it is a framework bug, not a malformed but
  # tolerable request.
  class MissingHookIdError < Error
  end

  # Parses the `x-ankusa-*` headers of one delivery, whatever the mapping's own
  # case behavior: lookup here is always case-insensitive.
  #
  # See "HTTP handoff" in docs/integrations.md for the delivery contract this
  # mirrors. A receiver must dedupe on `x-ankusa-id`: delivery is at-least-once,
  # so the same hook can arrive twice after a retry.
  def self.parse_headers(headers)
    lowered = headers.to_h { |key, value| [key.to_s.downcase, value] }

    hook_id = lowered["x-ankusa-id"]
    raise MissingHookIdError, "missing x-ankusa-id header" if hook_id.nil? || hook_id.empty?

    HookHeaders.new(
      id: hook_id,
      source: lowered.fetch("x-ankusa-source", ""),
      tenant: lowered["x-ankusa-tenant"],
      content_type: lowered["content-type"],
      dedupe_key: presence(lowered["x-ankusa-dedupe-key"]),
      replay_id: presence(lowered["x-ankusa-replay-id"])
    )
  end

  # An empty header counts as absent: Ankusa only sends these when it has a
  # value, and a proxy that rewrites a header to "" should not fabricate one.
  def self.presence(value)
    (value.nil? || value.empty?) ? nil : value
  end
  private_class_method :presence
end
