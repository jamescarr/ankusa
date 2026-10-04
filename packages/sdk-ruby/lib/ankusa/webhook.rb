# frozen_string_literal: true

module Ankusa
  # The identity of one HTTP-sink delivery: the headers Ankusa's HTTP sink
  # attaches to every delivery.
  #
  # `content_type`, `tenant`, `dedupe_key`, `replay_id` and `idempotency_key` are
  # nil when the delivery had no such header (a tenant only travels when the
  # source has one; `dedupe_key` only when the source declared a dedupe key;
  # `replay_id` only when the delivery is a replay; `idempotency_key` is
  # missing only from a sender that predates the header).
  HookHeaders = Data.define(
    :id, :source, :tenant, :content_type, :dedupe_key, :replay_id, :idempotency_key
  ) do
    # `Data.define` made `idempotency_key` the reader of the parsed header; keep
    # that value reachable here and let the helper below take over the name.
    # The parsed header itself is still in `to_h[:idempotency_key]`.
    alias_method :shipped_idempotency_key, :idempotency_key
    private :shipped_idempotency_key

    # The idempotency key for this delivery: the key Ankusa shipped in
    # `x-ankusa-idempotency-key` when it is non-empty; for a delivery that
    # predates the header, `tenant:source:dedupe_key` (tenant "default" when
    # there is none) when a non-empty `dedupe_key` is set, else `id`.
    #
    # A replay keeps the original `id` and `dedupe_key` and adds `replay_id`, so
    # by default a replay produces the same key as the delivery it replays and a
    # receiver that already processed it drops it. Pass `include_replay: true`
    # to reprocess replays instead.
    def idempotency_key(include_replay: false)
      IdempotencyKey.build(
        shipped: shipped_idempotency_key, tenant: tenant, source: source,
        dedupe_key: dedupe_key, id: id, replay_id: replay_id, include_replay: include_replay
      )
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
      replay_id: presence(lowered["x-ankusa-replay-id"]),
      idempotency_key: presence(lowered["x-ankusa-idempotency-key"])
    )
  end

  # An empty header counts as absent: Ankusa only sends these when it has a
  # value, and a proxy that rewrites a header to "" should not fabricate one.
  def self.presence(value)
    (value.nil? || value.empty?) ? nil : value
  end
  private_class_method :presence
end
