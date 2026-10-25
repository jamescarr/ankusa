# frozen_string_literal: true

module Ankusa
  # How Ankusa verified the hook at ingest. `status` is `ok`, `failed` or
  # `skipped`, but other values pass through untouched (forward-compatible).
  # `flagged` is true when a failed check was accepted
  # (`on_verify_failure: accept_flag`): treat the delivery as unverified.
  Verification = Data.define(:status, :flagged) do
    # Whether the node verified the hook and nothing flags it: true only for
    # status "ok" with `flagged` false. A flagged hook, a "failed" or "skipped"
    # one, and any other status are untrusted.
    def trusted?
      status == "ok" && flagged == false
    end
  end

  # The identity of one HTTP-sink delivery: the headers Ankusa's HTTP sink
  # attaches to every delivery.
  #
  # `content_type`, `tenant`, `dedupe_key`, `replay_id` and `idempotency_key` are
  # nil when the delivery had no such header (a tenant only travels when the
  # source has one; `dedupe_key` only when the source declared a dedupe key;
  # `replay_id` only when the delivery is a replay; `idempotency_key` is
  # missing only from a sender that predates the header). `verification` is nil
  # when `x-ankusa-verification` is absent or empty.
  HookHeaders = Data.define(
    :id, :source, :tenant, :content_type, :dedupe_key, :replay_id, :idempotency_key, :verification
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

    # Whether the node verified this delivery at ingest and nothing flags it
    # (see `Verification#trusted?`). A delivery with no `x-ankusa-verification`
    # header (a node older than the field) is untrusted.
    def trusted?
      verification&.trusted? == true
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
      idempotency_key: presence(lowered["x-ankusa-idempotency-key"]),
      verification: parse_verification(lowered)
    )
  end

  # An empty header counts as absent: Ankusa only sends these when it has a
  # value, and a proxy that rewrites a header to "" should not fabricate one.
  def self.presence(value)
    (value.nil? || value.empty?) ? nil : value
  end
  private_class_method :presence

  # `nil` when `x-ankusa-verification` is absent or empty; otherwise the
  # header's value as `status`, and `flagged` true exactly when
  # `x-ankusa-verification-flagged` is the literal string "true".
  def self.parse_verification(lowered)
    status = presence(lowered["x-ankusa-verification"])
    return nil if status.nil?

    Verification.new(status: status, flagged: lowered["x-ankusa-verification-flagged"] == "true")
  end
  private_class_method :parse_verification

  # An accepted delivery: its identity (`HookHeaders`), and whether its
  # signature verified (`false` only for an unsigned delivery `allow_unsigned`
  # let through).
  ReceivedDelivery = Data.define(:headers, :verified)

  # Judges one HTTP-sink delivery: is it signed, and what is its identity.
  #
  # Ankusa's HTTP sink signs every delivery unless it is configured
  # `allow_unsigned: true`, so a receiver is closed by default: without the
  # sink's `secrets` (or an explicit `allow_unsigned: true`) it refuses every
  # delivery, because any host that can reach the receiver's URL could
  # otherwise inject hooks. With `secrets` the delivery MUST verify
  # (`Ankusa.verify_signature`); `allow_unsigned` does not weaken that.
  #
  # `body` must be the raw bytes received. Raises `InvalidSignatureError`
  # (answer 401) when there is no valid signature -- `invalid_secret` when no
  # secret is configured and `allow_unsigned` is not set -- and
  # `MissingHookIdError` (400) when `x-ankusa-id` is absent. The signature is
  # judged first.
  def self.receive_delivery(headers, body, secrets: [], allow_unsigned: false, tolerance_seconds: 300, now: nil)
    configured = Array(secrets)
    verified =
      if !configured.empty?
        verify_signature(headers, body, configured, tolerance_seconds: tolerance_seconds, now: now)
        true
      elsif allow_unsigned
        false
      else
        raise InvalidSignatureError.new(
          "no secret configured: deliveries are signed, so the receiver needs the sink's secret " \
          "(or allow_unsigned: true)",
          code: "invalid_secret"
        )
      end

    ReceivedDelivery.new(headers: parse_headers(headers), verified: verified)
  end
end
