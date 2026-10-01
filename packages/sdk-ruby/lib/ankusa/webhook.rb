# frozen_string_literal: true

module Ankusa
  # The identity of one HTTP-sink delivery: the headers Ankusa's HTTP sink
  # attaches to every delivery.
  #
  # `content_type` and `tenant` are nil when the delivery had no such header
  # (a tenant only travels when the source has one).
  HookHeaders = Data.define(:id, :source, :tenant, :content_type)

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
      content_type: lowered["content-type"]
    )
  end
end
