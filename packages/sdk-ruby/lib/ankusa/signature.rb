# frozen_string_literal: true

require "openssl"

module Ankusa
  # A delivery whose Standard Webhooks signature does not verify. Never
  # retryable: answer `401`; the sender retries with the same bytes.
  #
  # `code` is one of `invalid_secret`, `missing_header`, `invalid_timestamp`,
  # `timestamp_out_of_tolerance`, `no_matching_signature`; `field` names the
  # header at fault (nil for `invalid_secret`).
  class InvalidSignatureError < Error
    attr_reader :code, :field

    def initialize(message, code:, field: nil)
      super(message)
      @code = code
      @field = field
    end

    def retryable? = false
  end

  # A verified delivery's `webhook-id` and `webhook-timestamp`.
  VerifiedSignature = Data.define(:id, :timestamp)

  BASE64_PATTERN = %r{\A(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?\z}
  private_constant :BASE64_PATTERN

  # Verifies the Standard Webhooks signature an HTTP sink with a `secret` adds
  # (https://www.standardwebhooks.com/).
  #
  # `webhook-signature` holds space-separated `v1,<base64>` entries, each an
  # HMAC-SHA256 over `<webhook-id>.<webhook-timestamp>.<body>`. Any `v1` entry
  # matching any secret (`whsec_` + base64, or any other string used as its own
  # bytes) passes, compared in constant time, provided `webhook-timestamp` is
  # within `tolerance_seconds` of `now` (the clock by default). `body` must be
  # the raw bytes received. Raises `InvalidSignatureError`.
  def self.verify_signature(headers, body, secrets, tolerance_seconds: 300, now: nil)
    keys = signature_keys(Array(secrets))
    lowered = headers.to_h { |key, value| [key.to_s.downcase, value] }

    required = lambda do |name|
      value = lowered[name]
      raise InvalidSignatureError.new("missing #{name} header", code: "missing_header", field: name) if value.nil? || value.empty?

      value
    end

    hook_id = required.call("webhook-id")
    raw_timestamp = required.call("webhook-timestamp")
    signature = required.call("webhook-signature")

    unless raw_timestamp.match?(/\A[0-9]+\z/)
      raise InvalidSignatureError.new("webhook-timestamp is not a unix time",
        code: "invalid_timestamp", field: "webhook-timestamp")
    end

    timestamp = Integer(raw_timestamp, 10)
    current = now || Time.now.to_i
    if (current - timestamp).abs > tolerance_seconds
      raise InvalidSignatureError.new("webhook-timestamp is outside the tolerance window",
        code: "timestamp_out_of_tolerance", field: "webhook-timestamp")
    end

    signed = "#{hook_id}.#{raw_timestamp}.".b + body.to_s.b
    candidates = signature.split(" ").filter_map { |entry| entry.delete_prefix("v1,") if entry.start_with?("v1,") }

    keys.each do |key|
      expected = [OpenSSL::HMAC.digest("SHA256", key, signed)].pack("m0")
      candidates.each do |candidate|
        next unless candidate.bytesize == expected.bytesize

        return VerifiedSignature.new(id: hook_id, timestamp: timestamp) if OpenSSL.fixed_length_secure_compare(candidate, expected)
      end
    end

    raise InvalidSignatureError.new("no webhook-signature entry matches",
      code: "no_matching_signature", field: "webhook-signature")
  end

  def self.signature_keys(secrets)
    raise InvalidSignatureError.new("no secret configured", code: "invalid_secret") if secrets.empty?

    secrets.map do |secret|
      if secret.start_with?("whsec_")
        encoded = secret.delete_prefix("whsec_")
        unless !encoded.empty? && BASE64_PATTERN.match?(encoded)
          raise InvalidSignatureError.new("a whsec_ secret is not valid base64", code: "invalid_secret")
        end

        encoded.unpack1("m0")
      elsif secret.empty?
        raise InvalidSignatureError.new("an empty secret", code: "invalid_secret")
      else
        secret.b
      end
    end
  end
  private_class_method :signature_keys
end
