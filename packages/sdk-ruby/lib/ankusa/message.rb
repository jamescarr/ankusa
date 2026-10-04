# frozen_string_literal: true

require "digest"
require "json"

module Ankusa
  # Raised by `Ankusa.decode_message` when a queue message fails one of the v1
  # decode rules. Always worth dead-lettering, never worth retrying: the bytes
  # will not change on a redelivery.
  #
  # `code` names the rule that failed (`invalid_json`, `unsupported_version`,
  # `invalid_field`, `ambiguous_body`, `missing_body`, `invalid_body_base64`,
  # `size_mismatch`, `integrity`, `tenant_mismatch`); `field` names the offending
  # key when the rule is about one, and is nil otherwise.
  class InvalidMessageError < Error
    attr_reader :code, :field

    def initialize(message, code:, field: nil)
      super(message)
      @code = code
      @field = field
    end

    def retryable? = false
  end

  # The one idempotency-key rule, shared by `Message` and `HookHeaders` so it
  # lives in one place.
  #
  # The key Ankusa shipped (`shipped`) when it is a non-empty string; otherwise,
  # for a hook from a node that predates the field, `tenant:source:dedupe_key`
  # (tenant "default" when nil) for a non-empty `dedupe_key`, else `id`. With
  # `include_replay` and a `replay_id`, `#replay:<replay_id>` is appended.
  module IdempotencyKey
    def self.build(shipped:, tenant:, source:, dedupe_key:, id:, replay_id:, include_replay:)
      key =
        if !shipped.nil? && !shipped.empty?
          shipped
        elsif !dedupe_key.nil? && !dedupe_key.empty?
          "#{tenant || "default"}:#{source}:#{dedupe_key}"
        else
          id
        end
      key += "#replay:#{replay_id}" if include_replay && !replay_id.nil?
      key
    end
  end
  private_constant :IdempotencyKey

  # One decoded v1 queue message. `body` holds the inline bytes (or nil for the
  # claim form, where the bytes are fetched with `Ankusa::ClaimCheckClient`);
  # `claim` holds the ref string in that case. `headers` is the forwarded
  # provider request headers, `{}` when the message carried none.
  #
  # `idempotency_key` is the one value a consumer should dedupe on, as
  # `Message#idempotency_key` (the field is nil only for a message from a node
  # that predates it); see `Ankusa.decode_message` for the decode rules.
  Message = Data.define(
    :v, :id, :source_id, :tenant_id, :received_at, :content_type, :size,
    :body, :claim, :sha256, :dedupe_key, :replay_id, :idempotency_key, :headers
  ) do
    # `Data.define` made `idempotency_key` the reader of the decoded field; keep
    # that value reachable here and let the helper below take over the name.
    # The decoded field itself is still in `to_h[:idempotency_key]`.
    alias_method :shipped_idempotency_key, :idempotency_key
    private :shipped_idempotency_key

    # The idempotency key for this message: the key Ankusa shipped in the
    # `idempotency_key` field when it is non-empty; for a message that predates
    # the field, `tenant:source_id:dedupe_key` (tenant "default" when there is
    # none) when a non-empty `dedupe_key` is set, else `id`.
    #
    # A replay keeps the original `id` and `dedupe_key` and adds `replay_id`, so
    # by default a replay produces the same key as the delivery it replays and a
    # consumer that already processed it drops it. Pass `include_replay: true`
    # to reprocess replays instead.
    def idempotency_key(include_replay: false)
      IdempotencyKey.build(
        shipped: shipped_idempotency_key, tenant: tenant_id, source: source_id,
        dedupe_key: dedupe_key, id: id, replay_id: replay_id, include_replay: include_replay
      )
    end
  end

  # The optional string-keyed fields that may be a string, null, or absent.
  # Checked in this order, so the first offending key is the one reported.
  MESSAGE_STRING_FIELDS = %w[tenant_id content_type dedupe_key replay_id idempotency_key].freeze
  private_constant :MESSAGE_STRING_FIELDS

  # Decodes one v1 queue message (the JSON body a sink or broker delivers) into
  # a `Message`, applying every rule in "Consuming queue messages" of the SDK
  # README in order; the first failure wins.
  #
  # Raises `InvalidMessageError` for anything malformed. The `sha256` is checked
  # against the inline bytes here, but a claim's bytes are only checked by
  # `ClaimCheckClient#redeem`, after it fetches them.
  def self.decode_message(data)
    parsed =
      begin
        JSON.parse(data)
      rescue JSON::ParserError
        raise InvalidMessageError.new("message is not JSON", code: "invalid_json")
      end

    unless parsed.is_a?(Hash)
      raise InvalidMessageError.new("message is not a JSON object", code: "not_an_object")
    end

    version = parsed["v"]
    unless version.is_a?(Integer) && version == 1
      raise InvalidMessageError.new("unsupported message version: #{version.inspect}", code: "unsupported_version")
    end

    id = parsed["id"]
    invalid_field("id") unless id.is_a?(String) && !id.empty?

    source_id = parsed["source_id"]
    invalid_field("source_id") unless source_id.is_a?(String)

    received_at = parsed["received_at"]
    invalid_field("received_at") unless received_at.is_a?(Integer)

    size = parsed["size"]
    invalid_field("size") unless size.is_a?(Integer) && size >= 0

    MESSAGE_STRING_FIELDS.each do |field|
      value = parsed[field]
      invalid_field(field) unless value.nil? || value.is_a?(String)
    end

    headers = parsed.key?("headers") ? parsed["headers"] : {}
    invalid_field("headers") unless headers.is_a?(Hash) && headers.each_value.all? { |value| value.is_a?(String) }

    sha256 = parsed["sha256"]
    valid_sha256 = sha256.nil? || (sha256.is_a?(String) && SHA256_PATTERN.match?(sha256))
    invalid_field("sha256") unless valid_sha256

    body_base64 = parsed["body_base64"]
    claim = parsed["claim"]

    unless body_base64.nil? || claim.nil?
      raise InvalidMessageError.new("message has both body_base64 and claim", code: "ambiguous_body")
    end

    if body_base64.nil? && claim.nil?
      raise InvalidMessageError.new("message has neither body_base64 nor claim", code: "missing_body")
    end

    body = nil
    parsed_claim = nil

    if body_base64.nil?
      parsed_claim = parse_claim(claim)
      invalid_field("sha256") if sha256.nil?
    else
      body = decode_body(body_base64)
      if body.bytesize != size
        raise InvalidMessageError.new("body is #{body.bytesize} bytes, size says #{size}", code: "size_mismatch")
      end

      if !sha256.nil? && Digest::SHA256.hexdigest(body) != sha256
        raise InvalidMessageError.new("body sha256 does not match", code: "integrity")
      end
    end

    tenant_id = parsed["tenant_id"]
    if !parsed_claim.nil? && !tenant_id.nil? && parsed_claim.tenant_id != tenant_id
      raise InvalidMessageError.new(
        "claim tenant #{parsed_claim.tenant_id} does not match tenant_id #{tenant_id}",
        code: "tenant_mismatch"
      )
    end

    Message.new(
      v: 1,
      id: id,
      source_id: source_id,
      tenant_id: tenant_id,
      received_at: received_at,
      content_type: parsed["content_type"],
      size: size,
      body: body,
      claim: claim,
      sha256: sha256,
      dedupe_key: parsed["dedupe_key"],
      replay_id: parsed["replay_id"],
      idempotency_key: parsed["idempotency_key"],
      headers: headers
    )
  end

  def self.invalid_field(field)
    raise InvalidMessageError.new("invalid message field: #{field}", code: "invalid_field", field: field)
  end
  private_class_method :invalid_field

  def self.decode_body(body_base64)
    unless body_base64.is_a?(String)
      raise InvalidMessageError.new("body_base64 is not a string", code: "invalid_body_base64")
    end

    body_base64.unpack1("m0")
  rescue ArgumentError
    raise InvalidMessageError.new("body_base64 is not valid base64", code: "invalid_body_base64")
  end
  private_class_method :decode_body

  # A claim that doesn't parse is an invalid *field*, not an invalid claim-ref
  # error: the message itself is what a consumer rejects, so it carries the same
  # `invalid_field` code as every other bad key.
  def self.parse_claim(claim)
    Ankusa.parse_claim_ref(claim)
  rescue InvalidClaimRefError
    invalid_field("claim")
  end
  private_class_method :parse_claim
end
