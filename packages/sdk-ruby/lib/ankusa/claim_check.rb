# frozen_string_literal: true

require "digest"

module Ankusa
  # Every failure the claim-check client can raise.
  #
  # `retryable?` is the whole point of this hierarchy: a caller (a queue
  # consumer, typically) needs exactly one bit -- dead-letter or retry -- and
  # nothing here requires it to know the gateway's status codes to get that
  # right.
  #
  # Non-retryable: the ref or expected sha256 is malformed, the gateway said
  # 404/other 4xx, or the bytes that came back don't match the sha256.
  # Retryable: the gateway said 5xx/503, or the request never completed
  # (network error, timeout).
  class ClaimCheckError < Error
    def retryable? = false
  end

  # The ref string isn't a `urn:ankusa:claim:v1:<tenant>:<claim_id>`
  # claim-check ref, or the expected sha256 isn't 64-char lowercase hex.
  class InvalidClaimRefError < ClaimCheckError
  end

  # The gateway returned 404: no such object, expired by retention or never
  # written.
  class ClaimNotFoundError < ClaimCheckError
  end

  # The gateway rejected the request (400 or any other non-404 4xx).
  class ClaimRejectedError < ClaimCheckError
    attr_reader :status, :body

    def initialize(message, status:, body:)
      super(message)
      @status = status
      @body = body
    end
  end

  # The sha256 of the bytes the gateway returned doesn't match the expected
  # sha256 the queue message carries. The gateway itself never checks this --
  # see "Redeem a claim" in docs/claim-check.md -- so this is the reader's own
  # end-to-end check, always run before `redeem` returns.
  class ClaimIntegrityError < ClaimCheckError
  end

  # The gateway is unreachable, or answered 5xx/503. Safe to retry.
  class ClaimCheckUnavailableError < ClaimCheckError
    def retryable? = true
  end

  # A parsed claim-check ref, ready to become a GET /v1/claims/... request.
  ParsedClaimRef = Data.define(:tenant_id, :claim_id, :path)

  # Mirrors `#/components/schemas/Ref` in priv/openapi/claim_check.v1.yaml --
  # keep the two in sync. A ref is one string:
  #   urn:ankusa:claim:v1:<tenant>:<claim_id>
  #
  # \A...\z, never ^...$: Ruby's ^ and $ are line anchors, so a ref ending in a
  # newline would otherwise parse.
  CLAIM_REF_PATTERN = /\Aurn:ankusa:claim:v1:([A-Za-z0-9_-]{1,64}):([0-7][0-9A-HJKMNP-TV-Z]{25})\z/

  SHA256_PATTERN = /\A[0-9a-f]{64}\z/

  # Parses a claim-check ref (the `claim` field of a queue message) into the
  # tenant id, claim id, and GET /v1/claims/{tenant_id}/{claim_id} path. Raises
  # `InvalidClaimRefError` -- never worth retrying -- if `ref` isn't a
  # well-formed ref.
  def self.parse_claim_ref(ref)
    match = ref.is_a?(String) ? CLAIM_REF_PATTERN.match(ref) : nil
    raise InvalidClaimRefError, "invalid claim-check ref: #{ref}" if match.nil?

    tenant_id = match[1]
    claim_id = match[2]
    ParsedClaimRef.new(tenant_id: tenant_id, claim_id: claim_id, path: "/v1/claims/#{tenant_id}/#{claim_id}")
  end

  # Redeem claim-check refs against a deployment's claim-check gateway.
  #
  # The gateway itself does no authentication or authorization (see
  # docs/claim-check.md) -- `headers` is for whatever a deployer's own boundary
  # (service mesh, an API gateway) expects in front of it.
  class ClaimCheckClient
    def initialize(base_url, headers: nil, timeout: 10.0, transport: nil)
      @connection = Connection.new(base_url, headers: headers, timeout: timeout, transport: transport)
    end

    # Redeem a claim-check ref: fetch its bytes and verify them against
    # `sha256` (the queue message's `sha256` field, 64-char lowercase hex)
    # before returning. The gateway does not check integrity itself -- see
    # "Redeem a claim" in docs/claim-check.md -- so this end-to-end check always
    # runs here.
    #
    # Raises a `ClaimCheckError`; check `retryable?` to sort a failure into
    # dead-letter (false) or retry (true).
    def redeem(ref, sha256)
      parsed = Ankusa.parse_claim_ref(ref)
      unless sha256.is_a?(String) && SHA256_PATTERN.match?(sha256)
        raise InvalidClaimRefError, "invalid claim sha256: #{sha256.inspect}"
      end

      body = fetch_bytes(parsed)
      if Digest::SHA256.hexdigest(body) != sha256
        raise ClaimIntegrityError, "claim sha256 mismatch for #{parsed.tenant_id}/#{parsed.claim_id}"
      end

      body
    end

    # Liveness probe: GET /health.
    def health
      response = @connection.request("GET", "/health")
      status = response.status
      if status != 200
        raise ClaimCheckUnavailableError, "claim-check gateway health check failed (#{status})"
      end

      Connection.parse_json(response.body)
    rescue Transport::Error => e
      raise ClaimCheckUnavailableError, "claim-check gateway unreachable: #{e.message}"
    rescue JSON::ParserError
      raise ClaimCheckUnavailableError, "claim-check gateway health check returned a non-JSON body (#{status})"
    end

    private

    def fetch_bytes(parsed)
      response = @connection.request("GET", parsed.path)
      status = response.status
      if status == 404
        raise ClaimNotFoundError, "claim not found: #{parsed.tenant_id}/#{parsed.claim_id}"
      end

      if status >= 400 && status < 500
        body = Connection.error_body(response)
        raise ClaimRejectedError.new(
          "claim-check rejected redeem (#{status}): #{body.inspect}",
          status: status,
          body: body
        )
      end

      if status != 200
        raise ClaimCheckUnavailableError,
          "claim-check gateway error (#{status}): #{Connection.error_body(response).inspect}"
      end

      response.body
    rescue Transport::Error => e
      raise ClaimCheckUnavailableError, "claim-check gateway unreachable: #{e.message}"
    end
  end
end
