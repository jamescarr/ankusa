# frozen_string_literal: true

module Ankusa
  # Every failure the source-management client can raise.
  #
  # Every subclass carries the HTTP `status` and decoded `body` of the response
  # that produced it (both nil when no response was involved, e.g. a transport
  # failure). Unlike the other families this one has no `retryable?`, matching
  # the reference SDK.
  class SourcesError < Error
    attr_reader :status, :body

    def initialize(message = nil, status: nil, body: nil)
      super(message)
      @status = status
      @body = body
    end
  end

  # 404: no such source for this tenant.
  class SourceNotFoundError < SourcesError
  end

  # 409 source_exists: a source with that name already exists; or
  # 409 source_has_deliveries: a delete found the source still has undelivered
  # hooks. The body's "error" says which.
  class SourceConflictError < SourcesError
  end

  # 409 source_store_read_only: the deployment's source store is a static seed,
  # so writes are impossible.
  class SourceStoreReadOnlyError < SourcesError
  end

  # 400: bad tenant, bad source name, or a spec the server rejected.
  #
  # `message` is the server's own `message` (for a bad spec) or the `error` code
  # (for a bad tenant/name, which has no message).
  class SourceInvalidError < SourcesError
  end

  # The admin API is unreachable, timed out, or answered 5xx. Safe to retry.
  class SourcesUnavailableError < SourcesError
  end

  # GET /health reported a version other than `expected_version`.
  class VersionMismatchError < SourcesError
  end

  # The writable fields of a source, as submitted to POST/PUT.
  #
  # `verify` is optional (absent means `{"type" => "none"}` on the server);
  # `sinks` is required and must be non-empty -- the server validates all of
  # this exactly as the YAML config does.
  SourceSpec = Data.define(:sinks, :verify, :on_verify_failure) do
    def initialize(sinks:, verify: nil, on_verify_failure: nil) = super

    # The JSON body for a create/update, omitting unset (nil) fields.
    def to_request_body
      body = {"sinks" => sinks}
      body["verify"] = verify unless verify.nil?
      body["on_verify_failure"] = on_verify_failure unless on_verify_failure.nil?
      body
    end
  end

  # A stored source as the admin API reports it: redacted spec plus the derived
  # identity fields.
  Source = Data.define(:tenant, :name, :source_id, :ingest_path, :verify, :on_verify_failure, :sinks) do
    def self.from_h(data)
      new(
        tenant: data.fetch("tenant"),
        name: data.fetch("name"),
        source_id: data.fetch("source_id"),
        ingest_path: data.fetch("ingest_path"),
        verify: data["verify"] || {"type" => "none"},
        on_verify_failure: data["on_verify_failure"],
        sinks: data["sinks"] || []
      )
    end
  end

  # Manage a deployment's tenant-scoped sources via `Ankusa.Admin.Router` (the
  # same `admin.port` as the operator API).
  #
  # The router does no authentication (by design, like the rest of this port),
  # and every source it returns has its secrets redacted. So a `Source` read
  # back here is never useful for editing: resending its `verify` map is not the
  # same as resending the stored secret. Callers that hold the secret supply it
  # through `SourceSpec`.
  #
  # Only `[A-Za-z0-9_-]{1,64}` tenants and names are accepted, and both are
  # checked before any path is built, so a caller-supplied name cannot escape
  # its tenant through URL normalization.
  #
  # `expected_version` is an optional safety latch: when set, the first API call
  # fetches GET /health once, compares its `"version"` field against the
  # expected value, caches the fetched version, and raises
  # `VersionMismatchError` on any mismatch. Every subsequent call re-checks the
  # cached value without another request.
  class SourcesClient
    # The same rule `Ankusa::CLAIM_REF_PATTERN` uses for its tenant. Anything
    # outside it is rejected before a path is built: URL parsers normalize dot
    # segments, so an unvalidated "../" would escape the tenant scope before the
    # server sees it.
    SAFE_ID = /\A[A-Za-z0-9_-]{1,64}\z/

    def initialize(base_url, expected_version: nil, timeout: 10.0, transport: nil)
      @connection = Connection.new(base_url, timeout: timeout, transport: transport)
      @expected_version = expected_version
      @server_version = nil
    end

    # The deployment's Ankusa version: GET /health ["version"].
    #
    # Fetched once and cached; when `expected_version` was set this also
    # enforces it, so a mismatched deployment raises `VersionMismatchError` here
    # too.
    def server_version
      @server_version = fetch_version if @server_version.nil?
      check_version
      @server_version
    end

    # List a tenant's sources: GET /v1/tenants/<tenant>/sources.
    def list_sources(tenant)
      validate_tenant(tenant)
      ensure_version
      response = request("GET", "/v1/tenants/#{tenant}/sources")
      raise_for_status(response)
      parse_json(response).fetch("entries").map { |entry| Source.from_h(entry) }
    end

    # Fetch one source: GET /v1/tenants/<tenant>/sources/<name>.
    def get_source(tenant, name)
      validate_tenant(tenant)
      validate_name(name)
      ensure_version
      response = request("GET", "/v1/tenants/#{tenant}/sources/#{name}")
      raise_for_status(response)
      Source.from_h(parse_json(response))
    end

    # Create a source: POST /v1/tenants/<tenant>/sources.
    #
    # The source name travels in the body (plus `name`); the tenant comes from
    # the URL and wins over any `"tenant"` key inside `spec`.
    def create_source(tenant, name, spec)
      validate_tenant(tenant)
      validate_name(name)
      ensure_version
      body = spec.to_request_body.merge("name" => name)
      response = request("POST", "/v1/tenants/#{tenant}/sources", json: body)
      raise_for_status(response)
      Source.from_h(parse_json(response))
    end

    # Replace a source: PUT /v1/tenants/<tenant>/sources/<name>.
    #
    # The name comes from the URL; a `"name"` key inside `spec` is never sent
    # (`SourceSpec` has no such field).
    def update_source(tenant, name, spec)
      validate_tenant(tenant)
      validate_name(name)
      ensure_version
      response = request("PUT", "/v1/tenants/#{tenant}/sources/#{name}", json: spec.to_request_body)
      raise_for_status(response)
      Source.from_h(parse_json(response))
    end

    # Delete a source: DELETE /v1/tenants/<tenant>/sources/<name>.
    #
    # Succeeds with no return value (the server answers 204 with an empty body);
    # a missing source raises `SourceNotFoundError`. A source that still has
    # undelivered hooks on the server's node raises `SourceConflictError` whose
    # body's "error" is "source_has_deliveries" (with "pending"/"inflight"
    # counts); this method never sends the admin API's ?deliveries=dead_letter.
    def delete_source(tenant, name)
      validate_tenant(tenant)
      validate_name(name)
      ensure_version
      response = request("DELETE", "/v1/tenants/#{tenant}/sources/#{name}")
      raise_for_status(response)
      nil
    end

    private

    # The optional version latch: only when `expected_version` is set does the
    # first API call fetch /health once, cache the version, and enforce it.
    def ensure_version
      return if @expected_version.nil?

      @server_version = fetch_version if @server_version.nil?
      check_version
    end

    def check_version
      return if @expected_version.nil? || @server_version == @expected_version

      raise VersionMismatchError,
        "expected Ankusa version #{@expected_version.inspect}, server reports #{@server_version.inspect}"
    end

    def fetch_version
      response = request("GET", "/health")
      status = response.status
      if status != 200
        body = Connection.error_body(response)
        raise SourcesUnavailableError.new(
          "ankusa admin API health check failed (#{status}): #{body.inspect}",
          status: status,
          body: body
        )
      end

      begin
        data = Connection.parse_json(response.body)
      rescue JSON::ParserError
        raise SourcesUnavailableError, "ankusa admin API health check returned a non-JSON body (#{status})"
      end

      version = data.is_a?(Hash) ? data["version"] : nil
      unless version.is_a?(String)
        raise SourcesUnavailableError,
          "ankusa admin API health check returned no version (#{status}): #{data.inspect}"
      end

      version
    end

    def request(http_method, path, json: nil)
      @connection.request(http_method, path, json: json)
    rescue Transport::Error => e
      raise SourcesUnavailableError, "ankusa admin API unreachable: #{e.message}"
    end

    def parse_json(response)
      Connection.parse_json(response.body)
    rescue JSON::ParserError
      # Deliberate deviation from the reference SDK, which lets its decoder's
      # ValueError escape here: every other failure this client can produce is a
      # SourcesError, so a 2xx that isn't JSON is one too.
      raise SourcesUnavailableError.new(
        "ankusa admin API returned a non-JSON body (#{response.status})",
        status: response.status
      )
    end

    def raise_for_status(response)
      status = response.status
      return if status >= 200 && status < 300

      body = Connection.error_body(response)
      if status == 404
        raise SourceNotFoundError.new("source not found (#{status}): #{body.inspect}", status: status, body: body)
      end

      if status == 400
        raise SourceInvalidError.new(invalid_message(body, status), status: status, body: body)
      end

      if status == 409
        if body.is_a?(Hash) && body["error"] == "source_store_read_only"
          raise SourceStoreReadOnlyError.new(
            "source store is read-only (#{status}): #{body.inspect}",
            status: status,
            body: body
          )
        end

        raise SourceConflictError.new("source already exists (#{status}): #{body.inspect}", status: status, body: body)
      end

      raise SourcesUnavailableError.new("ankusa admin API error (#{status}): #{body.inspect}", status: status, body: body)
    end

    def invalid_message(body, status)
      if body.is_a?(Hash)
        message = body["message"] || body["error"]
        return message if message.is_a?(String)
      end

      "invalid source (#{status}): #{body.inspect}"
    end

    # Reject a tenant that is not `[A-Za-z0-9_-]{1,64}` before any path is
    # built.
    def validate_tenant(tenant)
      return if tenant.is_a?(String) && SAFE_ID.match?(tenant)

      raise SourceInvalidError, "invalid tenant: #{tenant.inspect}"
    end

    # Reject a source name that is not `[A-Za-z0-9_-]{1,64}` before any path is
    # built.
    def validate_name(name)
      return if name.is_a?(String) && SAFE_ID.match?(name)

      raise SourceInvalidError, "invalid source name: #{name.inspect}"
    end
  end
end
