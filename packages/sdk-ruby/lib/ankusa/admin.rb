# frozen_string_literal: true

module Ankusa
  # Every failure the admin client can raise.
  #
  # `retryable?` is the whole point of this hierarchy, exactly as in the
  # claim-check client: a caller (an operator script, a dashboard) needs one bit
  # -- retry, or surface the rejection -- and nothing here requires it to know
  # the operator API's status codes to get that right.
  #
  # Non-retryable: the listener said 409 role_not_enabled (ask another node) or
  # another 4xx (a rejected filter). Retryable: the listener said 5xx, or the
  # request never completed (network error, timeout).
  class AdminError < Error
    def retryable? = false
  end

  # The listener is unreachable, or answered 5xx. Safe to retry.
  class AdminUnavailableError < AdminError
    def retryable? = true
  end

  # The listener returned 409 role_not_enabled: this node does not run the role
  # the operation needs. Ask another node; `role` names it.
  class RoleNotEnabledError < AdminError
    attr_reader :role

    def initialize(message, role:)
      super(message)
      @role = role
    end
  end

  # The listener rejected the request (any other 4xx).
  #
  # `status` is the HTTP status; `code` is the body's `error` field
  # (`invalid_filter`, ...).
  class AdminRejectedError < AdminError
    attr_reader :status, :code

    def initialize(message, status:, code:)
      super(message)
      @status = status
      @code = code
    end
  end

  # Operate against the operator API (`admin.port`, default 4002): health,
  # Prometheus metrics, the redacted config, the dead-letter queue, and the
  # quarantine list. Responses are node-local by design, so a fleet operator
  # scrapes every node's admin port.
  #
  # Like the claim-check client, the listener itself performs no authentication
  # -- `headers` is for whatever a deployer's own boundary (service mesh, an API
  # gateway) expects in front of it.
  class AdminClient
    def initialize(base_url, headers: nil, timeout: 10.0, transport: nil)
      @connection = Connection.new(base_url, headers: headers, timeout: timeout, transport: transport)
    end

    # Liveness probe: GET /health -> {status, instance, roles}.
    def health
      json(request("GET", "/health"))
    end

    # GET /metrics -> the Prometheus text exposition body.
    def metrics
      request("GET", "/metrics").body.dup.force_encoding(Encoding::UTF_8)
    end

    # GET /v1/config -> the effective, redacted configuration.
    def config
      json(request("GET", "/v1/config"))
    end

    # GET /v1/dlq -> a page of dead-lettered hooks, newest first.
    #
    # `params` may carry `source_id`, `since` and `limit`; absent keys are
    # omitted from the query string rather than sent as `=`.
    def list_dead_letters(params = nil)
      json(request("GET", "/v1/dlq", query: params))
    end

    # POST /v1/dlq/replay -> {replayed}.
    #
    # `filter` is a filter, not a payload; omitted or empty (`{}`) replays
    # everything.
    def replay_dead_letters(filter = nil)
      json(request("POST", "/v1/dlq/replay", json: filter || {}))
    end

    # GET /v1/quarantine -> recent quarantined hooks, newest first.
    #
    # `params` may carry `limit`; absent keys are omitted from the query string
    # rather than sent as `=`.
    def list_quarantined(params = nil)
      json(request("GET", "/v1/quarantine", query: params))
    end

    private

    def request(http_method, path, query: nil, json: nil)
      response = @connection.request(http_method, path, query: query, json: json)
      raise_for_status(response)
      response
    rescue Transport::Error => e
      raise AdminUnavailableError, "admin listener unreachable: #{e.message}"
    end

    def json(response)
      Connection.parse_json(response.body)
    rescue JSON::ParserError
      raise AdminUnavailableError, "admin listener returned a non-JSON body (#{response.status})"
    end

    def raise_for_status(response)
      status = response.status
      return if status >= 200 && status < 300

      body = Connection.error_body(response)
      code = body.is_a?(Hash) ? body["error"] : nil

      if status == 409 && code == "role_not_enabled"
        role = body.is_a?(Hash) ? body["role"] : nil
        raise RoleNotEnabledError.new("role not enabled on this node: #{role.inspect}", role: role)
      end

      if status >= 400 && status < 500
        raise AdminRejectedError.new(
          "admin listener rejected the request (#{status}): #{body.inspect}",
          status: status,
          code: code
        )
      end

      # Everything else that isn't 2xx is retryable: 1xx, an unfollowed 3xx
      # redirect, and 5xx.
      raise AdminUnavailableError, "admin listener error (#{status}): #{body.inspect}"
    end
  end
end
