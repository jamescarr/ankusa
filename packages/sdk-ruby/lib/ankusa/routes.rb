# frozen_string_literal: true

module Ankusa
  # Every failure the routes client can raise.
  #
  # `retryable?` is the whole point of this hierarchy, exactly as in the
  # claim-check client: a caller (an operator script, a controller loop) needs
  # one bit -- leave the table alone and retry, or surface the rejection.
  #
  # Non-retryable: the listener said 404 (no such route) or another 4xx (a
  # rejected write, a duplicate, the cap), or the id was unusable before any
  # request was sent. Retryable: the listener said 5xx/503 store_unavailable, or
  # the request never completed (network error, timeout).
  class RoutesError < Error
    def retryable? = false
  end

  # The route `id` cannot be used to build a path.
  #
  # Raised before any request is sent. An id that isn't a string, is empty, or
  # is exactly `.` or `..` is refused: URL parsers normalize those away, so
  # `get_route("..")` would quietly hit /admin/ and return the list page as if
  # it were a route. Every other id is percent-encoded as one path segment,
  # never refused.
  class InvalidRouteIdError < RoutesError
  end

  # The listener is unreachable, or answered 5xx/503. Safe to retry.
  class RoutesUnavailableError < RoutesError
    def retryable? = true
  end

  # The listener returned 404: no such route.
  class RouteNotFoundError < RoutesError
  end

  # The listener rejected the request (400 or any other non-404 4xx).
  #
  # `code` is the body's `error` field (`invalid_route`, `duplicate_route`,
  # `too_many_routes`, ...); `field`, `message`, `conflicting_id` and
  # `max_routes` are carried through when the body supplies them.
  class RoutesRejectedError < RoutesError
    attr_reader :status, :code, :field, :conflicting_id, :max_routes

    def initialize(message, status:, code:, field: nil, conflicting_id: nil, max_routes: nil)
      super(message)
      @status = status
      @code = code
      @field = field
      @conflicting_id = conflicting_id
      @max_routes = max_routes
    end
  end

  # Manage routes and global IP rules on the route-management listener
  # (`routes.admin.port`, default 4003).
  #
  # Route ids are percent-encoded as a single path segment, so `/`, `?`, `#` and
  # `%` in an id can't reshape the URL; an id that isn't a string, is empty, or
  # is `.`/`..` raises `InvalidRouteIdError` before any request is sent.
  class RoutesClient
    def initialize(base_url, headers: nil, timeout: 10.0, transport: nil)
      @connection = Connection.new(base_url, headers: headers, timeout: timeout, transport: transport)
    end

    # Liveness probe: GET /health -> {status, routes}.
    def health
      json(request("GET", "/health"))
    end

    # GET /admin/routes -> a page of route definitions.
    #
    # `params` may carry `enabled`, `limit` and `cursor`; absent keys are
    # omitted from the query string rather than sent as `=`.
    def list_routes(params = nil)
      json(request("GET", "/admin/routes", query: params))
    end

    # POST /admin/routes -> the stored route, timestamps included.
    def create_route(input)
      json(request("POST", "/admin/routes", json: input))
    end

    # GET /admin/routes/{id} -> the route.
    def get_route(id)
      json(request("GET", "/admin/routes/#{route_path(id)}"))
    end

    # PUT /admin/routes/{id} -> the replaced (or created) route.
    def replace_route(id, input)
      json(request("PUT", "/admin/routes/#{route_path(id)}", json: input))
    end

    # PATCH /admin/routes/{id} -> the patched route.
    def update_route(id, patch)
      json(request("PATCH", "/admin/routes/#{route_path(id)}", json: patch))
    end

    # DELETE /admin/routes/{id} (204, no body).
    def delete_route(id)
      request("DELETE", "/admin/routes/#{route_path(id)}")
      nil
    end

    # GET /admin/ip-rules -> the global IP rules.
    def get_ip_rules
      json(request("GET", "/admin/ip-rules"))
    end

    # PUT /admin/ip-rules -> the stored rules, as parsed.
    def put_ip_rules(rules)
      json(request("PUT", "/admin/ip-rules", json: rules))
    end

    # POST /admin/routes/test -> the dry-run decision for `request`.
    def test_route(request)
      json(self.request("POST", "/admin/routes/test", json: request))
    end

    private

    def request(http_method, path, query: nil, json: nil)
      response = @connection.request(http_method, path, query: query, json: json)
      raise_for_status(response)
      response
    rescue Transport::Error => e
      raise RoutesUnavailableError, "routes listener unreachable: #{e.message}"
    end

    def json(response)
      Connection.parse_json(response.body)
    rescue JSON::ParserError
      raise RoutesUnavailableError, "routes listener returned a non-JSON body (#{response.status})"
    end

    def raise_for_status(response)
      status = response.status
      return if status >= 200 && status < 300

      if status == 404
        raise RouteNotFoundError, "route not found (#{status})"
      end

      if status >= 400 && status < 500
        body = Connection.error_body(response)
        raise RoutesRejectedError.new(
          rejection_message(status, body),
          status: status,
          code: field(body, "error"),
          field: field(body, "field"),
          conflicting_id: field(body, "conflicting_id"),
          max_routes: field(body, "max_routes")
        )
      end

      raise RoutesUnavailableError, "routes listener error (#{status}): #{Connection.error_body(response).inspect}"
    end

    # The server's own `message` when the body carries a string one, else the
    # rejection rendered whole.
    def rejection_message(status, body)
      message = field(body, "message")
      return message if message.is_a?(String)

      "routes listener rejected the request (#{status}): #{body.inspect}"
    end

    def field(body, name)
      body.is_a?(Hash) ? body[name] : nil
    end

    # Validates `id` and percent-encodes it as ONE path segment.
    #
    # `.` and `..` (and an empty id) are refused rather than encoded: a URL
    # parser normalizes them away before the request is sent, so `..` would
    # become /admin/ and an empty id or `.` the collection endpoint -- the
    # caller would get the list page back as if it were a route. Everything else
    # travels with `/`, `?`, `#`, `%` and space escaped as %2F %3F %23 %25 %20
    # instead of reshaping the URL.
    def route_path(id)
      raise InvalidRouteIdError, "route id must be a string, got #{id.inspect}" unless id.is_a?(String)
      raise InvalidRouteIdError, "invalid route id #{id.inspect}" if ["", ".", ".."].include?(id)

      id.b.gsub(/[^A-Za-z0-9_.~-]/) { |char| format("%%%02X", char.ord) }
    end
  end
end
