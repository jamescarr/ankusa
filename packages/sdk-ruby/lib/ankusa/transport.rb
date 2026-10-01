# frozen_string_literal: true

require "net/http"
require "openssl"
require "uri"
require "zlib"

module Ankusa
  # The injectable HTTP hook every client sends through.
  #
  # A transport is any object that responds to `call(Request) -> Response`,
  # including a lambda. `Transport::NetHTTP` is the default, used when a client
  # is built without one; tests inject a lambda and never touch the network.
  module Transport
    # One HTTP request. `http_method` is an upcased String -- not `method`,
    # which would shadow `Object#method` -- and `url` is a full `URI::HTTP`.
    Request = Data.define(:http_method, :url, :headers, :body)

    # One HTTP response. `headers` has lowercase String keys; `body` is a binary
    # String, `""` when the response carried no body.
    Response = Data.define(:status, :headers, :body)

    # Raised for any transport failure: connection refused, DNS failure, a
    # timeout, a TLS error, a truncated response. An injected transport raises
    # it too, to signal unreachable without a network.
    class Error < Ankusa::Error
    end

    # The default transport: one `Net::HTTP` request per call, no redirects, no
    # retries, and one timeout for connecting, reading, and writing.
    class NetHTTP
      def initialize(timeout:)
        @timeout = timeout
      end

      def call(request)
        url = request.url
        # hostname, not host: for an IPv6 literal host is "[::1]" with the
        # brackets still on, and Net::HTTP.addr_port adds them back from the
        # bare address, so "[::1]" would go to DNS as written.
        http = Net::HTTP.new(url.hostname, url.port)
        http.use_ssl = url.scheme == "https"
        http.open_timeout = @timeout
        http.read_timeout = @timeout
        http.write_timeout = @timeout
        # Net::HTTP retries idempotent requests (GET, PUT, DELETE, ...) once by
        # default, which would send -- and so in the conformance vectors,
        # record -- a second request.
        http.max_retries = 0

        http_request = Net::HTTPGenericRequest.new(
          request.http_method,
          !request.body.nil?,
          request.http_method != "HEAD",
          url.request_uri,
          request.headers
        )
        http_request.body = request.body unless request.body.nil?

        response = http.request(http_request)
        Response.new(
          status: response.code.to_i,
          headers: response.each_header.to_h,
          body: (response.body || "").b
        )
      rescue SystemCallError, IOError, SocketError, Timeout::Error, OpenSSL::SSL::SSLError,
        Net::ProtocolError, Net::HTTPBadResponse, Zlib::Error => e
        raise Error, "#{e.class}: #{e.message}"
      end
    end
  end
end
