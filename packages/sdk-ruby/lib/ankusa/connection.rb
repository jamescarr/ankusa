# frozen_string_literal: true

require "json"
require "uri"

module Ankusa
  # Shared by every client, so the URL, query, body, and JSON rules live in one
  # place.
  #
  # @api private
  class Connection
    attr_reader :base_url, :headers

    def initialize(base_url, headers: nil, timeout: 10.0, transport: nil)
      @base_url = base_url.to_s.chomp("/")
      @headers = (headers || {}).to_h { |key, value| [key.to_s, value.to_s] }
      @transport = transport || Transport::NetHTTP.new(timeout: timeout)
    end

    # One request. Raises `Transport::Error` for anything that went wrong on the
    # wire; mapping that to a client error is the caller's job.
    #
    # `query` entries whose value is nil are dropped, so an absent parameter is
    # never sent as `=`. A non-nil `json` becomes the body, with the client
    # headers extended by `content-type: application/json` -- a request-specific
    # header of the same name wins.
    def request(http_method, path, query: nil, json: nil)
      url = URI.parse("#{@base_url}#{path}#{query_string(query)}")
      headers = @headers.dup
      body = nil
      unless json.nil?
        body = JSON.generate(json)
        headers["content-type"] = "application/json"
      end

      @transport.call(Transport::Request.new(http_method: http_method, url: url, headers: headers, body: body))
    end

    # Parses a response body as JSON. Raises `JSON::ParserError`, which each
    # caller maps to its own error.
    def self.parse_json(body)
      JSON.parse(body.dup.force_encoding(Encoding::UTF_8))
    end

    # The decoded error body: JSON when the body is JSON regardless of its
    # content-type, otherwise the raw text with invalid bytes scrubbed. An empty
    # body gives `""`.
    def self.error_body(response)
      parse_json(response.body)
    rescue JSON::ParserError
      response.body.dup.force_encoding(Encoding::UTF_8).scrub
    end

    private

    def query_string(query)
      pairs = (query || {}).reject { |_, value| value.nil? }.to_a
      return "" if pairs.empty?

      "?#{URI.encode_www_form(pairs)}"
    end
  end
end
