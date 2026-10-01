# frozen_string_literal: true

require "json"
require "minitest/autorun"
require "ankusa/sdk"

# Shared test helpers: `injected` and `json_response` are what every unit test
# builds its client with, so nothing in the suite touches the network.
module InjectedTransport
  # A transport lambda that records every request and answers with whatever
  # `responder` returns:
  #
  #   transport, recorded = injected(->(request) { json_response(200, {}) })
  #   Ankusa::RoutesClient.new("http://gateway.invalid", transport: transport)
  def injected(responder)
    recorded = []
    transport = lambda do |request|
      recorded << request
      responder.call(request)
    end
    [transport, recorded]
  end

  # A Transport::Response carrying `payload` as compact JSON.
  def json_response(status, payload)
    Ankusa::Transport::Response.new(status: status, headers: {}, body: JSON.generate(payload))
  end
end

Minitest::Test.include(InjectedTransport)
