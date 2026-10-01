# frozen_string_literal: true

require_relative "lib/ankusa/sdk/version"

Gem::Specification.new do |spec|
  spec.name = "ankusa-sdk"
  spec.version = Ankusa::SDK::VERSION
  spec.authors = ["James Carr"]

  spec.summary = "Client SDK for Ankusa deployments."
  spec.description = "Client SDK for Ankusa deployments. Bundles the claim-check gateway client, " \
    "the route-management and operator (admin) clients, the source-management client, and a " \
    "webhook-receiving header helper; more clients (ingest) land here as they're built."
  spec.homepage = "https://github.com/jamescarr/ankusa/tree/main/packages/sdk-ruby"
  spec.license = "Apache-2.0"
  spec.required_ruby_version = ">= 3.3"

  spec.metadata = {
    "homepage_uri" => spec.homepage,
    "source_code_uri" => "https://github.com/jamescarr/ankusa",
    "bug_tracker_uri" => "https://github.com/jamescarr/ankusa/issues",
    "changelog_uri" => "https://github.com/jamescarr/ankusa/blob/main/packages/sdk-ruby/CHANGELOG.md"
  }

  spec.files = Dir["lib/**/*.rb"] + %w[README.md CHANGELOG.md LICENSE]
  spec.require_paths = ["lib"]
end
