# Changelog

All notable changes to `ankusa-sdk` (the RubyGems gem) are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-10-01

### Added

- `Ankusa::ClaimCheckClient`: `redeem(ref, sha256)` parses a claim-check ref,
  fetches its bytes from the gateway, and verifies them against the message's
  sha256 before returning them; `health` for a liveness probe.
- `Ankusa::RoutesClient`: the route-management client
  (`routes.admin.port`) — `health`, `list_routes`, `create_route`, `get_route`,
  `replace_route`, `update_route`, `delete_route`, `get_ip_rules`,
  `put_ip_rules`, `test_route`.
- `Ankusa::AdminClient`: the operator client (`admin.port`) — `health`,
  `metrics`, `config`, `list_dead_letters`, `replay_dead_letters`,
  `list_quarantined`.
- `Ankusa::SourcesClient`, a client for `Ankusa.Admin.Router`: the tenant-scoped
  source API's `server_version`, `list_sources`, `get_source`, `create_source`,
  `update_source`, and `delete_source`, with `Ankusa::SourceSpec` (the writable
  spec, `to_request_body` omits unset fields) and `Ankusa::Source` (a stored,
  redacted source).
- `Ankusa.parse_headers(headers)`, for receivers of Ankusa's HTTP sink: parses
  `x-ankusa-id`/`x-ankusa-source`/`x-ankusa-tenant`/`content-type` into a
  `HookHeaders` value, raising `MissingHookIdError` if `x-ankusa-id` is absent
  or empty. `Ankusa.parse_claim_ref` is exported the same way.
- `Ankusa::Transport`, the injectable transport hook (`Transport::Request` in,
  `Transport::Response` out) with the default `Transport::NetHTTP`: one
  connection per request, no redirects, no retries.
- A `ClaimCheckError`/`RoutesError`/`AdminError`/`SourcesError` hierarchy under
  `Ankusa::Error`, every class carrying a `retryable?` bit (except the sources
  family, which carries `.status` and `.body` instead), so a consumer needs
  exactly one bit to route a failure to dead-letter or retry.

[Unreleased]: https://github.com/jamescarr/ankusa/compare/sdk-ruby-v0.1.0...HEAD
[0.1.0]: https://github.com/jamescarr/ankusa/releases/tag/sdk-ruby-v0.1.0
