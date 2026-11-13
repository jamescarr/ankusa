# Changelog

All notable changes to `ankusa_gateway` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **The gateway, carved out of core (ADR 0005).** `Ankusa.Gateway` serves the
  claims API that was `Ankusa.ClaimCheck.Router` (`GET|HEAD
  /v1/claims/:tenant_id/:claim_id`, signed refs, ranges, the manifest check),
  plus `GET /ready`. Start it after `{Ankusa.Instance, config}` with
  `{Ankusa.Gateway, Ankusa.Gateway.Config.new(...)}`; a gateway-only node is a
  core instance with `roles: []`. Its own failure domain, `:gateway`.
- **An authentication chain** (`gateway.auth.authenticators`, at most 8):
  `token` (static tokens with scopes, groups and claims), `jwt` (one per
  issuer, `groups_claim`, `header`, `subject_prefix`; every one's keys on one
  `Ankusa.Admin.Auth.JWT.Keys` server) and `mtls` (identities mapped to scopes
  and groups), or a custom `Ankusa.Gateway.Authenticator`. The plug assigns
  `principal`, `principal_via` and `subject_key`.
- **A policy engine** (`gateway.authz`): `Ankusa.Gateway.Authorizer` steps in
  `:first`, `:all` or `:any` mode, `on_unavailable: :fail | :abstain | :deny`
  per step; the built-in `scopes` authorizer is the claims API's rule, and the
  default `claims` policy. A custom authorizer replaces core's
  `claim_check.authorize` hook.
- `Ankusa.Gateway.Metrics` for core's `metrics.extra`:
  `ankusa_gateway_requests_total{api,status}` and
  `ankusa_gateway_request_duration_seconds{api}` (were
  `ankusa_claim_gateway_requests_total` and
  `ankusa_claim_gateway_duration_seconds` in core), from
  `[:ankusa, :gateway, :request]`.
- `priv/openapi/claim_check.v1.yaml` moved here from core, with `/ready`, and
  `auth_unavailable` / `authz_unavailable` among the error codes.
