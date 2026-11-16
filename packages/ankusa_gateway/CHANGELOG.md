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
- **The authentication and authorization caches** (`gateway.auth.cache`,
  `gateway.authz.cache`): three bounded caches per instance (`:authn`,
  `:authz`, `:acl`). An authentication hit ends the chain at the first
  authenticator that reads the credential, with no signature check or issuer
  peek; a decision hit skips the authorizer. An allow lives `allow_ttl_ms`
  (5 s) and a deny or abstain `deny_ttl_ms` (1 s), with `refresh_ahead` and
  `serve_stale_ms`.
- **`Ankusa.Gateway.Caller`**, one process per remote backend: per-key
  coalescing, an adaptive in-flight limit, a bounded per-partition queue, the
  policy deadline, batching through `decide_many/3` and a breaker.
- **The engine's kinds** — `:inline`, `:cached` and `:remote` — keyed
  `{authorizer, subject_key, action, scope}`, with `authz.deadline_ms` over
  the whole policy and a `ttl_ms` hint capped by `allow_ttl_max_ms`.
- **The `rules` authorizer** (`type: rules`): ordered rules of effect,
  actions, spaces, key patterns and `when` predicates (groups, scopes,
  tenants, principals, claims), decided from the token alone.
- **`Ankusa.Gateway.Pattern` and `Ankusa.Gateway.Template`**: compiled key
  patterns (`{name}`, `*`, `**`) and `{{ var }}` / `{{ var ?? fallback }}`
  templates, with URLs templated after the origin only and percent-encoded.
- **`Ankusa.Gateway.Authz.Remote`**, the common options of a `:remote`
  authorizer (`timeout_ms`, `max_inflight`, `queue_max`, batching, breaker,
  per-authorizer TTLs).
- `Ankusa.Gateway.Metrics` gains the authentication, decision, caller and
  cache series (`ankusa_gateway_authn_total`,
  `ankusa_gateway_authz_decisions_total`, `ankusa_gateway_authz_duration_seconds`,
  `ankusa_gateway_calls_total`, `ankusa_gateway_call_duration_seconds`,
  `ankusa_gateway_caller_limit`/`_inflight`/`_queued`,
  `ankusa_gateway_caller_breaker_transitions_total`,
  `ankusa_gateway_cache_entries`, `ankusa_gateway_cache_bytes`), and
  `Ankusa.Gateway.Gauges` samples the caches every 5 s.
- `GET /ready` lists each authorizer that exports `ready?/3` under
  `authorizers` (`"ok"` / `"not_ready"`); a `not_ready` one answers `503`.
- `bench/authz_bench.exs` (`mise run bench:gateway`): self-asserting budgets
  for a `rules` decision, a cached remote decision, a warm authentication and
  the auth overhead on a claim read.
- **The objects API** (`gateway.spaces`, `gateway.objects`): `GET`/`HEAD`/
  `PUT`/`DELETE` on `/v1/objects/:space/*key` and one page of a listing on
  `/v1/objects/:space`, buffered and size-capped (`max_object_bytes`,
  `read_first_bytes`, a byte budget), decided by the space's policy; one
  canonical key however it arrives, `Ankusa.Gateway.Spaces`,
  `Ankusa.Gateway.Http.store_error/2` and `priv/openapi/gateway.v1.yaml`.
