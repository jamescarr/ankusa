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
- **Sharing** (`gateway.sharing`): native grants, ownership and share links.
  With `enabled` the gateway defines the `grants` authorizer (an object's
  owner, or a live `user:`/`group:`/`tenant:`/`anyone`/`link` grant on the
  object or any of its ancestor prefixes; at most 34 cache reads, the misses
  read concurrently), appends the `link` authenticator, and makes `PUT` record
  `ankusa-owner` and `ankusa-incarnation` on a new object (carried over
  unchanged on an overwrite; a lost race is one retry, then `409 conflict`).
  An ACL document v1 (`Ankusa.Gateway.ACL`, `Ankusa.Gateway.ACL.Store`) lives
  at `.ankusa/acl/v1/` in the space's own store, compare-and-swap written,
  cached for `acl_cache_ttl_ms`, and dead when its `incarnation` is not the
  object's current one — the grants of a deleted object never land on a later
  object of the same key.
- **The Sharing API v1**: `GET /v1/spaces/:space/acl`, `POST
  /v1/spaces/:space/grants`, `DELETE /v1/spaces/:space/grants/:id` and `POST
  /v1/spaces/:space/links` (`404` unless sharing is on), each authorizing
  `share` and — for a new grant or link — every action it hands out (`403`
  naming the missing `action`), each mutation writing an **audit record v1**
  under `.ankusa/audit/v1/dt=<date>/` first (`503 audit_unavailable` and
  nothing changed when that fails).
- **Share links v1** (`Ankusa.Gateway.Sharing.Link`, `Ankusa.Gateway.Authn.Link`):
  `/v1/objects/<space>/<key>?link=&exp=&sig=`, an unpadded base64url
  HMAC-SHA256 over `urn:ankusa:link:v1:<space>:<grant id>:<exp>` under
  `sharing.signing_keys` (first signs, any verifies; rotate in front).
  `410 link_expired` past `exp`; the credential is accepted on `GET`/`HEAD` of
  one object only, and only the `grants` step ever sees it.
- `priv/openapi/gateway.v1.yaml` documents the Sharing API (`Grant`, `Acl`,
  `Link`, `GrantRequest`, `LinkRequest`), the share-link query parameters and
  `410 link_expired` on `GET`/`HEAD`, `409 conflict` on `PUT`, and the
  reserved `ankusa-owner`/`ankusa-incarnation` metadata.
- `Ankusa.Gateway.Metrics` gains
  `ankusa_gateway_shares_total{instance,action,result}` from
  `[:ankusa, :gateway, :share]`.
