# Changelog

All notable changes to `ankusa_gateway` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **The `table` authorizer: permissions from the customer's own tables.**
  `Ankusa.Gateway.Authz.Table` decides from rows of `principal`, `action`,
  `space`, `prefix` and `effect`, matched against the caller's identities
  (`user:<id>`, each group, each tenant by default), the action or `*`, the
  space or `*`, and the key or one of its ancestor directories; any deny wins,
  no match abstains. `mode: :snapshot` loads the rows into an ETS table at boot
  and every `refresh_ms` (`Ankusa.Gateway.Authz.Table.Snapshot`; `version/2`
  skips an unchanged load; `max_rows`/`max_bytes` keep the previous table;
  `stale_after_ms` fails closed; a new generation orphans the old decisions),
  and `mode: :lookup` asks the source once per cache miss. A snapshot that has
  never loaded holds `/ready` at `503` for at most `ready_timeout_ms`.
- **Table sources in the library**: `Source.Object` (NDJSON objects in a store,
  by `key` or `prefix`) and `Source.Http` (paged JSON over https). A third,
  `ankusa_gateway_postgres`, is its own package. An object source whose store
  and prefix overlap a `gateway.spaces` space is refused at boot.
- `ankusa_gateway_table_refreshes_total`, `ankusa_gateway_table_rows` and
  `ankusa_gateway_table_bytes`, from `[:ankusa, :gateway, :table_refresh]`.

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
  per-authorizer TTLs), and the validation of a backend's address: `url!`
  (a literal, `https` or loopback `http` or `allow_insecure_http: true`),
  `headers!` (at most 16) and `req_options!`.
- **The `http` authorizer** (`Ankusa.Gateway.Authz.Http`, `type: http`): the
  OpenID AuthZEN Access Evaluation API (`/access/v1/evaluation`, batched
  through `/access/v1/evaluations`, `404`/`405`/`501` there memoized for five
  minutes) or a bespoke API with `protocol: :template` (`request`,
  `allow_path`, `ttl_path`, an optional bulk endpoint). `subject` and
  `resource` are JSON shapes of templates, a leaf that is one list-valued
  variable renders as an array, a `cache-control: max-age` names an allow's
  freshness, and `forward_authorization` sends the caller's own credential
  (the decision is then keyed by it).
- **The `openfga` authorizer** (`Ankusa.Gateway.Authz.OpenFGA`, `type:
  openfga`): `check` and `batch-check` with per-check correlation ids, a
  `relations` entry per action, `resources` mapping a space and a key pattern
  to one object (`vars` from the pattern), `contextual_tuples` expanded one per
  group (at most 100), and identifiers built from request data denied
  `:unmappable` when they are `*`, contain `#`, whitespace or a control
  character.
- The gateway's redacted config view now shows the PDP authorizers'
  non-secret keys (`protocol`, paths, actions, relations, consistency, store
  and model ids, templates); `headers` and `api_token` stay hidden.
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
- **`type: okta`**, Okta's issuer layout expanded at boot
  (`Ankusa.Gateway.Authn.Okta`) into a `jwt` authenticator for a custom
  authorization server (`issuer`/`jwks_url` derived from `domain` and
  `authorization_server`, `groups_claim: groups`) or an `introspection` one for
  `authorization_server: org` (org-server access tokens are opaque). Any `jwt`
  or `introspection` option in the entry overrides what it derives; no network
  at boot.
- **`Ankusa.Gateway.Authn.Introspection`** (`type: :introspection`): RFC 7662
  token introspection, one `Ankusa.Gateway.Caller` per authenticator (a hundred
  requests carrying one token make one call), Basic client credentials, a
  form-encoded body built by `URI.encode_query/1`, checked `iss`/`aud`, the
  response as the principal's claims, negative caching of an inactive token.
  At most one per chain, and nothing that reads the bearer token may follow it.
- **Enrichers** (`gateway.auth.enrichers`, at most 4,
  `Ankusa.Gateway.Enricher`): groups and claims added to a freshly
  authenticated principal before it is fingerprinted and cached —
  `okta_groups` (Okta's Users API, paged through the `link` header,
  `max_groups`) and `http` (a JSON API of your own, URL templated after a
  literal origin, `groups_path`/`claims_path`). Per-enricher result cache
  (`ttl_ms`) through its own `Caller`; `only`, `on_unavailable: :fail | :skip`,
  `timeout_ms`.
