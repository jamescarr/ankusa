# ADR 0005: The standalone gateway

- Status: accepted, 2026-11-13
- New wire format: Objects API v1, Sharing API v1, ACL document v1, share link v1, audit record v1, config_version 2
- Builds on: (none)

## Context

The claim-check gateway is the HTTP listener that hands a queue worker the bytes behind a claim-check ref (`docs/claim-check.md`). Today it is part of core and of nothing else:

- **It is a role of core.** `Ankusa.Config.@roles` lists `:claim_check`; `Ankusa.Instance.claim_check_children/2` starts `Ankusa.ClaimCheck.Router` on `claim_check.port` beside the cache and the reader. The listener's settings are core config keys: `claim_check.port`, `ip`, `tls`, `auth` (`%{mode, allow_insecure, tokens, jwt, mtls}`), `authorize`, `max_concurrent_reads` and `integrity_check`. The only way to run a gateway is to run core with that role, in the `jamescarr/ankusa` image or embedded in an Elixir application.
- **Its authorization is a fixed scope vocabulary.** A caller passes when it holds `claims:read` and `tenant:<tenant_id>` (or `tenant:*`) — scopes drawn from `Ankusa.Principal.scopes/0`, which `Ankusa.Admin.Auth` refuses to extend (`"... has unknown scopes ...; known: ..."`). A principal carries `id`, `scopes` and `tenants`; the claims of the token that authenticated it, and any groups, are dropped.
- **The one escape hatch is unreachable from YAML.** `claim_check.authorize` (`{module, fun, args}`) replaces the scope rule, but it is an Elixir term: the YAML loader (`AnkusaServer.Config`) has no key for it, so the image cannot use it, and it can neither be combined with the scope rule nor cached.
- **The blob store adapters cannot serve arbitrary objects.** `Ankusa.BlobStore` has `put`, `put_if_absent`, `get`, `get_range`, `delete` and `list`: no `head`, no version (etag or generation) on a read, no conditional write against a version, no metadata, no paged listing with a delimiter. Claims never need them (packs are written once under fresh names); anything that stores mutable objects behind the gateway does.

What is wanted: the gateway as its own server that keeps serving claim checks exactly as today and can also be deployed alone as a permissioned facade in front of any object store — objects a group can read and write, share with someone else, and revoke — with permissioning that can come from a JWT alone, from Okta, from OpenFGA, from a customer's own permissions API (with caching and bulk calls) or from a customer's own tables, and that does not slow the read path down.

## Decision

### Packages and dependency direction

```
async_api_spex  <-  ankusa (core)  <-  ankusa_gateway (hex)  <-  ankusa_gateway_postgres (hex)
                                              ^                        ^
                                   ankusa_gateway_server (docker, image jamescarr/ankusa-gateway)
                                              ^
                                   ankusa_server (docker, image jamescarr/ankusa)
```

- The HTTP gateway and its authentication leave `packages/ankusa` for a new library, `packages/ankusa_gateway` (Hex `ankusa_gateway`, modules `Ankusa.Gateway.*`). It has its own image, `jamescarr/ankusa-gateway`, built from `packages/ankusa_gateway_server` (modules `AnkusaGatewayServer.*`), and its own version. Table-derived permissions from Postgres ship as `packages/ankusa_gateway_postgres`.
- `jamescarr/ankusa` still bundles the gateway, so YAML with `roles: [claim_check]` keeps working. Elixir embedders of that role add the `ankusa_gateway` dependency.
- `ankusa_server` path-depends on `ankusa_gateway_server` because the full image is a superset of the gateway image: the YAML document machinery, secrets, logging, tracing and the `gateway:` section parser live once, in the lower package. There is no third "kit" package.
- The new hex packages follow `packages/ankusa_redis/mix.exs`: path dependencies on `../ankusa` (and `async_api_spex` with `override: true`) in `:dev`/`:test`, the Hex requirement otherwise; an 80 % coverage floor; `.formatter.exs`, `README.md`, `CHANGELOG.md`, `LICENSE`. All three start at `@version "0.0.0"` (the first release is `release:prepare minor`, so `0.1.0`). `ankusa_gateway` requires `{:ankusa, "~> 0.8"}`: core `0.8.0` is the first release with the seams below and without `Ankusa.ClaimCheck.Router`, and is published first. `ankusa_gateway_postgres` requires `{:ankusa, "~> 0.8"}` and `{:ankusa_gateway, "~> 0.1"}`.
- Every project that reaches core through `ankusa_gateway` pins core and `async_api_spex` by path with `override: true` (Mix evaluates a path dependency's `mix.exs` under `:prod`). Each new Mix project commits its `mix.lock`; each project that gains a path dependency is re-locked in the same change.

### Core after the carve

Core keeps claim writing (`check_in/4`, `check_in_batch/2`, `Ref`, `Pack`, `Stores`, `Sweeper`), the read engine as library code (`Ankusa.ClaimCheck.locate/read_bytes/digest`), and the read-side processes as startable child specs (`Ankusa.ClaimCheck.Cache`, `Ankusa.ClaimCheck.Reader`). Core loses `Ankusa.ClaimCheck.Router`, the `:claim_check` role, and these config keys:

|Removed from core `claim_check`|Lands at|
|---|---|
|`port`, `ip`, `tls`|`gateway.port`, `gateway.ip`, `gateway.tls`|
|`auth` (`%{mode, allow_insecure, tokens, jwt, mtls}`)|`gateway.auth` (`%{allow_insecure, authenticators: [...]}`)|
|`authorize` (`{m, f, a}`, Elixir only)|deleted; a module implementing `Ankusa.Gateway.Authorizer`, named in `gateway.authz.authorizers`|
|`max_concurrent_reads`|`gateway.max_concurrent_requests`|
|`integrity_check`|`gateway.claims.integrity_check`|
|role `:claim_check`|deleted from `Ankusa.Config.@roles`; a gateway-only node is a core instance with `roles: []` plus `Ankusa.Gateway`|

Every other `claim_check.*` key stays in core unchanged (stores, placement, key layout, ref signing, `accept_unsigned_refs`, `read_*`, write and retention keys). A node with `roles: []` does no queue work under any `wal`.

Core gains, additively: `claims` and `groups` on `Ankusa.Principal`; a public `Ankusa.Admin.Auth.JWT.verify/5` and `principal/2`; named realms on `Ankusa.Admin.Auth.JWT.Keys` (`server:` and `realms:` options, so a second keys server can serve the gateway's issuers); `metrics.extra` (modules exporting `metrics/1`, so a library outside core adds series to the node's `/metrics`); `Ankusa.BlobStore.Pool.child_specs_for/2`; `Ankusa.Admin.Redact.closed_view/2`; and five optional object callbacks on `Ankusa.BlobStore` (below).

### Runtime shape

An embedder (and both images) start two children, in this order, under a `:one_for_one` supervisor:

```elixir
{Ankusa.Instance, %Ankusa.Config{}}          # roles may be []
{Ankusa.Gateway, %Ankusa.Gateway.Config{}}   # same :instance name
```

`Ankusa.Gateway.Supervisor` (`:rest_for_one`, `auto_shutdown: :any_significant`, registered `Ankusa.via(instance, :gateway)`) children:

1. `{Ankusa.Instance.RegistryWatch, instance}` (significant: a Registry restart stops the gateway so its parent restarts it re-registered).
2. `{Ankusa.Instance.Isolated, instance: i, domain: :gateway, children: [...]}` with, in order:
   1. one `Ankusa.Admin.Auth.JWT.Keys` server (`server: :gateway_auth_keys`, one realm per `jwt` authenticator), only when the chain has one;
   2. `Ankusa.Gateway.Cache` x3 (`:authn`, `:authz`, `:acl`);
   3. every other authenticator's child specs, in chain order, then every enricher's;
   4. one `Ankusa.Gateway.Caller` per `:remote` authorizer, then each authorizer's child specs;
   5. the blob store pools of the spaces' stores (`Ankusa.BlobStore.Pool.child_specs_for/2`);
   6. `Ankusa.ClaimCheck.Cache` (when `gateway.claims.enabled` and `claim_check.read_cache_bytes > 0`);
   7. `Ankusa.ClaimCheck.Reader` (always: it owns the request gate);
   8. `Ankusa.Gateway.Gauges`;
   9. the listener, last: Bandit with core's `listener.*` limits, `gateway.tls`, `gateway.ip`, `gateway.port` and the plug `{Ankusa.Gateway.Router, [instance: instance]}`.

The supervisor refuses to start (`ArgumentError "Ankusa.Gateway: start Ankusa.Instance <instance> first"`) unless the instance's config is published and its `:instance` process is registered. Every gateway plug reads the instance from `conn.assigns.ankusa_opts[:instance]` and the gateway config from `Ankusa.Gateway.config(instance)`.

**Isolation.** The gateway is its own failure domain, `:gateway` (`Ankusa.Instance.Isolated`): its crashes do not restart the instance's other domains, and theirs do not restart it. A PDP outage, a breaker opening and a table snapshot going stale fail closed per policy step (`503 authz_unavailable`, or the step's `on_unavailable`) and never change `/ready`. The one authorization condition that holds `/ready` at `503` is a `table` authorizer that has not loaded its first snapshot, and only for its `ready_timeout_ms`.

### One config format, two images: `config_version: 2`

A file without `config_version` is version 1 and is migrated on load by one shared step: `claim_check.{port,ip,tls}` move to `gateway.{port,ip,tls}`, `claim_check.max_concurrent_reads` to `gateway.max_concurrent_requests`, `claim_check.integrity_check` to `gateway.claims.integrity_check`, and `claim_check.auth` (`mode`, `allow_insecure`, `tokens`, `jwt`, `mtls`) to `gateway.auth` (`allow_insecure`, and `authenticators`: `[]` for mode `none`, otherwise one entry of type `token`, `jwt` or `mtls` carrying the old block). A target that already exists, or a templated `mode`, is a `ConfigError`. A version-2 file that still has one of the moved `claim_check` keys is a `ConfigError` naming the new key.

The gateway image reads the same format restricted to the root keys `config_version node log listener metrics tracing secrets egress storage claim_check gateway`; any other root key is a `ConfigError` (`"<key>: not available in the gateway image (jamescarr/ankusa-gateway); use jamescarr/ankusa"` for a key the full image knows). In the full image the gateway runs when `node.roles` contains `claim_check`. Environment variables keep their names in both images: `ANKUSA_CLAIM_CHECK_PORT` → `gateway.port`, `ANKUSA_CLAIM_CHECK_IP` → `gateway.ip`, `ANKUSA_CLAIM_CHECK_TOKEN` → `gateway.auth.authenticators` (the list only).

`gateway:` keys: `port` (`4001`), `ip` (`127.0.0.1`), `tls`, `max_concurrent_requests` (`1024`, `0` = no cap), `auth` (`allow_insecure`, `authenticators`, `cache`, `enrichers`), `authz` (`deadline_ms`, `cache`, `authorizers`, `policies`), `claims` (`enabled`, `policy`, `integrity_check`), `spaces`, `objects`, `sharing`. No `gateway.*` key is reloadable.

### Request pipeline (every route but `GET /health`, `GET /ready`)

1. **Authenticate**: ordered authenticators (`token`, `jwt`, `mtls`, `introspection`, `okta` — expanded to `jwt` or `introspection` — and `link`, or a custom module implementing `Ankusa.Gateway.Authenticator`). Each answers `{:ok, principal, expires_at | nil}`, `:skip` (the credential is not in its form), `{:error, :unauthenticated}` (its form, invalid: stop, `401`) or `{:error, {:unavailable, reason}}` (`503 auth_unavailable`). All skip → `401`. An empty chain gives every request the anonymous operator principal, and is legal only on loopback or with `allow_insecure: true`. The plug assigns, and caches together, `principal`, `principal_via` (`:token | :jwt | :mtls | :introspection | :link | :anonymous`, or a custom module) and `subject_key`: `binary_part(:crypto.hash(:sha256, :erlang.term_to_binary({via, id, scopes, tenants, groups, claims}, [:deterministic])), 0, 16)`, computed once per authentication. The authentication cache is keyed `{authenticator index, sha256(credential)}` (credentials over 16 KiB are not cached), bounded in entries and bytes, and lives no longer than the credential's own expiry.
2. **Enrich** (optional, on an authentication-cache miss only): enrichers (`okta_groups`, `http`) add groups and claims; the enriched principal is what is fingerprinted and cached.
3. **Authorize**: the resource's policy evaluates its authorizer steps. Policy modes: `first` (first allow or deny wins), `all` (first deny wins, else allow if any allowed), `any` (first allow wins); all-abstain is deny in every mode. A step that is unavailable ends the decision as unavailable (`on_unavailable: fail`, the default) or counts as an abstain or a deny. Authorizers are `:inline` (run in the request), `:cached` (decision cache keyed `{name, subject_key, action, scope}`) or `:remote` (the same cache, misses through one `Ankusa.Gateway.Caller` per backend: coalescing per key, an adaptive in-flight limit, a bounded queue with per-partition fairness, batching, a breaker and the policy deadline). Built-in types: `scopes` (today's rule), `rules` (pattern and template rules over principal, groups, scopes, tenants and claims), `grants` (native grants, below), `http` (AuthZEN 1.0 evaluation and evaluations, or a templated bespoke permissions API with bulk calls), `openfga` (check and batch-check), `table` (snapshot or lookup of `(principal, action, space, prefix, effect)` rows from Postgres, an object or a paged HTTP feed). Results: `403 {"error":"forbidden"}` (with `"scope"` when the deciding step named one), `503 {"error":"authz_unavailable"}` with `retry-after` 1–3.
4. **Share links**: a request authenticated by a share link (`principal_via == :link`) is evaluated by the `grants` authorizer alone, for `:read` on an `:object` alone: every other step counts as an abstain, and every other action or resource kind is denied before any step runs.
5. **Admit**: `Ankusa.ClaimCheck.Reader.admit(instance, gateway.max_concurrent_requests)`, else `503 overloaded`.
6. **Serve**.

Authorizer and authenticator contracts (`Ankusa.Gateway.Authorizer`, `Ankusa.Gateway.Authenticator`, `Ankusa.Gateway.Authz.Request`, `Ankusa.Gateway.Enricher`, `Ankusa.Gateway.Authz.Table.Source`) are public behaviours of `ankusa_gateway`, so a deployment adds its own without forking.

### Wire formats

**Claims API v1** — unchanged (`GET|HEAD /v1/claims/{tenant_id}/{claim_id}`, signed refs, ranges, `x-ankusa-sha256`, every status it answers today), plus `GET /ready` (unauthenticated; `200` or `503` with `retry-after: 1`). A claim's authorization resource is `%{kind: :claim, space: "claims", key: "<tenant_id>/<claim_id>", tenant: tenant_id}`. With `gateway.claims.enabled: false` the claim routes answer `404`. Telemetry: `[:ankusa, :gateway, :request]` with `api: :claims` replaces `[:ankusa, :claim_check, :gateway]`; the metric series become `ankusa_gateway_requests_total{api}` and `ankusa_gateway_request_duration_seconds{api}`.

**Objects API v1** (`priv/openapi/gateway.v1.yaml`). A space (`gateway.spaces.<name>`, `claims` reserved) names a store (`{module, opts}` implementing the object callbacks below), a key prefix, a policy, `max_object_bytes` (64 MiB), `read_first_bytes` (1 MiB) and `read_only`. Two spaces on one store may not have overlapping prefixes. A key is valid UTF-8, 1–1024 bytes, at most 32 segments, no empty, `.` or `..` segment, no `\`, no control byte, and no segment starting with `.ankusa`; in a path it is the raw segments after the space, each percent-decoded once, a decoded `/` or `\` refused; from a query or a body it is the decoded string. A bad key is `400 {"error":"invalid_key"}`. Order on every route: authenticate → space (`404`) → key → authorize → admit → serve.

|Route|Action|Answers|
|---|---|---|
|`GET /v1/objects/{space}/{key...}`|`read`|`200`/`206` with `content-type` (stored), `etag: "<version>"`, `last-modified`, `accept-ranges: bytes`, `content-range` on `206`, `cache-control: private, no-cache`, `x-content-type-options: nosniff`, `content-disposition: attachment`, `content-security-policy: default-src 'none'; sandbox`, one `x-ankusa-meta-<k>` per stored metadata pair; `304` for a matching `If-None-Match`; `416` with `content-range: bytes */<size>`; `422 {"error":"object_too_large","max_bytes":N}` past `max_object_bytes`. Read as a first ranged read of `read_first_bytes`, then the rest pinned to the first read's version (one restart on a version change, then `503 store_unavailable`)|
|`HEAD` same path|`read`|the same headers and `content-length`, no body|
|`PUT` same path|`write`|`content-length` required (`411 {"error":"length_required"}`), `<= max_object_bytes` (`413 {"error":"too_large"}`); `x-ankusa-meta-<name>` request headers become metadata (≤ 16 pairs, name `[a-z0-9-]{1,64}` not starting with `ankusa-`, value ≤ 1024 printable ASCII bytes, else `400 {"error":"invalid_metadata"}`); `if-none-match: *` and `if-match: "<v>"` are conditions (`412 {"error":"precondition_failed"}`); `200 {"space","key","size","version"}` with `etag`. `405 {"error":"method_not_allowed"}` on a `read_only` space|
|`DELETE` same path|`delete`|`204`; `405` on a `read_only` space|
|`GET /v1/objects/{space}?prefix=&delimiter=&cursor=&limit=`|`list` on the prefix|`200 {"objects":[{"key","size"}],"prefixes":[...],"next_cursor":<string or null>}`; `delimiter` absent or `/` (`400 {"error":"invalid_delimiter"}`); `limit` 1–1000 (100); entries under a `.ankusa` segment are never listed. Authorizing `list` on a prefix shows every key under it|

Store errors on every route: not found `404 {"error":"not_found"}`; a store that refuses the gateway's credential `503 {"error":"store_forbidden"}` with `retry-after: 60`; an integrity failure `502 {"error":"integrity_mismatch"}`; anything else `503 {"error":"store_unavailable"}` with `retry-after` 1–3. The bytes in flight on a node are bounded by `gateway.objects.max_inflight_bytes` (`503 {"error":"overloaded"}`).

**Sharing API v1** (`priv/openapi/gateway.v1.yaml`; with `gateway.sharing.enabled`, else `404`). Every route names a resource (`key` of an existing object, or `prefix`), authorizes `share` on it against the space's policy, and for a mutation writes an audit record before changing the ACL document. `POST grants` and `POST links` also authorize the caller for every action the new grant carries (`403 {"error":"forbidden","action":"<action>"}` on the first refusal).

|Route|Body / query|Answers|
|---|---|---|
|`GET /v1/spaces/{space}/acl?key=<k>` or `?prefix=<p>`|exactly one|`200 {"owner": <the object's ankusa-owner or null>, "grants": [...]}`|
|`POST /v1/spaces/{space}/grants`|`{"resource": {"key": k} or {"prefix": p}, "subject": s, "actions": [...], "expires_in_s": n or null}`|`201` the grant; `400 {"error":"invalid_grant"}`; `409 {"error":"too_many_grants"}` past `max_grants_per_resource` (256)|
|`DELETE /v1/spaces/{space}/grants/{id}?key=<k>` or `?prefix=<p>`|the resource the id was made on|`204` (idempotent); `400 {"error":"invalid_grant"}` when the id's kind and hash are not that resource's|
|`POST /v1/spaces/{space}/links`|`{"key": k, "expires_in_s": n}`, `1 <= n <= link_max_ttl_s` (7 days)|`201 {"id","expires_at","path"}`; a grant with subject `link` and actions `["read"]`|

A conflicting concurrent update after five compare-and-swap rounds is `409 {"error":"conflict"}`; an audit write that fails is `503 {"error":"audit_unavailable"}` and nothing changes. Request bodies are capped at 64 KiB.

**ACL document v1**, stored in the space's own store at `<space.prefix>.ankusa/acl/v1/<o|p>/<lowercase hex sha256 of the object key or prefix>`, `content-type: application/json`:

```json
{"v": 1, "space": "reports", "resource": {"kind": "object", "key": "teams/eng/q3.pdf"},
 "incarnation": "01JD0Z2B7M3N4P5Q6R7S8T9V0W",
 "grants": [{"id": "o<64 hex>.01JD0Z3K9Q4W8X2Y7V6T5S4R3P", "subject": "group:eng", "actions": ["read"],
             "created_by": "alice", "created_at": 1790000000, "expires_at": null}],
 "updated_at": 1790000000}
```

- `subject` is `user:<principal id>`, `group:<name>`, `tenant:<id>`, `anyone` (any authenticated principal that is not a link) or `link`.
- `incarnation` is the `ankusa-incarnation` metadata of the object the grants were made on (`null` for an object without one, and always for a prefix). A document whose incarnation is not the object's current one is dead: nothing reads its grants and the next update replaces it, so a deleted object's grants never land on a later object of the same key.
- A grant id is `<o|p><64 hex resource hash>.<ULID>` and is never trusted alone: every route that takes one checks the hash against the resource it names.
- Updates are compare-and-swap on the document's store version (`put_object` with `if: :absent | {:version, v}`), at most five rounds. Reads go through a node-local cache of `gateway.sharing.acl_cache_ttl_ms` (2 s).
- Ownership: with sharing on, creating an object through the gateway records `ankusa-owner` (the creating principal, unless anonymous) and a fresh `ankusa-incarnation` as object metadata; overwriting carries both over unchanged and never makes anyone an owner. The two names are never accepted from, or returned in, `x-ankusa-meta-*` headers. Deleting an object deletes its ACL document best-effort.
- The `grants` authorizer allows when the object's owner is the caller (not a link, not anonymous), or when an unexpired grant on the object or any ancestor prefix (`""`, `"a/"`, `"a/b/"`, …) carries the action and its subject matches the caller; otherwise it abstains. A claim request abstains.

**Share link v1**: canonical string `urn:ankusa:link:v1:<space>:<grant id>:<exp_unix>`; signature = unpadded base64url HMAC-SHA256 under `gateway.sharing.signing_keys` (sign with the first, accept any, constant-time compare), 43 characters; URL `/v1/objects/<space>/<key>?link=<grant id>&exp=<exp_unix>&sig=<signature>`. The `link` authenticator reads only `GET`/`HEAD` on an object path; `exp` is `[1-9][0-9]{0,11}`; the id's hash must be the key's. A bad link is `401`; a valid signature past `exp` is `410 {"error":"link_expired"}`; the principal is `link:<grant id>`. A revoked link still authenticates and is refused by `grants` (`403`) once the ACL cache entry ages out.

**Audit record v1**: before each sharing mutation, `put_object(if: :absent)` of `<space.prefix>.ankusa/audit/v1/dt=<UTC date>/<ULID>.json`:

```json
{"v":1,"id":"<ULID>","at":<unix ms>,"actor":"<principal id>","remote_ip":"<peer ip>","request_id":"<x-request-id or a UUIDv7>",
 "action":"grant.create"|"grant.revoke"|"link.create","space":"...","resource":{...},"grant":{...}}
```

When the ACL update then fails, a second record `{"action":"<action>.failed","ref":"<first id>","reason":...}` is written best-effort.

**Object callbacks on `Ankusa.BlobStore`** (optional; S3, GCS, Azure, OCI and LocalFS implement them): `head_object/3`, `get_object/3` (`:range`, `:if` `{:version, v}`, `:max_bytes`), `put_object/4` (`:content_type`, `:metadata`, `:if` `:absent | {:version, v}`), `delete_object/3` (a missing key is `:ok`), `list_page/3` (`:cursor`, `:limit`, `:delimiter`). `object_meta` is `%{size, version, content_type, last_modified, metadata}`; `version` is the provider's etag or generation (LocalFS: a sidecar ULID, or the content's sha256 for an object written out of band). A large `put_object` sends content type and metadata on the request that starts the multipart upload and the condition on the one that completes it, and takes the version from the completing response. LocalFS's conditions are exclusive within one node (or one connected cluster) and not crash-atomic.

## Consequences

- **Elixir embedders of `roles: [:claim_check]` must add `ankusa_gateway`** and start `{Ankusa.Gateway, %Ankusa.Gateway.Config{}}` after `{Ankusa.Instance, config}`. `Ankusa.Config.new(roles: [:claim_check])` raises `unknown Ankusa role`, and the moved `claim_check.*` keys raise `unknown Ankusa.Config key`. `claim_check.authorize` hooks become modules implementing `Ankusa.Gateway.Authorizer`.
- **YAML keeps loading**: a version-1 file is migrated; a version-2 file uses `gateway:`. The environment variables are unchanged. The Helm chart's claim-check workload runs the gateway image.
- **Metric series are renamed**: `ankusa_claim_gateway_requests_total` and `ankusa_claim_gateway_duration_seconds` become `ankusa_gateway_requests_total{api="claims"}` and `ankusa_gateway_request_duration_seconds{api="claims"}`. Nothing under `deploy/monitoring` names the old series.
- **Revocation is eventually consistent across nodes**: a revoked native grant (or the grants of a deleted and re-created object) can still be honoured by another gateway node for up to `gateway.sharing.acl_cache_ttl_ms` (2 s by default). Decision caches trade freshness for speed the same way (allow 5 s, deny 1 s, authentication 60 s, enricher groups 60 s); every one is configurable, and `serve_stale_ms` is `0` by default (fail closed).
- **The gateway image still contains core**: the RocksDB NIF is compiled into it and never opened (`roles: []`, no store).
- **Two images, two tags**: `docker.yml` builds `ankusa_server-v*` and `ankusa_gateway_server-v*` from one workflow (so the cosign keyless identity both smoke scripts verify does not change); release tooling reads each image's name from its package's `IMAGE` file.
- **Release order**: core `0.8.0`, then `ankusa_gateway` `0.1.0`, then `ankusa_gateway_postgres` and `ankusa_gateway_server` `0.1.0`.
- **Later work must keep true**: a share link only ever reaches the `grants` step and only for `read` on an object; request data never chooses a PDP, IdP or enricher origin (URLs are literal, or templated past the authority with percent-encoded values); an authorization outage never changes `/ready` except for an unloaded `table` snapshot within `ready_timeout_ms`; a key never contains a segment starting with `.ankusa`.

## Proof

Each milestone of the implementation owns its files; each fails if the decision is not honoured.

- Core seams: `packages/ankusa/test/ankusa/admin/auth_test.exs` (`JWT.verify/5`, a second `Keys` server with named realms), `metrics_test.exs` (`metrics.extra`), `blob_store_pool_test.exs` (`child_specs_for/2`), `crypto_test.exs` (`Redact.closed_view/2`).
- The carve: `packages/ankusa_gateway/test/ankusa/gateway/claims_router_test.exs` (every case of the old claim router, plus a custom authorizer), `authn_test.exs`, `authz_test.exs` (the three policy modes and `on_unavailable`), `config_test.exs`, `openapi_test.exs`, `supervisor_test.exs` (a real socket, `/ready`, the metrics series, `roles: []` with `wal: :none`); `packages/ankusa/test/ankusa/config_test.exs` (the removed role and keys); `packages/ankusa_server/test/ankusa_server/config_test.exs` (version-1 migration, the moved-key refusal, `ANKUSA_CLAIM_CHECK_TOKEN`).
- The gateway server: `packages/ankusa_gateway_server/test/ankusa_gateway_server/config_test.exs` (root-key refusals, env overrides, `rel/gateway.yml`), `secrets_test.exs`, `application_test.exs`, `log_formatter_test.exs`; `packages/ankusa_gateway_server/scripts/smoke.sh` (`mise run docker:smoke ankusa_gateway_server`).
- The engine: `caller_test.exs` (coalescing, limit, queue and partition shedding, deadlines, batching, breaker, the 99:1 partition fairness case), `authz_engine_test.exs` (cache keys, TTLs, refresh-ahead, stale-on-error, the share-link restriction), `authn_cache_test.exs`, `pattern_test.exs`, `template_test.exs` (including `compile_url!/3`), `rules_test.exs`, `metrics_test.exs`; `packages/ankusa_gateway/bench/authz_bench.exs` (`mise run bench:gateway`, the budgets of the read path).
- Object callbacks: `packages/ankusa/test/ankusa/blob_store_objects_test.exs` and the `objects` describes of `blob_store_s3_test.exs`, `blob_store_gcs_test.exs`, `blob_store_azure_integration_test.exs`, `blob_store_oci_integration_test.exs` (`mise run test:integration`).
- Objects API: `objects_router_test.exs` (every status above, the key rules, the byte budget), `openapi_test.exs` (every documented path is routed and secured), `config_test.exs` (space overlap, prefixes).
- Sharing: `acl_test.exs`, `acl_store_test.exs` (concurrent compare-and-swap, cache TTL), `grants_test.exs`, `sharing_router_test.exs` (each route, audit before mutation, link create/redeem/expire/tamper/revoke, incarnation after delete and re-create, no escalation of actions), `link_test.exs`.
- External PDPs: `http_authorizer_test.exs` (AuthZEN single and batch, template protocol, fallbacks, unavailability), `openfga_test.exs`.
- Okta: `okta_test.exs`, `introspection_test.exs`, `enricher_test.exs`.
- Tables: `table_test.exs` (matching, deny precedence, `max_rows`/`max_bytes`, staleness, `/ready` within `ready_timeout_ms`), `table_sources_test.exs`; `packages/ankusa_gateway_postgres/test/ankusa/gateway/postgres/source_test.exs` (snapshot and lookup against Postgres, read-only transactions, bound parameters).

**This ADR's own proof:** the PR merges, and `mise run check:links` passes.
