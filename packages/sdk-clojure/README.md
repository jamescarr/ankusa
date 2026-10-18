# ankusa-clj (Clojure)

The Clojure client SDK for [Ankusa](https://github.com/jamescarr/ankusa)
deployments: the claim-check gateway client, the route-management client
(`routes.admin.port`), the operator (`admin.port`) client, the tenant-scoped
sources client, the queue-message decoder, and a helper for receiving Ankusa's
HTTP sink deliveries. It implements exactly the surface in
[`conformance/`](https://github.com/jamescarr/ankusa/tree/main/conformance),
the language-neutral vectors every Ankusa SDK passes.

Plain Clojure on the JDK (Java 17 or newer, Clojure 1.12). The one runtime
dependency is `org.clojure/data.json`; HTTP goes through the JDK's own
`java.net.http.HttpClient`. A client is an immutable value and safe to share
across threads. It is a native Clojure library, not a wrapper over the Java SDK.

## Install

Not on Clojars until its first release, so depend on the git tree until then:

```clojure
;; deps.edn
{:deps {io.github.jamescarr/ankusa {:git/url "https://github.com/jamescarr/ankusa"
                                    :git/sha "<a commit sha>"
                                    :deps/root "packages/sdk-clojure"}}}
```

Once released, the Clojars coordinate is `io.github.jamescarr/ankusa-clj`:

```clojure
{:deps {io.github.jamescarr/ankusa-clj {:mvn/version "0.0.0"}}} ; the Clojars version once released
```

(It is `ankusa-clj`, not `ankusa-sdk`: Maven Central already has
`io.github.jamescarr:ankusa-sdk` for the Java SDK, and the Clojure CLI resolves
from Central and Clojars together.)

## Claim-check client

Redeem a claim-check ref, verify the bytes it returns against the sha256 the
queue message carries next to it, and classify failures into dead-letter vs.
retry, without holding any object-store credentials.

```clojure
(require '[ankusa.sdk.claim-check :as claim-check]
         '[ankusa.sdk.errors :as errors])

(def gateway (claim-check/client (System/getenv "CLAIM_CHECK_URL")))

(try
  ;; claim:  urn:ankusa:claim:v1:<tenant>:<claim_id>   (claim_id: uppercase ULID)
  ;; sha256: 64 lowercase hex chars, the digest of the claim's bytes
  (claim-check/redeem gateway claim sha256) ; => byte[]
  (catch clojure.lang.ExceptionInfo e
    (when-not (errors/retryable? e)
      ;; bad ref, 404, or an integrity mismatch: dead-letter, don't requeue
      )
    (throw e)))
```

`redeem` parses the ref, checks `sha256` is 64 lowercase hex chars, fetches
`GET /v1/claims/{tenant_id}/{claim_id}`, and verifies the bytes against
`sha256` before returning them. `health` hits `GET /health` for a liveness
probe. `ankusa.sdk.claim-ref/parse` splits a ref without any request.

## Routes client

Manage route definitions and the global IP rules on the route-management
listener (`routes.admin.port`, default 4003). Responses are decoded JSON with
keyword keys; request bodies are maps with whatever keys the API documents.

```clojure
(require '[ankusa.sdk.routes :as routes])

(def r (routes/client "http://localhost:4003"))

(routes/create-route r {"id" "stripe" "path" "/webhooks/stripe"})
(routes/list-routes r {:enabled true :limit 10}) ; => {:routes [...] :next_cursor nil}
(routes/get-ip-rules r)
(routes/test-route r {"method" "POST" "path" "/webhooks/stripe" "ip" "203.0.113.7"})
```

Functions: `health`, `list-routes`, `create-route`, `get-route`,
`replace-route`, `update-route`, `delete-route`, `get-ip-rules`, `put-ip-rules`,
`test-route`. The id-taking functions reject an id that is not a string, is
empty, or is exactly `.` or `..` with `InvalidRouteIdError` before making any
request; every other id is percent-encoded as one path segment. `delete-route`
returns `nil`. A list's query parameters go out in the order given (a map of up
to eight keys keeps its order; pass a seq of `[k v]` pairs for more) and `nil`
values are left out.

## Admin client

The operator API on `admin.port` (default 4002): health, Prometheus metrics,
the redacted config, the DLQ, replay jobs, and the quarantine list.

```clojure
(require '[ankusa.sdk.admin :as admin])

(def a (admin/client "http://localhost:4002"))

(admin/health a)                                  ; => {:status "ok" :instance ... :roles [...]}
(admin/metrics a)                                 ; => Prometheus text, a string
(admin/list-dead-letters a {:limit 10})
(admin/list-quarantined a)

;; Replay dead letters ("dlq"), redrive an archived time window ("archive"), or
;; re-verify and release held hooks ("quarantine"), without flooding live traffic.
(def replay (admin/create-replay a {"kind" "dlq" "source_id" "demo" "rate" 500}))
(admin/get-replay a (:id replay))
(admin/update-replay a (:id replay) {"state" "paused"})
(admin/list-replays a)
```

`create-replay` answers `202` with a new job, or `200` with an existing running
or paused job whose filter matches, so a proxy retry is idempotent. A job moves
rows only while dispatch has spare capacity, so it slows down rather than
delaying live hooks; `update-replay` pauses, resumes, cancels, or re-paces one.
`get-replay` on a missing id is an `AdminRejectedError` with `:status 404` and
`:code "replay_not_found"`, and `update-replay` on a finished job is one with
`:status 409` and `:code "replay_finished"`.

`metrics` is empty on a node that has not captured, dispatched, or redeemed
anything yet: a Prometheus series only exists once its first event fires.

## Sources client

Tenant-scoped source definitions on the admin listener.

```clojure
(require '[ankusa.sdk.sources :as sources])

(def s (sources/client "http://localhost:4002"))

(sources/list-sources s "acme") ; => [{:tenant "acme" :name "billing" :source-id ... :ingest-path ...} ...]
(sources/create-source s "acme" "billing" {:sinks [{"type" "log"}]})
(sources/delete-source s "acme" "billing")
```

A spec is `{:sinks [...] :verify {...} :on-verify-failure "reject"}`; `nil`
fields are omitted from the request. Tenants and names must match
`[A-Za-z0-9_-]{1,64}`; anything else is a `SourceInvalidError` before any
request. Pass `:expected-version` to have a call check `/health` and raise
`VersionMismatchError` against a different server. The client is immutable, so
run `verify-version` once at startup and keep the client it returns: it carries
the fetched version, and later calls skip the probe.

```clojure
(def s (sources/verify-version (sources/client url {:expected-version "0.3.0"})))
```

A server whose source store is static answers writes with
`SourceStoreReadOnlyError`.

## Webhook helper

A worker consuming Ankusa's HTTP sink doesn't need a client — it needs the
hook's identity, which Ankusa attaches as headers:

```clojure
(require '[ankusa.sdk.webhook :as webhook]
         '[ankusa.sdk.idempotency :as idempotency])

(let [hook (webhook/parse-headers (:headers request))] ; Ring's header map
  ;; (:id hook) (:source hook) (:tenant hook) (:content-type hook)
  ;; (:dedupe-key hook) (:replay-id hook) (:idempotency-key hook)
  (idempotency/key hook)) ; the key Ankusa shipped; same rule as the queue message
```

`parse-headers` also takes a seq of `[name value]` pairs (header names matched
case-insensitively; the first of a repeated name wins). `x-ankusa-id` is
required — a missing or empty one is `MissingHookIdError`. Delivery is
at-least-once: dedupe on `idempotency/key`, not the hook id. `:source` defaults
to `""`; `:tenant` and `:content-type` are `nil` when absent, as are
`:dedupe-key`, `:replay-id` and `:idempotency-key` (also when present but empty).

### Verifying signed deliveries

An HTTP sink with a `secret` signs every delivery the
[Standard Webhooks](https://www.standardwebhooks.com/) way. Verify the raw
body before trusting it; answer `401` when it fails:

```clojure
(require '[ankusa.sdk.signature :as signature])

(try
  (signature/verify (:headers request) raw-body-bytes [secret])
  (catch clojure.lang.ExceptionInfo e
    {:status 401 :body (str "invalid signature: " (:code (ex-data e)))}))
```

Secrets are `whsec_` + base64, or any other string used as its own UTF-8
bytes; pass several during a rotation. `{:tolerance-seconds 300}` (the
default) bounds the `webhook-timestamp` window. Signatures are compared with
`MessageDigest/isEqual`.

## Consuming queue messages

Ankusa publishes each delivery as a JSON v1 message over the configured
messaging sink (RabbitMQ, Kafka, NATS, Redis); the HTTP sink sends the
original body plus `x-ankusa-*` headers instead.
Decode it, verify the body, and dedupe before doing any work: delivery is
at-least-once, and a provider retry or a replay must not re-run the effect.

```clojure
(require '[ankusa.sdk.message :as message]
         '[ankusa.sdk.idempotency :as idempotency])

(let [msg (message/decode payload)          ; a string or byte[]; throws InvalidMessageError
      hook (message/->hook msg gateway)     ; redeems the claim when the body is by reference
      key (idempotency/key hook)]           ; drops replays of processed events
  (process! key (:body hook)))
```

`decode` validates in the contract's order and throws `InvalidMessageError` with
a machine-readable `:code` (`"invalid_json"`, `"not_an_object"`,
`"unsupported_version"`, `"invalid_field"`, `"ambiguous_body"`, `"missing_body"`,
`"invalid_body_base64"`, `"size_mismatch"`, `"integrity"`, `"tenant_mismatch"`)
and, for a field failure, `:field`. It is never retryable: the same bytes always
fail the same way, so dead-letter them. The result is
`{:id :source-id :tenant-id :received-at :content-type :size :body :claim
:sha256 :dedupe-key :replay-id :idempotency-key :headers}`, with `:body` a
`byte[]` for an inline message and `nil` for a claim.

`->hook` requires a claim-check client even for an inline message, so a
consumer cannot forget to supply one and then fail the day a payload outgrows
the sink's inline limit. An inline message makes no request; a claim is
redeemed and a redemption failure propagates unchanged, with its `:retryable`.

Ankusa computes the idempotency key once per hook and ships it as the message's
`idempotency_key`; `idempotency/key` returns it. It is
`tenant:source_id:dedupe_key` when the source extracted a provider event key,
so a provider's retry of one event collapses even across brokers, and two
tenants that share a provider event id stay apart; otherwise it is the delivery
`id`, so a broker redelivery collapses. For a message from a node that predates
the field the helper computes the same key itself (tenant `default` when there
is none). Pass `{:include-replay true}` only when a replay must be processed
again. It accepts a decoded message, a hook, or parsed headers.

Record the key in a processed-ids table in the same transaction as the effect,
and skip an insert that conflicts:

```sql
CREATE TABLE processed_webhooks (
  idempotency_key text PRIMARY KEY,
  processed_at    timestamptz NOT NULL DEFAULT now()
);
```

```clojure
(jdbc/with-transaction [tx ds]
  (let [result (jdbc/execute-one! tx ["INSERT INTO processed_webhooks (idempotency_key)
                                       VALUES (?) ON CONFLICT DO NOTHING" key])]
    (when-not (zero? (:next.jdbc/update-count result))
      (apply-the-effect! tx body)))) ; already processed otherwise
```

## Options

Every client constructor takes `(client base-url)` or `(client base-url opts)`:

```clojure
(admin/client "http://localhost:4002"
              {:headers {"authorization" (str "Bearer " token)}
               :timeout-ms 30000})        ; default 10000, connect through last body byte
```

Every call is one request with no retries, and a redirect is never followed: a
`3xx` is an unavailable listener. Header names may be strings or keywords and
are lowercased. Header values print as `#ankusa.sdk/redacted`, so a client never
leaks a credential through `pr`, `pprint`, or a log line.

### The transport hook

`:transport` replaces the network. It is a function of one request map to one
response map, which is how tests run without a server:

```clojure
;; request
{:method "GET"
 :url "http://host:4002/v1/dlq?limit=10"
 :headers {"authorization" "Bearer ..."}
 :body nil               ; a byte[] when there is a JSON body
 :timeout-ms 10000}

;; response
{:status 200
 :headers {"content-type" "application/json"}
 :body (.getBytes "{\"entries\":[]}" "UTF-8")} ; a byte[], or nil for no body
```

Throwing any `Exception` is a transport failure (`:reason :transport`, with the
exception as the `ex-cause`). A transport must not follow redirects, and should
bound the whole call by `:timeout-ms`.

## Errors

Every failure is an `ex-info` whose `ex-data` has a `:type` keyword in the
`ankusa.sdk` namespace and that type's keys, all always present (`nil` when
they do not apply; keys are kebab-case). `ankusa.sdk.errors/error-type` returns
the keyword (or `nil` for an exception that is not the SDK's) and
`retryable?` is the one bit that decides dead-letter vs. retry:

```clojure
(try
  (routes/get-route r "stripe")
  (catch clojure.lang.ExceptionInfo e
    (case (errors/error-type e)
      :ankusa.sdk/RouteNotFoundError nil
      :ankusa.sdk/RoutesUnavailableError (retry-later!)
      (throw e))))
```

| `:type` (in `:ankusa.sdk`) | `:retryable` | Cause | Other keys |
| --- | --- | --- | --- |
| `InvalidClaimRefError` | `false` | `ref` isn't a well-formed claim-check URN, or `sha256` isn't 64 lowercase hex chars | |
| `ClaimNotFoundError` | `false` | gateway `404`: expired by retention, or never written | |
| `ClaimRejectedError` | `false` | gateway `4xx` other than `404`, `408`, `429` | `:status`, `:body` |
| `ClaimIntegrityError` | `false` | the bytes' sha256 doesn't match the expected `sha256` | |
| `ClaimCheckUnavailableError` | `true` | gateway unreachable, timeout, `5xx`, `408`, `429`, an unfollowed `3xx`, or any other non-`200` | `:status`, `:reason` |
| `MissingHookIdError` | `false` | `x-ankusa-id` is absent or empty | |
| `InvalidSignatureError` | `false` | a signed delivery does not verify | `:code`, `:field` |
| `InvalidMessageError` | `false` | a queue message is not a valid v1 message | `:code`, `:field` |
| `InvalidRouteIdError` | `false` | route id is not a string, empty, or exactly `.`/`..` | |
| `RouteNotFoundError` | `false` | routes listener `404` | |
| `RoutesRejectedError` | `false` | routes listener `4xx` other than `404` | `:status`, `:code`, `:field`, `:message`, `:conflicting-id`, `:max-routes` |
| `RoutesUnavailableError` | `true` | routes listener unreachable, `5xx`, an unfollowed `3xx`, or a non-JSON success body | `:status`, `:reason` |
| `RoleNotEnabledError` | `false` | admin listener `409` `role_not_enabled` | `:role` |
| `AdminRejectedError` | `false` | admin listener `4xx` other than that 409 | `:status`, `:code` |
| `AdminUnavailableError` | `true` | admin listener unreachable, `5xx`, an unfollowed `3xx`, or a non-JSON success body | `:status`, `:reason` |
| `SourceNotFoundError`, `SourceConflictError`, `SourceStoreReadOnlyError`, `SourceInvalidError`, `VersionMismatchError` | `false` | sources `404`, `409`, read-only store, `400` or invalid id, version mismatch | `:status`, `:body` |
| `SourcesUnavailableError` | `true` | admin listener unreachable, `5xx`, or any other unexpected status | `:status`, `:body` |

`:reason` is `:status` (the server answered a status the client cannot use;
`:status` holds it), `:transport` (nothing usable came back; `:status` is `nil`),
or `:invalid-json` (a `200` whose body was not JSON).

Caller misuse is an `IllegalArgumentException` instead, raised before any
request: a base URL that is not an absolute http(s) URL, an unknown option, a
header the JDK client refuses to set (`host`, `content-length`, ...), a header
value with CR or LF, a non-positive timeout, or a `decode` input that is not a
string or `byte[]`.

## Develop

```sh
mise run check:package sdk-clojure   # cljfmt, clj-kondo, tests, jar + POM checks
mise run check:conformance           # this package's cases plus every other SDK's
```

The toolchain is the Clojure CLI (`.mise/conf.d/sdk-clojure.toml`); the tools
run as `deps.edn` aliases: `clojure -X:test` (tests),
`clojure -M:fmt check|fix src test build.clj` (cljfmt),
`clojure -M:lint --lint src build.clj` (clj-kondo), and
`clojure -T:build jar` (the jar and POM). The conformance runner,
`test/ankusa/sdk/conformance_test.clj`, reads the vectors from
`../../conformance/cases/` and requires only the public `ankusa.sdk.*`
namespaces. `mise run release:prepare minor sdk-clojure` (then `release:tag`) is
the release flow; see
[`docs/releasing.md`](https://github.com/jamescarr/ankusa/blob/main/docs/releasing.md).
