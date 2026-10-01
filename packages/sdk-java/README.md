# ankusa-sdk (Java)

The Java client SDK for [Ankusa](https://github.com/jamescarr/ankusa)
deployments: the claim-check gateway client, the route-management client
(`routes.admin.port`), the operator (`admin.port`) client, the tenant-scoped
sources client, and a helper for receiving Ankusa's HTTP sink deliveries. It
implements exactly the surface in
[`conformance/`](https://github.com/jamescarr/ankusa/tree/main/conformance),
the language-neutral vectors every Ankusa SDK passes.

Java 17 or newer. Runtime dependencies are Jackson 3
(`tools.jackson.core:jackson-databind`) and the JSpecify nullness annotations;
HTTP goes through the JDK's own `java.net.http.HttpClient`. Every client is
immutable and safe to share across threads.

## Install

Not on Maven Central until the first release (0.3.0). Until then, build it
into your local Maven repository and depend on version `0.0.0`:

```sh
cd packages/sdk-java && sbt --server --batch publishM2
```

Maven:

```xml
<dependency>
  <groupId>io.github.jamescarr</groupId>
  <artifactId>ankusa-sdk</artifactId>
  <version>0.0.0</version> <!-- 0.3.0 from Maven Central once released -->
</dependency>
```

Gradle (with `mavenLocal()` in `repositories` until the release):

```kotlin
implementation("io.github.jamescarr:ankusa-sdk:0.0.0") // 0.3.0 once released
```

On the module path the jar is the automatic module `io.github.jamescarr.ankusa`.

## Claim-check client

Redeem a claim-check ref, verify the bytes it returns against the sha256 the
queue message carries next to it, and classify failures into dead-letter vs.
retry, without holding any object-store credentials.

```java
import io.github.jamescarr.ankusa.claimcheck.ClaimCheckClient;
import io.github.jamescarr.ankusa.claimcheck.ClaimCheckError;

ClaimCheckClient claimCheck = new ClaimCheckClient(System.getenv("CLAIM_CHECK_URL"));

try {
  // claim:  urn:ankusa:claim:v1:<tenant>:<claim_id>   (claim_id: uppercase ULID)
  // sha256: 64 lowercase hex chars, the digest of the claim's bytes
  byte[] body = claimCheck.redeem(claim, sha256);
} catch (ClaimCheckError e) {
  if (!e.retryable()) {
    // bad ref, 404, or an integrity mismatch: dead-letter, don't requeue
  }
  throw e;
}
```

`redeem` parses the ref, checks `sha256` is 64 lowercase hex chars, fetches
`GET /v1/claims/{tenant_id}/{claim_id}`, and verifies the bytes against
`sha256` before returning them. `health()` hits `GET /health` for a liveness
probe. `ParsedClaimRef.parse(ref)` splits a ref without any request.

## Routes client

Manage route definitions and the global IP rules on the route-management
listener (`routes.admin.port`, default 4003).

```java
RoutesClient routes = new RoutesClient("http://localhost:4003");

routes.createRoute(RouteInput.builder("/webhooks/stripe").id("stripe").build());
RoutePage page = routes.listRoutes(ListRoutesParams.builder().enabled(true).limit(10).build());
IpRules rules = routes.getIpRules();
DryRunResult result = routes.testRoute(new DryRunRequest("POST", "/webhooks/stripe", "203.0.113.7"));
```

Methods: `health`, `listRoutes`, `createRoute`, `getRoute`, `replaceRoute`,
`updateRoute`, `deleteRoute`, `getIpRules`, `putIpRules`, `testRoute`. The
id-taking methods reject an id that is null, empty, or exactly `.`/`..` with
`InvalidRouteIdError` before making any request; every other id is
percent-encoded as one path segment.

## Admin client

The operator API on `admin.port` (default 4002): health, Prometheus metrics,
the redacted config, the DLQ, and the quarantine list.

```java
AdminClient admin = new AdminClient("http://localhost:4002");

AdminHealth health = admin.health();                    // status, instance, roles
String metrics = admin.metrics();                       // Prometheus text
DlqPage dlq = admin.listDeadLetters(ListDeadLettersParams.builder().limit(10).build());
Replayed replayed = admin.replayDeadLetters(ReplayFilter.builder().sourceId("demo").build());
QuarantinePage quarantine = admin.listQuarantined();
```

`metrics()` is empty on a node that has not captured, dispatched, or redeemed
anything yet: a Prometheus series only exists once its first event fires.

## Sources client

Tenant-scoped source definitions on the admin listener.

```java
SourcesClient sources = new SourcesClient("http://localhost:4002");

List<Source> all = sources.listSources("acme");
Source created =
    sources.createSource(
        "acme", "billing", new SourceSpec(List.of(Map.of("type", "log")), null, null));
sources.deleteSource("acme", "billing");
```

Tenants and names must match `[A-Za-z0-9_-]{1,64}`; anything else is a
`SourceInvalidError` before any request. Pass an expected version
(`new SourcesClient(url, ClientOptions.defaults(), "0.3.0")`) to have the first
call check `/health` once and raise `VersionMismatchError` on every call against
a different server. A server whose source store is static answers writes with
`SourceStoreReadOnlyError`.

## Webhook helper

A worker consuming Ankusa's HTTP sink doesn't need a client — it needs the
hook's identity, which Ankusa attaches as headers:

```java
HookHeaders hook = HookHeaders.parse(request::getHeader); // e.g. HttpServletRequest
// hook.id(), hook.source(), hook.tenant(), hook.contentType()
```

`HookHeaders.parse` also takes a `Map<String, List<String>>` (header names
matched case-insensitively). `x-ankusa-id` is required — a missing or empty
one is `MissingHookIdError`; dedupe on it, since delivery is at-least-once.
`source()` defaults to `""`; `tenant()` and `contentType()` are null when
absent.

## Options

```java
ClientOptions options =
    ClientOptions.builder()
        .header("authorization", "Bearer " + token)
        .timeout(Duration.ofSeconds(30)) // default 10 s, connect through last body byte
        .build();
AdminClient admin = new AdminClient("http://localhost:4002", options);
```

Every call is one request with no retries, and a redirect is never followed: a
`3xx` is an unavailable listener. `ClientOptions.builder().transport(...)`
replaces the network — a `Transport` is one method from `TransportRequest` to
`TransportResponse`, which is how tests run without a server.

## Errors

Every failure is an unchecked `AnkusaException`, and each client family seals
its own subtypes (`ClaimCheckError`, `RoutesError`, `AdminError`,
`SourcesError`), so one `retryable()` bit decides dead-letter vs. retry:

| Type | `retryable()` | Cause |
| --- | --- | --- |
| `InvalidClaimRefError` | `false` | `ref` isn't a well-formed claim-check URN, or `sha256` isn't 64 lowercase hex chars |
| `ClaimNotFoundError` | `false` | gateway `404`: expired by retention, or never written |
| `ClaimRejectedError` | `false` | gateway `4xx` other than `404` (`status()`, `body()`) |
| `ClaimIntegrityError` | `false` | the bytes' sha256 doesn't match the expected `sha256` |
| `ClaimCheckUnavailableError` | `true` | gateway unreachable, timeout, `5xx`, an unfollowed `3xx`, or any other non-`200` |
| `MissingHookIdError` | `false` | `x-ankusa-id` is absent or empty |
| `InvalidRouteIdError` | `false` | route id is null, empty, or exactly `.`/`..` |
| `RouteNotFoundError` | `false` | routes listener `404` |
| `RoutesRejectedError` | `false` | routes listener `4xx` other than `404` (`status()`, `code()`, `field()`, `detail()`, `conflictingId()`, `maxRoutes()`) |
| `RoutesUnavailableError` | `true` | routes listener unreachable, `5xx`, an unfollowed `3xx`, or a non-JSON success body |
| `RoleNotEnabledError` | `false` | admin listener `409` `role_not_enabled` (`role()`) |
| `AdminRejectedError` | `false` | admin listener `4xx` other than that 409 (`status()`, `code()`) |
| `AdminUnavailableError` | `true` | admin listener unreachable, `5xx`, or an unfollowed `3xx` |
| `SourceNotFoundError`, `SourceConflictError`, `SourceStoreReadOnlyError`, `SourceInvalidError`, `VersionMismatchError` | `false` | sources `404`, `409`, read-only store, `400` or invalid id, version mismatch |
| `SourcesUnavailableError` | `true` | admin listener unreachable, `5xx`, or any other unexpected status |

Caller misuse is an `IllegalArgumentException` instead, raised before any
request: a base URL that is not an absolute http(s) URL, a header the JDK
client refuses to set (`host`, `content-length`, ...), a header value with CR
or LF, a non-positive timeout, or a body that cannot be encoded as JSON.

## Develop

```sh
mise run check:package sdk-java   # javafmtCheckAll, testFull, doc, publish + bundle checks
mise run check:conformance        # this package's cases plus every other SDK's
```

The build is sbt 2 (`packages/sdk-java/build.sbt`); run it as
`sbt --server --batch "<cmd>; <cmd>"`. sbt 2 caches `test`, so use `testFull`
or `testOnly`. The conformance runner,
`src/test/java/io/github/jamescarr/ankusa/conformance/ConformanceTest.java`,
reads the vectors from `../../conformance/cases/` and imports only the public
API. For the jdtls language server, `sbt --server --batch jdtlsProject` writes
the `.project`/`.classpath` it reads. `mise run release:prepare minor sdk-java`
(then `release:tag`) is the release flow; see
[`docs/releasing.md`](https://github.com/jamescarr/ankusa/blob/main/docs/releasing.md).
