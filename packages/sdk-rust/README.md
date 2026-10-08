# ankusa

Client SDK for [Ankusa](https://github.com/jamescarr/ankusa), the self-hosted
webhook receiver: claim-check redemption, webhook header parsing, queue-message
decoding, and clients for the route-management and operator APIs.

Async only, on Tokio: every call is an `async fn` you drive yourself, and every
request is bounded by `tokio::time::timeout`, so the runtime needs its timers (a
plain `#[tokio::main]` has them). The crate's own HTTP client is `reqwest`,
behind a `Transport` hook you can replace with anything that speaks
`http::Request<Bytes>`/`http::Response<Bytes>`.

## Install

```sh
cargo add ankusa
```

The crate is async: add `tokio = { version = "1", features = ["macros",
"rt-multi-thread"] }` to your binary — or any other Tokio runtime, as long as
its time driver is enabled. TLS uses `rustls` by default;
`default-features = false` builds a plain-HTTP-only client.

## Claim-check client

A queue worker that receives `claim_ref` in a job message calls `redeem`: it
parses the ref, fetches the bytes from the claim-check gateway, and verifies
them against the expected sha256 before handing them over.

```rust,no_run
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
use ankusa::ClaimCheckClient;

let client = ClaimCheckClient::new("http://127.0.0.1:4001")?;
let bytes = client
    .redeem(
        "urn:ankusa:claim:v1:acme:01M39VMD8RA3C5HR4RBV67Y002",
        "8e1bed597394cf01672e49232d9929c8bc6b3d6ea4e2f489517ec88701f01581",
    )
    .await?;
println!("{} bytes", bytes.len());
# Ok(())
# }
```

`parse_claim_ref` is exported standalone, for a consumer that wants the tenant
and claim id without a request:

```rust
use ankusa::parse_claim_ref;

let parsed = parse_claim_ref("urn:ankusa:claim:v1:acme:01M39VMD8RA3C5HR4RBV67Y002")?;
assert_eq!(parsed.tenant_id(), "acme");
assert_eq!(parsed.path(), "/v1/claims/acme/01M39VMD8RA3C5HR4RBV67Y002");
# Ok::<(), ankusa::InvalidClaimRefError>(())
```

Every failure is a `ClaimCheckError`; `is_retryable()` distinguishes "try again
later" (the gateway was unavailable) from "give up or dead-letter" (the claim is
gone, the sha256 did not match, the gateway rejected the request):

```rust,no_run
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
use ankusa::ClaimCheckClient;

let client = ClaimCheckClient::new("http://127.0.0.1:4001")?;
match client
    .redeem("urn:ankusa:claim:v1:acme:01M39VMD8RA3C5HR4RBV67Y002", "8e1bed597394cf01672e49232d9929c8bc6b3d6ea4e2f489517ec88701f01581")
    .await
{
    Err(err) if err.is_retryable() => eprintln!("retry later: {err}"),
    Err(err) => eprintln!("give up: {err}"),
    Ok(bytes) => println!("{} bytes", bytes.len()),
}
# Ok(())
# }
```

## Webhook helper

For the receiving side of Ankusa's HTTP sink: `parse_headers` reads the
`x-ankusa-*` headers (plus `content-type`) out of a `http::HeaderMap`,
case-insensitively, and fails when `x-ankusa-id` is missing or empty.

```rust
use ankusa::{parse_headers, MissingHookIdError};
use ankusa::http::HeaderMap;

fn hook_id(headers: &HeaderMap) -> Result<&str, MissingHookIdError> {
    Ok(parse_headers(headers)?.id)
}

let mut headers = HeaderMap::new();
headers.insert("x-ankusa-id", "01a0".parse()?);
headers.insert("x-ankusa-source", "demo".parse()?);
assert_eq!(hook_id(&headers)?, "01a0");
# Ok::<(), Box<dyn std::error::Error>>(())
```

`HookHeaders` also carries `dedupe_key` (from `x-ankusa-dedupe-key`),
`replay_id` (from `x-ankusa-replay-id`) and `idempotency_key` (from
`x-ankusa-idempotency-key`), each `None` when the header is absent or empty,
and its `idempotency_key(include_replay)` applies the same rule as `Message`
below.

## Consuming queue messages

An Ankusa sink delivers the `v: 1` queue message as JSON: the body inline
(`body_base64`) or as a claim-check ref (`claim`), the body's `sha256`, the
provider's `dedupe_key`, the `idempotency_key` Ankusa computed for the hook,
and forwarded `headers`, and a `replay_id` when the
delivery is a replay. `decode_message` validates all of it and, on any
malformed input, returns an `InvalidMessageError` that is never retryable
(`err.code` is `invalid_json`, `size_mismatch`, `integrity`, …; `err.field`
names the offending key when the code is `invalid_field`).

Key a processed-ids table on `idempotency_key`: the key Ankusa shipped
(`tenant:source_id:dedupe_key` when the hook has a provider event key, else
`id`). For a message from a node that predates the field it computes the same
key itself, with tenant `default` when there is none. Pass
`include_replay: true` only if the consumer must reprocess replays — the
default drops replays of events it already processed.

```rust
use ankusa::{InvalidMessageError, decode_message};
use std::collections::HashSet;

/// Returns `false` when the delivery was already processed.
fn first_delivery(
    raw: &[u8],
    processed: &mut HashSet<String>,
) -> Result<bool, InvalidMessageError> {
    let message = decode_message(raw)?;
    let key = message.idempotency_key(false);
    if !processed.insert(key) {
        return Ok(false); // a provider retry, or a replay of an event we ran
    }
    // ... perform the effect, keyed by `key` ...
    Ok(true)
}
```

Both body forms are supported: an inline message exposes the bytes as
`message.body`, while a claim-form message exposes the ref as `message.claim`
for a `ClaimCheckClient::redeem` call.

## Routes client

`RoutesClient` drives the route-management listener (`routes.admin.port`): the
route table this node enforces, plus the global IP rules and a dry run that
shows which route a request would match.

```rust,no_run
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
use ankusa::{ListRoutesParams, RouteInput, RoutesClient};

let routes = RoutesClient::new("http://127.0.0.1:4003")?;
let created = routes
    .create_route(&RouteInput {
        id: Some("stripe".to_owned()),
        path: "/webhooks/stripe".to_owned(),
        metadata: Some([("owner".to_owned(), "payments".into())].into_iter().collect()),
        ..RouteInput::default()
    })
    .await?;
println!("{} captures {:?}", created.id, created.methods);

let page = routes
    .list_routes(&ListRoutesParams {
        enabled: Some(true),
        limit: Some(10),
        ..ListRoutesParams::default()
    })
    .await?;
println!("{} routes, next cursor {:?}", page.routes.len(), page.next_cursor);
# Ok(())
# }
```

Route ids travel as one percent-encoded path segment, and an id that is empty,
`.` or `..` is refused with `RoutesError::InvalidRouteId` before any request is
sent.

## Admin client

`AdminClient` drives the operator listener (`admin.port`): health, Prometheus
metrics, the redacted config, the dead-letter queue, and quarantine.

```rust,no_run
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
use ankusa::{AdminClient, ListDeadLettersParams};

let admin = AdminClient::new("http://127.0.0.1:4002")?;
let health = admin.health().await?;
if !health.roles.iter().any(|role| role == "dispatch") {
    eprintln!("this node does not run dispatch");
}

let page = admin
    .list_dead_letters(&ListDeadLettersParams {
        source_id: Some("demo".to_owned()),
        limit: Some(100),
        ..ListDeadLettersParams::default()
    })
    .await?;
println!("{} of {} dead letters", page.entries.len(), page.total);
# Ok(())
# }
```

Replay jobs are managed on the same listener. `create_replay` starts one (a
`dlq` job over dead-lettered rows, or an `archive` job over a time window) and
returns the running job when the same spec is posted twice, so a proxy retry is
safe:

```rust,no_run
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
use ankusa::{AdminClient, ReplayPatch, ReplaySpec};

let admin = AdminClient::new("http://127.0.0.1:4002")?;
let replay = admin
    .create_replay(&ReplaySpec {
        kind: "dlq".to_owned(),
        source_id: Some("stripe".to_owned()),
        rate: Some(500),
        ..ReplaySpec::default()
    })
    .await?;
println!("replay {} is {}", replay.id, replay.state);

let jobs = admin.list_replays().await?;
println!("{} jobs", jobs.replays.len());

let paused = admin
    .update_replay(
        &replay.id,
        &ReplayPatch {
            state: Some("paused".to_owned()),
            ..ReplayPatch::default()
        },
    )
    .await?;
println!("replay {} is now {}", paused.id, paused.state);
# Ok(())
# }
```

## Custom transport

`ClientBuilder` shares a base URL, headers, and a timeout across whichever
client you build, and `Transport` replaces the network entirely — useful for a
proxy, a fake in a test, or an HTTP stack you already have. A transport must
not follow redirects: a `3xx` is reported as "unavailable". The timeout stays
Tokio's — every request is bounded by `tokio::time::timeout`, whatever the
transport — so a Tokio runtime with timers is still required.

```rust,no_run
use ankusa::{ClientBuilder, Transport, TransportError};
use ankusa::bytes::Bytes;
use ankusa::http::{Request, Response};
use std::future::Future;

#[derive(Debug)]
struct Echo;

impl Transport for Echo {
    fn send(
        &self,
        _request: Request<Bytes>,
    ) -> impl Future<Output = Result<Response<Bytes>, TransportError>> + Send {
        async move {
            Response::builder()
                .status(200)
                .body(Bytes::from_static(br#"{"status":"ok"}"#))
                .map_err(TransportError::new)
        }
    }
}

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let client = ClientBuilder::with_transport("http://gateway.invalid", Echo)
    .header("authorization", "Bearer t0k3n")
    .timeout(std::time::Duration::from_secs(5))
    .claim_check()?;
let health = client.health().await?;
println!("{}", health.status);
# Ok(())
# }
```

## Develop

```sh
cargo test --locked            # unit tests, README doctests, and the conformance vectors
mise run check:package sdk-rust # format, clippy -D warnings, tests, docs, cargo package
```

The conformance suite runs the language-neutral vectors in
[`conformance/`](https://github.com/jamescarr/ankusa/tree/main/conformance)
against this crate's public API, so `ankusa` passes the same conformance cases the
TypeScript and Python SDKs do.
