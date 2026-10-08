Subreddit: r/rust
Title: I built a self-hosted webhook receiver (Elixir) with a Rust SDK for the worker side — durable before 2xx, retries, DLQ, replay
Flair: Show & Tell
Status: ready

This is my own project, so take it as a pitch. I built Ankusa, a self-hosted webhook receiver: it answers `2xx` only after the hook is durably accepted, then delivers to HTTP, RabbitMQ, Kafka, NATS or Redis sinks with retries, a dead-letter queue and replay. The Rust SDK is the worker side of that.

It is async on Tokio, MSRV 1.85, and `ClientBuilder::with_transport` lets you inject a `Transport` so tests never touch the network. `redeem` fetches a claim-check body and verifies its sha256, and `is_retryable()` on `ClaimCheckError` separates "try again later" from "give up or dead-letter". The SDK does not verify provider signatures, because Ankusa does that at the edge, and it ships no broker client: bring your own.

All eight SDKs run the same 131 shared conformance cases, so behavior should not drift between languages. Delivery is at-least-once, so your worker must be idempotent. Dedupe on `x-ankusa-idempotency-key` (`ankusa_idempotency_key` on queue headers).

Every phase of my kind-cluster load test, including the one that kills pods mid-run, reported `missing: 0` (measured on an Apple M4 Pro laptop under kind/OrbStack, not a production-scale claim). Everything here is 0.x and APIs may change.

```
cargo add ankusa

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
```

SDK README: https://github.com/jamescarr/ankusa/blob/main/packages/sdk-rust/README.md
Launch post: {{BLOG_URL}}/ankusa-launch
Repo: https://github.com/jamescarr/ankusa
