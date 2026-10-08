Subreddit: r/java
Title: I built a self-hosted webhook receiver (Elixir) with a Java SDK for the worker side — durable before 2xx, retries, DLQ, replay
Flair: none
Status: gated: sdk-java 0.3.0 on Maven Central
Note: do not post this until `mise run status` shows `sdk-java ... yes`. The first Maven Central release is 0.3.0; until then the SDK is in the repo only.

This is my own project, so read it as a launch post. I built Ankusa, a self-hosted webhook receiver written in Elixir that never answers `2xx` until the hook is durably accepted, then retries to your sinks, dead-letters on give-up, and replays on demand. It runs as one container and one YAML file, and your worker can be in any language.

The Java side is `io.github.jamescarr:ankusa-sdk`. It targets Java 17 or newer and goes through the JDK's own `java.net.http.HttpClient`; the runtime dependencies are Jackson 3 and the JSpecify nullness annotations. It parses the `x-ankusa-*` headers, decodes queue messages, redeems claim-check references with a sha256 check, and gives every error a `retryable()` bit so you know whether to requeue or dead-letter. It does not verify provider signatures, because Ankusa does that at the edge, and it ships no broker client, so bring your own.

The SDK passes the same 131 shared conformance cases as the other seven SDKs.

Delivery is at-least-once, so the same hook can reach you twice. Make your consumer idempotent and dedupe on the `ankusa_idempotency_key` header (`x-ankusa-idempotency-key` over HTTP). Everything is 0.x and APIs may change.

On numbers: in a Kubernetes chaos run where I deleted two Ankusa pods and a consumer pod mid-load, every phase, including chaos, reported `missing: 0` (measured on an Apple M4 Pro laptop under kind/OrbStack, not a production-scale claim).

```
io.github.jamescarr:ankusa-sdk

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

SDK README: https://github.com/jamescarr/ankusa/blob/main/packages/sdk-java/README.md
Launch post: {{BLOG_URL}}/ankusa-launch
Repo: https://github.com/jamescarr/ankusa
