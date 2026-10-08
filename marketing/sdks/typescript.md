Subreddit: r/typescript (cross-post: r/node)
Title: I built a self-hosted webhook receiver (Elixir) with a TypeScript SDK for the worker side — durable before 2xx, retries, DLQ, replay
Flair: Show & Tell
Status: ready

This is my own project, so take it as a pitch. I built Ankusa, a self-hosted webhook receiver: it answers `2xx` only after the hook is durably accepted, then delivers to HTTP, RabbitMQ, Kafka, NATS or Redis sinks with retries, a dead-letter queue and replay. The TypeScript SDK is the worker side of that.

It is Node 20 or newer, ESM only, and its types are generated from the claim-check OpenAPI spec. `parseHeaders` takes Express's `req.headers` and returns the hook's identity from the `x-ankusa-*` headers, and `redeem()` fetches a claim-check body and verifies its sha256. Every failure is a `ClaimCheckError` with a `retryable` boolean, so the worker knows whether to requeue or dead-letter. The SDK does not verify provider signatures, because Ankusa does that at the edge, and it ships no broker client: bring your own.

All eight SDKs run the same 131 shared conformance cases, so behavior should not drift between languages. Delivery is at-least-once, so your worker must be idempotent. Dedupe on `x-ankusa-idempotency-key` (`ankusa_idempotency_key` on queue headers).

Every phase of my kind-cluster load test, including the one that kills pods mid-run, reported `missing: 0` (measured on an Apple M4 Pro laptop under kind/OrbStack, not a production-scale claim). Everything here is 0.x and APIs may change.

```
npm install ankusa

//   claim:  urn:ankusa:claim:v1:<tenant>:<claim_id>   (claim_id: uppercase ULID)
//   sha256: 64 lowercase hex chars, the digest of the claim's bytes
async function resolveBody(ref: string, sha256: string): Promise<Buffer> {
  try {
    return await claimCheck.redeem(ref, sha256);
  } catch (err) {
    if (err instanceof ClaimCheckError && !err.retryable) {
      // bad ref, 404, or an integrity mismatch: dead-letter, don't requeue
      throw err;
    }
    // gateway unreachable or 5xx: safe to retry
    throw err;
  }
}
```

SDK README: https://github.com/jamescarr/ankusa/blob/main/packages/sdk-typescript/README.md
Launch post: {{BLOG_URL}}/ankusa-launch
Repo: https://github.com/jamescarr/ankusa
