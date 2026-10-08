Subreddit: r/Python
Title: I built a self-hosted webhook receiver (Elixir) with a Python SDK for the worker side — durable before 2xx, retries, DLQ, replay
Flair: Showcase
Status: ready

This is my own project, so take it as a pitch. I built Ankusa, a self-hosted webhook receiver: it answers `2xx` only after the hook is durably accepted, then delivers to HTTP, RabbitMQ, Kafka, NATS or Redis sinks with retries, a dead-letter queue and replay. The Python SDK is the worker side of that.

It needs Python 3.11 or newer. `ClaimCheckClient` owns an `httpx.Client` and is a context manager, so `with ClaimCheckClient(...) as claim_check:` closes it for you. `redeem()` fetches a claim-check body and verifies its sha256, and every failure is a `ClaimCheckError` with a `retryable` attribute, so the worker knows whether to requeue or dead-letter. The SDK does not verify provider signatures, because Ankusa does that at the edge, and it ships no broker client: bring your own.

All eight SDKs run the same 131 shared conformance cases, so behavior should not drift between languages. Delivery is at-least-once, so your worker must be idempotent. Dedupe on `x-ankusa-idempotency-key` (`ankusa_idempotency_key` on queue headers).

Every phase of my kind-cluster load test, including the one that kills pods mid-run, reported `missing: 0` (measured on an Apple M4 Pro laptop under kind/OrbStack, not a production-scale claim). Everything here is 0.x and APIs may change.

```
pip install ankusa

claim_check = ClaimCheckClient(os.environ.get("CLAIM_CHECK_URL", "http://localhost:4001"))

# A queue message that carries a claim also carries its sha256:
#   "claim":  "urn:ankusa:claim:v1:<tenant>:<claim_id>"  (claim_id: uppercase ULID)
#   "sha256": 64-char lowercase hex of the claim's bytes
def resolve_body(message: dict) -> bytes:
    try:
        return claim_check.redeem(message["claim"], message["sha256"])
    except ClaimCheckError as err:
        if not err.retryable:
            # bad ref/sha256, 404, or an integrity mismatch: dead-letter, don't requeue
            raise
        # gateway unreachable or 5xx: safe to retry
        raise
```

SDK README: https://github.com/jamescarr/ankusa/blob/main/packages/sdk-python/README.md
Launch post: {{BLOG_URL}}/ankusa-launch
Repo: https://github.com/jamescarr/ankusa
