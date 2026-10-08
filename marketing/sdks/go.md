Subreddit: r/golang
Title: I built a self-hosted webhook receiver (Elixir) with a Go SDK for the worker side — durable before 2xx, retries, DLQ, replay
Flair: Show & Tell
Status: ready

This is my own project, so take it as a pitch. I built Ankusa, a self-hosted webhook receiver: it answers `2xx` only after the hook is durably accepted, then delivers to HTTP, RabbitMQ, Kafka, NATS or Redis sinks with retries, a dead-letter queue and replay. The Go SDK is the worker side of that.

It uses the standard library only; `go.mod` has no `require`. `Redeem` fetches a claim-check body and verifies its sha256, and every failure implements `ankusa.Error` with `Retryable() bool`, so `errors.As(err, &apiErr) && !apiErr.Retryable()` tells you to dead-letter instead of requeue. The SDK does not verify provider signatures, because Ankusa does that at the edge, and it ships no broker client: bring your own.

All eight SDKs run the same 131 shared conformance cases, so behavior should not drift between languages. Delivery is at-least-once, so your worker must be idempotent. Dedupe on `x-ankusa-idempotency-key` (`ankusa_idempotency_key` on queue headers).

Every phase of my kind-cluster load test, including the one that kills pods mid-run, reported `missing: 0` (measured on an Apple M4 Pro laptop under kind/OrbStack, not a production-scale claim). Everything here is 0.x and APIs may change.

```
go get github.com/jamescarr/ankusa/packages/sdk-go

body, err := claimCheck.Redeem(ctx, ref, sha256)
if err != nil {
    var apiErr ankusa.Error
    if errors.As(err, &apiErr) && !apiErr.Retryable() {
        // bad ref, 404, or an integrity mismatch: dead-letter, don't requeue
    }
    return err
}
_ = body // the verified bytes
```

SDK README: https://github.com/jamescarr/ankusa/blob/main/packages/sdk-go/README.md
Launch post: {{BLOG_URL}}/ankusa-launch
Repo: https://github.com/jamescarr/ankusa
