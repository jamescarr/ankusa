# Claim Check Pattern

**Problem:** Queue throughput degrades with large messages. Most queuing systems (RabbitMQ, SQS, Kafka) are optimized for small messages. A 5 MB webhook payload bloats memory, slows dispatch, and can exceed broker limits.

**Solution:** Check large payloads into durable blob storage (S3/GCS/LocalFS), publish a small ticket, and let consumers redeem it over HTTP — without needing blob-store credentials of their own.

Ankusa implements this automatically for every queue-style sink (`Sink.RabbitMQ`, `Sink.Kafka`). Payloads under `:inline_max_bytes` (default 8 KiB) ride along base64-encoded; anything larger is checked in and the message carries a claim ticket instead.

## Message shape

Small (inline):
```json
{
  "v": 1,
  "id": "01a0b1c2...",
  "source_id": "stripe",
  "tenant_id": "acme",
  "received_at": 1737500000000,
  "content_type": "application/json",
  "size": 245,
  "body_base64": "eyJpZCI6..."
}
```

Large (claim-checked):
```json
{
  "v": 1,
  "id": "01a0b1c2...",
  "source_id": "stripe",
  "tenant_id": "acme",
  "received_at": 1737500000000,
  "content_type": "application/octet-stream",
  "size": 524288,
  "claim": {
    "v": 1,
    "tenant_id": "acme",
    "id": "01a0b1c2...",
    "size": 524288,
    "sha256": "d4e5f6a7...",
    "content_type": "application/octet-stream"
  }
}
```

The `claim` object is a redemption ticket. Consumers POST it to the claim-check gateway and get the original bytes back.

## Topology options

### Single service (default)

Dispatch calls `Ankusa.ClaimCheck` directly through the configured blob store. No HTTP hop, no separate process. This is the default: one service, two roles (`:dispatch` and `:claim_check`), serving both ingest and claim redemption.

```elixir
config :ankusa, :default,
  roles: [:edge, :dispatch, :storage, :claim_check],
  storage: %{blob_store: {Ankusa.BlobStore.S3, [bucket: "webhooks"]}},
  claim_check: %{
    port: 4001,
    api_tokens: %{"worker-1" => "secret-token-here"},
    adapter: {Ankusa.ClaimCheck.Direct, []}  # reuses storage.blob_store
  }
```

### Distributed (claim-check gateway)

A dedicated claim-check service with `:claim_check` role only. Ingest nodes use `Ankusa.ClaimCheck.Remote` to check in over HTTP; consumers redeem from the same gateway. This separates blob credentials: only the gateway holds them.

**Gateway node:**
```elixir
config :ankusa, :claim_gateway,
  roles: [:claim_check],  # ONLY claim_check
  claim_check: %{
    port: 4001,
    api_tokens: %{
      "ingest-1" => "ingest-secret",
      "worker-1" => "worker-secret"
    },
    adapter: {Ankusa.ClaimCheck.Direct, blob_store: {Ankusa.BlobStore.S3, [...]}}
  }
```

**Ingest node (no S3 credentials):**
```elixir
config :ankusa, :default,
  roles: [:edge, :dispatch, :storage],
  claim_check: %{
    adapter: {Ankusa.ClaimCheck.Remote,
      url: "http://claim-gateway:4001",
      token: "ingest-secret"}
  }
```

Workers call `GET /v1/claims/:id` with `Authorization: Bearer worker-secret`.

See `examples/kafka-sqs-consumer/` for a worked Docker Compose topology with separated roles.

## Security

### Authentication

Every request needs `Authorization: Bearer <token>`, checked against `claim_check.api_tokens`. The token is hashed (SHA-256) and compared; plaintext tokens never hit logs.

Tokens are defined per instance, not per tenant. Use distinct tokens for different client types (ingest vs. worker) to scope revocation.

### No tenant isolation

The claim-check gateway serves every tenant's claims under one HTTP surface. A valid token can redeem *any* claim by `id`. Consumers MUST verify `tenant_id` matches their expected scope before processing.

This is not a bug — it mirrors the queue itself, which also carries every tenant's messages. The claim ticket rides the same queue message; if a consumer can see the message, it can redeem the claim.

Multi-tenant isolation belongs upstream: route tenants to separate queues/topics, or filter at the worker.

### Integrity

The `sha256` field is the hex-encoded SHA-256 of the original bytes. Consumers SHOULD verify it after redemption. A mismatch means corruption or tampering.

## Retention

`claim_check.retention_days` (LocalFS only) sweeps expired claims. `nil` disables it. S3/GCS lifecycle policies handle their own retention.

Consumers SHOULD redeem promptly. Claims are immutable; once written, they stay until swept or manually deleted.

## Examples

- `examples/rabbitmq-consumer/` — RabbitMQ sink with claim check
- `examples/kafka-sqs-consumer/` — Kafka sink, bridge to SQS, claim redemption from a worker with no S3 credentials

Both show the same pattern: large webhook → ingest checks in to S3 → queue carries ticket → worker redeems over HTTP.
