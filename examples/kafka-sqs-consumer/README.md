# Kafka → SQS Consumer Example

Full end-to-end topology: Ankusa writes to Redpanda, a Redpanda Connect bridge
consumes from Kafka and publishes to SQS, and a TypeScript worker processes from
SQS — redeeming claim-checked payloads from the claim-check service over HTTP,
with **no direct S3 credentials**.

This demonstrates:
- `Ankusa.Sink.Kafka` producing to a topic
- Claim Check pattern with payloads > 8 KiB
- Messaging Bridge (Redpanda Connect): Kafka → SQS
- SQS FIFO ordering per `MessageGroupId` (Kafka key)
- Worker deduplication on envelope `id`
- Dead-letter queue for permanent failures

## Architecture

```
┌─────────┐  POST    ┌────────┐  Kafka   ┌─────────┐
│ curl /  │ ──────▶ │ ingest │ ───────▶ │ Redpanda│
│ Provider│  hooks   │  app   │  topic   │ (Kafka) │
└─────────┘          └────────┘          └─────────┘
                          │                    │
                          │ check-in           │ consume
                          ▼                    ▼
                     ┌─────────┐         ┌──────────┐
                     │  floci  │         │  bridge  │
                     │ S3 + SQS│ ◀───────│ (Redpanda│
                     └─────────┘  FIFO   │ Connect) │
                          │              └──────────┘
                     GET  │                    │
                    /claims│               SendMessage
                          │                    ▼
                     ┌────┴────┐         ┌─────────┐
                     │  claim  │         │   SQS   │
                     │  -check │         │  FIFO   │
                     │ service │         │  queue  │
                     └─────────┘         └─────────┘
                                               │
                                               │ ReceiveMessage
                                               ▼
                                         ┌──────────┐
                                         │ TypeScript│
                                         │  worker  │
                                         └──────────┘
                                               │
                                               │ permanent error
                                               ▼
                                         ┌──────────┐
                                         │   DLQ    │
                                         └──────────┘
```

## Running

```sh
docker compose up -d --wait
docker compose logs -f worker
```

Send a small webhook:
```sh
curl -X POST http://localhost:4000/hooks/demo \
  -H "Content-Type: application/json" \
  -d '{"test":"small payload"}'
```

Send a large webhook (triggers claim check):
```sh
dd if=/dev/urandom bs=1024 count=20 | base64 | \
  curl -X POST http://localhost:4000/hooks/demo \
    -H "Content-Type: application/octet-stream" \
    --data-binary @-
```

Watch the worker logs to see:
- Small payloads decoded inline
- Large payloads redeemed via claim check
- Deduplication preventing duplicate processing
- SQS FIFO ordering per tenant/source

Check Redpanda Console: http://localhost:8080
Check topic messages and consumer group lag.

## Cleanup

```sh
docker compose down -v
```

## Components

- **ingest**: Ankusa edge+dispatch+storage, writing to Kafka
- **claim-check**: Ankusa claim-check role, serving GET /v1/claims/:id
- **redpanda**: Kafka-compatible broker
- **redpanda-console**: Web UI showing topic, messages, consumer groups
- **bridge**: Redpanda Connect (Kafka input → SQS output)
- **floci**: Fake S3 + SQS (claims + queue)
- **worker**: TypeScript consumer reading from SQS FIFO
