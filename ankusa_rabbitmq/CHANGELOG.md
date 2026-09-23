# Changelog

All notable changes to `ankusa_rabbitmq` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Messages carry a format version, `"v": 1`. Additive: consumers that ignore
  unknown keys need no change.

### Changed

- The message is built by `Ankusa.Sink.Message` (new in `ankusa`), shared
  with `ankusa_kafka`, so both sinks publish byte-identical messages.

### Changed (breaking)

- `Ankusa.Sink.RabbitMQ`'s fat-payload path now checks bodies in through
  `Ankusa.ClaimCheck` (new in `ankusa`) instead of writing directly to a
  `Ankusa.BlobStore`. The message shape changes: `{"blob": {"store", "key",
  "size"}}` is now `{"claim": {"v", "tenant_id", "id", "size", "sha256",
  "content_type"}}` — a consumer redeems the ticket via
  `Ankusa.ClaimCheck.redeem/3` (or, for a non-BEAM consumer, the
  `:claim_check` role's HTTP API) instead of reading the pointed-to object
  directly. The `:blob_store` and `:blob_key_prefix` sink opts are removed;
  configure `claim_check.adapter` on the instance instead.
  **Rollout:** drain consumer queues of `blob`-shaped messages before
  deploying this version; old `raw/...` objects are orphaned and can be
  deleted afterwards.

## [0.1.0] - 2026-09-22

### Added

- `Ankusa.Sink.RabbitMQ`: publishes delivered hooks to a RabbitMQ exchange
  (never a queue — consumers own their own queue/binding). Bodies under
  `:inline_max_bytes` ride along base64-encoded; larger bodies are offloaded
  to a `Ankusa.BlobStore` with the message carrying a pointer.
  Publisher-confirmed delivery; supervised, auto-reconnecting connection per
  `(instance, exchange)`.

[Unreleased]: https://github.com/ankusa-elixir/ankusa/compare/ankusa_rabbitmq-v0.1.0...HEAD
[0.1.0]: https://github.com/ankusa-elixir/ankusa/releases/tag/ankusa_rabbitmq-v0.1.0
