# Changelog

All notable changes to `ankusa_nats` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-09-27

### Added

- `Ankusa.Sink.NATS`: publishes delivered hooks to a NATS JetStream subject
  on a stream the operator owns (the sink never creates one). Delivery is
  acknowledged by JetStream's publish ack, not by the write to the socket.
- `c:Ankusa.Sink.ordering_key/2`, implemented by this sink as the subject:
  the scope order is defined within ("the order the stream received it").
  Dispatch serializes deliveries sharing it and runs different subjects
  concurrently.
- Message format v1 (shared with `ankusa_rabbitmq`/`ankusa_kafka`), claim
  check integration for payloads over `:inline_max_bytes` (default 64 KiB), and
  the same five NATS headers the Kafka sink sets on a record.
- Supervised, self-reconnecting gnat connection per
  `{instance, connection}`; `deliver/3` returns `{:error, :not_connected}`
  while one is down, which the source's retry policy already handles.

### Changed

- The message `claim` field is now a claim-check ref URN string instead of a
  nested ticket object, breaking for consumers; the message `v` stays `1`.

[Unreleased]: https://github.com/jamescarr/ankusa/compare/ankusa_nats-v0.2.0...HEAD
[0.2.0]: https://github.com/jamescarr/ankusa/releases/tag/ankusa_nats-v0.2.0
