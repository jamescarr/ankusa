# Changelog

All notable changes to `ankusa_rabbitmq` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `c:Ankusa.Sink.ordering_key/2`, implemented by this sink as the routing key:
  RabbitMQ orders per queue, and the routing key decides the queue. Dispatch
  now serializes deliveries sharing a key and runs different keys concurrently.

### Changed

- The message `claim` field is now a claim-check ref URN string instead of a
  nested ticket object — breaking for consumers; the message `v` stays `1`.

## [0.1.0] - 2026-09-23

### Added

- `Ankusa.Sink.RabbitMQ`: publishes delivered hooks to a RabbitMQ exchange
  (never a queue — consumers own their own queue/binding). Bodies under
  `:inline_max_bytes` ride along base64-encoded; larger bodies are checked
  in through `Ankusa.ClaimCheck`, with the message carrying a ticket.
  Publisher-confirmed delivery; supervised, auto-reconnecting connection per
  `(instance, exchange)`.
- Messages carry a format version, `"v": 1`. Additive: consumers that ignore
  unknown keys need no change.

### Changed

- The message is built by `Ankusa.Sink.Message` (in `ankusa`), shared
  with `ankusa_kafka`, so both sinks publish byte-identical messages.

[Unreleased]: https://github.com/jamescarr/ankusa/compare/ankusa_rabbitmq-v0.1.0...HEAD
[0.1.0]: https://github.com/jamescarr/ankusa/releases/tag/ankusa_rabbitmq-v0.1.0
