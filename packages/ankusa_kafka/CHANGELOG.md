# Changelog

All notable changes to `ankusa_kafka` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `Ankusa.Sink.Kafka` implements `c:Ankusa.Sink.describe/2`, advertising its
  brokers, topic, and record key to the AsyncAPI document the admin API serves.
  Credentials (`:sasl`) are never part of it.

## [0.3.0] - 2026-10-01

No code changes. Released as 0.3.0 in step with `ankusa` 0.3.0 and the other packages in this release.

## [0.2.1] - 2026-09-28

### Added

- A `sha256` field on a claim-checked message: the lowercase hex digest of the
  payload, for the reader to verify the bytes it redeems against. It used to
  live inside the claim ref.

### Changed

- The message `claim` field is `urn:ankusa:claim:v1:<tenant>:<claim_id>`: a
  tenant and a canonical (uppercase) ULID, where it used to carry the pack id,
  byte offset/length, and digest. The message `v` stays `1`. Both come from
  `Ankusa.Sink.Message` in `ankusa` 0.2.1.

## [0.2.0] - 2026-09-27

### Added

- `c:Ankusa.Sink.ordering_key/2`, implemented by this sink as the record key,
  which is exactly the partition (and so the ordering) scope. Dispatch now
  serializes deliveries sharing a key and runs different keys concurrently.
  "Per-key ordering" is stated to the pipeline instead of assumed.

### Changed

- The message `claim` field is now a claim-check ref URN string instead of a
  nested ticket object, breaking for consumers; the message `v` stays `1`.
- `:inline_max_bytes` defaults to 64 KiB (was 8 KiB), read from
  `Ankusa.Sink.Message.inline_max_bytes/1`.
- Depends on `ankusa` `~> 0.2`: core 0.1 has neither
  `Ankusa.Sink.Message.inline_max_bytes/1` nor `c:Ankusa.Sink.ordering_key/2`.

## [0.1.0] - 2026-09-23

### Added

- Initial release: `Ankusa.Sink.Kafka` adapter
- Message format v1 (shared with `ankusa_rabbitmq`)
- Claim Check integration for payloads > `inline_max_bytes`
- Per-key ordering via configurable record key
- Synchronous produce with `acks=all`
- Kafka headers: `ankusa_id`, `ankusa_source_id`, `ankusa_tenant_id`, `ankusa_message_version`, `content_type`

[Unreleased]: https://github.com/jamescarr/ankusa/compare/ankusa_kafka-v0.3.0...HEAD
[0.3.0]: https://github.com/jamescarr/ankusa/compare/ankusa_kafka-v0.2.1...ankusa_kafka-v0.3.0
[0.2.1]: https://github.com/jamescarr/ankusa/compare/ankusa_kafka-v0.2.0...ankusa_kafka-v0.2.1
[0.2.0]: https://github.com/jamescarr/ankusa/compare/ankusa_kafka-v0.1.0...ankusa_kafka-v0.2.0
[0.1.0]: https://github.com/jamescarr/ankusa/releases/tag/ankusa_kafka-v0.1.0
