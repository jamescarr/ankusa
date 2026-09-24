# Changelog

All notable changes to `ankusa_kafka` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `c:Ankusa.Sink.ordering_key/2`, implemented by this sink as the record key,
  which is exactly the partition (and so the ordering) scope. Dispatch now
  serializes deliveries sharing a key and runs different keys concurrently —
  "per-key ordering" is stated to the pipeline instead of assumed.

## [0.1.0] - 2026-09-23

### Added

- Initial release: `Ankusa.Sink.Kafka` adapter
- Message format v1 (shared with `ankusa_rabbitmq`)
- Claim Check integration for payloads > `inline_max_bytes`
- Per-key ordering via configurable record key
- Synchronous produce with `acks=all`
- Kafka headers: `ankusa_id`, `ankusa_source_id`, `ankusa_tenant_id`, `ankusa_message_version`, `content_type`

[Unreleased]: https://github.com/jamescarr/ankusa/compare/ankusa_kafka-v0.1.0...HEAD
[0.1.0]: https://github.com/jamescarr/ankusa/releases/tag/ankusa_kafka-v0.1.0
