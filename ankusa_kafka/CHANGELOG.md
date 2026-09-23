# Changelog

All notable changes to `ankusa_kafka` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial release: `Ankusa.Sink.Kafka` adapter
- Message format v1 (shared with `ankusa_rabbitmq`)
- Claim Check integration for payloads > `inline_max_bytes`
- Per-key ordering via configurable record key
- Synchronous produce with `acks=all`
- Kafka headers: `ankusa_id`, `ankusa_source_id`, `ankusa_tenant_id`, `ankusa_message_version`, `content_type`
