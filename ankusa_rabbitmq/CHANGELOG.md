# Changelog

All notable changes to `ankusa_rabbitmq` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
