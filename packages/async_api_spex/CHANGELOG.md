# Changelog

All notable changes to `async_api_spex` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-10-02

### Added

- Structs modelling the AsyncAPI 3.0 document: `AsyncApiSpex.Document`,
  `.Info`, `.Server`, `.Channel`, `.Parameter`, `.Operation`, `.Message`,
  `.CorrelationId`, `.Tag`, `.Reference`, and `.Components`.
- `use AsyncApiSpex.Schema` and `use AsyncApiSpex.Message` for declaring
  reusable schemas and messages, compiled into components at resolve time.
- `AsyncApiSpex.resolve/1`, `to_map/1`, `encode!/1`, and `validate/1`, with
  component extraction, lowerCamel JSON encoding, and named validation errors.
- `AsyncApiSpex.Plug.RenderSpec`, a Plug that serves an encoded document with
  the `application/asyncapi+json` content type.
- `mix async_api_spex.gen` to validate a spec module and write its document to
  a file.

[Unreleased]: https://github.com/jamescarr/ankusa/compare/async_api_spex-v0.1.0...HEAD
[0.1.0]: https://github.com/jamescarr/ankusa/releases/tag/async_api_spex-v0.1.0
