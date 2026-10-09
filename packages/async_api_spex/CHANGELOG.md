# Changelog

All notable changes to `async_api_spex` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-10-08

### Added

- `use AsyncApiSpex.Schema, fields: [...]` derives a schema from an existing
  `defstruct`; every struct key becomes a property and `fields:` refines it
  with a type, `required`, and a `description`. `title:` and `description:`
  describe the schema.
- `use AsyncApiSpex.Channel` declares a channel, the server it lives on, and
  its operation on the module that publishes to it or consumes from it.
- `use AsyncApiSpex.Spec` implements `spec/0` by assembling a document from
  channel modules, listed with `channels:` or discovered with `otp_app:`.

### Changed

- `use AsyncApiSpex.Schema` without `:schema` or `:fields` now raises
  "requires exactly one of :schema or :fields".
- **`AsyncApiSpex.validate/1` is stricter.** A document with a component
  schema that holds a module that does not `use AsyncApiSpex.Schema` now fails
  `validate/1` and `mix async_api_spex.gen`.

### Fixed

- A schema that references itself no longer makes the resolver loop forever.

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

[Unreleased]: https://github.com/jamescarr/ankusa/compare/async_api_spex-v0.2.0...HEAD
[0.2.0]: https://github.com/jamescarr/ankusa/releases/tag/async_api_spex-v0.2.0
[0.1.0]: https://hex.pm/packages/async_api_spex/0.1.0
