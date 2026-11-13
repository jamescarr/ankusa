# Changelog

All notable changes to `ankusa_gateway_postgres` are documented here. Format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `Ankusa.Gateway.Postgres.Source`, the `table` authorizer's Postgres source:
  the operator's query (snapshot, with an optional `version_query`) or
  `lookup_query` (one query per cache miss, request data as bound parameters),
  in a read-only transaction, over TLS unless the host is loopback.
