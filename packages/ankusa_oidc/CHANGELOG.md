# Changelog

All notable changes to `ankusa_oidc` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `Ankusa.Auth.OIDC`: an `Ankusa.Auth.Verifier` for OpenID Connect bearer JWTs.
  Verifies the signature against the identity provider's JWKS (found by
  discovery or `:jwks_uri`, cached, refreshed hourly and on an unknown `kid`),
  and `iss`, `aud`, `exp` and `nbf`. `none` and `HS*` algorithms are refused.
