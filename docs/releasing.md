# Releasing

Two independent release tracks: four Hex packages, and one server image.

## Hex packages

Four independently versioned Hex packages (`ankusa`, `ankusa_postgres`,
`ankusa_rabbitmq`, `ankusa_kafka`), each with its own `mix.exs` `version` and
`CHANGELOG.md`. The release flow is tag-driven — the Elixir/Hex norm — not
triggered by every push to `main`:

1. In a PR: bump `@version` in the package's `mix.exs` (follow
   [SemVer](https://semver.org/)) and move the relevant entries from that
   package's `CHANGELOG.md` `[Unreleased]` section under a new dated
   heading.
2. Merge.
3. From `main`, tag and push: `git tag <pkg>-vX.Y.Z && git push origin
   <pkg>-vX.Y.Z`. [`.github/workflows/release.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release.yml)
   triggers on that tag, resolves the package and directory from it,
   verifies the tag's version matches `mix.exs` and that the `CHANGELOG.md`
   has a matching dated entry, runs the full test suite
   ([`.github/workflows/ci.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/ci.yml)
   via `workflow_call`, so a core tag can't ship something that breaks an
   adapter), then publishes the package and its docs to Hex and cuts a
   GitHub release from the CHANGELOG section.
4. The first release is ordered: tag `ankusa-v0.1.0` and wait for its
   publish job to finish green before tagging the adapters. Their `:prod`
   deps resolve `ankusa` from Hex — an adapter tag pushed before core is
   live fails at `MIX_ENV=prod mix deps.get` and is re-run from the Actions
   UI once core has landed.
5. `HEX_API_KEY` is a repository secret from a Hex key scoped to
   `api:write` — generate one from the Hex.pm dashboard (Keys) and add it
   under repo Settings → Secrets and variables → Actions.
6. The pre-tag gate is
   [`examples/oban-consumer/run.sh`](https://github.com/jamescarr/ankusa/blob/main/examples/oban-consumer/run.sh)
   passing locally.

[`.github/workflows/ci.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/ci.yml) runs the same
tests (plus `mix format --check-formatted` and
`mix compile --warnings-as-errors`) on every PR and push, independent of
the release workflow — a red CI check is a merge blocker regardless of
whether anything's being released.

## The server image

Every push to `main` touching `lib/**`, `priv/**`, `mix.*`, `ankusa_*/**`, or the
docker/ci workflows publishes `jamescarr/ankusa:edge` and `:sha-<short>` for
`linux/amd64` and `linux/arm64`. Pull requests running the same paths build and
smoke-test the image without pushing.

A release is the tag `ankusa_server-vX.Y.Z`, and the gate is:

1. `mise run e2e` — the kind + Oban end-to-end proof, locally.
2. `mise run release:preflight-server` — checks `@version` in
   `ankusa_server/mix.exs`, a matching `## [X.Y.Z]` heading in
   `ankusa_server/CHANGELOG.md`, that the tag is free locally and on `origin`,
   that the `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` repo secrets exist, and
   that the version is not already on Docker Hub.
3. `mise run release:tag-server` → `mise run release:watch-server` → `mise run
   release:verify-server`.

The tag run builds both architectures natively, runs
[`ankusa_server/scripts/smoke.sh`](https://github.com/jamescarr/ankusa/blob/main/ankusa_server/scripts/smoke.sh)
against the built image (the same script as `mise run docker:smoke`), and only
then publishes `X.Y.Z`, `X.Y`, `latest` (plus `X` from 1.0 on). A prerelease
version publishes its exact tag only, so nobody can pull `latest` and get an rc.
The same job pushes `ankusa_server/README.md` as the Docker Hub repository
description and cuts a GitHub release from the CHANGELOG section.

Secrets: `DOCKERHUB_USERNAME`, and `DOCKERHUB_TOKEN` — a Docker Hub personal
access token with Read & Write.
