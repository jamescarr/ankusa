# Releasing

Every releasable artifact is a directory under `packages/`, versioned on its
own with its own `CHANGELOG.md`. The directory name is the git tag prefix, and
the files in it decide where it publishes:

| Package | Kind | Tag | Published by |
| --- | --- | --- | --- |
| `ankusa`, `ankusa_rabbitmq`, `ankusa_kafka`, `ankusa_nats`, `ankusa_redis` | Hex (`mix.exs`) | `<pkg>-vX.Y.Z` | [`release.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release.yml) |
| `ankusa_server` | Docker image `jamescarr/ankusa` (`Dockerfile`) | `ankusa_server-vX.Y.Z` | [`docker.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/docker.yml) |
| `sdk-typescript` | npm package `ankusa` (`package.json`) | `sdk-typescript-vX.Y.Z` | [`release-npm.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release-npm.yml) |
| `sdk-python` | PyPI package `ankusa` (`pyproject.toml`) | `sdk-python-vX.Y.Z` | [`release-python.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release-python.yml) |
| `sdk-rust` | crates.io crate `ankusa` (`Cargo.toml`) | `sdk-rust-vX.Y.Z` | [`release-crates.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release-crates.yml) |

The npm, PyPI, and crates.io packages are all named `ankusa`; each one's tag
prefix is its directory name, not the package name, so none can be confused
with the Hex core's `ankusa-vX.Y.Z` or with each other.

## The flow

The same for every kind:

```sh
mise run status                                   # version, last tag, published?, commits since
mise run release:prepare minor ankusa_nats        # patch|minor|major|X.Y.Z, one or more packages
# review and merge the PR it opens, then:
git switch main && git pull
mise run release:tag ankusa_nats
mise run release:watch ankusa_nats
mise run release:verify ankusa_nats
```

1. **`release:prepare <bump> <pkgs>…`** refuses a dirty tree or a `HEAD` that
   isn't `origin/main`, and refuses a package whose `CHANGELOG.md` has nothing
   under `[Unreleased]`. For each package it sets the version (`@version` in
   `mix.exs`, `npm version` for npm, or `uv version` for Python), opens a
   dated `## [X.Y.Z]` heading
   under `[Unreleased]`, and points the footer compare links at the new tag.
   It commits all of them on `release/<tag>`, pushes, and opens a PR whose
   body is the release notes. `--no-pr` stops after the local commit.
2. **Merge the PR.** CI runs on it like any other.
3. **`release:tag <pkgs>…`** from the merged `main` runs `release:preflight`
   first: clean tree, `HEAD == origin/main`, a `## [X.Y.Z]` heading, the tag
   free locally and on `origin`, the version not yet on its registry, and the
   package's repo secrets present. Then it pushes one tag per package.
4. The tag triggers the package's workflow. It verifies the tag against the
   version and CHANGELOG rather than trusting it, runs the full test suite
   ([`ci.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/ci.yml)
   via `workflow_call`, so a core tag can't ship something that breaks an
   adapter), publishes, and cuts a GitHub release from the CHANGELOG section
   (`mise run release:notes <pkg> [version]` prints the same text).
5. **`release:watch <pkg>`** follows that run; **`release:verify <pkgs>…`**
   confirms the version is live on its registry.

The pre-tag gate is `mise run e2e` (the kind + Oban end-to-end proof,
[`examples/oban-consumer/run.sh`](https://github.com/jamescarr/ankusa/blob/main/examples/oban-consumer/run.sh))
passing locally.

### Ordering: core first

Tag `ankusa` **alone** first, and wait for `release:watch ankusa` to finish
green before tagging any adapter. The adapters' `:prod` deps resolve `ankusa`
from Hex, not the path dep, so an adapter tag pushed before core is live
fails at `MIX_ENV=prod mix deps.get`. `release:preflight` enforces this: an
adapter's preflight fails until the core version in the tree is on Hex.
`release:prepare` can still bump core and adapters together in one PR.

The server image and the npm, Python, and Rust SDKs have no ordering
constraint: the image builds from this checkout, and none of the SDKs depends
on anything here.

## The server image

Every push to `main` touching `packages/ankusa*/**`, `.dockerignore`, or the
docker/ci workflows publishes `jamescarr/ankusa:edge` and `:sha-<short>` for
`linux/amd64` and `linux/arm64`. Pull requests touching the same paths build and
smoke-test the image without pushing.

The tag run builds both architectures natively, runs
[`packages/ankusa_server/scripts/smoke.sh`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/scripts/smoke.sh)
against the built image (the same script as `mise run docker:smoke`), and only
then publishes `X.Y.Z`, `X.Y`, `latest` (plus `X` from 1.0 on). A prerelease
version publishes its exact tag only, so nobody can pull `latest` and get an rc.
The same job pushes `packages/ankusa_server/README.md` as the Docker Hub
repository description. `release:verify ankusa_server` also checks `latest`
(for a non-prerelease) and that the tag carries both architectures.

## Secrets

Repository secrets, under Settings → Secrets and variables → Actions.
`release:preflight` checks the ones the package needs before any tag exists.

- **Hex:** `HEX_API_KEY`, a Hex key scoped to `api:write`: generate one from
  the Hex.pm dashboard (Keys).
- **Docker Hub:** `DOCKERHUB_USERNAME`, and `DOCKERHUB_TOKEN`: a Docker Hub
  personal access token with Read & Write.
- **npm:** `NPM_TOKEN`, an npmjs.com **Automation** access token (Account →
  Access Tokens → Generate New Token → Automation: this type bypasses
  2FA-on-publish, which a personal "Publish" token does not), scoped to the
  `ankusa` package once it exists, or unscoped for the first publish.
- **PyPI:** no repository secret — publishing uses
  [Trusted Publishing](https://docs.pypi.org/trusted-publishers/) (OIDC). The
  one-time setup lives on PyPI, not here: see
  [Python SDK (PyPI)](#python-sdk-pypi) below.
- **crates.io:** `CARGO_REGISTRY_TOKEN`, a crates.io API token carrying the
  `publish-new` and `publish-update` scopes, generated at
  <https://crates.io/settings/tokens> (Account Settings → API Tokens). See
  [Rust SDK (crates.io)](#rust-sdk-cratesio) below for why it is a token
  rather than OIDC.

## Python SDK (PyPI)

`sdk-python` publishes as the `ankusa` project on PyPI. There is no token to
store or rotate: [`release-python.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release-python.yml)
authenticates with PyPI [Trusted Publishing](https://docs.pypi.org/trusted-publishers/)
(OIDC), so the only setup is on PyPI itself, and the only manual step in the
whole flow.

### One-time PyPI setup

`ankusa` does not exist on PyPI yet, so register a **pending** trusted
publisher: the first successful publish creates the project and binds the
name to this repo. From the PyPI account that will own the package:

1. Sign in at <https://pypi.org/account/login/>. PyPI requires 2FA before you
   can manage publishers.
2. Open <https://pypi.org/manage/account/publishing/> (**Publishing** under
   your account, not the project settings — there is no project yet).
3. Under **Add a new pending publisher**, fill in exactly:

   | Field | Value |
   | --- | --- |
   | PyPI Project Name | `ankusa` |
   | Owner | `jamescarr` |
   | Repository name | `ankusa` |
   | Workflow name | `release-python.yml` |
   | Environment name | `pypi` |

4. Save. Nothing else on PyPI is needed: the `pypi` GitHub environment is
   created on first use, and every later release publishes without further
   PyPI changes.

The project name has to be free, and PyPI names are first-come. Check before
the first tag:

```sh
curl -s -o /dev/null -w '%{http_code}\n' https://pypi.org/pypi/ankusa/json   # 404 = available
```

If it is taken, change `[project] name` in
[`packages/sdk-python/pyproject.toml`](https://github.com/jamescarr/ankusa/blob/main/packages/sdk-python/pyproject.toml)
(and this document) before tagging; `release:preflight` and
`release:verify` read that field, so nothing else changes.

Optionally add the `pypi` environment under Settings → Environments first if
you want to gate publishes behind a required reviewer; an environment with no
protection rules behaves like none.

### Release commands

Same flow as every other package ([above](#the-flow)); only the package name
differs. With no ordering constraint against the Hex packages:

```sh
mise run status                            # version, last tag, published?, commits since
mise run release:prepare minor sdk-python  # or patch | major | 0.2.1
# review and merge the PR it opens, then:
git switch main && git pull
mise run release:tag sdk-python
mise run release:watch sdk-python          # the tag run builds and publishes
mise run release:verify sdk-python         # HTTP 200 for the version on PyPI
```

`release:prepare` runs `uv version`, which rewrites both `pyproject.toml` and
`uv.lock` (so the release PR keeps `uv sync --locked` passing);
`release:tag`'s preflight confirms the version is still unpublished on PyPI
and the tag is free; the tag run builds the sdist and wheel with `uv build`,
publishes through the pending publisher, and cuts the GitHub release from the
CHANGELOG section.

## Rust SDK (crates.io)

`sdk-rust` publishes as the `ankusa` crate on crates.io, from
[`release-crates.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release-crates.yml),
authenticated with the `CARGO_REGISTRY_TOKEN` repository secret.

### One-time crates.io setup

The crate name has to be free, and crates.io names are first-come. Check
before the first tag:

```sh
curl -s -o /dev/null -w '%{http_code}\n' \
  -A 'ankusa-release (https://github.com/jamescarr/ankusa)' \
  https://crates.io/api/v1/crates/ankusa/0.1.0   # 404 = available
```

The `-A` is not optional: crates.io answers `403` to curl's own user agent.
`pkg_registry_code` in `.mise/lib/pkg.sh` (so `mise run status`,
`release:preflight`, and `release:verify`) sends the same one.

Then create the token at <https://crates.io/settings/tokens> (Account Settings
→ API Tokens) with two scopes. `publish-new` allows publishing a crate name
that does not exist yet, so it is needed for the first release only;
`publish-update` allows a new version of a crate you already own, so it covers
every release after that. Store it as the `CARGO_REGISTRY_TOKEN` secret.

This is a token rather than crates.io
[Trusted Publishing](https://crates.io/docs/trusted-publishing) because a
trusted publisher is configured on the crate's own settings page, so the crate
has to exist before the OIDC flow can be used — which is what the first
token-driven release creates. Switching to it afterwards is three changes, not
one:

1. Register the publisher, on the crate's settings page, for workflow
   `release-crates.yml`.
2. Give the `publish` job `id-token: write`, and add a step that fetches the
   short-lived token —
   [`rust-lang/crates-io-auth-action@v1`](https://github.com/rust-lang/crates-io-auth-action),
   whose `token` output becomes `CARGO_REGISTRY_TOKEN` for the `cargo publish`
   step.
3. Drop `cargo` from `pkg_secrets` in `.mise/lib/pkg.sh`, or
   `release:preflight` fails with "repo secret CARGO_REGISTRY_TOKEN is not
   set".

If the name is taken before the first release, change `[package] name` in
[`packages/sdk-rust/Cargo.toml`](https://github.com/jamescarr/ankusa/blob/main/packages/sdk-rust/Cargo.toml)
(and this document) before tagging; the directory and the tag prefix stay
`sdk-rust`.

### Release commands

Same flow as every other package ([above](#the-flow)); only the package name
differs. With no ordering constraint against the Hex packages:

```sh
mise run status                            # version, last tag, published?, commits since
mise run release:prepare minor sdk-rust    # or patch | major | 0.1.0
# review and merge the PR it opens, then:
git switch main && git pull
mise run release:tag sdk-rust
mise run release:watch sdk-rust            # the tag run builds and publishes
mise run release:verify sdk-rust           # HTTP 200 for the version on crates.io
```

`release:prepare` sets `[package] version` in `Cargo.toml` and refreshes
`Cargo.lock` with `cargo update --workspace` (the lock records the crate's own
version, so `--locked` fails without it); `release:tag`'s preflight confirms
the version is still unpublished on crates.io and the tag is free; the tag run
publishes with `cargo publish --locked` on the same Rust toolchain `.mise.toml`
pins for `check:package sdk-rust`, and cuts the GitHub release from the
CHANGELOG section.
