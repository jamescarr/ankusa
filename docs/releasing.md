# Releasing

Every releasable artifact is a directory under `packages/`, versioned on its
own with its own `CHANGELOG.md`. Those changelogs are marked `merge=union` in
`.gitattributes`, so PRs that both append under `## [Unreleased]` merge
without a textual conflict. GitHub's mergeability check does not run merge
drivers, so a PR may still read CONFLICTING until its branch merges `main`
locally and pushes. The directory name is the git tag prefix, and the files in
it decide where it publishes:

| Package | Kind | Tag | Published by |
| --- | --- | --- | --- |
| `ankusa`, `ankusa_rabbitmq`, `ankusa_kafka`, `ankusa_nats`, `ankusa_redis`, `async_api_spex` | Hex (`mix.exs`) | `<pkg>-vX.Y.Z` | [`release.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release.yml) |
| `ankusa_server` | Docker image `jamescarr/ankusa` (`Dockerfile`) | `ankusa_server-vX.Y.Z` | [`docker.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/docker.yml) |
| `sdk-typescript` | npm package `ankusa` (`package.json`) | `sdk-typescript-vX.Y.Z` | [`release-npm.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release-npm.yml) |
| `sdk-python` | PyPI package `ankusa` (`pyproject.toml`) | `sdk-python-vX.Y.Z` | [`release-python.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release-python.yml) |
| `sdk-rust` | crates.io crate `ankusa` (`Cargo.toml`) | `sdk-rust-vX.Y.Z` | [`release-crates.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release-crates.yml) |
| `sdk-ruby` | RubyGems gem `ankusa-sdk` (`ankusa-sdk.gemspec`) | `sdk-ruby-vX.Y.Z` | [`release-ruby.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release-ruby.yml) |
| `sdk-go` | Go module `github.com/jamescarr/ankusa/packages/sdk-go` (`go.mod`) | `sdk-go-vX.Y.Z`, then `packages/sdk-go/vX.Y.Z` after CI | [`release-go.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release-go.yml) |
| `sdk-php` | Packagist package `jamescarr/ankusa` (`composer.json`) | `sdk-php-vX.Y.Z` | [`release-php.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release-php.yml) |
| `sdk-elixir` | Hex package `ankusa_sdk` (`mix.exs`) | `sdk-elixir-vX.Y.Z` | [`release.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release.yml) |
| `sdk-java` | Maven Central `io.github.jamescarr:ankusa-sdk` (`build.sbt`) | `sdk-java-vX.Y.Z` | [`release-maven.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release-maven.yml) |
| `sdk-clojure` | Clojars `io.github.jamescarr/ankusa-clj` (`deps.edn`, `build.clj`) | `sdk-clojure-vX.Y.Z` | [`release-clojars.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release-clojars.yml) |

The npm, PyPI, and crates.io packages are all named `ankusa`. The RubyGems gem
is `ankusa-sdk` because RubyGems' `ankusa` belongs to an unrelated project; the
Elixir SDK publishes as `ankusa_sdk` because Hex's `ankusa` is this repo's
core; the Java SDK publishes under the `io.github.jamescarr` group because
that is the namespace GitHub verifies for this account; and the Clojure SDK
is `ankusa-clj` because the Clojure CLI resolves from Maven Central and
Clojars together, so `ankusa-sdk` in the same group would collide with the Java
SDK's artifact. Each tag prefix is the directory name, not the package name, so
none can be confused with the Hex core's `ankusa-vX.Y.Z` or with each other.

A release is four moves: bump the version and merge the PR, tag, let the tag's
workflow publish, confirm the registry serves the version. The first two are
the same commands for every kind. What the tag sets off is not, and that is
what the per-kind sections below describe.

## Before you tag

### 1. See where the packages stand

```sh
mise run status
```

One row per package: kind, current version, last tag, whether that version is
published, and how many commits the package's directory has since its last
tag.

### 2. Bump, date the CHANGELOG, open the release PR

```sh
mise run release:prepare minor ankusa_nats
```

The bump is `patch`, `minor`, `major`, or an explicit `X.Y.Z`, and you can name
more than one package. `release:prepare` refuses a dirty tree or a `HEAD` that
is not `origin/main`, and refuses a package whose `CHANGELOG.md` has nothing
under `[Unreleased]`.

For each package it sets the version, opens a dated `## [X.Y.Z]` heading under
`[Unreleased]`, and points the footer compare links at the new tag. The file it
rewrites depends on the kind:

| Kind | Version lives in | Moved by |
| --- | --- | --- |
| Hex | `@version` in `mix.exs` | `.mise/lib/release.exs` |
| crates.io | `[package] version` in `Cargo.toml` | `release.exs` + `cargo update --workspace` |
| RubyGems | `VERSION` in `lib/**/version.rb` | `release.exs` + `bundle lock` |
| Go | `const Version` in `version.go` | `release.exs` |
| npm | `package.json` | `npm version` |
| PyPI | `pyproject.toml` | `uv version` |
| Packagist | `public const string VERSION` in `src/Version.php` | `release.exs` |
| Maven Central | `version` in `build.sbt` | `release.exs` |
| Clojars | `(def version ...)` in `build.clj` | `release.exs` |

It also refreshes the lockfiles that record a package's own version, because CI
on the release PR runs them locked: `uv.lock` and `Cargo.lock` through the
commands above, `Gemfile.lock` through `bundle lock`. When the bump includes an
npm or Python package it additionally re-locks the examples that consume it
(`npm install --package-lock-only`, `uv lock`), since their manifests record
the resolved version and `check:examples` installs from the lock.

Then it commits everything on `release/<tag>`, pushes, and opens a PR whose
body is the release notes. `--no-pr` stops after the local commit.

### 3. Merge the PR, and know the gates

CI runs on the release PR like any other. Before the tag:

- `ci.yml` is green on the commit you are about to tag. That is the pre-tag
  gate; the workflows the tag starts call `ci.yml` again rather than trusting
  the tag.
- [`e2e.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/e2e.yml)
  runs on the tag push itself. The latest nightly `e2e.yml` and
  [`soak.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/soak.yml)
  runs on `main` are read before tagging, not rerun.
- Tagging `ankusa` needs the power-loss crash drill
  (`scripts/drills/flakey.sh`,
  [`testing.md`](testing.md#crash-consistency-drill-mise-run-testcrash)).
  `release:preflight` reads the `crash drill (power-loss cut,
  device-mapper)` job of `ci.yml`'s push run for `HEAD`: it fails when that run
  is missing, red, or skipped the drill (a runner without device-mapper). In
  that last case the drill is a manual step, run on a privileged Docker host
  before tagging.

Nobody runs the long drills locally to release
([`AGENTS.md`](https://github.com/jamescarr/ankusa/blob/main/AGENTS.md#long-drills-run-in-ci-not-here)).
The drill's evidence is the CI run on the commit, linked in the release PR.

### 4. Order the tags

`async_api_spex` comes before core: `ankusa` depends on it, and builds under
`:prod` resolve it from Hex, so a core tag pushed before `async_api_spex` is
live fails at `MIX_ENV=prod mix deps.get`. Tag `async_api_spex` alone first,
wait for `release:watch async_api_spex`, then continue.

Tag `ankusa` alone next, and wait for `release:watch ankusa` to finish green
before tagging any adapter. The adapters' `:prod` deps resolve `ankusa` from
Hex, not the path dep, so an adapter tag pushed before core is live fails at
the same step. `release:preflight` enforces both rules: an adapter's preflight
fails until the core version in the tree is on Hex, and core's fails until
`async_api_spex`'s is. `release:prepare` can still bump core and adapters
together in one PR.

The server image and every SDK have no ordering constraint: the image builds
from this checkout, and no SDK depends on anything here. `sdk-elixir` is a Hex
package but has no `ankusa` dependency either, so `release:preflight` skips the
core-first check for it (the check applies only to packages whose `mix.exs`
depends on `ankusa`).

### Versions: the Elixir family shares major.minor

`ankusa`, `ankusa_kafka`, `ankusa_nats`, `ankusa_rabbitmq`, `ankusa_redis` and
the server image (`ankusa_server`) share their major.minor. The adapters
depend on core (`{:ankusa, "~> X.Y"}`) and the image bundles all of them, so
the shared number says which ones go together. A core minor or major release
bumps every one of them to the same `X.Y.0` in the same release PR; an adapter
with nothing else to say gets a one-line "version aligned with `ankusa` X.Y"
entry, and the adapters' `{:ankusa, "~> X.Y"}` follows. Patch versions are
each package's own: a fix to one is `X.Y.1` of that one alone.

The SDKs (`sdk-*`) and `async_api_spex` keep independent SemVer. No SDK
depends on any package here: they speak the wire format to a running server,
and their compatibility with a server is the queue message versions each one
accepts (`message.versions` in
[`conformance/features.json`](https://github.com/jamescarr/ankusa/blob/main/conformance/features.json)),
not a shared version number.

## The tag

```sh
git switch main && git pull
mise run release:tag ankusa_nats
mise run release:watch ankusa_nats
mise run release:verify ankusa_nats
```

`release:tag` runs `release:preflight` for every package named before it
creates anything. Preflight refuses:

- a dirty tree, or a `HEAD` that is not `origin/main`;
- a package whose `CHANGELOG.md` has no `## [X.Y.Z]` heading;
- a tag that exists locally or on `origin`;
- a version that is already on the package's registry;
- a missing repository secret the package's workflow needs;
- an adapter, or core, whose dependency is not on Hex yet (above).

It then pushes one tag per package. It pushes each tag separately on purpose:
GitHub creates no push events, and so starts no workflow runs, when more than
three tags arrive at once.

The tag starts that package's workflow. Every one of them verifies the tag
against the version in the tree and the CHANGELOG rather than trusting it,
runs the full test suite before publishing anything irreversible (`ci.yml` via
`workflow_call`, so a core tag cannot ship something that breaks an adapter),
publishes, and cuts a GitHub release from the CHANGELOG section. `mise run
release:notes <pkg> [version]` prints the same text.

`release:watch <pkg>` follows that run. `release:verify <pkgs>…` asks the
registry for the version and fails unless it answers `200`.

## Hex packages (core, adapters, `async_api_spex`)

Six packages publish to Hex from
[`release.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release.yml):
`ankusa`, the four adapters, and `async_api_spex`. The only credential is
`HEX_API_KEY`.

### Before you tag

Order the tags as described above. Then:

```sh
mise run status                          # version, last tag, published?, commits since
mise run release:prepare minor ankusa    # or patch | major | 0.7.0
# review and merge the PR it opens, then:
git switch main && git pull
mise run release:tag ankusa
mise run release:watch ankusa
mise run release:verify ankusa
```

### What runs

`resolve` maps the tag prefix to the package, checks the tag's version against
`mix.exs`, and checks that the CHANGELOG has the matching heading. `ci` runs
the whole suite through `workflow_call`. The publish job then runs `mix
deps.get` and `MIX_ENV=prod mix deps.get` (the second is where a missing core
release shows up), publishes the package with `MIX_ENV=prod mix hex.publish
package --yes`, publishes the docs separately with `MIX_ENV=prod mix hex.publish
docs --yes`, and creates the GitHub release.

### After

`release:verify` reads `hex.pm/api/packages/<app>/releases/<version>` and
expects `200`. The app name comes from `mix.exs`, not the directory, which is
why `sdk-elixir` verifies as `ankusa_sdk`.

## The server image

Every push to `main` touching `packages/ankusa*/**`,
`packages/async_api_spex/**`, `.dockerignore`, or `docker.yml`/`ci.yml`
publishes `jamescarr/ankusa:edge` and `:sha-<short>` for `linux/amd64` and
`linux/arm64`. Pull requests touching the same paths build and smoke-test the
image without pushing.

### Before you tag

In the release PR, set the Helm chart's `appVersion` in
[`deploy/helm/ankusa/Chart.yaml`](https://github.com/jamescarr/ankusa/tree/main/deploy/helm/ankusa)
to the new `ankusa_server` version, and bump the chart's own `version` when its
templates or values changed. The chart runs `jamescarr/ankusa:<appVersion>`
unless `image.tag` says otherwise.

Set the version and tag as usual:

```sh
mise run status                              # version, last tag, published?, commits since
mise run release:prepare minor ankusa_server # or patch | major | 0.7.0
# review and merge the PR it opens, then:
git switch main && git pull
mise run release:tag ankusa_server
mise run release:watch ankusa_server
mise run release:verify ankusa_server
```

### What runs

`resolve` turns the version into tags. A prerelease version publishes its
exact tag only, so nobody can pull `latest` and get an rc; a release publishes
`X.Y.Z`, `X.Y`, `latest`, and `X` from 1.0 on. On a tag run, `ci` runs the
whole suite first: a release must not ship an image built from code that does
not pass CI.

`build` then runs once per architecture, natively (an emulated arm64 build of a
BEAM release plus a C++ NIF takes long enough to time out). Each job builds
with a warm layer cache, smoke-tests the local image with
[`packages/ankusa_server/scripts/smoke.sh`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/scripts/smoke.sh),
and pushes its architecture by digest.

`merge` combines the digests into the tag set, then refuses the index unless it
carries an SPDX SBOM and a SLSA provenance attestation for both platforms. It
signs the index digest with `cosign sign --yes`, and smoke-tests
`jamescarr/ankusa@sha256:<index>`, whose first step is `cosign verify` with
this workflow's identity on `main` or a server tag. A failure leaves the job
red and no GitHub release.

On a tag, `publish-meta` pushes `packages/ankusa_server/README.md` as the
Docker Hub repository description and creates the GitHub release.

### After

`release:verify ankusa_server` checks the version tag and, for a non-prerelease,
`latest`, and confirms the tag carries both architectures.

### Supply chain: pins, SBOM, provenance, signature

The image is built only from bytes that cannot be renamed underneath it. That
is enforced in one place, `mise run lint:pins`, which runs in `mise run check`
and in CI's `lint` job, and fails on:

- a GitHub action in `.github/workflows/` that is not pinned to a 40-character
  commit SHA (a reusable workflow in this repo, `uses: ./…`, is exempt);
- the BuildKit and SBOM-scanner images `docker.yml` builds with, if they are
  not pinned by digest;
- a `FROM`, or the `# syntax=` frontend, in
  [`packages/ankusa_server/Dockerfile`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/Dockerfile)
  that is not pinned by digest (a stage name is not an image);
- an `image:` in a `packages/*/docker-compose.yml` or
  `packages/ankusa/docker-compose.integration.yml` that is not pinned by
  digest.

Each pin carries the tag it came from as a trailing comment, which is the only
part a human reads. Bump the pin and the comment together. The commands are in
the header of `.mise/tasks/lint/pins`, and in short:

```sh
gh api repos/<owner>/<repo>/git/ref/tags/<tag> -q .object      # tag -> commit (deref an annotated tag with git/tags/<sha>)
docker buildx imagetools inspect <image:tag> --format '{{json .Manifest.Digest}}'   # image -> digest
docker compose -f <file> config --images                        # the refs a compose bump has to cover
```

From there the release is a chain of attestations on one digest. `build` pushes
each architecture with `provenance: mode=max` (SLSA provenance) and an SPDX
SBOM from the digest-pinned BuildKit Syft scanner; both land as attestation
manifests beside the image. `merge` checks exactly those two attestations for
both platforms before it creates a tag set, so an image without an SBOM, or
with provenance for one architecture, is never named or signed. The signature
is keyless: `cosign sign --yes` uses the job's OIDC token (`id-token: write`)
as the identity, and the signature goes to the public Rekor log, so there is no
key to leak. Because the index digest is what is signed, a tag that later moves
is no longer the signed image.

`mise run docker:smoke` builds locally and prints `cosign verify: skipped`,
since a local build is unsigned. To verify a published image, use
`SMOKE_COSIGN_IMAGE=jamescarr/ankusa:X.Y.Z mise run docker:smoke`, or
`packages/ankusa_server/scripts/smoke.sh jamescarr/ankusa@sha256:…` (needs
`cosign`). How a consumer verifies is in the image's
[README](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/README.md#verify-an-image).
Separately, each CI package job uploads `mix hex.audit`'s report as the
`hex-audit-<package>` artifact.

## npm (TypeScript SDK)

`sdk-typescript` publishes as the `ankusa` package on npm, from
[`release-npm.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release-npm.yml).
It publishes through npm
[Trusted Publishing](https://docs.npmjs.com/trusted-publishers/) (OIDC) while
the repository variable `NPM_AUTH` is `oidc`, which it is; an `NPM_TOKEN`
secret remains the fallback (see [Secrets](#secrets)).

### Before you tag

The trusted publisher on npmjs.com (`ankusa` → Settings → Trusted publishing:
GitHub Actions, user `jamescarr`, repo `ankusa`, workflow `release-npm.yml`, no
environment) must have **`npm publish`** ticked under Allowed actions. An entry
created after 2026-09-03 allows only `npm stage publish` by default, and this
job's direct `npm publish` then fails with `403 … OIDC permission denied for
this action`. Entries cannot be edited: delete and recreate one, and complete
its first publish within 2 days or it expires.

```sh
mise run status                                # version, last tag, published?, commits since
mise run release:prepare minor sdk-typescript  # or patch | major | 0.7.0
# review and merge the PR it opens, then:
git switch main && git pull
mise run release:tag sdk-typescript
mise run release:watch sdk-typescript
mise run release:verify sdk-typescript         # HTTP 200 for the version on npm
```

`release:prepare` runs `npm version --no-git-tag-version --ignore-scripts`,
which rewrites `package.json`, and re-locks the npm examples.

### What runs

`resolve` checks the tag against `package.json` and the CHANGELOG heading,
`ci` runs the whole suite, and the publish job runs `npm ci` and `npm run
build` in `packages/sdk-typescript` before `npm publish --provenance --access
public`. It upgrades npm to `^11.5.1` first in the OIDC path, which is the
minimum trusted publishing needs, and deliberately does not set a registry URL
there: the placeholder `setup-node` leaves for an unset token turns npm's auth
refusal into a misleading "package not found". It then creates the GitHub
release.

### After

`release:verify sdk-typescript` reads `registry.npmjs.org/ankusa/<version>`.
The published tarball carries npm's provenance attestation because of
`--provenance`.

## Python SDK (PyPI)

`sdk-python` publishes as the `ankusa` project on PyPI. There is no token to
store or rotate:
[`release-python.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release-python.yml)
authenticates with PyPI
[Trusted Publishing](https://docs.pypi.org/trusted-publishers/) (OIDC), so the
only setup lives on PyPI.

### Before you tag

The publisher is already bound to this repo, and `ankusa` is live on PyPI; a
release needs nothing here. If the project ever moves to a new name, register
a **pending** trusted publisher first: until the project exists there is no
project settings page to use. From the PyPI account that will own the package:

1. Sign in at <https://pypi.org/account/login/>. PyPI requires 2FA before you
   can manage publishers.
2. Open <https://pypi.org/manage/account/publishing/> (**Publishing** under
   your account, not the project settings).
3. Under **Add a new pending publisher**, fill in exactly:

   | Field | Value |
   | --- | --- |
   | PyPI Project Name | `ankusa` |
   | Owner | `jamescarr` |
   | Repository name | `ankusa` |
   | Workflow name | `release-python.yml` |
   | Environment name | `pypi` |

4. Save. The `pypi` GitHub environment is created on first use, and every
   later release publishes without further PyPI changes.

The project name has to be free, and PyPI names are first-come. Check before
tagging under a new name:

```sh
curl -s -o /dev/null -w '%{http_code}\n' https://pypi.org/pypi/ankusa/json   # 404 = available
```

If it is taken, change `[project] name` in
[`packages/sdk-python/pyproject.toml`](https://github.com/jamescarr/ankusa/blob/main/packages/sdk-python/pyproject.toml)
(and this document) before tagging; `release:preflight` and `release:verify`
read that field, so nothing else changes.

Optionally add the `pypi` environment under Settings → Environments first if
you want to gate publishes behind a required reviewer; an environment with no
protection rules behaves like none.

Then bump and tag:

```sh
mise run status                            # version, last tag, published?, commits since
mise run release:prepare minor sdk-python  # or patch | major | 0.2.1
# review and merge the PR it opens, then:
git switch main && git pull
mise run release:tag sdk-python
mise run release:watch sdk-python
mise run release:verify sdk-python         # HTTP 200 for the version on PyPI
```

`release:prepare` runs `uv version`, which rewrites both `pyproject.toml` and
`uv.lock`, so the release PR keeps `uv sync --locked` passing. There is no
ordering constraint against the Hex packages.

### What runs

`release:preflight` confirms the version is still unpublished on PyPI and the
tag is free. The tag run checks the tag against `pyproject.toml` and the
CHANGELOG, waits for the full CI suite, builds the sdist and wheel with `uv
build`, publishes through the trusted publisher, and cuts the GitHub release
from the CHANGELOG section.

### After

`release:verify sdk-python` reads PyPI's JSON API for the version.

## Rust SDK (crates.io)

`sdk-rust` publishes as the `ankusa` crate on crates.io, from
[`release-crates.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release-crates.yml),
authenticated with the `CARGO_REGISTRY_TOKEN` repository secret.

### Before you tag

The token carries two scopes: `publish-new`, which allows publishing a crate
name that does not exist yet (the first release only), and `publish-update`,
which covers every release after that. Generate or rotate it at
<https://crates.io/settings/tokens> (Account Settings → API Tokens) and store
it as `CARGO_REGISTRY_TOKEN`.

The crate name has to be free, and crates.io names are first-come. Check before
tagging under a new name:

```sh
curl -s -o /dev/null -w '%{http_code}\n' \
  -A 'ankusa-release (https://github.com/jamescarr/ankusa)' \
  https://crates.io/api/v1/crates/ankusa/0.1.0   # 404 = available
```

The `-A` is not optional: crates.io answers `403` to curl's own user agent.
`pkg_registry_code` in `.mise/lib/pkg.sh` (so `mise run status`,
`release:preflight`, and `release:verify`) sends the same one.

If the name is taken, change `[package] name` in
[`packages/sdk-rust/Cargo.toml`](https://github.com/jamescarr/ankusa/blob/main/packages/sdk-rust/Cargo.toml)
(and this document) before tagging; the directory and the tag prefix stay
`sdk-rust`.

```sh
mise run status                            # version, last tag, published?, commits since
mise run release:prepare minor sdk-rust    # or patch | major | 0.1.0
# review and merge the PR it opens, then:
git switch main && git pull
mise run release:tag sdk-rust
mise run release:watch sdk-rust
mise run release:verify sdk-rust           # HTTP 200 for the version on crates.io
```

`release:prepare` sets `[package] version` in `Cargo.toml` and refreshes
`Cargo.lock` with `cargo update --workspace`, because the lock records the
crate's own version and `--locked` fails without it. There is no ordering
constraint against the Hex packages.

### What runs

Preflight confirms the version is still unpublished on crates.io and the tag is
free. The tag run publishes with `cargo publish --locked` on the Rust toolchain
`.mise/conf.d/sdk-rust.toml` pins for `check:package sdk-rust`, and cuts the
GitHub release from the CHANGELOG section.

This is a token rather than crates.io
[Trusted Publishing](https://crates.io/docs/trusted-publishing) because a
trusted publisher is configured on the crate's own settings page, so the crate
has to exist before the OIDC flow can be used. Switching to it afterwards is
three changes, not one:

1. Register the publisher, on the crate's settings page, for workflow
   `release-crates.yml`.
2. Give the `publish` job `id-token: write`, and add a step that fetches the
   short-lived token,
   [`rust-lang/crates-io-auth-action@v1`](https://github.com/rust-lang/crates-io-auth-action),
   whose `token` output becomes `CARGO_REGISTRY_TOKEN` for the `cargo publish`
   step.
3. Drop `cargo` from `pkg_secrets` in `.mise/lib/pkg.sh`, or
   `release:preflight` fails with "repo secret CARGO_REGISTRY_TOKEN is not
   set".

### After

`release:verify sdk-rust` asks crates.io for the version. A published version
is immutable.

## Ruby SDK (RubyGems)

`sdk-ruby` publishes as the `ankusa-sdk` gem on RubyGems, because RubyGems'
`ankusa` belongs to an unrelated project. There is no token to store or
rotate: [`release-ruby.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release-ruby.yml)
authenticates with RubyGems
[Trusted Publishing](https://guides.rubygems.org/trusted-publishing/) (OIDC),
so the only setup lives on RubyGems.

### Before you tag

The publisher is already bound to this repo, and `ankusa-sdk` is live on
RubyGems; a release needs nothing here. If the gem ever moves to a new name,
register a **pending** trusted publisher first: until the gem exists there is
no gem settings page to use. From the RubyGems account that will own the gem:

1. Sign in at <https://rubygems.org/session/new>.
2. Open <https://rubygems.org/profile/oidc/pending_trusted_publishers>
   (**Trusted publishers** under your profile).
3. Under **Pending trusted publishers**, fill in exactly:

   | Field | Value |
   | --- | --- |
   | Gem name | `ankusa-sdk` |
   | Repository owner | `jamescarr` |
   | Repository name | `ankusa` |
   | Workflow filename | `release-ruby.yml` |
   | Environment | `rubygems` |

   Leave the Workflow Repository fields blank.

4. Save. The `rubygems` GitHub environment is created on first use, and every
   later release publishes without further RubyGems changes.

The gem name has to be free, and RubyGems names are first-come. Check before
tagging under a new name:

```sh
curl -s -o /dev/null -w '%{http_code}\n' https://rubygems.org/api/v1/gems/ankusa-sdk.json   # 404 = available
```

If it is taken, change `spec.name` in
[`packages/sdk-ruby/ankusa-sdk.gemspec`](https://github.com/jamescarr/ankusa/blob/main/packages/sdk-ruby/ankusa-sdk.gemspec)
(and this document) before tagging; `release:preflight` and `release:verify`
read that field, so nothing else changes.

Optionally add the `rubygems` environment under Settings → Environments first
if you want to gate publishes behind a required reviewer; an environment with
no protection rules behaves like none.

```sh
mise run status                             # version, last tag, published?, commits since
mise run release:prepare minor sdk-ruby     # or patch | major | 0.2.1
# review and merge the PR it opens, then:
git switch main && git pull
mise run release:tag sdk-ruby
mise run release:watch sdk-ruby
mise run release:verify sdk-ruby            # HTTP 200 for the version on RubyGems
```

`release:prepare` rewrites `lib/ankusa/sdk/version.rb` (through
[`.mise/lib/release.exs`](https://github.com/jamescarr/ankusa/blob/main/.mise/lib/release.exs))
and runs `bundle lock`, so the release PR keeps the frozen `bundle install` in
CI passing. There is no ordering constraint against the Hex packages.

### What runs

Preflight confirms the version is still unpublished on RubyGems and the tag is
free. The tag run builds the gem with `gem build --strict`, pushes it through
the trusted publisher, and cuts the GitHub release from the CHANGELOG section.

### After

`release:verify sdk-ruby` reads RubyGems' API for the version.

## Go module (proxy.golang.org)

`sdk-go` publishes as the Go module
`github.com/jamescarr/ankusa/packages/sdk-go` (package name `ankusa`), with no
token to store: the module proxy reads the pushed git tag. A module in a
subdirectory needs a *second* tag, `packages/sdk-go/vX.Y.Z`. That is the one
`go get` resolves; `sdk-go-vX.Y.Z` is the repo convention.

### Before you tag

```sh
mise run status                            # version, last tag, published?, commits since
mise run release:prepare minor sdk-go      # or patch | major | 0.1.0
# review and merge the PR it opens, then:
git switch main && git pull
mise run release:tag sdk-go                # pushes sdk-go-vX.Y.Z
mise run release:watch sdk-go              # the tag run pushes packages/sdk-go/vX.Y.Z
mise run release:verify sdk-go             # HTTP 200 for the version on the proxy
```

`release:prepare` rewrites the `const Version` line in
[`packages/sdk-go/version.go`](https://github.com/jamescarr/ankusa/blob/main/packages/sdk-go/version.go).

### What runs

`release-go.yml` pushes the module tag only after the full test suite passes,
because a proxy and checksum-database version is immutable: it must never point
at a commit CI has not run on. The workflow then waits up to 40 minutes for the
proxy to serve the version, which is also what lists the module on pkg.go.dev.

Two steps treat Go differently from the other kinds. Preflight does not ask the
proxy about the version, because a request made before the module tag exists
caches a miss there for up to about 30 minutes (the mirror does not re-check on
every request), which is exactly the state a release starts in. It checks the
module tag on `origin` instead, which is the same fact. `release:verify` does
ask the proxy, but only after the workflow's wait has passed.

### After

`release:verify sdk-go` asks `proxy.golang.org` for the version. The repository
has to be public for the proxy to fetch it. If it is private, the module tag is
still pushed, but consumers need `GOPRIVATE=github.com/jamescarr/ankusa` and
pkg.go.dev never lists the module.

## PHP SDK (Packagist)

`sdk-php` publishes as the `jamescarr/ankusa` package on
[Packagist](https://packagist.org/), which installs it with `composer require
jamescarr/ankusa`. Packagist reads `composer.json` at the root of a
repository, so it cannot be pointed at this monorepo.
[`release-php.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release-php.yml)
pushes the package to the **read-only split mirror**
[`jamescarr/ankusa-php`](https://github.com/jamescarr/ankusa-php) (tagged
`vX.Y.Z` there; the monorepo tag stays `sdk-php-vX.Y.Z`) and then asks
Packagist to re-crawl it. Development happens in `packages/sdk-php` only;
anything committed to the mirror is overwritten by the next release.

### Before you tag

The mirror, deploy key, and Packagist account already exist. The setup, if it
ever has to be repeated:

1. Create the public repository `jamescarr/ankusa-php` ("Read-only mirror of
   packages/sdk-php in jamescarr/ankusa"). Disable Issues, Wiki and Projects:
   every question about the code belongs in the main repo.
2. Give the release workflow write access to it:

   ```sh
   ssh-keygen -t ed25519 -N '' -C ankusa-php-mirror -f ankusa-php-deploy
   ```

   Add `ankusa-php-deploy.pub` as a **deploy key with write access** on
   `jamescarr/ankusa-php`, then store the private half on this repo and delete
   both files:

   ```sh
   gh secret set SDK_PHP_DEPLOY_KEY < ankusa-php-deploy
   rm ankusa-php-deploy ankusa-php-deploy.pub
   ```

3. Seed the mirror, so Packagist has something to import:

   ```sh
   git clone git@github.com:jamescarr/ankusa-php.git /tmp/ankusa-php
   cp -R packages/sdk-php/. /tmp/ankusa-php/
   (cd /tmp/ankusa-php && git add -A && git commit -m 'Seed from jamescarr/ankusa' && git push origin HEAD:main)
   ```

4. Submit <https://github.com/jamescarr/ankusa-php> at
   <https://packagist.org/packages/submit>. The package name comes from its
   `composer.json` (`jamescarr/ankusa`), and Packagist verifies the repository
   by reading that file.
5. From Packagist → Profile → API token, set the two remaining secrets:

   ```sh
   gh secret set PACKAGIST_USERNAME   # the Packagist account name
   gh secret set PACKAGIST_TOKEN      # the API token
   ```

   Use the account's **Safe** API token; if the `update-package` call in the
   workflow answers `403`, use the main API token instead.

Packagist names are first-come, so check before tagging under a new name:

```sh
curl -s -o /dev/null -w '%{http_code}\n' https://repo.packagist.org/p2/jamescarr/ankusa.json   # 404 = available
```

If `jamescarr/ankusa` is taken, change `name` in
[`packages/sdk-php/composer.json`](https://github.com/jamescarr/ankusa/blob/main/packages/sdk-php/composer.json)
(and this document) before tagging: `release:preflight`, `release:verify` and
the publish job all read that field, so nothing else changes.

```sh
mise run status                          # version, last tag, published?, commits since
mise run release:prepare minor sdk-php   # or patch | major | 0.2.1
# review and merge the PR it opens, then:
git switch main && git pull
mise run release:tag sdk-php
mise run release:watch sdk-php           # the tag run pushes the mirror and updates Packagist
mise run release:verify sdk-php          # HTTP 200 for the version on Packagist
```

`release:prepare` rewrites `public const string VERSION` in
[`src/Version.php`](https://github.com/jamescarr/ankusa/blob/main/packages/sdk-php/src/Version.php).
Composer locks do not record the root version, so nothing else needs rewriting.

### What runs

Preflight confirms the version is still unpublished on Packagist and the tag is
free. The tag run clones the mirror, replaces its contents with
`packages/sdk-php`, commits, tags `vX.Y.Z`, pushes, triggers a Packagist
update, polls `repo.packagist.org` until the version appears, and cuts the
GitHub release from the CHANGELOG section. A rerun is safe: if the mirror
already has the tag, the push is skipped.

Packagist installs from the mirror's GitHub zipball, so the archive carries
whatever `packages/sdk-php/.gitattributes` does not mark `export-ignore`;
tests and dev configs stay out of what users download.

### After

`release:verify sdk-php` looks for the version in Packagist's metadata, which
has no per-version endpoint.

## Elixir SDK (Hex)

`sdk-elixir` publishes as the `ankusa_sdk` package on Hex (Hex's `ankusa` is
this repo's core), from
[`packages/sdk-elixir/mix.exs`](https://github.com/jamescarr/ankusa/blob/main/packages/sdk-elixir/mix.exs),
using the same `HEX_API_KEY` secret as the core packages.

### Before you tag

The SDK is a pure HTTP client and does not depend on `ankusa` at all, so it has
no ordering constraint and `release:preflight` does not wait for core to be
live. Its registry lookup reads the app name from `mix.exs`, since the Hex name
and the directory name differ.

```sh
mise run status                            # version, last tag, published?, commits since
mise run release:prepare minor sdk-elixir  # or patch | major | 0.4.0
# review and merge the PR it opens, then:
git switch main && git pull
mise run release:tag sdk-elixir
mise run release:watch sdk-elixir          # the tag run builds and publishes to Hex
mise run release:verify sdk-elixir         # HTTP 200 for the version on hex.pm
```

`release:prepare` rewrites `@version` in `mix.exs`.

### What runs

Preflight confirms the tag is free and `ankusa_sdk`'s `@version` is still
unpublished. The tag run checks the tag against `mix.exs`, waits for the full
CI suite, publishes with `MIX_ENV=prod mix hex.publish package --yes` and the
docs, and cuts the GitHub release from the CHANGELOG section.

`mix.lock` is committed so CI's `mix deps.get --check-locked` gate pins CI to
known versions, but it is not in the package's `files`: Hex consumers resolve
`req ~> 0.7` and `plug ~> 1.18` themselves. Because the SDK has no `ankusa`
dependency, the publish job's `MIX_ENV=prod mix deps.get` resolves everything
from Hex, with no core-first wait.

### After

`release:verify sdk-elixir` checks the version under the app name
`ankusa_sdk`.

## Java SDK (Maven Central)

`sdk-java` publishes as `io.github.jamescarr:ankusa-sdk` on Maven Central, from
[`packages/sdk-java/build.sbt`](https://github.com/jamescarr/ankusa/blob/main/packages/sdk-java/build.sbt),
whose `organization`, `name` and `version` are the coordinates. `pkg_version`
and `pkg_registry_code` read them with `sed` and `release:prepare` rewrites
`version`, as they do for the other kinds.

The jar is built on JDK 25 but is Java 17 bytecode and Java 17 API
(`javacOptions` pins `--release 17`). It carries
`Automatic-Module-Name: io.github.jamescarr.ankusa` in its manifest (sbt has no
JPMS descriptor support, so there is no `module-info.java`) and has no `_3`
suffix or Scala dependency (`crossPaths := false`, `autoScalaLibrary :=
false`). `mise run check:package sdk-java` asserts all of that along with the
staged bundle's shape.

Publishing goes through the [Central
Portal](https://central.sonatype.com/), using sbt 2's built-in staging:
`publishSigned` stages the jar, sources, javadoc, POM and CycloneDX SBOM, each
with an armored `.asc` beside them, in
`packages/sdk-java/target/sona-staging`, and `sonaRelease` uploads that bundle
and waits for the portal to validate it. `mise run check:package sdk-java` runs
the unsigned `publish` and then checks the staged bundle, so the shape the
release signs is the shape CI gated on, including the SBOM, which sbt-sbom
attaches as the `cyclonedx` classifier artifact without any `build.sbt`
setting.

### Before you tag

The four secrets and the verified group already exist. The setup, if it ever
has to be repeated, is all by the repository owner:

1. Sign in to <https://central.sonatype.com/> with GitHub. That verifies
   `io.github.jamescarr` for the account, so no DNS TXT record is needed for
   this group id.
2. Generate a user token (Account → Generate User Token) and store its halves
   as the `SONATYPE_USERNAME` and `SONATYPE_PASSWORD` repository secrets. sbt 2
   reads both names out of the environment and appends them as a credential
   for `central.sonatype.com`; there is nothing to write to `~/.sbt`.
3. Create a PGP key pair, publish its public half to
   `keyserver.ubuntu.com`, and store the base64 of the armored secret key as
   `PGP_SECRET` and its passphrase as `PGP_PASSPHRASE`:

   ```sh
   gpg --quick-generate-key 'jamescarr <james.r.carr@gmail.com>' rsa4096 sign never
   gpg --keyserver keyserver.ubuntu.com --send-keys <key id>
   gpg --export-secret-keys --armor <key id> | base64 | tr -d '\n' | gh secret set PGP_SECRET
   ```

```sh
mise run status                            # version, last tag, published?, commits since
mise run release:prepare minor sdk-java    # or patch | major | 0.3.0
# review and merge the PR it opens, then:
git switch main && git pull
mise run release:tag sdk-java
mise run release:watch sdk-java            # the tag run signs, uploads, and waits for repo1
mise run release:verify sdk-java           # HTTP 200 for the POM on repo1.maven.org
```

### What runs

`publishSigned` stages the signed bundle and `sonaRelease` uploads it and waits
for the portal. The workflow then polls the CDN for up to an hour, because the
portal answers `PUBLISHED` before repo1 serves the POM. `sonaRelease` cannot be
exercised locally without the real token, so `release-maven.yml` is only
exercised by an actual tag.

### After

`release:verify sdk-java` reads the POM at
`https://repo1.maven.org/maven2/io/github/jamescarr/ankusa-sdk/<version>/ankusa-sdk-<version>.pom`,
so it confirms the portal's publish reached the CDN, not just that the portal
accepted the bundle.

## Clojure SDK (Clojars)

`sdk-clojure` publishes as `io.github.jamescarr/ankusa-clj` on Clojars, from
[`packages/sdk-clojure/build.clj`](https://github.com/jamescarr/ankusa/blob/main/packages/sdk-clojure/build.clj),
whose column-0 `(def lib '...)` and `(def version "...")` forms are the
coordinates. `pkg_version` and `pkg_registry_code` read them with `sed` and
`release:prepare` rewrites `version`, as they do for the other kinds.

The artifact is `ankusa-clj`, not `ankusa-sdk`: the Clojure CLI resolves from
Maven Central and Clojars together, and Central already has
`io.github.jamescarr:ankusa-sdk` for the Java SDK.

The jar is built by tools.build (`clojure -T:build jar`) and holds only
`ankusa/sdk/**` sources plus `META-INF`; its POM lists `org.clojure/clojure` and
`org.clojure/data.json` and no build or test tool. `mise run check:package
sdk-clojure` builds it and asserts both, so the shape the release uploads is the
shape CI gated on. The `format`, `lint`, and `test` tools are `deps.edn`
aliases (cljfmt, clj-kondo, the Cognitect test runner); mise has no entries for
them.

Publishing is `clojure -T:build deploy`, which uploads the jar `jar` already
built with deps-deploy and never rebuilds, so the jar the workflow attested is
the jar Clojars serves.

### Before you tag

The account, verified group, and secrets already exist. The setup, if it ever
has to be repeated, is all by the repository owner:

1. Create a Clojars account and verify the `io.github.jamescarr` group through
   GitHub (<https://clojars.org/verify/group>). That is what lets the account
   deploy under the group.
2. Generate a deploy token (Dashboard → Deploy Tokens) and store the account
   name as the `CLOJARS_USERNAME` repository secret and the token as
   `CLOJARS_PASSWORD`. deps-deploy reads both names out of the environment.

```sh
mise run status                              # version, last tag, published?, commits since
mise run release:prepare minor sdk-clojure   # or patch | major | 0.3.0
# review and merge the PR it opens, then:
git switch main && git pull
mise run release:tag sdk-clojure
mise run release:watch sdk-clojure           # the tag run builds, uploads, and waits for Clojars
mise run release:verify sdk-clojure          # HTTP 200 for the POM on repo.clojars.org
```

### What runs

The tag run builds with `clojure -T:build jar`, uploads with `clojure -T:build
deploy`, and then waits for `repo.clojars.org` to serve the POM.
`clojure -T:build deploy` cannot be exercised locally without the real token,
so `release-clojars.yml` is only exercised by an actual tag.

### After

`release:verify sdk-clojure` reads the POM at
`https://repo.clojars.org/io/github/jamescarr/ankusa-clj/<version>/ankusa-clj-<version>.pom`.

## Secrets

Repository secrets live under Settings → Secrets and variables → Actions.
`release:preflight` checks the ones the package needs before any tag exists.

- **Hex:** `HEX_API_KEY`, a Hex key scoped to `api:write`, generated from the
  Hex.pm dashboard (Keys).
- **Docker Hub:** `DOCKERHUB_USERNAME`, and `DOCKERHUB_TOKEN`, a Docker Hub
  personal access token with Read & Write.
- **npm:** none while the repository **variable** `NPM_AUTH` (Settings →
  Secrets and variables → Actions → Variables) is `oidc`, as it is:
  `release-npm.yml` publishes through npm Trusted Publishing (OIDC). With
  `NPM_AUTH` unset (or any other value) the job falls back to an `NPM_TOKEN`
  secret, a granular token with publish rights on `ankusa` that bypasses 2FA.
  npm is restricting those for direct publishing, so treat it as a stopgap. The
  trusted publisher itself is described in
  [npm (TypeScript SDK)](#npm-typescript-sdk).
- **PyPI:** no repository secret. Publishing uses
  [Trusted Publishing](https://docs.pypi.org/trusted-publishers/) (OIDC), set
  up on PyPI, not here: see [Python SDK (PyPI)](#python-sdk-pypi).
- **crates.io:** `CARGO_REGISTRY_TOKEN`, a crates.io API token carrying the
  `publish-new` and `publish-update` scopes, generated at
  <https://crates.io/settings/tokens>. See [Rust SDK
  (crates.io)](#rust-sdk-cratesio) for why it is a token rather than OIDC.
- **RubyGems:** no repository secret. Trusted Publishing (OIDC); see [Ruby SDK
  (RubyGems)](#ruby-sdk-rubygems).
- **Go:** no repository secret. The module proxy reads the pushed git tag; see
  [Go module (proxy.golang.org)](#go-module-proxygolangorg).
- **Packagist (the PHP SDK):** `SDK_PHP_DEPLOY_KEY`, an SSH deploy key with
  **write access** to the read-only mirror `jamescarr/ankusa-php`;
  `PACKAGIST_USERNAME`, the Packagist account name; and `PACKAGIST_TOKEN`, that
  account's API token. Setup: see [PHP SDK (Packagist)](#php-sdk-packagist).
- **Maven Central (the Java SDK):** `SONATYPE_USERNAME` and
  `SONATYPE_PASSWORD`, a Central Portal user token's two halves, plus
  `PGP_SECRET` (base64 of the armored secret signing key) and
  `PGP_PASSPHRASE`. The Central Portal has no OIDC trusted publishing, so all
  four are repository secrets. Setup: see [Java SDK (Maven
  Central)](#java-sdk-maven-central).
- **Clojars (the Clojure SDK):** `CLOJARS_USERNAME`, the Clojars account that
  owns the group, and `CLOJARS_PASSWORD`, a deploy token for that account (not
  its password). Clojars has no OIDC trusted publishing, so both are
  repository secrets. Setup: see [Clojure SDK
  (Clojars)](#clojure-sdk-clojars).
