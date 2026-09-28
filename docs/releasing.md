# Releasing

Every releasable artifact is a directory under `packages/`, versioned on its
own with its own `CHANGELOG.md`. The directory name is the git tag prefix, and
the files in it decide where it publishes:

| Package | Kind | Tag | Published by |
| --- | --- | --- | --- |
| `ankusa`, `ankusa_rabbitmq`, `ankusa_kafka`, `ankusa_nats` | Hex (`mix.exs`) | `<pkg>-vX.Y.Z` | [`release.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release.yml) |
| `ankusa_server` | Docker image `jamescarr/ankusa` (`Dockerfile`) | `ankusa_server-vX.Y.Z` | [`docker.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/docker.yml) |
| `sdk-typescript` | npm package `ankusa` (`package.json`) | `sdk-typescript-vX.Y.Z` | [`release-npm.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/release-npm.yml) |

The npm package is also named `ankusa`; its tag prefix is the directory name,
not the package name, so it can't be confused with the Hex core's
`ankusa-vX.Y.Z`.

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
   `mix.exs`, or `npm version` for npm), opens a dated `## [X.Y.Z]` heading
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

The server image and the npm SDK have no ordering constraint: the image builds
from this checkout, and the SDK depends on nothing here.

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
