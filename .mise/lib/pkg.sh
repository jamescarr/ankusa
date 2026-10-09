# Package helpers shared by .mise/tasks/* and the GitHub workflows.
#
# Sourced only; sets no shell options. Every releasable artifact is a
# directory under packages/; its name is the git tag prefix (`<name>-vX.Y.Z`)
# and its kind comes from the files in it:
#   package.json -> npm, else Dockerfile -> docker, else mix.exs -> hex,
#   else pyproject.toml -> python, else Cargo.toml -> cargo, else *.gemspec -> ruby,
#   else go.mod -> go, else composer.json -> php, else build.sbt -> java,
#   else deps.edn -> clojure.
#
# Written for bash 3.2 (macOS /bin/bash): no mapfile, no associative arrays.

ROOT="${MISE_PROJECT_ROOT:-$(git rev-parse --show-toplevel)}"
GITHUB_REPO=jamescarr/ankusa
DOCKER_IMAGE=jamescarr/ankusa

# stderr in both cases: a failure inside `x=$(pkg_...)` must still be seen, and
# the Actions runner picks `::error::` up from stderr as well as stdout.
fail() {
  if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
    printf '::error::%s\n' "$*" >&2
  else
    printf '\nFAIL: %s\n' "$*" >&2
  fi
  exit 1
}

# Every directory under packages/, in byte order, whatever its kind.
_pkg_all() {
  local d
  for d in "$ROOT"/packages/*/; do
    [ -d "$d" ] || continue
    d="${d%/}"
    printf '%s\n' "${d##*/}"
  done | LC_ALL=C sort
}

pkg_dir() {
  local name="${1:-}"
  case "$name" in
    "" | */* | .*) fail "unknown package '$name' (known: $(echo $(_pkg_all)))" ;;
  esac
  [ -d "$ROOT/packages/$name" ] || fail "unknown package $name (known: $(echo $(_pkg_all)))"
  printf 'packages/%s\n' "$name"
}

# The *.gemspec in packages/<name> (nothing when there is none).
_pkg_gemspec() {
  local f
  for f in "$ROOT/$1"/*.gemspec; do
    if [ -f "$f" ]; then printf '%s\n' "$f"; fi
    return 0
  done
}

# A column-0 `<key> := "<value>"` setting from packages/<name>/build.sbt.
_pkg_sbt_setting() { sed -n "s/^$2 := \"\(.*\)\"\$/\1/p" "$ROOT/$1/build.sbt"; }

# The `(def lib '<group>/<artifact>)` coordinate from packages/<name>/build.clj.
_pkg_clj_lib() { sed -n "s/^(def lib '\(.*\))\$/\1/p" "$ROOT/$1/build.clj"; }

pkg_kind() {
  local dir
  dir=$(pkg_dir "$1") || exit 1
  if [ -f "$ROOT/$dir/package.json" ]; then
    echo npm
  elif [ -f "$ROOT/$dir/Dockerfile" ]; then
    echo docker
  elif [ -f "$ROOT/$dir/mix.exs" ]; then
    echo hex
  elif [ -f "$ROOT/$dir/pyproject.toml" ]; then
    echo python
  elif [ -f "$ROOT/$dir/Cargo.toml" ]; then
    echo cargo
  elif [ -n "$(_pkg_gemspec "$dir")" ]; then
    echo ruby
  elif [ -f "$ROOT/$dir/go.mod" ]; then
    echo go
  elif [ -f "$ROOT/$dir/composer.json" ]; then
    echo php
  elif [ -f "$ROOT/$dir/build.sbt" ]; then
    echo java
  elif [ -f "$ROOT/$dir/deps.edn" ]; then
    echo clojure
  else
    fail "$dir has no package.json, Dockerfile, mix.exs, pyproject.toml, Cargo.toml, *.gemspec, go.mod, composer.json, build.sbt, or deps.edn"
  fi
}

# `ankusa` first (everything else depends on it), then the other Hex packages,
# then Docker images, then npm packages, then Python ones, then Cargo ones, then
# Ruby gems, then Go modules, then PHP ones, then Java ones, then Clojure ones;
# byte order within each group.
pkg_names() {
  local kind name all
  all=$(_pkg_all)
  if [ -d "$ROOT/packages/ankusa" ]; then echo ankusa; fi
  for kind in hex docker npm python cargo ruby go php java clojure; do
    for name in $all; do
      if [ "$name" != ankusa ] && [ "$(pkg_kind "$name")" = "$kind" ]; then
        printf '%s\n' "$name"
      fi
    done
  done
}

# pkg_names as a one-line JSON array (the CI matrix).
pkg_json() {
  local name sep=""
  printf '['
  for name in $(pkg_names); do
    printf '%s"%s"' "$sep" "$name"
    sep=","
  done
  printf ']\n'
}

pkg_version() {
  local dir kind v
  dir=$(pkg_dir "$1") || exit 1
  kind=$(pkg_kind "$1") || exit 1
  case "$kind" in
    hex | docker) v=$(sed -n 's/^  @version "\(.*\)"$/\1/p' "$ROOT/$dir/mix.exs") ;;
    npm) v=$(node -p "require('$ROOT/$dir/package.json').version") ;;
    python) v=$(sed -n 's/^version = "\(.*\)"$/\1/p' "$ROOT/$dir/pyproject.toml") ;;
    # The range matters: `[[test]] name = …` is also a column-0 `name =`.
    cargo) v=$(sed -n '/^\[package\]/,/^\[/ s/^version = "\(.*\)"$/\1/p' "$ROOT/$dir/Cargo.toml") ;;
    ruby) v=$(ruby -e 'print Gem::Specification.load(ARGV[0]).version' "$(_pkg_gemspec "$dir")") ;;
    go) v=$(sed -n 's/^const Version = "\(.*\)"$/\1/p' "$ROOT/$dir/version.go") ;;
    php) v=$(sed -n "s/^    public const string VERSION = '\(.*\)';\$/\1/p" "$ROOT/$dir/src/Version.php") ;;
    java) v=$(_pkg_sbt_setting "$dir" version) ;;
    clojure) v=$(sed -n 's/^(def version "\(.*\)")$/\1/p' "$ROOT/$dir/build.clj") ;;
  esac
  [ -n "$v" ] || fail "could not read the version of $1 from $dir"
  printf '%s\n' "$v"
}

pkg_tag() {
  local v="${2:-}"
  if [ -z "$v" ]; then
    v=$(pkg_version "$1") || exit 1
  fi
  printf '%s-v%s\n' "$1" "$v"
}

# The tag the Go module proxy resolves for a module in packages/<name>/:
# `packages/<name>/vX.Y.Z`. Pushed by release-go.yml after CI passes.
pkg_go_tag() { printf '%s/v%s\n' "$(pkg_dir "$1")" "$2"; }

pkg_from_tag() {
  local name="${1%-v*}"
  pkg_dir "$name" >/dev/null || exit 1
  printf '%s\n' "$name"
}

pkg_has_heading() {
  local dir
  dir=$(pkg_dir "$1") || exit 1
  grep -q "^## \[$2\]" "$ROOT/$dir/CHANGELOG.md"
}

# The CHANGELOG section for a version, without its heading: the GitHub
# release notes.
pkg_notes() {
  local dir
  dir=$(pkg_dir "$1") || exit 1
  pkg_has_heading "$1" "$2" || fail "$dir/CHANGELOG.md has no '## [$2]' heading"
  awk -v v="$2" '$0 ~ "^## \\[" v "\\]" {f=1; next} f && /^## \[/ {exit} f' "$ROOT/$dir/CHANGELOG.md"
}

# HTTP status of NAME@VERSION on its registry: 200 published, 404 not.
pkg_registry_code() {
  local dir kind url npm_name pypi_name crate_name gem_name hex_name module composer_name meta code maven_group maven_artifact clj_lib clj_group clj_artifact
  dir=$(pkg_dir "$1") || exit 1
  kind=$(pkg_kind "$1") || exit 1
  case "$kind" in
    hex)
      # The package directory is the git-tag prefix, not necessarily the Hex
      # name (`sdk-elixir` publishes as `ankusa_sdk`), so read the app it
      # declares.
      hex_name=$(sed -n 's/^      app: :\([a-z0-9_]*\),$/\1/p' "$ROOT/$dir/mix.exs")
      [ -n "$hex_name" ] || fail "could not read the app name from $dir/mix.exs"
      url="https://hex.pm/api/packages/$hex_name/releases/$2"
      ;;
    docker) url="https://hub.docker.com/v2/namespaces/${DOCKER_IMAGE%%/*}/repositories/${DOCKER_IMAGE#*/}/tags/$2" ;;
    npm)
      npm_name=$(node -p "require('$ROOT/$dir/package.json').name")
      url="https://registry.npmjs.org/$npm_name/$2"
      ;;
    python)
      pypi_name=$(sed -n 's/^name = "\(.*\)"$/\1/p' "$ROOT/$dir/pyproject.toml")
      url="https://pypi.org/pypi/$pypi_name/$2/json"
      ;;
    cargo)
      # The range matters: the `[[test]]` table's `name` is column-0 too.
      crate_name=$(sed -n '/^\[package\]/,/^\[/ s/^name = "\(.*\)"$/\1/p' "$ROOT/$dir/Cargo.toml")
      url="https://crates.io/api/v1/crates/$crate_name/$2"
      ;;
    ruby)
      gem_name=$(ruby -e 'print Gem::Specification.load(ARGV[0]).name' "$(_pkg_gemspec "$dir")")
      url="https://rubygems.org/api/v2/rubygems/$gem_name/versions/$2.json"
      ;;
    go)
      module=$(sed -n 's/^module //p' "$ROOT/$dir/go.mod")
      url="https://proxy.golang.org/$module/@v/v$2.info"
      ;;
    php)
      # Packagist has no per-version endpoint: fetch the package's metadata and
      # look for the version in it.
      composer_name=$(node -p "require('$ROOT/$dir/composer.json').name")
      meta=$(mktemp)
      code=$(curl -s -o "$meta" -w '%{http_code}' "https://repo.packagist.org/p2/$composer_name.json" || true)
      if [ "$code" = 200 ]; then
        code=$(node -e 'const [f, n, v] = process.argv.slice(1); const m = JSON.parse(require("fs").readFileSync(f, "utf8")); console.log((m.packages[n] || []).some((e) => String(e.version).replace(/^v/, "") === v) ? 200 : 404)' "$meta" "$composer_name" "$2")
      fi
      rm -f "$meta"
      printf '%s' "$code"
      return
      ;;
    java)
      # Coordinates come from build.sbt; repo1 serves the POM once Central has
      # synced the release.
      maven_group=$(_pkg_sbt_setting "$dir" organization)
      maven_artifact=$(_pkg_sbt_setting "$dir" name)
      [ -n "$maven_group" ] && [ -n "$maven_artifact" ] || fail "could not read organization/name from $dir/build.sbt"
      url="https://repo1.maven.org/maven2/${maven_group//.//}/$maven_artifact/$2/$maven_artifact-$2.pom"
      ;;
    clojure)
      # Coordinates come from build.clj; the Clojars repo serves the POM as
      # soon as a deploy lands.
      clj_lib=$(_pkg_clj_lib "$dir")
      [ -n "$clj_lib" ] || fail "could not read the lib coordinate from $dir/build.clj"
      clj_group=${clj_lib%/*}
      clj_artifact=${clj_lib#*/}
      url="https://repo.clojars.org/${clj_group//.//}/$clj_artifact/$2/$clj_artifact-$2.pom"
      ;;
  esac
  # curl prints 000 and exits non-zero when the registry is unreachable; the
  # 000 is the useful part. crates.io answers 403 to curl's own user agent, so
  # every lookup carries one that names this repo.
  curl -s -A "ankusa-release (https://github.com/$GITHUB_REPO)" -o /dev/null -w '%{http_code}' "$url" || true
}

pkg_workflow() {
  case "$(pkg_kind "$1")" in
    hex) echo release.yml ;;
    docker) echo docker.yml ;;
    npm) echo release-npm.yml ;;
    python) echo release-python.yml ;;
    cargo) echo release-crates.yml ;;
    ruby) echo release-ruby.yml ;;
    go) echo release-go.yml ;;
    php) echo release-php.yml ;;
    java) echo release-maven.yml ;;
    clojure) echo release-clojars.yml ;;
  esac
}

pkg_secrets() {
  case "$(pkg_kind "$1")" in
    hex) echo HEX_API_KEY ;;
    docker) echo DOCKERHUB_USERNAME DOCKERHUB_TOKEN ;;
    npm) echo NPM_TOKEN ;;
    # PyPI publishes via Trusted Publishing (OIDC): no repo secret needed.
    python) : ;;
    # crates.io Trusted Publishing needs the crate to exist before a publisher
    # can be registered for it, so the first publish is token-driven; the
    # switch afterwards is in docs/releasing.md.
    cargo) echo CARGO_REGISTRY_TOKEN ;;
    # RubyGems publishes via Trusted Publishing (OIDC): no repo secret needed.
    ruby) : ;;
    # The Go module proxy reads the git tag: no repo secret needed.
    go) : ;;
    # Packagist: the deploy key pushes to the read-only split mirror
    # jamescarr/ankusa-php, then the API token triggers the package update.
    php) echo SDK_PHP_DEPLOY_KEY PACKAGIST_USERNAME PACKAGIST_TOKEN ;;
    # Maven Central's Central Portal has no OIDC trusted publishing: a portal
    # user token, plus the PGP key sbt-pgp signs with.
    java) echo SONATYPE_USERNAME SONATYPE_PASSWORD PGP_SECRET PGP_PASSPHRASE ;;
    # Clojars takes a deploy token as the password; the token goes in
    # CLOJARS_PASSWORD.
    clojure) echo CLOJARS_USERNAME CLOJARS_PASSWORD ;;
  esac
}

# A tag is a promise about a commit: it must point at what CI ran on. Checked
# against origin/main rather than a branch name, because the flow is "merge the
# release PR, then tag the merged commit".
pkg_guard_main() {
  local local_sha remote_sha
  [ -z "$(git -C "$ROOT" status --porcelain)" ] || fail "working tree is dirty; commit or stash first"
  git -C "$ROOT" fetch --quiet origin main
  local_sha=$(git -C "$ROOT" rev-parse HEAD)
  remote_sha=$(git -C "$ROOT" rev-parse origin/main)
  [ "$local_sha" = "$remote_sha" ] || fail "HEAD ($local_sha) != origin/main ($remote_sha); merge and pull first"
  printf '  clean tree, HEAD == origin/main\n'
}

# Every Mix project relative to $ROOT: packages, then examples, then tools.
mix_projects() {
  local glob f
  for glob in 'packages/*' 'examples/*/*' 'tools/*'; do
    for f in "$ROOT"/$glob/mix.exs; do
      [ -f "$f" ] || continue
      f="${f%/mix.exs}"
      printf '%s\n' "${f#"$ROOT"/}"
    done | LC_ALL=C sort
  done
}
