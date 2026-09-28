# Package helpers shared by .mise/tasks/* and the GitHub workflows.
#
# Sourced only; sets no shell options. Every releasable artifact is a
# directory under packages/; its name is the git tag prefix (`<name>-vX.Y.Z`)
# and its kind comes from the files in it:
#   package.json -> npm, else Dockerfile -> docker, else mix.exs -> hex.
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

pkg_kind() {
  local dir
  dir=$(pkg_dir "$1") || exit 1
  if [ -f "$ROOT/$dir/package.json" ]; then
    echo npm
  elif [ -f "$ROOT/$dir/Dockerfile" ]; then
    echo docker
  elif [ -f "$ROOT/$dir/mix.exs" ]; then
    echo hex
  else
    fail "$dir has no package.json, Dockerfile, or mix.exs"
  fi
}

# `ankusa` first (everything else depends on it), then the other Hex packages,
# then Docker images, then npm packages; byte order within each group.
pkg_names() {
  local kind name all
  all=$(_pkg_all)
  if [ -d "$ROOT/packages/ankusa" ]; then echo ankusa; fi
  for kind in hex docker npm; do
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
  local dir kind url npm_name
  dir=$(pkg_dir "$1") || exit 1
  kind=$(pkg_kind "$1") || exit 1
  case "$kind" in
    hex) url="https://hex.pm/api/packages/$1/releases/$2" ;;
    docker) url="https://hub.docker.com/v2/namespaces/${DOCKER_IMAGE%%/*}/repositories/${DOCKER_IMAGE#*/}/tags/$2" ;;
    npm)
      npm_name=$(node -p "require('$ROOT/$dir/package.json').name")
      url="https://registry.npmjs.org/$npm_name/$2"
      ;;
  esac
  # curl prints 000 and exits non-zero when the registry is unreachable; the
  # 000 is the useful part.
  curl -s -o /dev/null -w '%{http_code}' "$url" || true
}

pkg_workflow() {
  case "$(pkg_kind "$1")" in
    hex) echo release.yml ;;
    docker) echo docker.yml ;;
    npm) echo release-npm.yml ;;
  esac
}

pkg_secrets() {
  case "$(pkg_kind "$1")" in
    hex) echo HEX_API_KEY ;;
    docker) echo DOCKERHUB_USERNAME DOCKERHUB_TOKEN ;;
    npm) echo NPM_TOKEN ;;
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
