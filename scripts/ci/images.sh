#!/usr/bin/env bash
#
# Carry CI's Docker images between runs in the Actions cache instead of
# pulling them from Docker Hub on every job. Anonymous pulls from shared
# runner IPs hit Hub's rate limit (`toomanyrequests` failed 11 of the last 21
# failed package jobs), and an authenticated pull only raises the ceiling.
#
#   scripts/ci/images.sh sync <dir> <ref>...
#   scripts/ci/images.sh load <dir> <ref>...
#   scripts/ci/images.sh save <dir> <ref>...
#
# One `<slug>.tar` per ref under <dir>, which the workflow points
# `actions/cache` at. `<slug>` is the ref with every character outside
# `[A-Za-z0-9._-]` turned into `-`, so the file name carries the whole ref.
#
# `sync` is what a job runs before its check: it loads the tars the cache
# restored and then pulls (and saves) only the refs the daemon still lacks, so
# a warm run does no network I/O. It also drops the tar of a ref this run does
# not use, so a digest bump does not carry the old image into the new entry.
# `load` and `save` are its two halves: `save` skips a ref whose tar is already
# there, which makes it the cheap `if: always()` step after a failed check.
#
# A ref is the digest-qualified one the compose files and the Dockerfile carry
# (`redis:8-alpine@sha256:…`), which is what `docker compose config --images`
# and crash.sh print. `docker compose up` defaults to `pull_policy: missing`,
# so an image loaded under that ref is not pulled again — but only on the
# containerd image store, which is what makes `docker load` record the digest
# the tar carries. On the classic overlay2 store a loaded image has no digest,
# `repo:tag@sha256:…` does not resolve, and `docker compose up` fetches the
# manifest again; ci.yml's `use the containerd image store` step switches the
# runner for that reason, and is a no-op where it is already the default.
#
# Written for bash 3.2 (macOS /bin/bash): no mapfile, no associative arrays.
set -euo pipefail

slug() {
  printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '-'
}

# What `docker save` is asked for: the tag. Saving `repo:tag@sha256:…` writes a
# tar with no RepoTags, and `docker load` then restores an image no compose
# file resolves; the tag alone saves the layers, with the digest alongside.
savable() {
  local ref=${1%@sha256:*}
  case ${ref##*/} in
    *:*) printf '%s' "$ref" ;;
    *) printf '%s:latest' "$ref" ;;
  esac
}

present() {
  docker image inspect "$1" >/dev/null 2>&1
}

# The tar for one ref, written under a name `load` never sees half-written.
save_ref() {
  local tar="$dir/$(slug "$1").tar" tag
  tag=$(savable "$1")
  # A pull by digest records no tag, and `docker save` of a digest-qualified
  # ref writes a tar with no RepoTags that nothing resolves; the tag is what
  # carries the layers into the tar, and the digest is what `docker load`
  # records beside it (see the header).
  present "$tag" || docker tag "$1" "$tag"
  docker save -o "$tar.tmp" "$tag"
  mv "$tar.tmp" "$tar"
}

load_tars() {
  [ -d "$dir" ] || return 0
  local tar
  for tar in "$dir"/*.tar; do
    [ -e "$tar" ] || continue
    docker load -i "$tar"
  done
}

# Drop tars for refs this run does not use: a `restore-keys` prefix hit hands
# back the previous digest's tars, and the next save would otherwise copy them
# into the new entry.
prune_tars() {
  [ -d "$dir" ] || return 0
  local tar name ref keep
  for tar in "$dir"/*.tar; do
    [ -e "$tar" ] || continue
    name=${tar##*/}
    keep=""
    for ref in "$@"; do
      if [ "$name" = "$(slug "$ref").tar" ]; then keep=1; break; fi
    done
    [ -n "$keep" ] || rm -f "$tar"
  done
}

cmd=${1:-}
usage() {
  echo "usage: images.sh containerd" >&2
  echo "       images.sh sync|load|save <dir> <ref>..." >&2
  exit 2
}

# Make `docker load` record the digest the tar carries. `docker compose up`
# asks the daemon for the exact `repo:tag@sha256:…` ref; on the classic
# overlay2 store a loaded image has no digest, so that ref misses and compose
# fetches the manifest from Docker Hub again. The containerd image store
# records the digest on load, and is the default from Docker 29 on.
#
# `jq` merges into an existing daemon.json rather than replacing it (the
# runner image ships jq); the restart needs a root shell, which the runner has.
containerd_store() {
  local info config=/etc/docker/daemon.json i
  info=$(docker info 2>/dev/null || true)
  case "$info" in
    *"driver-type: io.containerd.snapshotter"*)
      echo "the daemon already uses the containerd image store"
      return 0
      ;;
  esac
  sudo mkdir -p /etc/docker
  if sudo test -s "$config"; then
    sudo jq '.features["containerd-snapshotter"] = true' "$config" | sudo tee "$config.new" >/dev/null
    sudo mv "$config.new" "$config"
  else
    printf '%s\n' '{"features":{"containerd-snapshotter":true}}' | sudo tee "$config" >/dev/null
  fi
  sudo systemctl restart docker
  for i in $(seq 60); do
    docker info >/dev/null 2>&1 && break
    sleep 1
  done
  info=$(docker info 2>/dev/null || true)
  case "$info" in
    *"driver-type: io.containerd.snapshotter"*)
      echo "switched to the containerd image store"
      ;;
    *)
      echo "the daemon did not come back on the containerd image store" >&2
      exit 1
      ;;
  esac
}

if [ "$cmd" = containerd ]; then
  [ "$#" -eq 1 ] || usage
  containerd_store
  exit 0
fi

case "$cmd" in
  load | save | sync) ;;
  *) usage ;;
esac

dir=${2:-}
[ -n "$dir" ] && [ "$#" -gt 2 ] || usage
shift 2
refs=("$@")

mkdir -p "$dir"

case "$cmd" in
  load)
    load_tars
    for ref in "${refs[@]}"; do
      if present "$ref"; then echo "present: $ref"; else echo "missing: $ref"; fi
    done
    ;;
  save)
    for ref in "${refs[@]}"; do
      if [ -e "$dir/$(slug "$ref").tar" ]; then
        continue
      elif ! present "$ref"; then
        # `if: always()` runs this after a check that may have failed before
        # any pull: nothing to save, and no step failure to mask the cause.
        echo "not present: $ref"
        continue
      fi
      save_ref "$ref"
      echo "saved: $ref"
    done
    ;;
  sync)
    prune_tars "${refs[@]}"
    load_tars
    loaded=0 pulled=0 saved=0
    for ref in "${refs[@]}"; do
      pulled_now=""
      if present "$ref"; then
        loaded=$((loaded + 1))
      else
        docker pull "$ref"
        pulled=$((pulled + 1))
        pulled_now=1
      fi
      # A pulled ref is saved even over an existing tar: the tar that was
      # restored is what failed to produce the image.
      if [ -n "$pulled_now" ] || [ ! -e "$dir/$(slug "$ref").tar" ]; then
        save_ref "$ref"
        saved=$((saved + 1))
      fi
    done
    echo "images: loaded $loaded, pulled $pulled, saved $saved"
    ;;
esac
