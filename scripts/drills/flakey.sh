#!/usr/bin/env bash
#
# The crash drill on a device-mapper device: the same harness as `mise run
# test:crash`, on ext4 over a loop device whose I/O fails in 2 s windows, then
# a power-loss cut (every I/O fails, the instance is killed, the filesystem is
# repaired) so a hook acked without an fsync is gone and the drill reports
# `missing>0`. Needs `docker run --privileged`, loop devices and device-mapper
# in the Docker host's kernel.
#
# The windows use the `error` target, which every kernel with device-mapper has,
# not `dm-flakey` (a module some kernels lack); the faults are the same kind:
# the device answers I/O with EIO for 2 s, then is healthy again.
#
# Exit status: 0 every acked hook came back; 1 the drill failed; 2 this host
# cannot run it (release:preflight reports that as a skipped manual step).
set -euo pipefail

exec "$(dirname "${BASH_SOURCE[0]}")/crash.sh" dm
