#!/bin/sh
# Container entrypoint. POSIX sh (busybox ash on Alpine), no bashisms.
#
#   start         run the server (the default CMD)
#   check-config  validate the config and exit 0, or print the error and exit 78
#   print-config  print the effective, redacted config as JSON
#   version       print the server and core versions
#   healthcheck   exit 0 when the node is ready (GET /ready), the image's HEALTHCHECK
#   remote        an IEx shell attached to the running node (loopback distribution)
#   rpc EXPR      evaluate EXPR inside the running node and print what it prints
#
# Anything else is executed as-is, so `docker run jamescarr/ankusa sh` works.
set -e

case "${1:-start}" in
  start) exec /opt/ankusa/bin/ankusa start ;;
  check-config) exec /opt/ankusa/bin/ankusa eval 'AnkusaServer.CLI.check_config()' ;;
  print-config) exec /opt/ankusa/bin/ankusa eval 'AnkusaServer.CLI.print_config()' ;;
  version) exec /opt/ankusa/bin/ankusa eval 'AnkusaServer.CLI.version()' ;;
  # The ingress listener first (an :edge node always has it); a node without
  # the :edge role, or whose ingress listener terminates TLS (this probe is
  # plain HTTP), answers the same probe on the admin listener, then on the
  # metrics listener. All run `Ankusa.Health.ready/1`, so a 503 on one is a 503
  # on the others.
  healthcheck)
    curl -fsS "http://127.0.0.1:${ANKUSA_HTTP_PORT:-${PORT:-4000}}/ready" >/dev/null 2>&1 ||
      curl -fsS "http://127.0.0.1:${ANKUSA_ADMIN_PORT:-4002}/ready" >/dev/null 2>&1 ||
      exec curl -fsS "http://127.0.0.1:${ANKUSA_METRICS_PORT:-4004}/ready" >/dev/null
    ;;
  remote) exec /opt/ankusa/bin/ankusa remote ;;
  rpc) shift; exec /opt/ankusa/bin/ankusa rpc "$@" ;;
  *) exec "$@" ;;
esac
