#!/bin/sh
# Container entrypoint. POSIX sh (busybox ash on Alpine), no bashisms.
#
#   start         run the server (the default CMD)
#   check-config  validate the config and exit 0, or print the error and exit 78
#   print-config  print the effective, redacted config as JSON
#   version       print the server and core versions
#
# Anything else is executed as-is, so `docker run ankusa/ankusa sh` works.
set -e

case "${1:-start}" in
  start) exec /opt/ankusa/bin/ankusa start ;;
  check-config) exec /opt/ankusa/bin/ankusa eval 'AnkusaServer.CLI.check_config()' ;;
  print-config) exec /opt/ankusa/bin/ankusa eval 'AnkusaServer.CLI.print_config()' ;;
  version) exec /opt/ankusa/bin/ankusa eval 'AnkusaServer.CLI.version()' ;;
  *) exec "$@" ;;
esac
