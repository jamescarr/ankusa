#!/bin/sh
# Container entrypoint of jamescarr/ankusa-gateway. POSIX sh (busybox ash on
# Alpine), no bashisms.
#
#   start         run the gateway (the default CMD)
#   check-config  validate the config and exit 0, or print the error and exit 78
#   print-config  print the effective, redacted config as JSON
#   version       print the image's, the gateway library's and core's versions
#   healthcheck   exit 0 when the gateway is ready (GET /ready), the image's HEALTHCHECK
#   remote        an IEx shell attached to the running node (loopback distribution)
#   rpc EXPR      evaluate EXPR inside the running node and print what it prints
#
# Anything else is executed as-is, so `docker run jamescarr/ankusa-gateway sh` works.
set -e

case "${1:-start}" in
  start) exec /opt/ankusa-gateway/bin/ankusa_gateway start ;;
  check-config) exec /opt/ankusa-gateway/bin/ankusa_gateway eval 'AnkusaGatewayServer.CLI.check_config()' ;;
  print-config) exec /opt/ankusa-gateway/bin/ankusa_gateway eval 'AnkusaGatewayServer.CLI.print_config()' ;;
  version) exec /opt/ankusa-gateway/bin/ankusa_gateway eval 'AnkusaGatewayServer.CLI.version()' ;;
  # The gateway's own port first; a gateway that terminates TLS (this probe is
  # plain HTTP) answers the same probe on the metrics listener. Both run
  # `Ankusa.Health.ready/1`.
  healthcheck)
    curl -fsS "http://127.0.0.1:${ANKUSA_CLAIM_CHECK_PORT:-4001}/ready" >/dev/null 2>&1 ||
      exec curl -fsS "http://127.0.0.1:${ANKUSA_METRICS_PORT:-4004}/ready" >/dev/null
    ;;
  remote) exec /opt/ankusa-gateway/bin/ankusa_gateway remote ;;
  rpc) shift; exec /opt/ankusa-gateway/bin/ankusa_gateway rpc "$@" ;;
  *) exec "$@" ;;
esac
