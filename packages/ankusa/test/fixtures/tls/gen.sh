#!/usr/bin/env bash
#
# Regenerate the TLS fixtures the listener tests and the smoke drill use: a
# throwaway CA, a server certificate for localhost / 127.0.0.1 signed by it,
# and a client certificate signed by it. Valid for 100 years so no test ever
# fails on a calendar date. These keys protect nothing: they exist only in this
# repository, for tests.
#
#   packages/ankusa/test/fixtures/tls/gen.sh
set -euo pipefail
cd "$(dirname "$0")"

DAYS=36500

openssl req -x509 -newkey rsa:2048 -nodes -days "$DAYS" -subj "/CN=ankusa-test-ca" \
  -keyout ca-key.pem -out ca.pem \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null

issue() { # name subject extfile-contents
  openssl req -newkey rsa:2048 -nodes -subj "$2" -keyout "$1-key.pem" -out "$1.csr" 2>/dev/null
  printf '%s\n' "$3" >"$1.ext"
  openssl x509 -req -in "$1.csr" -CA ca.pem -CAkey ca-key.pem -CAcreateserial -days "$DAYS" \
    -extfile "$1.ext" -out "$1.pem" 2>/dev/null
  rm -f "$1.csr" "$1.ext"
}

issue server "/CN=localhost" "subjectAltName=DNS:localhost,IP:127.0.0.1
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth"

issue client "/CN=ankusa-test-client" "basicConstraints=CA:FALSE
keyUsage=digitalSignature
extendedKeyUsage=clientAuth"

# A second CA and a client it signed: a certificate the listener must refuse.
openssl req -x509 -newkey rsa:2048 -nodes -days "$DAYS" -subj "/CN=ankusa-other-ca" \
  -keyout other-ca-key.pem -out other-ca.pem 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj "/CN=ankusa-stranger" -keyout stranger-key.pem -out stranger.csr 2>/dev/null
printf 'extendedKeyUsage=clientAuth\n' >stranger.ext
openssl x509 -req -in stranger.csr -CA other-ca.pem -CAkey other-ca-key.pem -CAcreateserial \
  -days "$DAYS" -extfile stranger.ext -out stranger.pem 2>/dev/null
rm -f stranger.csr stranger.ext other-ca-key.pem *.srl
