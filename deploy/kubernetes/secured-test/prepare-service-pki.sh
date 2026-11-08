#!/bin/bash
# Generates a per-pod Kafka3O service CA and one certificate per identity, each with exactly one URI SAN and
# IP SAN 127.0.0.1, into separate memory volumes. Keys never leave the pod and change on every pod start.
set -euo pipefail
umask 077
root="${PKI_ROOT:-}"
work="$(mktemp -d)"
trap 'rm -rf -- "$work"' EXIT
openssl ecparam -name prime256v1 -genkey -noout -out "$work/ca.key"
openssl req -x509 -new -key "$work/ca.key" -sha256 -days 30 -subj "/CN=Kafka3O test service CA" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" -out "$work/ca.crt" 2>/dev/null
issue() {
  local directory="$1" uri="$2"
  rm -f -- "$directory/tls.key" "$directory/tls.crt" "$directory/ca.crt"
  openssl ecparam -name prime256v1 -genkey -noout -out "$directory/tls.key"
  openssl req -new -key "$directory/tls.key" -subj "/CN=127.0.0.1" -out "$work/request.csr" 2>/dev/null
  printf 'subjectAltName=URI:%s,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=critical,digitalSignature\nbasicConstraints=critical,CA:FALSE\n' \
    "$uri" > "$work/extensions"
  openssl x509 -req -in "$work/request.csr" -CA "$work/ca.crt" -CAkey "$work/ca.key" -CAcreateserial -sha256 -days 30 \
    -extfile "$work/extensions" -out "$directory/tls.crt" 2>/dev/null
  cp "$work/ca.crt" "$directory/ca.crt"
  openssl verify -CAfile "$directory/ca.crt" "$directory/tls.crt" >/dev/null
}
issue "$root"/run/pki-console "spiffe://kafka3o/console"
issue "$root"/run/pki-gateway-a "spiffe://kafka3o/gateway/kafka-a"
issue "$root"/run/pki-gateway-b "spiffe://kafka3o/gateway/kafka-b"
cp "$work/ca.crt" "$root"/run/pki-ca/ca.crt
chmod 0444 "$root"/run/pki-ca/ca.crt
echo "service PKI ready"
