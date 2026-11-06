#!/usr/bin/env bash
set -euo pipefail
umask 077

if [[ $# -ne 1 || ! "$1" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
  echo "Usage: create-test-secrets.sh <namespace>" >&2
  exit 2
fi
namespace="$1"

for broker in a b; do
  existing="$(kubectl -n "$namespace" get secret "kafka-$broker-security" --ignore-not-found -o name)"
  if [[ -n "$existing" ]]; then
    echo "Secret kafka-$broker-security already exists; refusing to replace it." >&2
    exit 1
  fi
done

work="$(mktemp -d /dev/shm/kafka3o-test-secrets.XXXXXXXX)"
cleanup() {
  for broker in a b; do
    directory="$work/$broker"
    if [[ -d "$directory" ]]; then
      rm -f -- "$directory/tls.crt" "$directory/tls.key" "$directory/sasl.password" \
        "$directory/sasl.salt" "$directory/sasl.saltedpassword"
      rmdir -- "$directory"
    fi
  done
  rmdir -- "$work"
}
trap cleanup EXIT

for broker in a b; do
  directory="$work/$broker"
  mkdir -- "$directory"
  hostname="kafka-$broker.$namespace.svc.cluster.local"
  san="DNS:$hostname,DNS:kafka-$broker.$namespace.svc,DNS:kafka-$broker.$namespace"
  san="$san,DNS:kafka-$broker,DNS:kafka-$broker-0.kafka-$broker.$namespace.svc.cluster.local"
  san="$san,DNS:localhost,IP:127.0.0.1"
  if ! openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 365 \
      -keyout "$directory/tls.key" -out "$directory/tls.crt" -subj "/CN=$hostname" \
      -addext "basicConstraints=critical,CA:TRUE" \
      -addext "keyUsage=critical,digitalSignature,keyEncipherment,keyCertSign" \
      -addext "extendedKeyUsage=serverAuth" -addext "subjectAltName=$san" >/dev/null 2>&1; then
    echo "Could not generate the test broker certificate." >&2
    exit 1
  fi
  python3 - "$directory" <<'PY'
import base64
import hashlib
import secrets
import sys
from pathlib import Path

directory = Path(sys.argv[1])
password = secrets.token_hex(32).encode("ascii")
salt = secrets.token_bytes(32)
salted = hashlib.pbkdf2_hmac("sha512", password, salt, 8192, dklen=64)
for name, value in {
    "sasl.password": password,
    "sasl.salt": base64.b64encode(salt),
    "sasl.saltedpassword": base64.b64encode(salted),
}.items():
    (directory / name).write_bytes(value)
PY
  kubectl -n "$namespace" create secret generic "kafka-$broker-security" \
    --from-file=tls.crt="$directory/tls.crt" \
    --from-file=tls.key="$directory/tls.key" \
    --from-file=sasl.password="$directory/sasl.password" \
    --from-file=sasl.salt="$directory/sasl.salt" \
    --from-file=sasl.saltedpassword="$directory/sasl.saltedpassword"
done
