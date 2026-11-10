#!/bin/bash
# Builds the secured M10 stack and replaces the kafka3o-manual pod on eksd-01. Run from the Gateway checkout with
# the UI checkout beside it. Kafka StatefulSets, volumes and Secrets are never modified. Usage: deploy.sh [ssh-host]
set -euo pipefail
host="${1:-john@192.168.1.109}"
here="$(cd "$(dirname "$0")" && pwd)"
gateway_repo="$(cd "$here/../../.." && pwd)"
ui_repo="${KAFKA3O_UI_DIR:-$(cd "$gateway_repo/../Kafka3O-UI" && pwd)}"
build="$(mktemp -d)"
trap 'rm -rf -- "$build"' EXIT

echo "building linux/amd64 binaries"
( cd "$gateway_repo" && for target in gateway:./cmd/gateway console:./cmd/console front:./deploy/kubernetes/secured-test/front; do
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$build/${target%%:*}" "${target#*:}"
  done )
echo "building the secured UI"
( cd "$ui_repo" && VITE_KAFKA3O_CONSOLE_SECURED=true npm run build >/dev/null && tar -czf "$build/site.tar.gz" -C dist . && npm run build >/dev/null )
hash="$(cat "$build/gateway" "$build/console" "$build/front" | shasum -a 256 | cut -c1-12)"
bin_dir="/var/lib/kafka3o-secured-$hash"

echo "delivering binaries to $host:$bin_dir"
for name in gateway console front; do
  scp -q -o BatchMode=yes "$build/$name" "$host:/tmp/kafka3o-$hash-$name"
done
ssh -o BatchMode=yes "$host" "set -e
  sudo install -d -m 0755 -o root -g root '$bin_dir'
  for name in gateway console front; do sudo install -m 0555 -o root -g root /tmp/kafka3o-$hash-\$name '$bin_dir/'\$name; rm -f /tmp/kafka3o-$hash-\$name; done
  sudo install -d -m 2770 -o 1000 -g 1000 /var/lib/kafka3o-console
  sudo chmod 2770 /var/lib/kafka3o-console
  sudo find /var/lib/kafka3o-console -maxdepth 1 -type f -name \"console.sqlite*\" -exec chmod 0600 {} +"

echo "exporting the rollback copy"
current="$(ssh -o BatchMode=yes "$host" "kubectl -n kafka3o-test-clusters get deploy kafka3o-manual -o json")"
if [ "$(printf '%s' "$current" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"].get("labels",{}).get("kafka3o.dev/mode",""))')" != secured ]; then
  # Server-managed fields are removed so the copy can be re-applied later.
  printf '%s' "$current" | python3 -c '
import json, sys
d = json.load(sys.stdin)
for key in ("resourceVersion", "uid", "creationTimestamp", "generation", "managedFields"):
    d["metadata"].pop(key, None)
d["metadata"].get("annotations", {}).pop("deployment.kubernetes.io/revision", None)
d.pop("status", None)
print(json.dumps(d))' | ssh -o BatchMode=yes "$host" "mkdir -p ~/kafka3o-rollback && umask 077 && cat > ~/kafka3o-rollback/kafka3o-manual-shared-token.json"
  echo "saved ~/kafka3o-rollback/kafka3o-manual-shared-token.json on $host"
else
  echo "already secured; keeping the existing rollback copy"
fi

echo "applying storage, configuration and the secured deployment"
scp -q -o BatchMode=yes "$here/storage.yaml" "$here/clusters.json" "$here/prepare-service-pki.sh" "$build/site.tar.gz" "$host:/tmp/"
ui_hash="$(shasum -a 256 "$build/site.tar.gz" | cut -c1-12)"
sed -e "s#__BIN_DIR__#$bin_dir#g" -e "s#__UI_HASH__#$ui_hash#g" "$here/deployment.yaml" | ssh -o BatchMode=yes "$host" "cat > /tmp/kafka3o-secured-deployment.yaml"
ssh -o BatchMode=yes "$host" "set -e
  cd /tmp
  kubectl apply -f storage.yaml
  # M15 Prometheus token for gateway-a, generated on the node once and never printed.
  if ! kubectl -n kafka3o-test-clusters get secret kafka3o-metrics-token >/dev/null 2>&1; then
    token_dir=\$(mktemp -d); umask 077
    head -c 32 /dev/urandom | base64 | tr -d '=+/\n' > \$token_dir/token
    kubectl -n kafka3o-test-clusters create secret generic kafka3o-metrics-token --from-file=token=\$token_dir/token
    rm -rf \$token_dir
  fi
  kubectl -n kafka3o-test-clusters create configmap kafka3o-secured-config --from-file=clusters.json --from-file=prepare-service-pki.sh --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n kafka3o-test-clusters create configmap kafka3o-secured-ui --from-file=site.tar.gz --dry-run=client -o yaml | kubectl apply -f -
  kubectl replace -f kafka3o-secured-deployment.yaml
  rm -f storage.yaml clusters.json prepare-service-pki.sh site.tar.gz kafka3o-secured-deployment.yaml
  kubectl -n kafka3o-test-clusters rollout status deploy/kafka3o-manual --timeout=300s"
echo "secured deployment applied ($bin_dir)"
