#!/bin/bash
# Restores the shared-token manual deployment saved by deploy.sh. The Console volume and its data are kept.
set -euo pipefail
host="${1:-john@192.168.1.109}"
ssh -o BatchMode=yes "$host" "set -e
  test -s ~/kafka3o-rollback/kafka3o-manual-shared-token.json
  kubectl -n kafka3o-test-clusters replace -f ~/kafka3o-rollback/kafka3o-manual-shared-token.json
  kubectl -n kafka3o-test-clusters rollout status deploy/kafka3o-manual --timeout=300s"
echo "shared-token deployment restored"
