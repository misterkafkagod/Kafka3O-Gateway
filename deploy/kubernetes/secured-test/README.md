# Secured EKS-D test deployment (M10)

This replaces the shared-token `kafka3o-manual` pod on `eksd-01` with the
secured stack. The Kafka StatefulSets, their volumes and their Secrets are not
changed. Access stays the same: forward port 5173 and open
`http://localhost:5173` (Chromium treats `localhost` as a secure context, so no
certificate needs to be trusted; see the
[cutover discovery](../../../../Kafka3O-UI/spec/discovery/eksd-https-cutover.md)).

Pod layout (all containers run as uid 1000 with a read-only root filesystem):

| Container | Listens on (pod loopback) | Role |
|---|---|---|
| `front` | `5173` (HTTP, forwarded) | Serves the secured UI; relays `/api` to the Console after checking Host/Origin |
| `console` | `8443` HTTPS, `9443` mTLS policy | Sign-in, sessions, grants, classification, delegation, audit; SQLite on PV `kafka3o-console` |
| `gateway-a`, `gateway-b` | `8081`, `8082` HTTPS (secured mode) | Accept only the Console with a current delegation; no shared token |

Init containers unpack the UI, generate a per-pod service CA and one
certificate per identity (`spiffe://kafka3o/console`,
`spiffe://kafka3o/gateway/kafka-a|kafka-b`) into memory volumes, and bootstrap
the first administrator once from the optional `kafka3o-console-bootstrap`
Secret (keys `secret` and `input`; delete it after the first start). The
Console keeps 7 daily SQLite snapshots in `/var/lib/kafka3o-console/backups`.

```bash
./deploy/kubernetes/secured-test/deploy.sh      # build, deliver, save rollback copy, replace the pod
./deploy/kubernetes/secured-test/rollback.sh    # restore the saved shared-token deployment
```

`deploy.sh` saves the previous deployment once to
`~/kafka3o-rollback/kafka3o-manual-shared-token.json` on the node (server fields
removed). Restore a Console snapshot by scaling the deployment to 0, copying a
snapshot over `/var/lib/kafka3o-console/console.sqlite` (owner uid 1000, mode
0600) and scaling back to 1. Cluster classification is held only by the
Console: `kafka-b` is Production and `kafka-a` non-Production.
