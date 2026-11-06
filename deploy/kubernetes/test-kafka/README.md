# Persistent Kafka test clusters on EKS-D

These manifests run two **independent, single-broker test clusters** on
`eksd-01`. They use persistent local volumes rather than temporary Kafka pods.
They are for synthetic test data only, not production or high availability.

| Broker | Kubernetes bootstrap address | SASL user | SSH-forwarded address |
| --- | --- | --- | --- |
| `kafka-a` | `kafka-a.kafka3o-test-clusters.svc.cluster.local:9092` | `kafka3otest-a` | `127.0.0.1:19092` |
| `kafka-b` | `kafka-b.kafka3o-test-clusters.svc.cluster.local:9092` | `kafka3otest-b` | `127.0.0.1:19093` |

Both require TLS and SCRAM-SHA-512. Each has a separate self-signed certificate,
test password, KRaft cluster ID and data directory. Topic auto-creation is off.
The advertised addresses depend on the listener: use the Kubernetes address
from a pod, or the exact loopback address above through an SSH forward. The
Services are cluster-internal and do not create a NodePort or load balancer.

## Security and durability limits

- The generated test credentials are in Kubernetes Secrets
  `kafka-a-security` and `kafka-b-security`. **Secrets are not encrypted at
  rest in this cluster's etcd.** Cluster and host administrators can inspect
  them, process environments and mounted files. Do not use real credentials.
- The client listeners require TLS and SASL. The broker and controller
  listeners are plaintext but bound to pod loopback. A namespace is not a
  firewall, and this EKS-D cluster's Flannel does not enforce NetworkPolicy.
- Certificates last 365 days. There is no automated renewal or credential
  rotation. Do not replace a Secret or reformat storage casually: the SCRAM
  verifier also lives in retained KRaft metadata, so replacing only the Secret
  will not rotate an initialized broker's credential.
- Each local PersistentVolume advertises 2 GiB and uses the `Retain` reclaim
  policy, but **2 GiB is not a filesystem quota**. Kafka's default retention
  targets are seven days and approximately 256 MiB per partition, with 64 MiB
  segments and periodic, segment-based deletion. Data can outlast either
  threshold, and topic settings can override them. These settings do not cap
  total disk use; monitor the shared node filesystem and its backing Proxmox
  thin pool. Free space reported inside the VM is not a guarantee of
  hypervisor storage headroom.
- Both volumes are on the same node, without replication or configured
  backups. Node or disk loss can lose both clusters. Pod replacement has been
  tested; a VM reboot has not. After a reboot, wait for the API server's
  `/readyz` before checking the workloads.

Check health and certificate expiry without displaying credentials:

```powershell
ssh john@192.168.1.109 "kubectl get --raw /readyz"
ssh john@192.168.1.109 "kubectl -n kafka3o-test-clusters get statefulsets,pods,pvc"
ssh john@192.168.1.109 "kubectl get pv kafka3o-test-a kafka3o-test-b"
ssh john@192.168.1.109 "kubectl -n kafka3o-test-clusters exec kafka-a-0 -c kafka -- openssl x509 -in /run/kafka3o-security/tls.crt -noout -enddate"
ssh john@192.168.1.109 "kubectl -n kafka3o-test-clusters exec kafka-b-0 -c kafka -- openssl x509 -in /run/kafka3o-security/tls.crt -noout -enddate"
ssh john@192.168.1.109 "df -h /var/lib/kafka3o-test-clusters"
```

## Read-only security checks

From PowerShell in the repository root, with Go and OpenSSH installed:

```powershell
.\deploy\kubernetes\test-kafka\run-read-only-checks.ps1 -SshHost "john@192.168.1.109"
```

The host needs working `kubectl`, `ss`, and `timeout`; SSH host-key verification
must already be set up. Ports 19092 and 19093 must be unused on both machines.
The runner forwards each pod through loopback only, checks authenticated
metadata, and verifies that wrong credentials and an untrusted CA are rejected.
It also sends Kafka **validate-only** requests containing an invalid broker
value and an unchanged topic partition count, and verifies the settings and
placement did not change. It does not apply configuration, create or delete
topics, or produce records. It transfers each test
password into its process environment without printing it, keeps public
certificates and SSH-forward output in temporary local files, and closes its
forwards afterward. Remote forwards also have a five-minute timeout. If
interrupted, inspect the ports and stop only processes you have verified belong
to this runner.

**Do not run the disposable integration tests against these brokers.** Those
tests create and delete topics and consumer groups and require a fresh broker
for every test. Use the runner above for the persistent instances; see the
[integration test guide](../../../test/integration/README.md) for disposable
fixtures.

For the separately running UI and two Gateways, see the
[manual EKS-D test guide](../manual-test/README.md).

## Connect a local gateway through SSH

Open a dedicated PowerShell terminal for the forward. Choose `a` or `b`; both
the local and remote loopback ports must be free. Keep the command running while
the gateway is in use:

```powershell
$sshHost = "john@192.168.1.109"
$broker = "a" # or "b"
$port = if ($broker -eq "a") { 19092 } elseif ($broker -eq "b") { 19093 } else { throw "Choose a or b" }
if (Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue) {
    throw "Local port $port is already in use"
}
$listeners = @(ssh -o BatchMode=yes -o StrictHostKeyChecking=yes $sshHost "ss -H -ltn 'sport = :$port'")
if ($LASTEXITCODE -ne 0 -or $listeners.Count -ne 0) { throw "Remote port unavailable" }
$remote = "timeout --signal=TERM --kill-after=5s 3600s kubectl -n kafka3o-test-clusters port-forward pod/kafka-$broker-0 $($port):9095 --address 127.0.0.1"
ssh -tt -o BatchMode=yes -o StrictHostKeyChecking=yes -o ExitOnForwardFailure=yes `
  -L ("127.0.0.1:{0}:127.0.0.1:{0}" -f $port) $sshHost $remote
```

In a second, dedicated PowerShell terminal, configure the corresponding
gateway's Kafka connection. The certificate is public; the password stays in
the process environment. Do not print it or save it in YAML, command history,
or logs. The following retrieves the generated **test-only** credential:

```powershell
$sshHost = "john@192.168.1.109"
$broker = "a" # use the same choice as the forward
$port = if ($broker -eq "a") { 19092 } elseif ($broker -eq "b") { 19093 } else { throw "Choose a or b" }
$ca = @(ssh -o BatchMode=yes -o StrictHostKeyChecking=yes $sshHost "kubectl -n kafka3o-test-clusters exec kafka-$broker-0 -c kafka -- cat /run/kafka3o-security/tls.crt")
if ($LASTEXITCODE -ne 0 -or $ca.Count -eq 0) { throw "Could not retrieve the public CA" }
$caPath = Join-Path ([IO.Path]::GetTempPath()) "kafka3o-$broker-$([guid]::NewGuid().ToString('N')).pem"
[IO.File]::WriteAllText($caPath, ($ca -join "`n") + "`n")
$password = ssh -o BatchMode=yes -o StrictHostKeyChecking=yes $sshHost "kubectl -n kafka3o-test-clusters exec kafka-$broker-0 -c kafka -- cat /run/kafka3o-security/sasl.password"
if ($LASTEXITCODE -ne 0 -or $password -isnot [string] -or $password -cnotmatch '^[0-9a-f]{64}$') {
    Remove-Item -LiteralPath $caPath
    Remove-Variable password -ErrorAction SilentlyContinue
    throw "Could not retrieve the test credential"
}
$env:KAFKA3O_KAFKA__BROKERS = "127.0.0.1:$port"
$env:KAFKA3O_KAFKA__TLS__ENABLED = "true"
$env:KAFKA3O_KAFKA__TLS__CAFILE = $caPath
$env:KAFKA3O_KAFKA__SASL__MECHANISM = "SCRAM-SHA-512"
$env:KAFKA3O_KAFKA__SASL__USERNAME = "kafka3otest-$broker"
Remove-Item Env:\KAFKA3O_KAFKA__SASL__PASSWORD -ErrorAction SilentlyContinue
$env:KAFKA3O_KAFKA_SASL_PASSWORD = $password
Remove-Variable password
```

Also configure the gateway's separate HTTPS certificate, private key and
bearer token as described in the [deployment guide](../../README.md), then run
`go run .\cmd\gateway` in that same terminal. Use a separate gateway process
and forward for each cluster. The forward ends after one hour or when its pod
is replaced; restart it as needed. When finished, stop the gateway and the
forward, remove the temporary public CA with
`Remove-Item -LiteralPath $caPath`, and close the dedicated terminal to discard
its environment.

For a gateway inside Kubernetes, use the corresponding internal address and
username in the table instead. Supply that broker's public `tls.crt` as the CA
file and its `sasl.password` through the process environment using the
deployment platform's Secret facilities. Mount only the needed keys; the
broker's private key is not a client credential.

## Initial provisioning on a fresh node

The manifests are pinned to node name `eksd-01`, namespace
`kafka3o-test-clusters`, and the local paths below. Before applying network
resources, follow the host's automatic-rollback and fresh-SSH-login procedure.
Do not run the initial setup against the existing brokers, replace their
Secrets, or delete their PVCs to restart them.

On a fresh node with the deployment files available locally, create **new,
empty** directories owned by UID/GID 1000 at
`/var/lib/kafka3o-test-clusters/kafka-a` and
`/var/lib/kafka3o-test-clusters/kafka-b`. Stop and investigate if either path
already exists. Then, from this directory on a Linux host with `kubectl`,
OpenSSL, Python 3 and writable `/dev/shm`:

```bash
kubectl apply -f namespace.yaml
bash ./create-test-secrets.sh kafka3o-test-clusters
kubectl apply -f storage.yaml -f services.yaml -f workloads.yaml
kubectl -n kafka3o-test-clusters rollout status statefulset/kafka-a --timeout=300s
kubectl -n kafka3o-test-clusters rollout status statefulset/kafka-b --timeout=300s
```

The Secret generator refuses to replace either existing Secret. If initial
creation is partial, stop and inspect before continuing. Preserve the script's
LF line endings when transferring it from Windows. Storage is formatted only
when its directory is empty and `meta.properties` is absent; a nonempty
unformatted directory fails instead of being overwritten. Deleting a namespace
can delete its Secrets and claims even though the underlying PVs are retained.
