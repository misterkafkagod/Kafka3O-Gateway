# Kafka integration tests

By default, the integration tests start a fresh Kafka container per test using
Docker/Testcontainers. Run them with
`go test -tags integration ./test/integration/... ./cmd/gateway ./internal/kafka`.

An alternative is a disposable single-node broker forwarded to a loopback IP.
Set `KAFKA3O_TEST_BROKER` to its numeric loopback address and port and set
`KAFKA3O_TEST_DISPOSABLE=true`. Run **exactly one** integration test per
`go test` process and recreate the broker with empty data before each test.
The tests create topics, produce records, create consumer groups, and delete
topics. Never point them at a shared or production broker.

The fixture checks the acknowledgement, loopback address, expected cluster ID,
single broker and advertised address, and rejects existing non-internal topics
or consumer groups. These checks reduce mistakes; they do not prove that the
broker is disposable. The test which stops Kafka still requires Docker because
the external fixture cannot control the broker's lifecycle.
The compaction regression waits up to 90 seconds for Kafka's log cleaner and
requires the broker to support compacted topics.

## Persistent secured brokers (read-only)

The persistent EKS-D brokers are not disposable. From the repository root, run
only the dedicated metadata, TLS/SCRAM rejection, and invalid validate-only
administration checks against them:

```powershell
.\deploy\kubernetes\test-kafka\run-read-only-checks.ps1 -SshHost "john@192.168.1.109"
```

This runner does not apply configuration, create or delete topics, or produce
records. The validate-only checks verify that existing configuration and topic
placement remain unchanged. Do not point
the disposable integration tests described below at the persistent clusters.
See the [persistent broker guide](../../deploy/kubernetes/test-kafka/README.md)
for prerequisites, connection details and security/storage limitations.

## Secured Kubernetes broker over SSH

`kubernetes/run-secure-kafka-tests.ps1` creates an isolated namespace and a
fresh secured broker for each test, runs the security-mode and authenticated
gateway lifecycle checks, and removes its resources. From PowerShell on Windows
in the gateway repository:

```powershell
.\test\integration\kubernetes\run-secure-kafka-tests.ps1 -SshHost "user@test-host"
```

The test host needs `kubectl`, `ss`, and `timeout`, and the local machine needs
Go and OpenSSH. Local and remote loopback ports 19092, 19095 and 19096 must be
unused. The pod binds all Kafka listeners to its own loopback interface and
generates a synthetic password and short-lived private key in an in-memory
volume; the runner transfers the password over SSH into its process environment
without printing it. Cluster and host administrators can still inspect the
test process and pod, so never use real credentials. The password, key,
configuration and data disappear when the pod is deleted.

The tests cover TLS with PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512, TLS alone,
and SASL alone, plus bad credentials, an untrusted certificate, writes and
message reads. The SASL-only listener exists solely to test the promised
compatibility; PLAIN without TLS exposes credentials and must not be used on
an untrusted network. If the runner is interrupted, inspect its labelled
`kafka3o-sec-*` namespace and owned forwards before manually cleaning them up.
As with the plaintext fixture, the broker identity checks do not prove that a
broker is disposable.

## Kubernetes broker over SSH

The example pod in `kubernetes/kafka-pod.yaml` uses `emptyDir`, no Kubernetes
Service, and the same Kafka version and cluster ID as the Docker fixture. It
advertises `127.0.0.1:19092`, so use the same local port when forwarding it.
The data is lost when the pod is deleted. A namespace does **not** isolate the
plaintext broker from other pods on a cluster without enforced network policy;
use an isolated test cluster and synthetic data only.

For example, from PowerShell in the gateway repository, with SSH access to a
test host whose `kubectl` already works:

```powershell
$hostName = "john@your-test-host"
$ns = "kafka3o-test-" + [guid]::NewGuid().ToString("N").Substring(0, 8)
ssh $hostName "kubectl create namespace $ns"
if ($LASTEXITCODE -ne 0) { throw "Could not create a new test namespace" }
Get-Content -Raw .\test\integration\kubernetes\kafka-pod.yaml |
  ssh $hostName "kubectl create -n $ns -f -"
if ($LASTEXITCODE -ne 0) { throw "Could not create the Kafka test pod" }
ssh $hostName "kubectl -n $ns wait --for=condition=Ready pod/kafka3o-integration --timeout=180s"
if ($LASTEXITCODE -ne 0) { throw "Kafka test pod did not become ready" }
```

In another terminal, use the same host and namespace values and keep this
command running while testing. The remote timeout prevents an abandoned
port-forward from running indefinitely if SSH disconnects:

```powershell
ssh -tt -o BatchMode=yes -o StrictHostKeyChecking=yes -o ExitOnForwardFailure=yes `
  -L 127.0.0.1:19092:127.0.0.1:19092 $hostName `
  "timeout --signal=TERM --kill-after=5s 300s kubectl -n $ns port-forward pod/kafka3o-integration 19092:9092 --address 127.0.0.1"
```

Run one selected test:

```powershell
$env:KAFKA3O_TEST_BROKER = "127.0.0.1:19092"
$env:KAFKA3O_TEST_DISPOSABLE = "true"
go test -count=1 -tags integration .\test\integration -run '^TestCreateTopic_CreatesAndDetectsConflict$'
```

Stop the forward, delete and recreate the pod, and restart the forward before
each additional test. Confirm the remote loopback port is no longer listening
before restarting: an interrupted SSH connection can leave `kubectl` running
until the timeout. If needed, stop only a process whose PID and complete
command you have verified belongs to this test.

```powershell
ssh $hostName "ss -ltnp 'sport = :19092'"
ssh $hostName "kubectl -n $ns delete pod kafka3o-integration --wait=true"
Get-Content -Raw .\test\integration\kubernetes\kafka-pod.yaml |
  ssh $hostName "kubectl create -n $ns -f -"
ssh $hostName "kubectl -n $ns wait --for=condition=Ready pod/kafka3o-integration --timeout=180s"
```

When finished, stop the forward, delete only the namespace created for this
run, and remove both environment variables:

```powershell
ssh $hostName "kubectl delete namespace $ns --wait=true"
Remove-Item Env:\KAFKA3O_TEST_BROKER, Env:\KAFKA3O_TEST_DISPOSABLE
```
