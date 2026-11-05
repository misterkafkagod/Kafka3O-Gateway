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
