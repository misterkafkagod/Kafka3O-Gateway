# Manual EKS-D test access

The manual-test deployment runs the UI and one Gateway for each persistent
Kafka test cluster on `eksd-01`. The UI, test proxy and Gateway listeners are
bound to pod loopback and are accessible from this PC through SSH forwarding.

Open `http://localhost:5173` in Chrome. Use `localhost`, not `127.0.0.1`.
The local test proxy authenticates Kafka A and Kafka B automatically, so no
token needs to be pasted and no Gateway certificate warning needs to be
accepted. The **Gateway credentials** panel identifies these connections as
automatic. Refresh the page after the manual deployment is updated.

In a cluster, open **Broker configuration** to inspect and change editable
per-broker Kafka settings. Open a topic and choose **Configuration &
partitions** to manage its settings, increase its partition count, or request
replica placement. Each change requires confirmation; partition increases
cannot be reversed, and reassignment moves data. The two test clusters have
one broker each, so they cannot demonstrate replication to multiple brokers.
Restart-required settings such as `auto.create.topics.enable` remain read-only.
These controls do not edit Gateway connection settings or cluster-wide broker
defaults.

The proxy reads the current test tokens from the pod's memory-backed runtime
volumes, keeps them out of the browser and verifies each Gateway's certificate.
Traffic between the PC and the VM is carried by SSH; the browser-to-forwarder
and proxy-to-Gateway connections stay on loopback. The UI build enables this
test-only mode with `VITE_KAFKA3O_MANUAL_PROXY=true` and uses
[`config-auto.json`](./config-auto.json). Normal UI builds still require HTTPS
Gateways and browser-managed tokens.

This convenience is **not user authentication**. Any local process able to
connect to the test UI can exercise its read/write Gateway permissions. The
proxy rejects other Host and Origin values, requires a browser request header,
and does not permit cross-origin preflights; these checks prevent ordinary
unrelated websites from using it but do not protect against local applications.
Use synthetic test data only and do not expose the forwarded port to the LAN.

If the SSH forward stops, restart it in a dedicated PowerShell terminal and
leave that terminal open:

```powershell
.\deploy\kubernetes\manual-test\start-forward.ps1
```

Ports 5173, 8081 and 8082 must be free on both the PC and `eksd-01`. If a
previous forward is stuck, inspect the listeners and stop only processes you
have verified belong to this deployment. The Gateway tokens and HTTPS keys
exist only in pod memory and change when the pod is replaced; reconnect the
forward after a replacement. The restarted proxy reads the new credentials
automatically. Existing Kafka data is kept separately on the persistent broker
volumes.

These are test services, not a production deployment. Kubernetes and host
administrators can inspect process environments and pod memory. For broker
storage and security limitations, see the
[persistent Kafka guide](../test-kafka/README.md).
