# kafka3o-gateway Helm chart

Deploys the Kafka Application Gateway: a Deployment with exec health probes, a ClusterIP Service, a ConfigMap holding `config.yaml`, a Secret with API keys and SASL credentials, a NetworkPolicy, a PodDisruptionBudget, and a HorizontalPodAutoscaler.

**Requires Kubernetes 1.30 or later.** The `preStop` hook uses the native `sleep` action, because the distroless image has no shell or `sleep` binary.

## Install

```sh
helm install kgw oci://ghcr.io/misterkafkagod/charts/kafka3o-gateway \
  --set 'kafka.bootstrap={broker-1:9092,broker-2:9092}' \
  --set-json 'auth.apiKeys=[{"id":"ops","tier":"operator","sha256":"<digest>"},{"id":"probe","tier":"reader","sha256":"<digest>"}]' \
  --set auth.probeApiKey='<raw reader key>'
```

Create keys with `kafka3o-gateway keygen --tier reader|operator`. Put only the `sha256` digests in `auth.apiKeys`. The probes need one **raw** reader key in `auth.probeApiKey`, and its digest must also be in `auth.apiKeys`. Every endpoint, `/health/*` included, requires a key.

Check a running release with `helm test kgw`: it calls `/v1/health/ready` through the Service with the probe key.

## The grace-period rule

`terminationGracePeriodSeconds` must be at least `ceil(bounds.maxTimeMs / 1000) + 10`, so the longest permitted scan can finish draining before Kubernetes kills the pod. `values.schema.json` enforces this, and `helm install` / `helm template` fail otherwise. For example, `bounds.maxTimeMs=60000` needs `terminationGracePeriodSeconds` of 70 or more.

JSON Schema can't do arithmetic, so the schema writes the rule out as one `if`/`then` clause per whole second of `maxTimeMs` (1–300 s). If you raise the `maxTimeMs` maximum, add the matching clauses.

## Values

| Key | Default | Description |
|---|---|---|
| `replicaCount` | `2` | Replicas when `autoscaling.enabled` is false. |
| `image.repository` | `ghcr.io/misterkafkagod/kafka3o-gateway` | Image repository. |
| `image.tag` | `""` | Image tag; defaults to the chart `appVersion`. |
| `image.digest` | `""` | `sha256:…` digest. When set, the image is pulled by digest and `tag` is ignored. Recommended in production. |
| `image.pullPolicy` | `IfNotPresent` | |
| `imagePullSecrets` | `[]` | |
| `nameOverride` / `fullnameOverride` | `""` | Override the generated names. |
| `serviceAccount.create` | `true` | Create a ServiceAccount (token automount is always off). |
| `serviceAccount.name` | `""` | Name to create or use; defaults to the full name. |
| `podAnnotations` / `podLabels` | `{}` | Extra pod metadata. |
| `nodeSelector` / `tolerations` / `affinity` | `{}` / `[]` / `{}` | Standard scheduling controls. |
| `resources` | requests `100m`/`128Mi`, limits `1`/`512Mi` | Container resources. |
| `terminationGracePeriodSeconds` | `30` | Pod grace period. See the grace-period rule above. |
| `preStopSleepSeconds` | `5` | Pause before shutdown starts, so the pod leaves Service endpoints before draining. |
| `service.type` | `ClusterIP` | |
| `service.port` | `8080` | Fixed at 8080: the exec probes always call `127.0.0.1:8080`. |
| `kafka.bootstrap` | `[]` | **Required.** Broker addresses. |
| `kafka.requestTimeout` | `30s` | Admin and produce timeout. |
| `kafka.tls.enabled` | `false` | TLS to the brokers. |
| `kafka.tls.secretName` | `""` | Secret with `ca.crt` (plus `tls.crt`/`tls.key` for mutual TLS), mounted at `/etc/kafka3o/tls`. Required when TLS is on. |
| `kafka.tls.mutual` | `false` | Present the client certificate from the same Secret. |
| `kafka.sasl.mechanism` | `""` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, or empty for none. |
| `kafka.sasl.username` / `kafka.sasl.password` | `""` | Stored in the Secret; used only when a mechanism is set. |
| `bounds.maxTimeMs` | `10000` | Scan `max_time` ceiling, in ms (1–300000). The scan default is the lower of this and 10 s. |
| `policy.readOnlyMode` | `false` | Block every write command. |
| `policy.dataPlaneLock` | `false` | Block message commands M1–M8; operators can bypass per request with `X-Break-Glass-Reason`. |
| `policy.disabledOperations` | `[]` | Command ids to switch off, e.g. `[T11, T12]`. |
| `cors.origins` | `[]` | Browser origins allowed. |
| `trustedProxies` | `[]` | CIDRs whose `X-Forwarded-For` is trusted. |
| `otlp.endpoint` | `""` | OTLP collector, e.g. `otel-collector.observability:4317`. Empty disables export. |
| `otlp.protocol` | `grpc` | `grpc` or `http`. |
| `audit.sink` | `stdout` | `stdout`, or `kafka` (stdout plus a topic, fail-closed). |
| `audit.topic` | `""` | Required for the `kafka` sink. Never created by the gateway. |
| `logLevel` | `info` | `debug`, `info`, `warn`, or `error`. |
| `auth.enabled` | `true` | `false` makes every caller an operator. Trusted networks only. |
| `auth.apiKeys` | `[]` | `{ id, tier, sha256 }` entries, rendered into `KGW_API_KEYS`. |
| `auth.probeApiKey` | `""` | Raw reader key the exec probes send, as `KGW_PROBE_API_KEY`. |
| `auth.existingSecret` | `""` | Use your own Secret instead. It must hold `KGW_API_KEYS`, `KGW_PROBE_API_KEY`, and (with SASL) `KGW_KAFKA_SASL_USERNAME` / `KGW_KAFKA_SASL_PASSWORD`. |
| `extraConfig` | `{}` | Merged over the rendered `config.yaml`, using the gateway's own key names (`configs/config.example.yaml`). An unknown key fails start-up. |
| `networkPolicy.enabled` | `true` | Render the NetworkPolicy. DNS egress (UDP/TCP 53) is always allowed. |
| `networkPolicy.brokers.cidrs` / `.ports` | `[]` / `[9092]` | Broker egress. **With no CIDRs the gateway can't reach Kafka**; set them. |
| `networkPolicy.otlp.cidrs` / `.ports` | `[]` / `[4317]` | Collector egress. |
| `networkPolicy.ingressNamespaceSelector` | `{matchLabels: {}}` | Namespaces allowed to call the gateway. The empty default allows every namespace; narrow it. |
| `podDisruptionBudget.enabled` / `.minAvailable` | `true` / `1` | |
| `autoscaling.enabled` | `true` | CPU-based HPA. The gateway is stateless, so scaling out is safe. |
| `autoscaling.minReplicas` / `.maxReplicas` | `2` / `5` | |
| `autoscaling.targetCPUUtilizationPercentage` | `70` | |
| `tests.image` | `curlimages/curl:8.16.0` | Image for `helm test`. The gateway image has no HTTP client. |

## What the chart sets that you can't change

- Pod and container security: `runAsNonRoot`, UID/GID 65532, `readOnlyRootFilesystem`, `allowPrivilegeEscalation: false`, all capabilities dropped, seccomp `RuntimeDefault`. `/tmp` is an `emptyDir`.
- Exec liveness and readiness probes: `/kafka3o-gateway probe --live` / `--ready`.
- A checksum of the config and Secret on the pod template, so changing either rolls the pods. The gateway reads configuration only at start-up.
