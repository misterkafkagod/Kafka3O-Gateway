# Operations

This guide is for whoever deploys and runs the Kafka3O Gateway. It covers configuration, the CLI, health probes, shutdown, the audit topic, the data-plane break-glass procedure, and known limits.

## Supported brokers

The gateway supports **Apache Kafka 3.3 through 4.x**. Older clusters are best-effort. When a command needs a feature the cluster doesn't have (for example C6 KRaft quorum status on a ZooKeeper-mode cluster), the gateway returns `502 KAFKA_ERROR` with `kafkaError.name = UNSUPPORTED_VERSION` rather than a partial answer. Every other command keeps working.

## Configuration

Configuration comes from one YAML file plus environment variables. The file path is `--config <path>` (or `--config=<path>`), falling back to `KGW_CONFIG`. `configs/config.example.yaml` lists every key with its default.

- Every key can be set by an environment variable: `KGW_` + the key path upper-cased, with `.` replaced by `_` (`kafka.bootstrap` → `KGW_KAFKA_BOOTSTRAP`). A few keys use a shorter name; the table below gives the exact variable for each.
- The environment always wins over the file. Lists are comma-separated in the environment; `auth.keys` is a JSON document.
- Everything is read once at start-up. Changing any value needs a restart.
- An unknown key, in the file or in the environment, fails start-up.
- Secrets (the SASL password, TLS private keys) belong in the environment or in mounted files, never in a committed YAML file. They are redacted from logs.
- `KGW_CONFIG` and `KGW_PROBE_API_KEY` are reserved: they aren't configuration keys, and setting them never trips the unknown-key check.

### Kafka connection

| Key | Environment | Default | Notes |
|---|---|---|---|
| `kafka.bootstrap` | `KGW_KAFKA_BOOTSTRAP` | `[]` | **Required.** Broker addresses, e.g. `broker-1:9092,broker-2:9092`. |
| `kafka.request_timeout` | `KGW_KAFKA_REQUEST_TIMEOUT` | `30s` | Admin and produce timeout. Expiry returns `504 KAFKA_TIMEOUT`. |
| `kafka.tls.enabled` | `KGW_KAFKA_TLS_ENABLED` | `false` | TLS to the brokers. |
| `kafka.tls.ca_file` | `KGW_KAFKA_TLS_CA_FILE` | `""` | CA bundle for the broker certificates. |
| `kafka.tls.cert_file` | `KGW_KAFKA_TLS_CERT_FILE` | `""` | Client certificate for mutual TLS; set together with `key_file`. |
| `kafka.tls.key_file` | `KGW_KAFKA_TLS_KEY_FILE` | `""` | Client private key. Mount as a secret. Redacted. |
| `kafka.sasl.mechanism` | `KGW_KAFKA_SASL_MECHANISM` | `""` | `""` (none), `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`. |
| `kafka.sasl.username` | `KGW_KAFKA_SASL_USERNAME` | `""` | |
| `kafka.sasl.password` | `KGW_KAFKA_SASL_PASSWORD` | `""` | Environment only. Redacted. |
| `kafka.producer.acks` | `KGW_KAFKA_PRODUCER_ACKS` | `all` | `all`, `1`, or `0`. |
| `kafka.producer.idempotent` | `KGW_KAFKA_PRODUCER_IDEMPOTENT` | `true` | |
| `kafka.consumer.isolation_level` | `KGW_KAFKA_ISOLATION_LEVEL` | `read_committed` | `read_committed` or `read_uncommitted`. |

### HTTP listener

| Key | Environment | Default | Notes |
|---|---|---|---|
| `http.addr` | `KGW_HTTP_ADDR` | `:8080` | The exec probes always call `127.0.0.1:8080`. Keep this port in a container, or the probes will fail. |
| `http.tls.cert_file` | `KGW_HTTP_TLS_CERT` (alias `KGW_HTTP_TLS_CERT_FILE`) | `""` | Optional native TLS; set together with `key_file`. |
| `http.tls.key_file` | `KGW_HTTP_TLS_KEY` (alias `KGW_HTTP_TLS_KEY_FILE`) | `""` | Redacted. |
| `http.read_header_timeout` | `KGW_HTTP_READ_HEADER_TIMEOUT` | `10s` | |
| `http.idle_timeout` | `KGW_HTTP_IDLE_TIMEOUT` | `60s` | |
| `http.write_timeout` | `KGW_HTTP_WRITE_TIMEOUT` | `0s` | `0` means derived: `bounds.scan.max_time.ceiling + 5s` (65s by default). This value is also the shutdown grace period. |
| `http.body_limit` | `KGW_HTTP_BODY_LIMIT` | `1MiB` | JSON request bodies. Bulk produce (M6) uses `bounds.bulk_produce_body` instead. |
| `http.trusted_proxies` | `KGW_HTTP_TRUSTED_PROXIES` | `[]` | CIDRs whose `X-Forwarded-For` is trusted for the audited client IP. |
| `http.cors.origins` | `KGW_CORS_ORIGINS` | `[]` | Browser origins allowed. `*` is rejected while auth is enabled. |
| `http.docs.enabled` | `KGW_DOCS_ENABLED` | `true` | Serves the `/docs` UI. It still requires an API key. |

### Authentication

| Key | Environment | Default | Notes |
|---|---|---|---|
| `auth.enabled` | `KGW_AUTH_ENABLED` | `true` | `false` treats every caller as an operator. Use only on a fully trusted network. |
| `auth.keys` | `KGW_API_KEYS` | `[]` | JSON list of `{ "id", "tier", "sha256" }`, where `tier` is `reader` or `operator`. At least one key is required while auth is enabled. Generate entries with `kafka3o-gateway keygen`. |

The gateway stores only the SHA-256 digest of each key, never the key itself. No endpoint is exempt from authentication, `/health/*` and `/docs` included.

### Policy switches

| Key | Environment | Default | Notes |
|---|---|---|---|
| `policy.read_only_mode` | `KGW_POLICY_READ_ONLY_MODE` | `false` | Blocks every write command with `403 READ_ONLY_MODE`. Break-glass does not bypass it. |
| `policy.data_plane_lock` | `KGW_POLICY_DATA_PLANE_LOCK` | `false` | Blocks the message commands M1–M8 with `403 DATA_PLANE_LOCKED`. Operators can bypass it per request; see [Break-glass](#break-glass-data-plane-lock-bypass). |
| `policy.disabled_operations` | `KGW_POLICY_DISABLED_OPERATIONS` | `[]` | Command ids to switch off, e.g. `T11,T12`. Blocked with `403 OPERATION_DISABLED`. Break-glass does not bypass it. |

### Bounds

A caller who omits a bound gets the default, and nobody can exceed the ceiling. Asking for more than the ceiling returns `400`.

| Key | Environment | Default | Applies to |
|---|---|---|---|
| `bounds.read.limit.default` | `KGW_BOUNDS_READ_LIMIT_DEFAULT` | `100` | M1 records returned |
| `bounds.read.limit.ceiling` | `KGW_BOUNDS_READ_LIMIT_CEILING` | `1000` | |
| `bounds.scan.max_scan.default` | `KGW_BOUNDS_SCAN_MAX_SCAN_DEFAULT` | `10000` | M3/M4 records evaluated |
| `bounds.scan.max_scan.ceiling` | `KGW_BOUNDS_SCAN_MAX_SCAN_CEILING` | `100000` | |
| `bounds.scan.max_matches.default` | `KGW_BOUNDS_SCAN_MAX_MATCHES_DEFAULT` | `100` | M3/M4 matches returned |
| `bounds.scan.max_matches.ceiling` | `KGW_BOUNDS_SCAN_MAX_MATCHES_CEILING` | `1000` | |
| `bounds.scan.max_bytes.default` | `KGW_BOUNDS_SCAN_MAX_BYTES_DEFAULT` | `10MiB` | Bytes read per scan |
| `bounds.scan.max_bytes.ceiling` | `KGW_BOUNDS_SCAN_MAX_BYTES_CEILING` | `100MiB` | |
| `bounds.scan.max_time.default` | `KGW_BOUNDS_SCAN_MAX_TIME_DEFAULT` | `10s` | Wall-clock time per scan |
| `bounds.scan.max_time.ceiling` | `KGW_BOUNDS_SCAN_MAX_TIME_CEILING` | `60s` | Also sets the derived `http.write_timeout` |
| `bounds.scan.regex_timeout` | `KGW_BOUNDS_SCAN_REGEX_TIMEOUT` | `100ms` | Per-record regex guard |
| `bounds.replay.limit.default` | `KGW_BOUNDS_REPLAY_LIMIT_DEFAULT` | `1000` | M8 records copied per call |
| `bounds.replay.limit.ceiling` | `KGW_BOUNDS_REPLAY_LIMIT_CEILING` | `10000` | |
| `bounds.bulk_produce_body` | `KGW_BOUNDS_BULK_PRODUCE_BODY` | `10MiB` | M6 request body |
| `bounds.throughput.seconds.default` | `KGW_BOUNDS_THROUGHPUT_SECONDS_DEFAULT` | `5` | C10 sample window |
| `bounds.throughput.seconds.ceiling` | `KGW_BOUNDS_THROUGHPUT_SECONDS_CEILING` | `60` | |
| `bounds.page.size.default` | `KGW_BOUNDS_PAGE_SIZE_DEFAULT` | `50` | Every list endpoint |
| `bounds.page.size.ceiling` | `KGW_BOUNDS_PAGE_SIZE_CEILING` | `500` | |

### Audit

| Key | Environment | Default | Notes |
|---|---|---|---|
| `audit.sink` | `KGW_AUDIT_SINK` | `stdout` | `stdout` (always on) or `kafka` (stdout plus a topic, fail-closed). |
| `audit.topic` | `KGW_AUDIT_TOPIC` | `""` | Required when the sink is `kafka`. Never created by the gateway; see [Audit topic provisioning](#audit-topic-provisioning). |

### Telemetry

| Key | Environment | Default | Notes |
|---|---|---|---|
| `telemetry.otlp.endpoint` | `KGW_TELEMETRY_OTLP_ENDPOINT` | `""` | e.g. `otel-collector:4317`. Empty disables export. |
| `telemetry.otlp.protocol` | `KGW_TELEMETRY_OTLP_PROTOCOL` | `grpc` | `grpc` or `http`. |
| `telemetry.service_name` | `KGW_TELEMETRY_SERVICE_NAME` | `kafka3o-gateway` | |
| `telemetry.log_level` | `KGW_TELEMETRY_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, or `error`. |

Request and response bodies are never logged, and SCRAM passwords never appear in logs or in the audit trail.

## Command line

The binary is `kafka3o-gateway`.

| Command | What it does | Exit code |
|---|---|---|
| `kafka3o-gateway [serve] [--config <path>]` | Runs the gateway. `serve` is the default and can be omitted. | `0` after a clean shutdown; `1` on a configuration or start-up error. |
| `kafka3o-gateway probe --live` / `--ready` | Health check for exec probes (below). Exactly one flag is required. | `0` healthy; `1` unhealthy or unreachable; `2` bad flags. |
| `kafka3o-gateway keygen --tier reader\|operator` | Prints a new API key and its SHA-256 digest. | `0`; `2` bad flags. |
| `kafka3o-gateway --version` | Prints the build version. | `0` |

`keygen` prints three lines:

```
tier=operator
key=<the key to hand to the caller>
sha256=<the digest to put in auth.keys>
```

Give `key` to the caller, who sends it in the `X-Api-Key` header. Put only `sha256` into `auth.keys`. The gateway can't recover a lost key; issue a new one.

## Health probes

| Endpoint | Meaning |
|---|---|
| `GET /v1/health/live` | The process is up. |
| `GET /v1/health/ready` | The Kafka cluster is reachable. Returns `503` while it isn't. The body also reports audit sink health, but audit health never affects readiness. |

Both endpoints require an API key; any `reader` key will do. The container image has no shell, so Kubernetes should use **exec probes** that run the binary itself:

```yaml
livenessProbe:
  exec:
    command: ["/kafka3o-gateway", "probe", "--live"]
readinessProbe:
  exec:
    command: ["/kafka3o-gateway", "probe", "--ready"]
env:
  - name: KGW_PROBE_API_KEY           # a reader-tier key, from a Secret
    valueFrom:
      secretKeyRef: { name: kafka3o-gateway, key: probe-api-key }
```

`probe` calls `http://127.0.0.1:8080/v1/health/...` with a 5-second timeout and sends `KGW_PROBE_API_KEY` as `X-Api-Key`.

## Shutdown

On `SIGTERM` or `SIGINT` the gateway shuts down in this order:

1. Stop accepting connections, then wait for in-flight requests to finish.
2. Flush and stop telemetry (traces and metrics).
3. Close the Kafka clients: the main client, then the audit client if the sink is `kafka`.

The whole sequence is bounded by the effective `http.write_timeout` (65s by default). No request can legitimately run longer than that.

In Kubernetes, set `terminationGracePeriodSeconds` to at least `ceil(max_time.ceiling in seconds) + 10`, and add a short `preStop` sleep (about 5s) so the endpoint is removed from the Service before draining starts. With a 60s scan ceiling that means 70s or more.

## Audit topic provisioning

When `audit.sink` is `kafka` (`KGW_AUDIT_SINK=kafka`), the gateway writes every audit event to `audit.topic` (`KGW_AUDIT_TOPIC`) in addition to stdout. The gateway **never creates this topic** — provision it yourself before pointing the gateway at it, with your own Kafka CLI tools:

| Setting | Required value | Why |
|---|---|---|
| `cleanup.policy` | `delete` | Audit events are a time-ordered log, not compacted state — a later event never supersedes an earlier one by key. |
| `retention.ms` | ≥ `31536000000` (1 year) | The audit trail is a compliance record; retention shorter than your audit/compliance window silently loses history. |
| `min.insync.replicas` | `2` | The gateway produces with `acks=all`; a topic without at least two in-sync replicas can still lose an acknowledged audit event on a broker failure. |

At start-up, the gateway verifies the topic exists and accepts a write (a small probe record). If either check fails, start-up logs an `ERROR` naming the topic and the underlying cause, but the process still starts: `/health/ready` stays `200 UP` and reports `audit: { sink: "kafka", healthy: false }` — readiness never gates on audit health. Every mutating request then fails closed with `503 AUDIT_UNAVAILABLE` until the topic is fixed (created, or made writable) and the gateway is restarted; there is no live retry.

The stdout/file sink (`audit.sink: stdout`) always runs alongside the Kafka sink when one is configured, so the audit trail is never lost even while the Kafka side is unhealthy — it is just not durable in the same way until the topic is fixed.

## Break-glass (data-plane lock bypass)

With `policy.data_plane_lock` on, the message commands M1–M8 return `403 DATA_PLANE_LOCKED`. The lock can't be turned off without a restart. An operator who needs one message call during an incident can bypass it for that single request:

1. Use an **operator** key.
2. Add the header `X-Break-Glass-Reason: <why, e.g. the incident id>`. It must be non-empty and is capped at 512 bytes; control characters are stripped.
3. Send the request. Only that request is let through.

What to expect:

- Every break-glass use is audited at **HIGH** severity with the reason, including reads (M1–M4) that are normally unaudited. Alert on HIGH audit events.
- A reader key with the header still gets `403 DATA_PLANE_LOCKED`, and the attempt is audited at HIGH.
- Break-glass bypasses the data-plane lock **only**. It never bypasses authentication, `read_only_mode`, or `disabled_operations`.

## DELETE requests carry a JSON body

These deletes take their `confirm` value in a JSON request body:

- `DELETE /v1/topics/{name}` (T7)
- `DELETE /v1/consumer-groups/{groupId}` (G5)
- `DELETE /v1/scram-users/{name}` (S1)

Some proxies, load balancers, and API gateways strip bodies from `DELETE` requests. When that happens, `confirm` arrives empty and the request **fails closed** with `400 CONFIRMATION_MISMATCH`. Nothing is deleted. If you see this for a `confirm` value you know was right, check the intermediaries between the caller and the gateway.
