# Operations

## Audit topic provisioning

When `audit.sink` is `kafka` (`KGW_AUDIT_SINK=kafka`), the gateway writes every audit event to `audit.topic` (`KGW_AUDIT_TOPIC`) in addition to stdout. The gateway **never creates this topic** — provision it yourself before pointing the gateway at it, with your own Kafka CLI tools:

| Setting | Required value | Why |
|---|---|---|
| `cleanup.policy` | `delete` | Audit events are a time-ordered log, not compacted state — a later event never supersedes an earlier one by key. |
| `retention.ms` | ≥ `31536000000` (1 year) | The audit trail is a compliance record; retention shorter than your audit/compliance window silently loses history. |
| `min.insync.replicas` | `2` | The gateway produces with `acks=all`; a topic without at least two in-sync replicas can still lose an acknowledged audit event on a broker failure. |

At start-up, the gateway verifies the topic exists and accepts a write (a small probe record). If either check fails, start-up logs an `ERROR` naming the topic and the underlying cause, but the process still starts: `/health/ready` stays `200 UP` and reports `audit: { sink: "kafka", healthy: false }` — readiness never gates on audit health. Every mutating request then fails closed with `503 AUDIT_UNAVAILABLE` until the topic is fixed (created, or made writable) and the gateway is restarted; there is no live retry.

The stdout/file sink (`audit.sink: stdout`) always runs alongside the Kafka sink when one is configured, so the audit trail is never lost even while the Kafka side is unhealthy — it is just not durable in the same way until the topic is fixed.
