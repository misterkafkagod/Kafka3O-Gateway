# Kafka3O Gateway

A headless, stateless **Kafka Application Gateway**: it connects to one Apache Kafka cluster and exposes its inspection, data-plane, and administration capabilities as a versioned, documented, access-tiered REST API — so a Kafdrop-like front-end (or any management application) can operate the cluster over HTTP without embedding a Kafka client or holding broker credentials.

> **Status:** pre-alpha. The specification is complete and audited; implementation is in progress phase by phase (see [`TASKS.md`](TASKS.md)).

## What it does

- **41 commands, 48 REST operations** — cluster and topic inspection, bounded message reads and searches (regex, JSONPath), produce, consumer-group inspection and administration, topic administration, replay, KRaft quorum, reassignments, SCRAM credentials, client quotas. The catalog is in [`FUNC-SPEC.md` §5](FUNC-SPEC.md).
- **Safety by construction** — reader / operator API-key tiers, global read-only mode, per-operation switches, a data-plane lock with audited break-glass, confirmation echo and dry-run on every destructive command, plan tokens for multi-target operations, and a two-phase, fail-closed audit log.
- **Bounded everything** — no read, search, or replay can run unboundedly; every bound has a default and a hard ceiling.
- **Stateless** — no database, no background jobs; configuration is read once at start-up.

## Supported brokers

Apache Kafka **3.3 → 4.x** (KRaft-era APIs). Older clusters are best-effort; a feature the cluster lacks surfaces as `502 KAFKA_ERROR` with `kafkaError.name = UNSUPPORTED_VERSION`.

## Quick start

Requires Go 1.27 (the `toolchain` directive in `go.mod` selects the exact patch release) and a reachable Kafka cluster.

```sh
make tools                                  # golangci-lint, govulncheck, benchstat
make build                                  # bin/kafka3o-gateway
go run ./cmd/gateway keygen --tier operator # prints key=... sha256=...
go run ./cmd/gateway keygen --tier reader
# put both digests and your bootstrap servers in configs/dev.yaml (see configs/config.example.yaml)
go run ./cmd/gateway --config configs/dev.yaml
curl -s -H "X-Api-Key: $OP_KEY" http://localhost:8080/health/ready
curl -s -H "X-Api-Key: $OP_KEY" http://localhost:8080/openapi.json | jq '.paths | keys'
```

Every endpoint — including `/health/*` and `/openapi.json` — requires an API key when authentication is enabled. Kubernetes probes therefore run `kafka3o-gateway probe --live|--ready` with a reader-tier key from the environment.

## Development

```sh
make lint      # golangci-lint (depguard boundaries, gosec, exhaustive, ...) + file-length guard
make cover     # tests with -race; 90 % overall, 100 % on gates and error tables
make fuzz      # every Fuzz* target for 10 s
make ci        # everything the PR pipeline runs
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the workflow and the code-review checklist, and [`SECURITY.md`](SECURITY.md) for the vulnerability policy.

## Specifications

This project is built spec-first. The three documents below are the source of truth and are kept in sync with the code:

| Document | Contents |
|---|---|
| [`FUNC-SPEC.md`](FUNC-SPEC.md) | Objectives, command catalog, interfaces, envelopes, error taxonomy, audit schema, behaviours, verification criteria |
| [`TECH-SPEC.md`](TECH-SPEC.md) | Stack and CVE audit, design patterns, SOLID constraints, testing strategy, topology, spec audit with the URL table |
| [`TASKS.md`](TASKS.md) | Phased implementation plan with Definitions of Done |

## Licence

Apache License 2.0 — see [`LICENSE`](LICENSE). Dependencies are limited to permissive licences (Apache-2.0 / MIT / BSD / ISC / MPL-2.0).
