# Implementation Tasks

**Product:** Kafka Application Gateway (`github.com/misterkafkagod/kafka3o`)
**Specs:** `FUNC-SPEC.md` rev 0.4 · `TECH-SPEC.md` rev 0.7 (audit §6 `STATUS: READY`)
**Created:** 2026-09-19 by Step 9 (`skill-task-creator`) · **Verified:** 2026-09-19 by Step 10 (`skill-task-verifier`) · **Audited:** 2026-09-19 by Step 11 (`skill-task-guardian`) — `STATUS: READY` (see the Step 11 audit at the end of this file)

## Conventions

- **Phase pattern.** Every command phase is a vertical slice: port methods → `franz` adapter → `fake` + `porttest` cases → service → Huma routes + DTOs → component tests → acceptance test(s) under `//go:build acceptance` → remove the phase's IDs from the bijection test's `pending` list.
- **Status fields.** Phase: `Not Started` · `In Progress` · `Awaiting Manual Verification` · `Verified`. Task: `Not Started` · `In Progress` · `Done`.
- **Citations.** `FUNC §x` = FUNC-SPEC.md, `TECH §x` = TECH-SPEC.md. Paths are relative to the repository root (TECH §5.1). In Definitions of Done, `VC` = FUNC §9.7 validation-criteria row, `4.5` = TECH §4.5 traceability row.
- **Definition of Done (Step 10).** Every task carries a `Tests (Definition of Done)` list naming exact Go test functions (`Test<Unit>_<Case>` per TECH §4.9), `porttest` case names, fuzz/bench targets, lint checks, or workflow runs — only the frameworks in TECH §4.2. A task is `Done` when every listed item passes in CI (or, for Level 2 / workflow items, in the named run).
- **Common setup for Manual Test Plans** (assumes a Go 1.27 toolchain, `curl`, `jq`, and a reachable cluster in `$KAFKA_BOOTSTRAP` where a step needs one — environment provisioning is out of scope by decision):
  - **CS1** `go run ./cmd/gateway keygen --tier operator` → prints `key=… sha256=…`; repeat with `--tier reader`. Export `OP_KEY`, `RD_KEY`.
  - **CS2** Write `configs/dev.yaml` (bootstrap `$KAFKA_BOOTSTRAP`, `apiKeys` = both digests, `auditSink: stdout`); `go run ./cmd/gateway --config configs/dev.yaml` → log `listening on :8080`.
  - **CS3** `curl -s -H "X-Api-Key: $OP_KEY" http://localhost:8080/v1/...` (use `$RD_KEY` where a plan says *reader*).

---

## Phase 1: Gateway boots, authenticates a key, reports health, serves OpenAPI
- **Phase Status:** Awaiting Manual Verification
- **Goal:** A human can start the binary against a cluster, get 401 without a key, 200 with one, see readiness reflect cluster reachability, and fetch `/openapi.json`.
- **Manual Test Plan:**
  1. `go build ./... && go test -race ./...` → both exit 0; the coverage summary prints ≥ 90 % for the gated packages.
  2. CS1 → two lines with `key=` and `sha256=`; running `keygen` twice gives different keys.
  3. CS2 → `listening on :8080` within 3 s.
  4. `curl -s -o /dev/null -w '%{http_code}' localhost:8080/health/live` → `401`; body `{"error":{"code":"UNAUTHENTICATED",…,"requestId":"…"}}`.
  5. CS3 `/health/live` → `200 {"status":"UP"}`; `/health/ready` → `200`, `cluster.reachable=true`, `brokersSeen ≥ 1`.
  6. Point CS2 at an unreachable bootstrap → `/health/ready` → `503`, `status=DOWN`; `/health/live` still `200`.
  7. `curl … /openapi.json | jq '.paths | keys'` → contains `/health/live` and `/health/ready`; each operation carries `x-command-id: "C3"`.
  8. `curl -i -H "X-Request-Id: abc-123" … /health/live` → response header `X-Request-Id: abc-123`; with `X-Request-Id: "bad value!"` → a generated id instead.
  9. `KGW_PROBE_API_KEY=$RD_KEY go run ./cmd/gateway probe --ready` → exit 0; with a wrong key → exit 1.
  10. Ctrl-C → log `shutdown complete`, exit 0 within 5 s.
- **Step 10 verification:** concrete ✓ · self-contained ✓ · automated coverage ✓ — step 10 is covered by `TestApp_ShutdownOrder` (added to Task 1.10 as an addition beyond TECH §4.5; flagged for Step 11).

### Task 1.1: Repository scaffold and lint policy
- **Status:** Done
- **Source:** TECH §1.0, §1.1 (Go 1.27.1, toolchain pin), §3.6 (enforcement matrix), §5.1 (tree), §5.3 (`depguard`)
- **Subtasks:**
  - [x] 1.1.1 `go.mod` with module `github.com/misterkafkagod/kafka3o`, `go 1.27`, `toolchain go1.27.1`; `go.sum` — TECH §5.1
  - [x] 1.1.2 `Makefile` targets: `lint`, `test`, `cover`, `fuzz`, `bench`, `build`, `image`, `openapi`, `acceptance`, `report` — TECH §5.1
  - [x] 1.1.3 `.golangci.yml`: `depguard` rules exactly as TECH §5.3 (12 rows), `gosec`, `exhaustive`, `funlen` 60/40, `gocyclo` 15, `gochecknoglobals`, `gochecknoinits`, `ireturn`, file-length check — TECH §3.6, S6, D1–D5
  - [x] 1.1.4 `renovate.json` (Go modules, GitHub Actions, Docker digests) — TECH §1.3 continuous policy
  - [x] 1.1.5 `LICENSE` (Apache-2.0), `README.md` (incl. supported broker range Kafka 3.3 → 4.x — TECH C3), `SECURITY.md`, `CONTRIBUTING.md` hosting the **code-review checklist** for the rules not machine-checked (S1–S5, I2, I5 — TECH §3.6), `CHANGELOG.md`, `.github/CODEOWNERS` — TECH §1.0, §3.6, §5.1, C3
  - [x] 1.1.6 `configs/config.example.yaml` skeleton (filled in by 1.2) — TECH §5.1
- **Tests (Definition of Done):**
  - `make lint` → `golangci-lint run ./...` exit 0 with every TECH §5.3 `depguard` rule loaded and `gosec`, `exhaustive`, `funlen`, `gocyclo`, `gochecknoglobals`, `gochecknoinits`, `ireturn` enabled — TECH §3.6
  - `make lint-negative` → a fixture file importing `github.com/twmb/franz-go/pkg/kgo` from `internal/kafka` fails `depguard` — TECH D1, §5.3
  - `go vet ./...` exit 0; `go mod verify` exit 0 — TECH §1.1
  - Review-checklist item (Step 11 G4): `CONTRIBUTING.md` contains a checklist entry for each of S1, S2, S3, S4, S5, I2, I5 quoting the rule; `README.md` states the supported broker range — TECH §3.6, C3

### Task 1.2: Configuration package
- **Status:** Done
- **Source:** FUNC §8.2 (configuration switches, sensitive values), §8.8 (bounds); TECH §1.1 (koanf v2.3.6), §5.4 (`KGW_` env, ConfigMap/Secret), §6.1 B4 (hashed keys), B6 (trusted proxies), §6.3 C6 (listener), configuration-keys list
- **Subtasks:**
  - [x] 1.2.1 `internal/config/config.go`: immutable `Config` with kafka (bootstrap, TLS, SASL mechanism/user/password), http (addr, TLS cert/key, `ReadHeaderTimeout` 10 s, `IdleTimeout` 60 s, `WriteTimeout` = maxTime ceiling + 5 s, body limit 1 MB, trusted proxies, CORS origins, docs enabled), auth (enabled, keys), policy (readOnlyMode, dataPlaneLock, disabledOperations), bounds (every §8.8 row incl. `maxMatches` 100/1 000), audit (sink, topic), telemetry (OTLP endpoint) — FUNC §8.8; TECH §6.3
  - [x] 1.2.2 `load.go`: koanf YAML file + `KGW_` env overrides (env wins); `--config` flag — TECH §1.1, §5.4
  - [x] 1.2.3 `validate.go`: ceilings ≥ defaults, key list `{id, tier ∈ reader|operator, sha256 hex-64}`, duplicate ids rejected, CORS wildcard forbidden when auth enabled, `WriteTimeout` derived — TECH §6.1 B4, C13
  - [x] 1.2.4 `redact.go`: `String()`/`slog.LogValuer` masking SASL password, TLS key, key digests — FUNC §8.2 sensitive; TECH §1.1
  - [x] 1.2.5 Unit tests incl. `testdata/` YAML fixtures; fill `configs/config.example.yaml` with every key and default — TECH §4.5, §5.1
- **Tests (Definition of Done):**
  - `TestLoad_YAMLThenEnvOverrides` — TECH §1.1, §5.4
  - `TestValidate_DuplicateKeyID_Fails`, `TestValidate_BadDigest_Fails`, `TestValidate_UnknownTier_Fails` — TECH §6.1 B4
  - `TestValidate_CeilingBelowDefault_Fails`, `TestValidate_WriteTimeoutDerivedFromMaxTime` — FUNC §8.8; TECH C6
  - `TestValidate_CORSWildcardWithAuth_Fails` — TECH C13
  - `TestRedact_SecretsMasked` (SASL password, TLS key, digests absent from `String()` and log output) — FUNC §8.2
  - `TestDefaults_MatchSpecTable` (every FUNC §8.8 row incl. `maxMatches` 100 / 1 000) — VC O3
  - `TestExampleConfig_LoadsAndListsEveryKey` (`configs/config.example.yaml` parses; every `Config` field present) — TECH §5.1

### Task 1.3: Command table
- **Status:** Done
- **Source:** FUNC §5.1–§5.3 (41 commands), §5.4–§5.6 (access, destructive, data-plane); TECH §2.3 (declarative command table), O1 (gates key off descriptor fields)
- **Subtasks:**
  - [x] 1.3.1 `internal/command/table.go`: `Descriptor{ID, Name, Access R|W, Destructive, DataPlane}` × 41 (destructive set = FUNC §5.6; data-plane = M1–M8) — FUNC §5.5–§5.6
  - [x] 1.3.2 `table_test.go`: 41 entries, unique ids, `Destructive ⇒ Access == W`, data-plane set == {M1…M8} — TECH O1
- **Tests (Definition of Done):**
  - `TestTable_Has41UniqueIDs` — FUNC §5
  - `TestTable_DestructiveSetMatchesSpec` (exactly T7, T8, T9, T10, T11, T12, G4, G5, G6, G7, M8, C5, C9, C12, S1, S2) — FUNC §5.6
  - `TestTable_DataPlaneIsM1ToM8` — FUNC F6
  - `TestTable_DestructiveImpliesW` — FUNC §5.6, §9.1 rules

### Task 1.4: Kafka Port — domain types, errors, role interfaces (Phase 1 subset)
- **Status:** Done
- **Source:** FUNC §8.1 (three protocol surfaces); TECH §2.2 (Ports & Adapters), I1 (three role interfaces), D1 (stdlib only), L2 (`Kind` semantics)
- **Subtasks:**
  - [x] 1.4.1 `internal/kafka/types.go`: `Broker`, `ClusterInfo`, `Topic`, `Partition`, `Record`, `Header`, `Group`, `ConfigEntry` (with `Source`, `IsSensitive`) — no JSON tags (I5) — FUNC §8.7
  - [x] 1.4.2 `errors.go`: `Error{Kind, Resource, KafkaCode, KafkaName, Cause}`, `Kind` enum (`NotFound`, `AlreadyExists`, `GroupActive`, `ReassignmentInProgress`, `Timeout`, `Unavailable`, `Broker`, `Unsupported`) — FUNC §8.4; TECH L2
  - [x] 1.4.3 `port.go`: `Admin`, `Consumer`, `Producer` interfaces; Phase-1 method `Admin.DescribeCluster(ctx)` — TECH I1
  - [x] 1.4.4 `depguard` proves `internal/kafka` imports stdlib only — TECH D1, §5.3
- **Tests (Definition of Done):**
  - `port_test.go` compile-time assertions `var _ Admin = (*fake.Fake)(nil)` etc. (compiled from `fake_test.go`) — TECH I1
  - `TestError_ErrorsAsExposesKindCodeName` — TECH L2
  - `TestTypes_NoJSONTags` (reflection over every exported struct in `internal/kafka`) — TECH I5
  - `make lint` passes the `internal/kafka` stdlib-only `depguard` rule — TECH D1

### Task 1.5: franz-go adapter (Phase 1 subset)
- **Status:** Done
- **Source:** TECH §1.1 (franz-go v1.21.7 / kadm v1.18.0, TLS/SASL), S4 (adapters only translate), L2/L3 (error and context semantics), D5 (kotel hook); FUNC X6
- **Subtasks:**
  - [x] 1.5.1 `internal/kafka/franz/client.go`: build `kgo.Client` (+ `kadm`) from config; `security.go`: TLS (min 1.2), SASL PLAIN / SCRAM-256 / SCRAM-512 / OAUTHBEARER — FUNC X6; TECH §1.1
  - [x] 1.5.2 `errors.go`: `kerr → kafka.Kind` table (unknown topic/partition → NotFound, topic exists → AlreadyExists, non-empty group → GroupActive, unsupported version → Unsupported, ctx deadline → Timeout, no brokers → Unavailable, rest → Broker) preserving code and name — FUNC §8.4; TECH L2, L3
  - [x] 1.5.3 `admin.go`: `DescribeCluster` via `kadm.DescribeCluster` — FUNC §8.7 C1
  - [x] 1.5.4 `plugin/kotel` hooks wired for traces/metrics — TECH §1.1, D5
- **Tests (Definition of Done):**
  - `TestFranz_KerrMapping_TableDriven` (every listed `kerr` → expected `Kind`, `KafkaCode` and `KafkaName` preserved; `context.DeadlineExceeded` → `Timeout`) — TECH L2, L3
  - `TestFranz_SecurityOptions_TLSMinVersionAndSASLMechanisms` (option construction only, no broker) — FUNC X6
  - Level 2: `porttest.Run(t, franz.New(cfg))` under `//go:build integration` passes the `DescribeCluster` cases (wired in Task 16.2) — TECH L1

### Task 1.6: Fake Kafka Port and contract suite (Phase 1 subset)
- **Status:** Done
- **Source:** TECH §4.3 (fake contract), §4.4, L1–L4 (`porttest`), P3 (test-only), §5.5
- **Subtasks:**
  - [x] 1.6.1 `internal/kafka/fake/fake.go` + `model.go`: in-memory model skeleton (brokers, topics, groups, users, quotas, reassignments, quorum) with mutex; injected `now func() time.Time`; package doc comment states that compaction is **not** simulated — TECH §4.3
  - [x] 1.6.2 `faults.go`: `FailNext(method, kind)`, `FailAlways`, `Latency(method, d)`, `Unreachable(bool)`; recording `Calls()`, `MutatingCalls()`, `AssertCalled`, `AssertNoCommits`, `AssertNoGroupJoin` — TECH §4.3
  - [x] 1.6.3 `seed.go`: `SeedBroker`, `SeedTopic`, `SeedGroup` — TECH §4.3
  - [x] 1.6.4 `admin.go`: `DescribeCluster` — FUNC §8.7 C1
  - [x] 1.6.5 `internal/kafka/porttest/suite.go` + `admin.go`: `Run(t, port)` with `DescribeCluster` happy path, `Unreachable → KindUnavailable`, ctx deadline → `KindTimeout`; `fake_test.go` runs it — TECH L1–L4
- **Tests (Definition of Done):**
  - `TestFake_PortContract` (invokes `porttest.Run(t, fake.New())`) — TECH L1
  - `porttest` cases `Admin_DescribeCluster_ReturnsSeededBrokers`, `Admin_Unreachable_KindUnavailable`, `Any_DeadlineExceeded_KindTimeout` — TECH L2, L3
  - `TestFake_FailNext_FiresOnce`, `TestFake_FailAlways_Persists`, `TestFake_Latency_HonoursCtx`, `TestFake_MutatingCallsExcludesReads` — TECH §4.3
  - `TestFake_Race` (concurrent seeds and calls under `-race`) — TECH §4.3

### Task 1.7: Service core and gates
- **Status:** Done
- **Source:** FUNC §9.1 (upper half: auth → tier → F2 → F3 → F6 → break-glass), F1–F3, F6, D5, O7, O9; TECH §2.0 P1, §2.3 (gate check as one function, `run[T]`), O1, T1 (100 % on gates)
- **Subtasks:**
  - [x] 1.7.1 `internal/service/core/caller.go`: `Caller{KeyID *string, Tier, BreakGlassReason string, RequestID, ClientIP, Client}` — FUNC §8.5; TECH P1
  - [x] 1.7.2 `policy.go`: `Policy{AuthEnabled, ReadOnly, DataPlaneLock, Disabled map[ID]bool}` from config — FUNC §8.2 switches
  - [x] 1.7.3 `errors.go`: `PolicyError{Code}` with codes `TierForbidden`, `ReadOnlyMode`, `OperationDisabled`, `DataPlaneLocked`, `ConfirmationMismatch`, `BoundExceeded`, `Validation` — FUNC §8.4
  - [x] 1.7.4 `run.go`: generic `run[T](ctx, caller, descriptor, fn)` calling `gates.Check` first — TECH §2.3
  - [x] 1.7.5 `internal/service/gates/check.go`: `Check(caller, descriptor, policy) error` implementing FUNC §9.1 nodes F–K3 exactly, break-glass bypasses F6 only, sets `HIGH` flag — FUNC §9.1 (+ C1), §9.5
  - [x] 1.7.6 `check_test.go`: full matrix (tier × access × F2 × F3 × F6 × header) — 100 % coverage — TECH T1, §4.5 O7/O9
  - [x] 1.7.7 `BenchmarkGatesCheck` in `check_test.go` (Step 11 G1) — TECH §4.7
- **Tests (Definition of Done):**
  - `TestGatesCheck_Matrix` (table: tier {reader, operator, auth-off} × access {R, W} × readOnly × disabled × lock × header → expected code or nil; every FUNC §9.1 exit F–K3 hit) — VC O7, O9
  - `TestGatesCheck_BreakGlassBypassesF6Only` (header present + readOnly → `ReadOnlyMode`) — FUNC §9.1 rules (C1)
  - `TestGatesCheck_ReaderBreakGlassIsDataPlaneLocked` — FUNC §9.5
  - `TestRun_CallsGatesBeforeFn` (fn not invoked when gate fails) — TECH §2.3
  - CI coverage gate reports 100 % for `internal/service/gates` — TECH T1
  - `BenchmarkGatesCheck` runs under `make bench` and reports `allocs/op` — TECH §4.7 (Step 11 G1)

### Task 1.8: HTTP layer — Huma, middleware, error tables, health, bijection test
- **Status:** Done
- **Source:** FUNC §8.2 (conventions), §8.3 (envelopes), §8.4 (taxonomy), §8.7 C3, O1, O5, O6, X1–X5; TECH §1.1 (Huma v2.39.1), O4, O5, §6.1 B2/B3/B6, §6.3 C2, C13, §6.2 (C3 routes)
- **Subtasks:**
  - [x] 1.8.1 `internal/api/api.go`: `New(deps) http.Handler` — Huma on `net/http` mux, base `/v1`, `/openapi.json`, `/docs` (config toggle), middleware chain order: request-id → api-key → CORS → otelhttp — TECH §1.1, §6.2
  - [x] 1.8.2 `middleware/requestid.go`: echo if `^[A-Za-z0-9._-]{1,128}$` else generate; set on response and context — FUNC X4, §8.2 (C2)
  - [x] 1.8.3 `middleware/apikey.go`: SHA-256 of presented key, `crypto/subtle` compare against configured digests, builds `core.Caller`; auth disabled → operator with nil key id; **no exempt routes** — FUNC §8.2 (B3); TECH §1.1 security primitives, §6.1 B4
  - [x] 1.8.4 `middleware/clientip.go`: trusted-proxy CIDRs → right-most untrusted `X-Forwarded-For`, else peer — TECH §6.1 B6
  - [x] 1.8.5 `middleware/cors.go`: origins from config; allow `X-Api-Key`, `X-Break-Glass-Reason`, `X-Request-Id`, `Content-Type`; expose `X-Request-Id`; no wildcard with auth — FUNC X5; TECH C13
  - [x] 1.8.6 `errors/tables.go` + `envelope.go`: `map[kafka.Kind]httpMapping`, `map[core.Code]httpMapping` → FUNC §8.4 rows; Huma error model replaced by `{ error: { code, message, status, requestId, kafkaError?, details? } }` with `application/json`; Huma validation errors → `VALIDATION_FAILED` with `details.fields[]` — FUNC §8.3, §8.4; TECH O4
  - [x] 1.8.7 `errors/tables_test.go`: iterate every `Kind` and `Code`, fail on a missing row — 100 % coverage — TECH O4, T1
  - [x] 1.8.8 `health/live.go`, `health/ready.go`: `/health/live` → `{status}`; `/health/ready` → `{status, cluster:{reachable, brokersSeen, latencyMs}, audit:{sink, healthy}}`, 503 when cluster DOWN (audit does not gate) — FUNC §8.7 C3, O6; TECH §6.1 B5
  - [x] 1.8.9 `x-command-id` operation extension helper; `openapi_test.go`: (a) every operation carries exactly one id present in `command.Table`; (b) every table id not in `pending` has ≥ 1 operation; (c) `pending` lists all ids except C3 — shrinks each phase; golden `testdata/openapi.golden.json` with `-update` — FUNC §9.7 O1 (reworded); TECH O5, §4.5 O1
  - [x] 1.8.10 Component tests via `testutil`: 401 shapes, request-id echo/generate, CORS preflight, health UP/DOWN with `fake.Unreachable` — TECH §4.5 O6/X4/X5
  - [x] 1.8.11 Fuzz target `FuzzAPIKeyHeader` for the `X-Api-Key` header parser (never panics; only exact digest matches authenticate) with seed corpus (Step 11 G2) — TECH §4.6
- **Tests (Definition of Done):**
  - `FuzzAPIKeyHeader` 10 s green in CI with committed seeds — TECH §4.6 (Step 11 G2)
  - `TestOpenAPI_EveryOperationHasExactlyOneKnownCommandID`, `TestOpenAPI_EveryNonPendingIDHasAnOperation`, `TestOpenAPI_PendingIsTableMinusImplemented`, `TestOpenAPI_Golden` — VC O1; TECH O5
  - `TestErrorTables_EveryKindMapped`, `TestErrorTables_EveryCodeMapped` (enum iteration; CI enforces 100 % on `internal/api/errors`) — TECH O4, T1
  - `TestEnvelope_ErrorShapeAndContentType` (`{error:{code,message,status,requestId}}`, `application/json`), `TestEnvelope_HumaValidationBecomesValidationFailedWithFields` — FUNC §8.3, §8.4
  - `TestMiddleware_RequestID_EchoesValid`, `TestMiddleware_RequestID_GeneratesForInvalid`, `TestMiddleware_RequestID_PresentOnErrors` — 4.5 X4; TECH C2
  - `TestMiddleware_APIKey_MissingIs401`, `TestMiddleware_APIKey_UnknownIs401`, `TestMiddleware_APIKey_ValidBuildsCallerWithTier`, `TestMiddleware_APIKey_AuthDisabledIsOperatorNilKeyID`, `TestMiddleware_APIKey_HealthAndOpenAPINotExempt` — VC O7; TECH §6.1 B3, B4
  - `TestMiddleware_ClientIP_TrustedProxyTakesRightmostUntrustedXFF`, `TestMiddleware_ClientIP_UntrustedPeerIgnoresXFF` — TECH §6.1 B6
  - `TestMiddleware_CORS_PreflightAllowsHeadersAndExposesRequestID`, `TestMiddleware_CORS_RejectsUnlistedOrigin` — 4.5 X5; TECH C13
  - `TestHealth_LiveAlways200`, `TestHealth_Ready503WhenFakeUnreachable`, `TestHealth_ReadyReportsAuditFieldWithoutGating` — VC O6; TECH §6.1 B5

### Task 1.9: Telemetry
- **Status:** Done
- **Source:** TECH §1.0 (OpenTelemetry mandatory), §1.1 (OTel v1.46.0, otelhttp, otelslog), §4.5 Telemetry, §5.1 `internal/telemetry`
- **Subtasks:**
  - [x] 1.9.1 `internal/telemetry/otel.go`: tracer + meter providers, OTLP gRPC/HTTP exporters from config, resource attributes, `Shutdown(ctx)` — TECH §1.1
  - [x] 1.9.2 `slog.go`: JSON `slog` handler to stdout bridged via `otelslog`; `requestId` on every line — FUNC X4; TECH §1.1 logging
  - [x] 1.9.3 `tracetest` in-memory exporter option for tests; component test asserts one server span per request with `requestId` attribute — TECH §4.5 Telemetry
  - [x] 1.9.4 `internal/telemetry/kafkatrace.go`: tracing decorators `TracedAdmin`, `TracedConsumer`, `TracedProducer` wrapping the three `kafka` role interfaces (one child span per port call, named by method, error status recorded); wired in `internal/app` around the franz adapters; `internal/telemetry` may import `internal/kafka` (Step 11 G3 — spec note N3) — TECH §4.5 Telemetry, D5, §2.2 interface rule
- **Tests (Definition of Done):**
  - `TestTelemetry_OneServerSpanPerRequestWithRequestIDAttribute` (tracetest) — 4.5 Telemetry
  - `TestTelemetry_LogLinesCarryRequestID` (captured slog output) — FUNC X4
  - `TestTelemetry_ShutdownFlushesExporters` — TECH §5.4 shutdown order
  - `TestTelemetry_ChildSpanPerKafkaCall` (decorated fake port; one child span per call under the request's server span, error status on `FailNext`) — 4.5 Telemetry (Step 11 G3)
  - `TestTelemetry_DecoratorsSatisfyPortInterfaces` (compile-time assertions) — TECH §2.2 interface rule

### Task 1.10: Composition root, entrypoint, `probe` and `keygen` subcommands
- **Status:** Done
- **Source:** TECH §2.0 P1–P3, §2.1 (`internal/app`), D2, §5.0 Y5, §5.4 (shutdown order, exec probes), §6.1 B3 (probe), B4 (keygen), §6.3 C6 (server timeouts)
- **Subtasks:**
  - [x] 1.10.1 `internal/app/run.go`: `Run(ctx, cfg) error` — config → telemetry → franz client → services → api → `http.Server` (timeouts from config, optional TLS); only importer of `franz` — TECH §2.1, D2, C6
  - [x] 1.10.2 Graceful shutdown: SIGTERM → stop accepting → drain ≤ maxTime + 5 s → flush audit → OTel shutdown → close `kgo` clients; log `shutdown complete` — TECH §5.4
  - [x] 1.10.3 `cmd/gateway/main.go`: subcommands `serve` (default, `--config`), `probe --live|--ready` (GET `127.0.0.1:8080/health/*` with `KGW_PROBE_API_KEY`, exit 0/1), `keygen --tier` (32 random bytes → base64url key + sha256), `--version` — TECH §6.1 B3/B4, §5.1
  - [x] 1.10.4 `depguard`: `cmd/gateway` imports only `app` and `config` — TECH §5.3
- **Tests (Definition of Done):**
  - `TestApp_ServerTimeoutsFromConfig` (`ReadHeaderTimeout` 10 s, `IdleTimeout` 60 s, `WriteTimeout` = ceiling + 5 s) — TECH C6
  - `TestApp_ShutdownOrder` (httptest server + recording sink + fake exporter: stop-accepting → in-flight drained → audit flushed → OTel shutdown → clients closed; `shutdown complete` logged) — TECH §5.4 *(Step 10 addition beyond §4.5)*
  - `TestKeygen_Produces32ByteKeyAndMatchingSHA256`, `TestKeygen_TwoRunsDiffer` — TECH §6.1 B4
  - `TestProbe_Exit0WhenReadyReturns200`, `TestProbe_Exit1OnWrongKey`, `TestProbe_Exit1On503` (httptest-backed) — TECH §6.1 B3
  - `make lint` passes the `cmd/gateway` and `internal/app` `depguard` rows — TECH §5.3

### Task 1.11: Test scaffolding and acceptance harness
- **Status:** Done
- **Source:** TECH §4.8 (acceptance design), §4.9 (conventions), §5.1 (`internal/testutil`, `test/acceptance`), §5.5
- **Subtasks:**
  - [x] 1.11.1 `internal/testutil/gateway.go`: `NewTestGateway(t, opts...)` → `httptest.Server` + fake port + recording sink + fixed clock + configured keys; `clock.go`; `http.go` request helpers — TECH §4.9
  - [x] 1.11.2 `test/acceptance/main_test.go` (`//go:build acceptance`): `TestMain` starts `app.Run` against `KAFKA_BOOTSTRAP` (+ TLS/SASL env), operator/reader keys from env; `helpers.go` with `acc-<runID>-` prefixing and `t.Cleanup` deletion — TECH §4.8
  - [x] 1.11.3 `TestAcceptance_C3_HealthLive`, `TestAcceptance_C3_HealthReady` — TECH §4.8
- **Tests (Definition of Done):**
  - `TestNewTestGateway_BootsWithFakeRecordingSinkAndFixedClock` — TECH §4.9
  - `TestNewTestGateway_OptionsApplyPolicyAndKeys` — TECH §4.9
  - Level 2: `TestAcceptance_C3_HealthLive`, `TestAcceptance_C3_HealthReady` pass with `-tags acceptance` — TECH §4.8
  - `go vet -tags acceptance ./test/...` exit 0 in CI (harness compiles without a cluster) — TECH §4.8

### Task 1.12: Continuous integration (PR pipeline)
- **Status:** Done
- **Source:** TECH §1.0 (CVE and licence policy), §1.3 (continuous policy), §4.10 (CI stages), T1, T3, T4, §5.1 (`ci.yml`)
- **Subtasks:**
  - [x] 1.12.1 `.github/workflows/ci.yml`: lint → `go test -race -covermode=atomic ./...` → coverage gate (90 % overall on the T1 package list; 100 % on `service/gates`, `api/errors`; `franz`, `app`, `cmd` excluded) → fuzz 10 s per target → `govulncheck` → build → `benchstat` artifact — TECH §4.10, T1
  - [x] 1.12.2 Licence scan step (permissive-only allow-list) failing on GPL/LGPL/AGPL/SSPL in the module graph — TECH §1.0
  - [x] 1.12.3 `Makefile` targets mirror every CI stage so they run identically locally — TECH §5.1
- **Tests (Definition of Done):**
  - `ci.yml` green on a clean PR; every stage listed in TECH §4.10 appears as a named job step — TECH §4.10
  - Negative PR (temporary): coverage forced below 90 % → the coverage step fails; a GPL-licensed module added → the licence step fails — TECH T1, §1.0
  - `make ci` runs lint, test, coverage gate, fuzz-short, govulncheck, build locally with the same exit semantics — TECH §5.1

---

## Phase 2: Cluster and topic inspection
- **Phase Status:** Not Started
- **Goal:** A human can list and describe topics, see broker configs with sensitive values masked, sizes, message counts in a time window, and the cluster health summary.
- **Manual Test Plan:**
  1. CS2.
  2. CS3 `GET /v1/cluster` → `clusterId`, `controllerId`, `brokers[]` with ≥ 1 entry.
  3. `GET /v1/cluster/brokers/<id>/config` → `configs[]`; an entry with `isSensitive:true` has `value:null`.
  4. `GET /v1/cluster/health` → numeric `partitions.underReplicated`, `partitions.offline`, `brokers.online`.
  5. Create topic `t-demo` with 3 partitions using your Kafka CLI. `GET /v1/topics?pattern=^t-` → `items[0].name="t-demo"`, `page.total=1`; `?pattern=(` → `400 INVALID_REGEX`.
  6. `GET /v1/topics/t-demo` → 3 partitions each with `beginOffset`, `endOffset`, `leader`, `isr`; `configs[]` entries carry `source` ∈ {`default`, `static`, `dynamic`}.
  7. `GET /v1/topics/t-demo/size` → `totalBytes ≥ 0`, per-partition `replicas[].logDir`.
  8. Produce 5 records with your CLI; `GET /v1/topics/t-demo/count?from=<1h ago ISO>&to=<now ISO>` → `total=5`.
  9. `GET /v1/topics/nope` → `404 NOT_FOUND`, `details.resource="topic"`.
  10. `GET /v1/topics?page=99` → `items=[]`, `page.total=1`.
  11. Repeat 2–8 with `$RD_KEY` → identical results (all `R`).
- **Step 10 verification:** concrete ✓ · self-contained ✓ · automated coverage ✓

### Task 2.1: Port, adapters, and contract cases for inspection
- **Status:** Not Started
- **Source:** FUNC §8.1 (Admin surface), §8.7 C1, C2, C4, T1–T4; TECH L1–L4
- **Subtasks:**
  - [ ] 2.1.1 Port methods: `DescribeBrokerConfigs`, `ListTopics`, `DescribeTopics`, `DescribeTopicConfigs`, `ListStartOffsets`, `ListEndOffsets`, `ListOffsetsAfterMilli`, `DescribeLogDirs`, `Metadata` (for health summary) — FUNC §8.7
  - [ ] 2.1.2 `franz/admin.go` implementations via `kadm`; sensitive configs preserved as `IsSensitive` with nil value; config source normalised to `default` / `static` / `dynamic` — TECH §1.1; FUNC §5.1 T2
  - [ ] 2.1.3 `fake/admin.go` implementations over the model (offsets, log-dir sizes, configs with source) — TECH §4.3
  - [ ] 2.1.4 `porttest/admin.go`: happy paths, `NotFound` for missing topic/broker, timestamp offset lookup — TECH L2
- **Tests (Definition of Done):**
  - `porttest` cases `Admin_DescribeBrokerConfigs_SensitiveHasNilValue`, `Admin_DescribeBrokerConfigs_MissingBrokerNotFound`, `Admin_ListTopics_IncludesInternalFlag`, `Admin_DescribeTopics_MissingNotFound`, `Admin_DescribeTopicConfigs_SourceNormalised`, `Admin_ListOffsets_StartEnd`, `Admin_ListOffsetsAfterMilli_FirstAtOrAfter`, `Admin_DescribeLogDirs_PerReplicaBytes` (fake in CI; franz at Level 2) — TECH L2
  - `TestFranz_ConfigSourceNormalisation` (unit: every kadm source enum → `default`/`static`/`dynamic`) — FUNC §5.1 T2

### Task 2.2: Cluster service (C1, C2, C4)
- **Status:** Not Started
- **Source:** FUNC §8.7 C1, C2, C4, §8.2 (sensitive values); TECH §6.3 C11 (affected cap), S2, I2
- **Subtasks:**
  - [ ] 2.2.1 `internal/service/cluster/service.go`: `Service` with `Admin` only (I2 note); `DescribeCluster`, `DescribeBrokerConfig` (mask sensitive), `HealthSummary` (URPs, offline, non-preferred leader, broker count; `affected` capped at 1 000 with `truncated`) — FUNC §8.7; TECH C11
  - [ ] 2.2.2 Unit tests over the fake — TECH §4.1 1a
- **Tests (Definition of Done):**
  - `TestClusterService_DescribeCluster_MapsBrokersAndController` — FUNC §8.7 C1
  - `TestClusterService_DescribeBrokerConfig_SensitiveValueNull` — FUNC §8.2
  - `TestClusterService_HealthSummary_CountsURPsOfflineNonPreferred` — FUNC §8.7 C4
  - `TestClusterService_HealthSummary_AffectedCappedAt1000WithTruncated` — TECH C11

### Task 2.3: Topic service (T1–T4)
- **Status:** Not Started
- **Source:** FUNC §8.7 T1–T4, §8.2 (`?pattern=` RE2 unanchored, pagination), X3; TECH §6.1 B7
- **Subtasks:**
  - [ ] 2.3.1 `internal/service/topic/service.go`: `List` (regex pattern compile → `INVALID_REGEX`, includeInternal, sort by name, page/pageSize with total), `Describe` (`approxMessageCount = Σ(end − begin)`), `Size`, `CountInWindow` — FUNC §8.7; TECH §6.1 B7
  - [ ] 2.3.2 Unit tests: pattern semantics, paging past end, approx count — TECH §4.5 Pagination
- **Tests (Definition of Done):**
  - `TestTopicService_List_PatternIsRE2Unanchored`, `TestTopicService_List_InvalidPatternIsInvalidRegex` — TECH §6.1 B7
  - `TestTopicService_List_SortedByNameStablePaging`, `TestTopicService_List_PagePastEndEmptyWithTotal`, `TestTopicService_List_IncludeInternalToggle` — 4.5 Pagination; FUNC X3
  - `TestTopicService_Describe_ApproxCountIsSumEndMinusBegin`, `TestTopicService_Describe_ConfigSourceNormalised`, `TestTopicService_Describe_MissingNotFound` — FUNC §8.7 T2
  - `TestTopicService_Size_TotalsPerPartitionAndReplica` — FUNC §8.7 T3
  - `TestTopicService_CountInWindow_ClampsToEndAndSums` — FUNC §8.7 T4

### Task 2.4: Routes, DTOs, component and acceptance tests (C1, C2, C4, T1–T4)
- **Status:** Not Started
- **Source:** TECH §6.2 (URL table), §4.5, §4.8, I5 (DTOs separate)
- **Subtasks:**
  - [ ] 2.4.1 `internal/api/cluster/{routes,dto,mapping}.go`: `GET /v1/cluster`, `GET /v1/cluster/brokers/{brokerId}/config`, `GET /v1/cluster/health` — TECH §6.2
  - [ ] 2.4.2 `internal/api/topic/{routes,dto,mapping}.go`: `GET /v1/topics`, `GET /v1/topics/{name}`, `/size`, `/count` — TECH §6.2
  - [ ] 2.4.3 Component tests: 404 envelope, masking, pagination, reader access, `AssertNoCommits` after each; every `kafka.Kind` injected via `FailNext` on `GET /v1/topics/{name}` maps to its FUNC §8.4 row (Step 11 N1) — TECH §4.5 O4/O5/Pagination
  - [ ] 2.4.4 Acceptance tests C1, C2, C4, T1–T4; remove ids from `pending` — TECH §4.8
- **Tests (Definition of Done):**
  - `TestAPI_ErrorMapping_EveryKindThroughHTTP` (table over every `kafka.Kind` via `fake.FailNext` → HTTP status, `code`, `kafkaError`, `requestId`) — VC O5; 4.5 O5 (Step 11 N1)
  - `TestAPI_Inspection_ReaderKeyAllowedOnAllRoutes` (C1, C2, C4, T1–T4) — VC O7
  - `TestAPI_T1_InvalidPattern400`, `TestAPI_T1_PaginationEnvelope`, `TestAPI_T2_Missing404Envelope`, `TestAPI_C2_SensitiveMasked` — VC O5; 4.5 Pagination
  - `TestAPI_Inspection_NoCommitsNoGroupJoinAfterEachRoute` — VC O4
  - `TestOpenAPI_Golden` updated; `pending` shrinks by 7 — VC O1
  - Level 2: `TestAcceptance_C1_DescribeCluster`, `TestAcceptance_C2_BrokerConfig`, `TestAcceptance_C4_HealthSummary`, `TestAcceptance_T1_ListTopics`, `TestAcceptance_T2_DescribeTopic`, `TestAcceptance_T3_TopicSize`, `TestAcceptance_T4_CountInWindow` — TECH §4.8

---

## Phase 3: Read messages
- **Phase Status:** Not Started
- **Goal:** A human can page through a topic's messages from any starting point, fetch one by offset, and see every bound honoured.
- **Manual Test Plan:**
  1. Seed 250 JSON records across `t-demo` with your CLI.
  2. CS3 `GET /v1/topics/t-demo/messages?from=beginning` → 100 items, `scan.stoppedBy="maxMessages"`, `reachedEnd=false`, `continuation` with 3 partition keys.
  3. `?from=offset:<continuation["0"]>&partition=0` → next page for partition 0; no offset repeats the previous page.
  4. `?from=latest&limit=10` → 10 items, `timestampMs` descending, `reachedEnd=true`.
  5. `?limit=5000` → `400 BOUND_EXCEEDED`.
  6. `?from=timestamp:<one day ahead ISO>` → `items=[]`, `reachedEnd=true`.
  7. `?format=base64` → `valueEncoding="base64"`; default request → `"json"`.
  8. `GET /v1/topics/t-demo/partitions/0/messages/3` → the record at offset 3; `/messages/999999` → `404 NOT_FOUND`.
  9. List consumer groups with your CLI → no group was created by the gateway.
- **Step 10 verification:** concrete ✓ · self-contained ✓ · automated coverage ✓

### Task 3.1: Consumer port and adapters
- **Status:** Not Started
- **Source:** FUNC §8.1 (manual assignment, no group, no commits), O4, X8, §9.2 (end snapshot); TECH §2.3 (client lifecycle), C4 (`read_committed`), L1–L4
- **Subtasks:**
  - [ ] 3.1.1 Port `Consumer`: `Assign(ctx, topic, partitions, startOffsets)`, `Poll(ctx) ([]Record, error)`, `Close()`; end snapshot obtained through `Admin.ListEndOffsets` — FUNC §9.2
  - [ ] 3.1.2 `franz/consumer.go`: dedicated `kgo.Client` per scan with `ConsumePartitions`, no group id, `FetchIsolationLevel` from config, closed at scan end — TECH §2.3, C4
  - [ ] 3.1.3 `fake/consumer.go`: serves the model's record log honouring `Latency` and `FailNext`; records no commit/join calls — TECH §4.3
  - [ ] 3.1.4 `porttest/consumer.go`: assign+poll from offset, from timestamp, beyond end; `AssertNoCommits`/`AssertNoGroupJoin` — TECH §4.5 O4
- **Tests (Definition of Done):**
  - `porttest` cases `Consumer_AssignAndPollFromOffset`, `Consumer_PollFromTimestampResolvedOffset`, `Consumer_BeyondEndReturnsNothing`, `Consumer_NoCommitNoGroupJoin`, `Consumer_CloseReleases` — VC O4; TECH L2
  - `TestFranzConsumer_OptionsNoGroupReadCommitted` (unit: option construction, no broker) — TECH C4, §2.3

### Task 3.2: Bounded scan loop and decoders
- **Status:** Not Started
- **Source:** FUNC §9.2 (state machine, end snapshot, maxTime wall-clock, continuation, `latest`), §8.2 (encodings, `format=`), §8.3 (scan envelope, record), §8.8 (bounds), §9.1 rules (`to` = min(to, endSnapshot), `latest` continuation — C9); TECH §2.3 (one scan loop), I4 (`Matcher` func type), §4.9 (injected `now`)
- **Subtasks:**
  - [ ] 3.2.1 `internal/scan/runner.go`: `Run(ctx, consumer, spec, matcher, emit) (Stats, error)` implementing Resolve → Assign → Poll → Evaluate → Bounded/Exhausted/Empty → Done; end snapshot at Resolve; `context.WithTimeout(maxTime)`; `stoppedBy`; per-partition continuation — FUNC §9.2
  - [ ] 3.2.2 `spec.go`: `Spec{Topic, Partitions, From (beginning|latest|offset|timestamp), To, MaxMessages, MaxBytes, MaxTime, Format}`; `latest` → start `max(begin, end − limit)`, sort by `timestampMs` desc, truncate; `to` → `min(to, endSnapshot)` — FUNC §9.2, C9
  - [ ] 3.2.3 `decode.go`: auto-detect JSON → UTF-8 → base64 for key/value; `format=` override; headers `string`/`base64` rule; `sizeBytes` — FUNC §8.2
  - [ ] 3.2.4 `matcher.go`: `type Matcher func(Record) MatchResult{Match, Skipped bool}`; `MatchAll` — TECH I4
  - [ ] 3.2.5 `stats.go`: `Stats{Scanned, Matched, Skipped, Bytes, ElapsedMs, ReachedEnd, StoppedBy, Continuation}` — FUNC §8.3
  - [ ] 3.2.6 Unit tests for every §9.2 transition with the fake and fixed clock: Empty, Exhausted, each Bounded cause, end snapshot ignores records appended mid-scan, continuation gap/overlap-free, `latest` ordering — TECH §4.5 Scan
- **Tests (Definition of Done):**
  - `TestScanRun_Empty_NoPartitionsOrFromAtEnd`, `TestScanRun_Exhausted_ReachedEndTrueStoppedByNil` — FUNC §9.2
  - `TestScanRun_Bounded_MaxMessages`, `TestScanRun_Bounded_MaxBytes`, `TestScanRun_Bounded_MaxTime_FixedClockWithLatency` — VC O3
  - `TestScanRun_EndSnapshot_IgnoresRecordsAppendedMidScan`, `TestScanRun_Continuation_NoGapNoOverlapAcrossPages` — 4.5 Scan
  - `TestScanRun_Latest_StartsAtEndMinusLimitSortedDescTruncated`, `TestScanRun_Latest_ContinuationIsStartOffsetsReachedEndTrue`, `TestScanRun_To_ClampsToEndSnapshot` — FUNC §9.1 rules (C9)
  - `TestScanRun_MaxTimeIsWallClockFromResolve` — FUNC §9.2
  - `TestDecode_JSONThenUTF8ThenBase64`, `TestDecode_FormatOverrideForcesEncoding`, `TestDecode_HeaderValuesStringOrBase64`, `TestDecode_SizeBytes` — 4.5 Decoding
  - `BenchmarkScanRun/1krecords`, `BenchmarkScanRun/10krecords`, `BenchmarkDecode/json`, `BenchmarkDecode/string`, `BenchmarkDecode/binary`, `BenchmarkEncodeScanEnvelope` compile and run under `make bench` — TECH §4.7

### Task 3.3: Message service (M1, M2)
- **Status:** Not Started
- **Source:** FUNC §8.7 M1, M2, §9.1 rules (M2 compacted-away 404 — C10), §5.6 (data-plane); TECH S2, I2 (`Consumer`, `Producer`)
- **Subtasks:**
  - [ ] 3.3.1 `internal/service/message/service.go`: `Read` (bounds validated against ceilings → `BoundExceeded`), `Get` (assign at offset, first record offset > requested → `NotFound`) — FUNC §8.7, C10
  - [ ] 3.3.2 Descriptors M1, M2 flagged `DataPlane` (gates ready for Phase 6) — FUNC F6
- **Tests (Definition of Done):**
  - `TestMessageService_Read_LimitAboveCeilingIsBoundExceeded`, `TestMessageService_Read_DefaultsApplied` — VC O3; FUNC §8.8
  - `TestMessageService_Get_ReturnsRecordAtOffset`, `TestMessageService_Get_CompactedAwayIsNotFound`, `TestMessageService_Get_OutOfRangeIsNotFound` — FUNC §8.7 M2; TECH C10
  - `TestMessageService_Read_NoCommitsNoGroupJoin` — VC O4

### Task 3.4: Routes, parsers, tests, fuzz, benchmarks (M1, M2)
- **Status:** Not Started
- **Source:** TECH §6.2, §4.5 (O3, Scan, Decoding), §4.6 (fuzz), §4.7 (benchmarks), §4.8
- **Subtasks:**
  - [ ] 3.4.1 `internal/api/message/routes.go`: `GET /v1/topics/{name}/messages`, `GET /v1/topics/{name}/partitions/{partition}/messages/{offset}`; `from=`/`to=` parser (`beginning|latest|offset:<n>|timestamp:<ms|iso>`) — TECH §6.2; FUNC §8.7 M1
  - [ ] 3.4.2 Component tests: O3 rows (limit > ceiling, `maxMessages`, `maxBytes`, `maxTime` via `Latency` + clock), decoding rows, M2 404 — TECH §4.5
  - [ ] 3.4.3 Fuzz targets: `scan.Decode`, header encoding, `from=`/`to=` parser; seed corpora in `testdata/fuzz/` — TECH §4.6
  - [ ] 3.4.4 `BenchmarkScanRun/{1k,10k}records`, `BenchmarkDecode/{json,string,binary}`, `BenchmarkEncodeScanEnvelope` — TECH §4.7
  - [ ] 3.4.5 Acceptance tests M1, M2; remove from `pending` — TECH §4.8
- **Tests (Definition of Done):**
  - `TestParseFrom_AllForms` (`beginning`, `latest`, `offset:n`, `timestamp:ms`, `timestamp:iso`, invalid → `VALIDATION_FAILED`) — FUNC §8.7 M1
  - `TestAPI_M1_LimitAboveCeiling400BoundExceeded`, `TestAPI_M1_StoppedByMaxMessagesWithContinuation`, `TestAPI_M1_StoppedByMaxBytes`, `TestAPI_M1_StoppedByMaxTime`, `TestAPI_M1_LatestOrdering`, `TestAPI_M1_FormatBase64` — VC O3; 4.5 Decoding
  - `TestAPI_M2_ReturnsRecord`, `TestAPI_M2_404Envelope` — VC O5
  - `FuzzDecode`, `FuzzHeaderEncoding`, `FuzzParseFromTo` run 10 s green in CI with committed seeds — TECH §4.6
  - Level 2: `TestAcceptance_M1_ReadMessages`, `TestAcceptance_M2_GetMessage` — TECH §4.8

---

## Phase 4: Search messages by regex and JSONPath
- **Phase Status:** Not Started
- **Goal:** A human can search a topic by regex or a structured JSONPath filter and see scan statistics including skipped records.
- **Manual Test Plan:**
  1. CS3 `POST /v1/topics/t-demo/messages/search` body `{"regex":"FAILED","from":"beginning"}` → only matching items; `scan.scanned ≥ scan.matched`.
  2. Body `{"regex":"("}` → `400 INVALID_REGEX`.
  3. Regex `(a+)+$` against 1 000 records → responds in < 2 s (linear-time engine).
  4. `POST /v1/topics/t-demo/messages/filter` body `{"filter":{"path":"$.status","op":"eq","value":"FAILED"}}` → same matches as step 1; `"op":"gt"` on a string field → 0 matches, no error.
  5. Seed one non-JSON record → the filter call reports `scan.skipped=1`.
  6. `"maxMatches":2` → exactly 2 items, `stoppedBy="maxMessages"`.
  7. Body containing `"limit":5` → `400 VALIDATION_FAILED`.
- **Step 10 verification:** concrete ✓ · self-contained ✓ · automated coverage ✓

### Task 4.1: Regex and JSONPath matchers
- **Status:** Not Started
- **Source:** FUNC §8.7 M3, M4, V4, §9.2 Evaluate (skip semantics), §8.8 (regex per-message timeout); TECH §1.1 (`regexp` RE2, `theory/jsonpath` v0.12.1), O3 rule, D5 (containment in `scan`)
- **Subtasks:**
  - [ ] 4.1.1 `internal/scan/regex.go`: compile (invalid → error mapped to `INVALID_REGEX`), `fields` selection (value/key/headers), `caseInsensitive`, per-record timeout guard (defence-in-depth) — FUNC §8.7 M3
  - [ ] 4.1.2 `jsonpath.go`: compile `path` (invalid → `INVALID_JSONPATH`), ops `eq neq contains regex exists gt lt gte lte`, match if any selected node satisfies; JSON-parse failure or type mismatch → skipped/non-match — FUNC V4, §9.2
  - [ ] 4.1.3 Unit tests: each op ± case, missing path, type mismatch, nested-quantifier pattern completes within a time bound — TECH §4.5 Regex/JSONPath
- **Tests (Definition of Done):**
  - `TestRegexMatcher_FieldsValueKeyHeaders`, `TestRegexMatcher_CaseInsensitive`, `TestRegexMatcher_InvalidPatternError`, `TestRegexMatcher_UndecodableIsSkipped` — FUNC §8.7 M3; §9.2
  - `TestRegexMatcher_NestedQuantifierCompletesWithinBound` (RE2 linear-time assertion, not a timeout) — 4.5 Regex/JSONPath
  - `TestJSONPathMatcher_Op_eq_Match`, `…_eq_NoMatch`, and the same pair for `neq`, `contains`, `regex`, `exists`, `gt`, `lt`, `gte`, `lte` (18 cases) — FUNC V4
  - `TestJSONPathMatcher_MissingPathNoMatch`, `TestJSONPathMatcher_TypeMismatchNoMatch`, `TestJSONPathMatcher_NonJSONSkipped`, `TestJSONPathMatcher_InvalidPathError`, `TestJSONPathMatcher_AnyNodeSatisfies` — FUNC V4, §9.2

### Task 4.2: Message service (M3, M4)
- **Status:** Not Started
- **Source:** FUNC §8.7 M3/M4 (`limit` not accepted; `maxScan`, `maxMatches`), §8.8 (`maxMatches` 100 / 1 000); TECH §6.1 B8
- **Subtasks:**
  - [ ] 4.2.1 `Search` and `Filter` on `message.Service` composing `scan.Run` with the matchers; `maxScan`/`maxMatches`/`maxBytes`/`maxTime` validated against ceilings — FUNC §8.8
  - [ ] 4.2.2 Unit tests for bound interplay (`maxMatches` stops before `maxScan`) — TECH §4.5 O3
- **Tests (Definition of Done):**
  - `TestMessageService_Search_MaxMatchesStopsBeforeMaxScan`, `TestMessageService_Search_MaxScanStopsBeforeMaxMatches` — VC O3; FUNC §8.8
  - `TestMessageService_Search_MaxMatchesAboveCeilingIsBoundExceeded` — TECH §6.1 B8
  - `TestMessageService_Filter_SkippedCountedNotErrored` — FUNC §9.2

### Task 4.3: Routes, tests, fuzz, benchmarks (M3, M4)
- **Status:** Not Started
- **Source:** TECH §6.2, §4.5, §4.6, §4.7, §4.8
- **Subtasks:**
  - [ ] 4.3.1 `POST /v1/topics/{name}/messages/search`, `POST /v1/topics/{name}/messages/filter`; DTOs reject `limit` (`VALIDATION_FAILED`) — TECH §6.2; TECH §6.1 B8
  - [ ] 4.3.2 Component tests: invalid regex/jsonpath → 400, skipped counting, each op through HTTP — TECH §4.5
  - [ ] 4.3.3 Fuzz target: JSONPath `path` compile never panics — TECH §4.6
  - [ ] 4.3.4 `BenchmarkRegexMatch`, `BenchmarkJSONPathEval` — TECH §4.7
  - [ ] 4.3.5 Acceptance tests M3, M4; remove from `pending` — TECH §4.8
- **Tests (Definition of Done):**
  - `TestAPI_M3_InvalidRegex400`, `TestAPI_M4_InvalidJSONPath400`, `TestAPI_M3M4_LimitRejected400ValidationFailed` — VC O5; TECH §6.1 B8
  - `TestAPI_M3_MatchesAndScanStats`, `TestAPI_M4_EachOpThroughHTTP` (table over the 9 ops), `TestAPI_M4_SkippedReported` — FUNC §8.3 scan envelope
  - `TestAPI_M3M4_ReaderAllowed` — VC O7
  - `FuzzJSONPathCompile` 10 s green — TECH §4.6
  - `BenchmarkRegexMatch`, `BenchmarkJSONPathEval` run under `make bench` — TECH §4.7
  - Level 2: `TestAcceptance_M3_SearchRegex`, `TestAcceptance_M4_FilterJSONPath` — TECH §4.8

---

## Phase 5: Produce messages, with audit
- **Phase Status:** Not Started
- **Goal:** An operator can produce single, batch, NDJSON-bulk, and tombstone records; every write is audited two-phase; a reader is refused.
- **Manual Test Plan:**
  1. CS2 with `auditSink: stdout`.
  2. CS3 `POST /v1/topics/t-demo/messages` body `{"records":[{"key":"k1","value":{"a":1}}]}` → `items[0].offset`; stdout shows an `ATTEMPT` then a `RESULT` audit line with `commandId:"M5"`, `severity:"INFO"`, `caller.keyId`.
  3. Same request with `$RD_KEY` → `403 TIER_FORBIDDEN` (the rejection audit line is asserted in Phase 6).
  4. Batch with a record carrying `"partition":99` → `400 BULK_VALIDATION_FAILED`, `details.items[0].index=0`; no `ATTEMPT` line.
  5. `POST /v1/topics/t-demo/messages/bulk` with an 11 MB NDJSON body → `413 PAYLOAD_TOO_LARGE`.
  6. `POST /v1/topics/t-demo/tombstones` `{"key":"k1"}` → `offset`; `GET …/messages?from=latest&limit=1` shows `value:null`.
  7. Restart with `auditSink: kafka`, `auditTopic: _kgw_audit` while the topic is absent → start-up logs ERROR; `/health/ready` → `200` with `audit.healthy=false`; step 2 → `503 AUDIT_UNAVAILABLE`.
  8. Create `_kgw_audit` with your CLI → step 2 succeeds; the topic contains an `ATTEMPT` and a `RESULT` record.
- **Step 10 verification:** concrete ✓ · self-contained ✓ (rejection-audit expectation moved to Phase 6 — F2) · automated coverage ✓

### Task 5.1: Producer port and adapters
- **Status:** Not Started
- **Source:** FUNC §8.1 (Producer surface), §8.7 M5; TECH C4 (`acks=all`, idempotent), L1–L4
- **Subtasks:**
  - [ ] 5.1.1 Port `Producer.Produce(ctx, records) ([]ProduceResult, error)` with per-record key/value/headers/partition/timestamp — FUNC §8.7 M5
  - [ ] 5.1.2 `franz/producer.go`: shared `kgo.Client` with `RequiredAcks(AllISRAcks)`, idempotence, `ProduceSync` — TECH §2.3, C4
  - [ ] 5.1.3 `fake/producer.go`: appends to the model with `now()` timestamp, honours `FailNext(Produce)` — TECH §4.3
  - [ ] 5.1.4 `porttest/producer.go`: produce then read back; partition out of range → error kind — TECH L2
- **Tests (Definition of Done):**
  - `porttest` cases `Producer_ProduceThenReadBackByteEqual`, `Producer_ExplicitPartitionHonoured`, `Producer_PartitionOutOfRangeIsError`, `Producer_TimestampPreservedOrNow` — TECH L2
  - `TestFranzProducer_OptionsAcksAllIdempotent` (unit: option construction) — TECH C4

### Task 5.2: Audit subsystem
- **Status:** Not Started
- **Source:** FUNC §8.5 (event schema, scope, two-phase, fail-closed, severity, caller nullability), F5, V2, V6, C14; TECH §2.2 (Sink interface), §2.3, I3, L5, §4.4 (`audittest`), C5 (dedicated producer), §6.1 B5
- **Subtasks:**
  - [ ] 5.2.1 `internal/audit/event.go`: `Event` exactly as FUNC §8.5 (`eventId`, `timestamp`, `requestId`, `phase`, `severity`, `commandId`, `commandName`, `target`, `caller{keyId nullable, tier, clientIp}`, `dryRun`, `breakGlass?`, `outcome?`, `error?`, `durationMs?`) — FUNC §8.5
  - [ ] 5.2.2 `sink.go`: `Sink interface { Write(ctx, Event) error }` — TECH I3
  - [ ] 5.2.3 `sink_slog.go`: structured JSON to stdout via the audit logger — FUNC F5
  - [ ] 5.2.4 `sink_kafka.go`: dedicated `Producer` (acks=all, idempotent) to `KGW_AUDIT_TOPIC`; bounded by ctx; returns errors (never blocks) — TECH C5, L5
  - [ ] 5.2.5 `auditor.go`: `Attempt(ctx, ev) error` (any sink error → error → fail-closed 503), `Result(ctx, ev)`; severity rules (INFO/WARN/HIGH incl. T7, T8, T11, T12, G5 success = HIGH; break-glass = HIGH) — FUNC V2, V6
  - [ ] 5.2.6 `audittest/recording.go`: `Events()`, `FailNext(phase)`, `FailAlways()` — TECH §4.4
  - [ ] 5.2.7 Unit tests: fail-closed with each sink type, severity table, concurrency (`-race`) — TECH L5, §4.5 V2/V6
- **Tests (Definition of Done):**
  - `TestAuditEvent_SchemaFieldsMatchSpec` (JSON keys exactly FUNC §8.5) — FUNC §8.5
  - `TestAuditor_AttemptThenResultOrdering`, `TestAuditor_DryRunEmitsSingleResult` — FUNC §8.5 two-phase
  - `TestAuditor_AttemptSinkFailureReturnsError_Slog`, `…_Kafka` (fake producer `FailNext`), `…_Recording` — VC V2; TECH L5
  - `TestAuditor_SeverityTable` (INFO success/dry-run; WARN rejected/failed; HIGH break-glass and successful T7, T8, T11, T12, G5) — VC V6
  - `TestAuditor_CallerKeyIDNilOn401AndAuthDisabled` — TECH C14
  - `TestKafkaSink_BoundedByCtxAndReturnsError`, `TestKafkaSink_UsesDedicatedProducer` — TECH C5, L5
  - `TestSlogSink_NeverLogsPasswordField` — FUNC §8.2; TECH C7
  - `TestAuditor_Race` — TECH §4.9

### Task 5.3: Bulk semantics and message service (M5, M6, M7)
- **Status:** Not Started
- **Source:** FUNC §9.4 (validate-all, then execute), V3, §8.3 (bulk envelope 200/207), §8.7 M5–M7, §8.8 (M6 body 10 MB); TECH §2.3
- **Subtasks:**
  - [ ] 5.3.1 `internal/service/core/bulk.go`: generic validate-all → `BulkValidationFailed{items}` (nothing executes, no ATTEMPT) → ATTEMPT → per-item execute → `BulkResult{items, summary}` → RESULT — FUNC §9.4
  - [ ] 5.3.2 `message.Service.Produce` (single object or `records[]`; validation: encodings, partition range, timestamp), `ProduceBulk` (NDJSON / JSON array stream, body limit → 413), `Tombstone` — FUNC §8.7 M5–M7
  - [ ] 5.3.3 Unit tests: one invalid item → nothing produced; injected per-item failure → 207 — TECH §4.5 V3
- **Tests (Definition of Done):**
  - `TestBulk_OneInvalidItemNothingExecutesNoAttempt`, `TestBulk_AllValidAttemptThenExecuteThenResult`, `TestBulk_ExecutionFailureIsMixedWithPerItemStatus`, `TestBulk_SummaryCounts` — VC V3; FUNC §9.4
  - `TestMessageService_Produce_SingleObjectAndArrayAccepted`, `TestMessageService_Produce_PartitionOutOfRangeInvalid`, `TestMessageService_Produce_EncodingsApplied` — FUNC §8.7 M5
  - `TestMessageService_ProduceBulk_NDJSONAndJSONArray`, `TestMessageService_ProduceBulk_BodyLimitIs413` — FUNC §8.7 M6, §8.8
  - `TestMessageService_Tombstone_NullValueProduced` — FUNC §8.7 M7

### Task 5.4: Audit-topic verification and readiness field
- **Status:** Not Started
- **Source:** TECH §6.1 B5 (never auto-create; verify at start-up; `/health/ready` reports without gating); FUNC §8.5 audit-topic bullet
- **Subtasks:**
  - [ ] 5.4.1 `internal/app`: when sink = kafka, verify topic exists and a probe produce succeeds; log ERROR on failure; expose sink health to `health/ready` — TECH §6.1 B5
  - [ ] 5.4.2 `docs/operations.md` section: audit topic settings (`cleanup.policy=delete`, `retention.ms ≥ 1 y`, `min.insync.replicas=2`) — TECH §6.1 B5
- **Tests (Definition of Done):**
  - `TestApp_AuditTopicMissing_LogsErrorAndMarksSinkUnhealthy` (fake without the topic) — TECH §6.1 B5
  - `TestApp_AuditTopicNeverAutoCreated` (fake `MutatingCalls()` contains no `CreateTopics` at start-up) — TECH §6.1 B5
  - `TestHealth_ReadyStaysUpWithAuditUnhealthy` — TECH §6.1 B5
  - 5.4.2: review-checklist item — the audit-topic settings paragraph exists in `docs/operations.md` (no automated test in TECH §4; accepted at Step 10)

### Task 5.5: Routes, tests, fuzz, acceptance (M5, M6, M7)
- **Status:** Not Started
- **Source:** TECH §6.2, §4.5 (O7, V2, V3), §4.6 (NDJSON fuzz), §4.8
- **Subtasks:**
  - [ ] 5.5.1 `POST /v1/topics/{name}/messages`, `POST …/messages/bulk` (`application/x-ndjson` and JSON array; `http.MaxBytesReader`), `POST …/tombstones` — TECH §6.2; FUNC §8.2 content types
  - [ ] 5.5.2 Component tests: O7 matrix for M5–M7 (reader → 403, no key → 401), ATTEMPT→RESULT ordering, rejection `RESULT` on 403, V2 (`audittest.FailNext(ATTEMPT)` → 503 + zero `MutatingCalls`), V3, 413 — TECH §4.5
  - [ ] 5.5.3 Fuzz target: NDJSON body parser — TECH §4.6
  - [ ] 5.5.4 Acceptance tests M5, M6, M7; remove from `pending` — TECH §4.8
- **Tests (Definition of Done):**
  - `TestAPI_M5M6M7_ReaderIs403TierForbidden`, `TestAPI_M5M6M7_NoKeyIs401` — VC O7
  - `TestAPI_M5_AttemptBeforeProduceBeforeResult` (recording sink + fake call order) — FUNC §8.5
  - `TestAPI_M5_AuditAttemptFailure503NoMutation` — VC V2
  - `TestAPI_M5_BulkValidationFailed400WithItems`, `TestAPI_M5_PartialExecutionFailure207` — VC V3
  - `TestAPI_M6_NDJSONAccepted`, `TestAPI_M6_OversizedBody413`, `TestAPI_M6_UnsupportedMediaType415` — FUNC §8.4
  - `TestAPI_M7_TombstoneOffsetReturned` — FUNC §8.7 M7
  - `FuzzNDJSONParser` 10 s green — TECH §4.6
  - Level 2: `TestAcceptance_M5_Produce`, `TestAcceptance_M6_ProduceBulk`, `TestAcceptance_M7_Tombstone` — TECH §4.8

---

## Phase 6: Read-only mode and the data-plane lock
- **Phase Status:** Not Started
- **Goal:** An operator can freeze all mutations (F2) or lock the message data plane (F6) by configuration, and break glass per request with a HIGH audit trail.
- **Manual Test Plan:**
  1. CS2 with `readOnlyMode: true` → CS3 produce → `403 READ_ONLY_MODE`; `GET /v1/topics` → `200`.
  2. Add `X-Break-Glass-Reason: incident-42` to the produce → still `403 READ_ONLY_MODE`.
  3. Restart with `readOnlyMode: false`, `dataPlaneLock: true` → `GET /v1/topics/t-demo/messages` → `403 DATA_PLANE_LOCKED`; `GET /v1/topics` → `200`.
  4. Same GET with `X-Break-Glass-Reason: incident-42` and `$OP_KEY` → `200`; audit line `severity:"HIGH"`, `breakGlass.reason:"incident-42"`, `commandId:"M1"`.
  5. Same with `$RD_KEY` → `403 DATA_PLANE_LOCKED`; audit line `severity:"HIGH"`, `outcome:"REJECTED"`.
  6. Header value of 600 bytes → the audit line shows a 512-byte reason.
  7. Restart with both switches off; produce with `$RD_KEY` → `403 TIER_FORBIDDEN`; one `RESULT` audit line with `outcome:"REJECTED"`, `severity:"WARN"`, `commandId:"M5"` (moved here from Phase 5 — F2).
- **Step 10 verification:** concrete ✓ · self-contained ✓ · automated coverage ✓

### Task 6.1: Wire F2 and F6, break-glass, and rejection audit events
- **Status:** Not Started
- **Source:** FUNC §9.1 rules (rejection events; break-glass bypasses F6 only — C1), §9.5 (data-plane lock), D5, O9, F2, F6, §8.2 (`X-Break-Glass-Reason` cap — C2); TECH §2.0 P1
- **Subtasks:**
  - [ ] 6.1.1 `middleware/apikey.go`: parse `X-Break-Glass-Reason` (cap 512 bytes, strip control chars) into `Caller.BreakGlassReason` — FUNC §8.2 (C2)
  - [ ] 6.1.2 `gates.Check` + `core.run`: emit `RESULT`/`REJECTED` audit on any 401/403 for `W` commands and on any break-glass attempt (WARN; HIGH when break-glass) — FUNC §9.1 rules
  - [ ] 6.1.3 Break-glass reads (M1–M4) emit a `HIGH` `RESULT` event on success — FUNC §9.5
  - [ ] 6.1.4 Config → `Policy` wiring for `readOnlyMode`, `dataPlaneLock` — FUNC §8.2 switches
- **Tests (Definition of Done):**
  - `TestMiddleware_BreakGlassReason_CappedAt512AndControlCharsStripped` — TECH C2
  - `TestRun_WRejectionEmitsResultRejectedWarn` (401 and each 403 code on a `W` command) — FUNC §9.1 rules
  - `TestRun_BreakGlassAttemptByReaderEmitsResultRejectedHigh` — FUNC §9.5
  - `TestRun_BreakGlassReadSuccessEmitsResultHighWithReason` — FUNC §9.5
  - `TestRun_BreakGlassDoesNotBypassReadOnly` — FUNC §9.1 rules (C1)
  - `TestPolicy_FromConfigSwitches` — FUNC §8.2

### Task 6.2: Tests and acceptance for F2 / F6
- **Status:** Not Started
- **Source:** TECH §4.5 (O9 rows, §9.5 row), §4.8
- **Subtasks:**
  - [ ] 6.2.1 Component tests: F2 → every implemented `W` → 403; F6 → M1–M7 → 403; operator + header → 200 + HIGH; reader + header → 403 + HIGH; header ignored under F2; reason truncation — TECH §4.5 O9
  - [ ] 6.2.2 Acceptance tests for lock and read-only behaviour (config-driven runs) — TECH §4.8
- **Tests (Definition of Done):**
  - `TestAPI_ReadOnly_EveryImplementedWIs403ReadOnlyMode` (iterates `command.Table` W entries with a route) — VC O9
  - `TestAPI_ReadOnly_RRoutesUnaffected` — VC O9
  - `TestAPI_Lock_M1ToM7Are403DataPlaneLocked`, `TestAPI_Lock_NonDataPlaneUnaffected` — VC O9
  - `TestAPI_Lock_OperatorBreakGlass200AndHighAudit`, `TestAPI_Lock_ReaderBreakGlass403AndHighAudit`, `TestAPI_Lock_HeaderUnderReadOnlyStill403` — VC O9; FUNC §9.5
  - `TestAPI_Rejection_ReaderOnProduceEmitsWarnRejectedAudit` — FUNC §9.1 rules
  - `TestAPI_Lock_DryRunStillLocked` (activates when M8 exists — Phase 11; registered now as skipped-until with a TODO reference) — FUNC §9.1 rules
  - Level 2: `TestAcceptance_F2_ReadOnlyMode`, `TestAcceptance_F6_DataPlaneLockAndBreakGlass` — TECH §4.8

---

## Phase 7: Consumer-group inspection
- **Phase Status:** Not Started
- **Goal:** A human can list groups, describe one with lag, and find which groups consume a topic.
- **Manual Test Plan:**
  1. Run a console consumer in group `g1` on `t-demo` with your CLI; stop it after a few records.
  2. CS3 `GET /v1/consumer-groups` → `g1` with `state` and `memberCount`.
  3. `GET /v1/consumer-groups/g1` → `offsets[]` with `committed`, `end`, `lag`; `totalLag` equals the sum.
  4. `GET /v1/topics/t-demo/consumer-groups` → `groups[0].groupId="g1"`.
  5. `GET /v1/consumer-groups/nope` → `404 NOT_FOUND`.
- **Step 10 verification:** concrete ✓ · self-contained ✓ · automated coverage ✓

### Task 7.1: Group port and adapters
- **Status:** Not Started
- **Source:** FUNC §8.1, §8.7 G1–G3; TECH L1–L4
- **Subtasks:**
  - [ ] 7.1.1 Port: `ListGroups`, `DescribeGroups`, `FetchGroupOffsets` — FUNC §8.7
  - [ ] 7.1.2 `franz`, `fake`, `porttest` cases (missing group → `NotFound`) — TECH L2
- **Tests (Definition of Done):**
  - `porttest` cases `Admin_ListGroups_StateAndMemberCount`, `Admin_DescribeGroups_MissingIsNotFound`, `Admin_FetchGroupOffsets_PerPartition` — TECH L2

### Task 7.2: Group service, routes, tests, acceptance (G1–G3)
- **Status:** Not Started
- **Source:** FUNC §8.7 G1–G3, X3; TECH §6.2, S2, I2 (`Admin` only)
- **Subtasks:**
  - [ ] 7.2.1 `internal/service/group/service.go`: `List` (state filter, paging by id), `Describe` (lag = end − committed per partition, `totalLag`), `ConsumersOfTopic` — FUNC §8.7
  - [ ] 7.2.2 `internal/api/group/routes.go`: `GET /v1/consumer-groups`, `GET /v1/consumer-groups/{groupId}`; `api/topic`: `GET /v1/topics/{name}/consumer-groups` — TECH §6.2
  - [ ] 7.2.3 Component tests (pagination, 404, `AssertNoGroupJoin`), acceptance G1–G3; remove from `pending` — TECH §4.5, §4.8
- **Tests (Definition of Done):**
  - `TestGroupService_Describe_LagIsEndMinusCommittedAndTotal`, `TestGroupService_List_StateFilterAndStablePaging`, `TestGroupService_ConsumersOfTopic_ReverseLookup` — FUNC §8.7 G1–G3; 4.5 Pagination
  - `TestAPI_G1_PaginationEnvelope`, `TestAPI_G2_404Envelope`, `TestAPI_G3_GroupsForTopic`, `TestAPI_Groups_NoGroupJoinNoCommits` — VC O4, O5
  - Level 2: `TestAcceptance_G1_ListGroups`, `TestAcceptance_G2_DescribeGroup`, `TestAcceptance_G3_TopicConsumers` — TECH §4.8

---

## Phase 8: Topic administration — create, bulk create, alter config, add partitions
- **Phase Status:** Not Started
- **Goal:** An operator can create topics (with validate-only), bulk-create, change configs and add partitions — the first destructive commands, with confirm echo, dry-run, and per-operation switches.
- **Manual Test Plan:**
  1. CS3 `POST /v1/topics` `{"name":"t-new","partitions":2,"replicationFactor":1}` → `201`; repeat → `409 ALREADY_EXISTS`; `?dryRun=true` with a new name → `200 {"dryRun":true,"plan":…}` and the topic does not exist afterwards.
  2. `POST /v1/batch/topics` with one already-existing name in the list → `400 BULK_VALIDATION_FAILED`; nothing created.
  3. `PATCH /v1/topics/t-new/config` `{"set":{"retention.ms":"60000"}}` (no `confirm`) → `400 CONFIRMATION_MISMATCH`; with `"confirm":"t-new"` → `200`, `configs[]` shows `retention.ms=60000`, `source="dynamic"`; `?dryRun=true` → `plan.changes[]` with `from`/`to`.
  4. `POST /v1/topics/t-new/partitions` `{"confirm":"t-new","partitions":1}` → `400 PARTITION_MISMATCH`; `"partitions":4` → `partitionCount=4`.
  5. Restart CS2 with `disabledOperations: ["T10"]` → step 4 → `403 OPERATION_DISABLED`; step 3 still works.
  6. Audit lines for T9 and T10 show `severity:"INFO"`, `phase` ATTEMPT then RESULT.
- **Step 10 verification:** concrete ✓ (step 3 `source` normalised — F3) · self-contained ✓ · automated coverage ✓

### Task 8.1: Plan / Apply helper for destructive commands
- **Status:** Not Started
- **Source:** FUNC F3, F4, O8, §8.6 (confirmation targets, plans), §9.1 lower half (confirm → dryRun → ATTEMPT → execute → RESULT), §9.1 rules (lock applies with `dryRun`); TECH §2.3 (Plan/Apply), O2
- **Subtasks:**
  - [ ] 8.1.1 `internal/service/core/destructive.go`: `Destructive[P, T](ctx, caller, descriptor, confirm, dryRun, plan func() (P, error), apply func(P) (T, error))` — checks F3 via `gates`, compares `confirm` to `plan.ConfirmTarget()`, returns `{dryRun:true, plan}` without ATTEMPT, else ATTEMPT → apply → RESULT (FAILED on error) — FUNC §9.1, §8.6
  - [ ] 8.1.2 `Plan` interface `{ ConfirmTarget() string }`; dry-run envelope type — FUNC §8.3 dry-run
  - [ ] 8.1.3 Unit tests: missing/wrong confirm, dry-run zero mutations, ATTEMPT/RESULT ordering, F3 switch — TECH §4.5 O8/O9
- **Tests (Definition of Done):**
  - `TestDestructive_MissingConfirmIsConfirmationMismatch`, `TestDestructive_WrongConfirmIsConfirmationMismatch` — VC O8
  - `TestDestructive_DryRunReturnsPlanWithZeroMutatingCallsAndSingleResultAudit` — VC O8; FUNC §8.5
  - `TestDestructive_AttemptThenApplyThenResultSucceeded`, `TestDestructive_ApplyErrorEmitsResultFailed` — FUNC §9.1
  - `TestDestructive_DisabledOperationIs403BeforePlan` — VC O9
  - `TestDestructive_AttemptSinkFailureAbortsBeforeApply` — VC V2

### Task 8.2: Port and adapters for topic administration
- **Status:** Not Started
- **Source:** FUNC §8.7 T5, T6, T9, T10; TECH L1–L4
- **Subtasks:**
  - [ ] 8.2.1 Port: `CreateTopics(ctx, specs, validateOnly)`, `IncrementalAlterTopicConfigs(set, reset)`, `CreatePartitions` — FUNC §8.7
  - [ ] 8.2.2 `franz` (kadm), `fake` (model mutation, `AlreadyExists`, partition decrease error), `porttest` cases — TECH L2
- **Tests (Definition of Done):**
  - `porttest` cases `Admin_CreateTopics_ValidateOnlyCreatesNothing`, `Admin_CreateTopics_ExistingIsAlreadyExists`, `Admin_IncrementalAlterTopicConfigs_SetAndResetToDefault`, `Admin_CreatePartitions_IncreaseSucceeds`, `Admin_CreatePartitions_DecreaseIsError` — TECH L2

### Task 8.3: Topic service, routes, tests, acceptance (T5, T6, T9, T10)
- **Status:** Not Started
- **Source:** FUNC §8.6 (T9, T10 rows), §8.7 T5, T6, T9, T10, §8.4 (`PARTITION_MISMATCH`, `ALREADY_EXISTS`), V3; TECH §6.2, §4.5
- **Subtasks:**
  - [ ] 8.3.1 `topic.Service.Create` (validate-only flag → plan), `CreateBulk` (validate-all: must not exist), `PlanAlterConfig`/`ApplyAlterConfig` (set + reset-to-default, plan `changes[]`), `PlanAddPartitions`/`Apply` (`to > from` else `PartitionMismatch`; warning in plan) — FUNC §8.6, §8.7
  - [ ] 8.3.2 Routes: `POST /v1/topics` (201), `POST /v1/batch/topics`, `PATCH /v1/topics/{name}/config`, `POST /v1/topics/{name}/partitions` — TECH §6.2
  - [ ] 8.3.3 Component tests: O8 rows for T9/T10, F3 (disable exactly one op), V3 for T6, 409, `dryRun` under data-plane lock unaffected (not data-plane) — TECH §4.5
  - [ ] 8.3.4 Acceptance tests T5, T6, T9, T10; remove from `pending` — TECH §4.8
- **Tests (Definition of Done):**
  - `TestTopicService_Create_ValidateOnlyReturnsPlanNoMutation`, `TestTopicService_Create_ExistingIsAlreadyExists` — FUNC §8.7 T5
  - `TestTopicService_CreateBulk_ExistingNameFailsValidationNothingCreated`, `TestTopicService_CreateBulk_PartialFailureIsMixed` — VC V3
  - `TestTopicService_AlterConfig_PlanChangesFromTo`, `TestTopicService_AlterConfig_ResetToDefault` — FUNC §8.6 T9
  - `TestTopicService_AddPartitions_ToNotGreaterIsPartitionMismatch`, `TestTopicService_AddPartitions_PlanCarriesWarning` — FUNC §8.6 T10
  - `TestAPI_T5_201Then409`, `TestAPI_T5_DryRun200PlanTopicAbsent`, `TestAPI_T6_400OnInvalidItem`, `TestAPI_T9_ConfirmationMismatch400`, `TestAPI_T9_DryRunPlan`, `TestAPI_T10_OperationDisabled403OnlyThatOp`, `TestAPI_T9T10_ResultSeverityInfo` — VC O8, O9; FUNC §8.5
  - Level 2: `TestAcceptance_T5_CreateTopic`, `TestAcceptance_T6_CreateTopicsBulk`, `TestAcceptance_T9_AlterConfig`, `TestAcceptance_T10_AddPartitions` — TECH §4.8

---

## Phase 9: Destructive topic operations
- **Phase Status:** Not Started
- **Goal:** An operator can delete a topic, bulk-delete by pattern with a plan token, truncate partitions, and purge — all HIGH-audited.
- **Manual Test Plan:**
  1. CS3 `DELETE /v1/topics/t-new` body `{"confirm":"t-new"}` → `200 {"deleted":"t-new"}`; audit `RESULT` line `severity:"HIGH"`.
  2. Create `tmp-a`, `tmp-b`. `POST /v1/batch/topics/delete?dryRun=true` `{"pattern":"^tmp-"}` → `plan.topics=["tmp-a","tmp-b"]`, `plan.planToken` (64 hex chars).
  3. Create `tmp-c`, then execute with `{"confirm":"<old token>","pattern":"^tmp-"}` → `400 CONFIRMATION_MISMATCH`; `details.plan.topics` lists three.
  4. Execute with the fresh token → `200`, `summary.ok=3`; the three topics are gone.
  5. Seed 10 records into `t-demo` partition 0. `POST /v1/topics/t-demo/delete-records` `{"confirm":"t-demo","offsets":{"0":5}}` → `partitions[0].lowWatermark=5`; `{"0":999}` → `400 VALIDATION_FAILED`.
  6. `POST /v1/topics/t-demo/purge` `{"confirm":"t-demo"}` → every partition `lowWatermark = endOffset`; `GET /v1/topics/t-demo/messages?from=beginning` → `items=[]`, `reachedEnd=true`.
- **Step 10 verification:** concrete ✓ · self-contained ✓ · automated coverage ✓

### Task 9.1: Plan token
- **Status:** Not Started
- **Source:** FUNC V5, §8.6 (plan token definition); TECH C8 (canonical form), §4.6 (fuzz)
- **Subtasks:**
  - [ ] 9.1.1 `internal/service/core/plantoken.go`: `Token(commandID, targets []string) string` = lower-case hex SHA-256 of `commandID + "\n" + sorted-unique targets joined by "\n"` — TECH C8
  - [ ] 9.1.2 Unit tests (order-independence, uniqueness per command) + fuzz target for canonicalisation — TECH §4.6
  - [ ] 9.1.3 `BenchmarkPlanToken` (1 and 1 000 targets) (Step 11 G1) — TECH §4.7
- **Tests (Definition of Done):**
  - `TestPlanToken_OrderIndependent`, `TestPlanToken_DuplicatesCollapsed`, `TestPlanToken_DistinctPerCommandID`, `TestPlanToken_Is64LowercaseHex`, `TestPlanToken_KnownVector` — TECH C8
  - `FuzzPlanTokenCanonical` (permutations produce equal tokens) 10 s green — TECH §4.6
  - `BenchmarkPlanToken/1target`, `BenchmarkPlanToken/1000targets` run under `make bench` — TECH §4.7 (Step 11 G1)

### Task 9.2: Port and adapters for deletion
- **Status:** Not Started
- **Source:** FUNC §8.7 T7, T8, T11, T12; TECH L1–L4
- **Subtasks:**
  - [ ] 9.2.1 Port: `DeleteTopics`, `DeleteRecords(map[partition]offset)` — FUNC §8.7
  - [ ] 9.2.2 `franz`, `fake` (raises begin offset; `truncateTo > end` error), `porttest` cases — TECH §4.3, L2
- **Tests (Definition of Done):**
  - `porttest` cases `Admin_DeleteTopics_RemovesTopic`, `Admin_DeleteTopics_MissingIsNotFound`, `Admin_DeleteRecords_RaisesBeginOffset`, `Admin_DeleteRecords_BeyondEndIsError` — TECH L2

### Task 9.3: Topic service, routes, tests, acceptance (T7, T8, T11, T12)
- **Status:** Not Started
- **Source:** FUNC §8.6 (T7, T8, T11, T12 rows), §8.7, V6 (HIGH), §8.2 `?pattern=`; TECH §6.2, §4.5 (O8, V6)
- **Subtasks:**
  - [ ] 9.3.1 `topic.Service`: `PlanDelete`/`ApplyDelete` (404 if missing), `PlanBulkDelete`/`Apply` (list or regex → resolved sorted topics + token; stale token → `ConfirmationMismatch` with fresh plan in `details`), `PlanDeleteRecords`/`Apply` (per partition `beginOffset`, `truncateTo`, `approxRecordsAffected`; `truncateTo ≤ end`), `PlanPurge`/`Apply` (= delete-records at end offsets) — FUNC §8.6
  - [ ] 9.3.2 Routes: `DELETE /v1/topics/{name}` (JSON body), `POST /v1/batch/topics/delete`, `POST /v1/topics/{name}/delete-records`, `POST /v1/topics/{name}/purge` — TECH §6.2, R3 note in OpenAPI descriptions
  - [ ] 9.3.3 Component tests: O8 rows incl. stale token, V6 HIGH on T7/T8/T11/T12, bulk 200/207 — TECH §4.5
  - [ ] 9.3.4 Acceptance tests T7, T8, T11, T12; remove from `pending` — TECH §4.8
- **Tests (Definition of Done):**
  - `TestTopicService_Delete_MissingIsNotFound`, `TestTopicService_Delete_PlanApproxMessages` — FUNC §8.6 T7
  - `TestTopicService_BulkDelete_PatternResolvesSortedTopics`, `TestTopicService_BulkDelete_ListMode`, `TestTopicService_BulkDelete_StaleTokenMismatchCarriesFreshPlan` — FUNC §8.6 T8, V5
  - `TestTopicService_DeleteRecords_PlanPerPartition`, `TestTopicService_DeleteRecords_TruncateAboveEndInvalid` — FUNC §8.6 T11
  - `TestTopicService_Purge_EqualsDeleteRecordsAtEndOffsets` — FUNC §8.6 T12
  - `TestAPI_T7_DeleteWithBodyConfirm`, `TestAPI_T8_DryRunReturnsTopicsAndToken`, `TestAPI_T8_StaleToken400WithDetailsPlan`, `TestAPI_T11_LowWatermarkReturned`, `TestAPI_T12_PurgeThenReadEmpty` — VC O8
  - `TestAPI_T7T8T11T12_ResultSeverityHigh` — VC V6
  - Level 2: `TestAcceptance_T7_DeleteTopic`, `TestAcceptance_T8_BulkDelete`, `TestAcceptance_T11_DeleteRecords`, `TestAcceptance_T12_Purge` — TECH §4.8

---

## Phase 10: Consumer-group administration
- **Phase Status:** Not Started
- **Goal:** An operator can reset, delete, evict members from, and clone offsets between consumer groups, with `GROUP_ACTIVE` protection.
- **Manual Test Plan:**
  1. With a live consumer in `g1`: CS3 `POST /v1/consumer-groups/g1/reset-offsets` `{"confirm":"g1","target":{"mode":"earliest"}}` → `409 GROUP_ACTIVE`.
  2. Stop the consumer → same call → `offsets[].after=0`; `{"mode":"timestamp","timestampMs":<far future>}` → `after` equals each partition's end offset.
  3. `POST /v1/consumer-groups/g2/reset-offsets` for a non-existent `g2` with `"topics":["t-demo"]`, `mode:"latest"` → `200`; `GET /v1/consumer-groups/g2` → offsets present (pre-seeded).
  4. `POST /v1/consumer-groups/g2/clone-offsets` `{"confirm":"g2","source":"g1"}` → `offsets[]` equal to `g1`'s.
  5. Start a consumer in `g1`; `POST /v1/consumer-groups/g1/remove-members` `{"confirm":"g1"}` → `removed[]` non-empty; `GET /v1/consumer-groups/g1` within 10 s shows `memberCount ≥ 1` again (F4).
  6. `DELETE /v1/consumer-groups/g2` `{"confirm":"g2"}` → `200 {"deleted":"g2"}`; audit `RESULT` `severity:"HIGH"`.
- **Step 10 verification:** concrete ✓ (step 5 rewritten — F4) · self-contained ✓ · automated coverage ✓

### Task 10.1: Port and adapters for group administration
- **Status:** Not Started
- **Source:** FUNC §8.7 G4–G7; TECH L1–L4
- **Subtasks:**
  - [ ] 10.1.1 Port: `CommitGroupOffsets(group, offsets)` (creates the group when absent), `DeleteGroups`, `LeaveGroup(group, members)` — FUNC §8.7
  - [ ] 10.1.2 `franz` (kadm incl. `LeaveGroup`), `fake` (`GroupActive` when members present), `porttest` cases — TECH §1.1, L2
- **Tests (Definition of Done):**
  - `porttest` cases `Admin_CommitGroupOffsets_CreatesAbsentGroup`, `Admin_CommitGroupOffsets_ActiveGroupIsGroupActive`, `Admin_DeleteGroups_ActiveIsGroupActive`, `Admin_DeleteGroups_InactiveRemoved`, `Admin_LeaveGroup_RemovesListedMembers` — TECH L2

### Task 10.2: Group service, routes, tests, acceptance (G4–G7)
- **Status:** Not Started
- **Source:** FUNC §8.6 (G4–G7 rows), §8.7 G4–G7 (G7 target in path), §9.1 rules (G4 timestamp clamping — C10), §8.4 `GROUP_ACTIVE`, V6 (G5 HIGH); TECH §6.2, §4.5
- **Subtasks:**
  - [ ] 10.2.1 `group.Service`: `PlanReset`/`Apply` (modes earliest/latest/offset/timestamp with clamping; explicit `offsets` map; `topics` scope; pre-seed), `PlanDelete`/`Apply`, `PlanRemoveMembers`/`Apply` (all or listed), `PlanCloneOffsets`/`Apply` (target inactive) — FUNC §8.6, C10
  - [ ] 10.2.2 Routes: `POST /v1/consumer-groups/{groupId}/reset-offsets`, `DELETE /v1/consumer-groups/{groupId}`, `POST …/remove-members`, `POST /v1/consumer-groups/{target}/clone-offsets` — TECH §6.2
  - [ ] 10.2.3 Component tests: 409 on active group, clamping, pre-seed, HIGH on G5, O8 rows — TECH §4.5
  - [ ] 10.2.4 Acceptance tests G4–G7; remove from `pending` — TECH §4.8
- **Tests (Definition of Done):**
  - `TestGroupService_Reset_EachMode` (earliest, latest, offset, explicit map), `TestGroupService_Reset_TimestampClampsToLatestAndEarliest`, `TestGroupService_Reset_ActiveGroupIs409`, `TestGroupService_Reset_PreSeedsAbsentGroup`, `TestGroupService_Reset_PlanBeforeAfter` — FUNC §8.6 G4; TECH C10
  - `TestGroupService_Delete_ActiveIs409`, `TestGroupService_RemoveMembers_AllOrListed`, `TestGroupService_Clone_TargetActiveIs409`, `TestGroupService_Clone_CopiesSourceOffsets` — FUNC §8.6 G5–G7
  - `TestAPI_G4_409Envelope`, `TestAPI_G5_ResultSeverityHigh`, `TestAPI_G6_RemovedList`, `TestAPI_G7_TargetInPathConfirmIsTarget` — VC O8, V6
  - Level 2: `TestAcceptance_G4_ResetOffsets`, `TestAcceptance_G5_DeleteGroup`, `TestAcceptance_G6_RemoveMembers`, `TestAcceptance_G7_CloneOffsets` — TECH §4.8

---

## Phase 11: Replay
- **Phase Status:** Not Started
- **Goal:** An operator can copy a bounded range of records from one topic to another and resume from a cursor.
- **Manual Test Plan:**
  1. Seed 2 500 records into `src`; create `dst` with the same partition count.
  2. CS3 `POST /v1/replays` `{"confirm":"dst","source":{"topic":"src","from":"beginning"},"target":{"topic":"dst"},"limit":1000}` → `copied=1000`, `reachedEnd=false`, `cursor` per partition.
  3. Repeat with `source.from` = the returned cursor, twice → third response `reachedEnd=true`; `GET /v1/topics/dst` → `approxMessageCount=2500`.
  4. `"target":{"topic":"dst2","preservePartition":true}` where `dst2` has fewer partitions → `400 PARTITION_MISMATCH`.
  5. Read the same offset from `src` and `dst` → identical key, value, headers, `timestampMs`.
  6. `"limit":50000` → `400 BOUND_EXCEEDED`.
- **Step 10 verification:** concrete ✓ · self-contained ✓ · automated coverage ✓

### Task 11.1: Replay service (M8)
- **Status:** Not Started
- **Source:** FUNC §9.3 (procedure, at-least-once), D4, §8.6 (M8 row), §8.7 M8, §8.8 (N 1 000 / 10 000), §8.4 (`PARTITION_MISMATCH`, 502 with `progress`); TECH §2.3 (one scan loop reuse)
- **Subtasks:**
  - [ ] 11.1.1 `internal/service/message/replay.go`: `PlanReplay` (resolve source partitions, start offsets, end snapshot, target partition count; `preservePartition` check; `estimatedRecords`), `ApplyReplay` (`scan.Run` with `MatchAll`, emit = produce verbatim, partition by key or preserved; cursor; mid-batch produce failure → `KafkaError` with `details.progress{copied, cursor}`) — FUNC §9.3
  - [ ] 11.1.2 OpenAPI description states at-least-once semantics — FUNC §9.3 step 5
- **Tests (Definition of Done):**
  - `TestReplay_Plan_ResolvesPartitionsSnapshotAndEstimate`, `TestReplay_Plan_PreservePartitionMismatch` — FUNC §9.3 step 1
  - `TestReplay_Apply_CopiedCountAndCursor`, `TestReplay_Apply_ResumeFromCursorNoGapNoOverlap`, `TestReplay_Apply_ReachedEndOnLastBatch` — FUNC §9.3 step 4
  - `TestReplay_Apply_MidBatchProduceFailureIsKafkaErrorWithProgress` — FUNC §9.3 step 4
  - `TestReplay_Apply_HeadersTimestampsKeyByteEqual`, `TestReplay_Apply_PartitionByKeyUnlessPreserved` — 4.5 Replay
  - `TestReplay_LimitAboveCeilingIsBoundExceeded`, `TestReplay_ConfirmIsTargetTopic` — FUNC §8.6 M8, §8.8
  - `TestReplay_UsesScanRunAndAttemptResultAudit` — TECH §2.3; FUNC §9.3 step 2

### Task 11.2: Route, tests, acceptance (M8)
- **Status:** Not Started
- **Source:** TECH §6.2, §4.5 (Replay row), §4.8
- **Subtasks:**
  - [ ] 11.2.1 `POST /v1/replays` — TECH §6.2
  - [ ] 11.2.2 Component tests: `preservePartition` mismatch, `FailNext(Produce)` mid-batch → 502 with `progress`, byte-equal headers/timestamps, cursor resume, bound ceiling — TECH §4.5
  - [ ] 11.2.3 Acceptance test M8; remove from `pending` — TECH §4.8
- **Tests (Definition of Done):**
  - `TestAPI_M8_PreservePartitionMismatch400`, `TestAPI_M8_MidBatchFailure502WithProgress`, `TestAPI_M8_CursorResume`, `TestAPI_M8_LimitAboveCeiling400`, `TestAPI_M8_LockedByDataPlaneLockEvenDryRun` (activates `TestAPI_Lock_DryRunStillLocked` from Task 6.2) — 4.5 Replay; FUNC §9.1 rules
  - `TestOpenAPI_Golden` contains the at-least-once description on `POST /v1/replays` — FUNC §9.3 step 5
  - Level 2: `TestAcceptance_M8_Replay` — TECH §4.8

---

## Phase 12: Advanced cluster operations
- **Phase Status:** Not Started
- **Goal:** An operator can inspect KRaft quorum, log dirs, reassignments, throughput; export and import topic definitions; alter broker config; run reassignments and leader elections.
- **Manual Test Plan:**
  1. CS3 `GET /v1/cluster/quorum` → `leaderId`, `voters[]` (KRaft cluster).
  2. `GET /v1/cluster/log-dirs` → per-broker `logDir`, `totalBytes`.
  3. `GET /v1/cluster/throughput?topic=t-demo&seconds=3` while producing → `messagesPerSecond > 0`, response after ≈ 3 s; `seconds=100` → `400 BOUND_EXCEEDED`.
  4. `GET /v1/cluster/export?pattern=^t-` → `topics[]` with `configs` overrides only.
  5. Edit the export (change `retention.ms`, add `t-imported`): `POST /v1/batch/topics/apply?dryRun=true` → `plan.create=["t-imported"]`, `plan.alter=[…]`, `planToken`; execute with the token → `200`; dry-run again → everything under `unchanged`.
  6. `PATCH /v1/cluster/brokers/<id>/config` `{"confirm":"<id>","set":{"log.retention.hours":"168"}}` → `200`.
  7. `POST /v1/cluster/reassignments?dryRun=true` moving `t-demo/0` to another broker → `plan.moves[]`, token; execute; `GET /v1/cluster/reassignments` lists it until complete.
  8. `POST /v1/cluster/elections?dryRun=true` `{"elect":{"type":"PREFERRED","partitions":[{"topic":"t-demo","partition":0}]}}` → `plan.elections[]`, `planToken` (F5).
  9. `POST /v1/cluster/elections` `{"confirm":"<token from step 8>","elect":{"type":"PREFERRED","partitions":[{"topic":"t-demo","partition":0}]}}` → bulk envelope with `summary.ok ≥ 1`.
- **Step 10 verification:** concrete ✓ (explicit elections dry-run — F5) · self-contained ✓ · automated coverage ✓

### Task 12.1: Port and adapters for advanced operations
- **Status:** Not Started
- **Source:** FUNC §8.7 C5–C9; TECH §1.1 (kadm coverage, `kmsg.DescribeQuorumRequest`), C3 (`UNSUPPORTED_VERSION` mapping), L1–L4
- **Subtasks:**
  - [ ] 12.1.1 Port: `DescribeQuorum`, `ListPartitionReassignments`, `AlterPartitionAssignments`, `CancelPartitionReassignments`, `ElectLeaders(type, partitions)`, `IncrementalAlterBrokerConfigs`, `DescribeLogDirs` (all brokers) — FUNC §8.7
  - [ ] 12.1.2 `franz/quorum.go` via raw `kmsg.DescribeQuorumRequest`; `UNSUPPORTED_VERSION` → `KindUnsupported` (502 with `kafkaError.name`) — TECH §1.1, C3
  - [ ] 12.1.3 `fake` model for quorum, reassignments, broker configs; `porttest` cases — TECH §4.3
- **Tests (Definition of Done):**
  - `porttest` cases `Admin_DescribeQuorum_LeaderAndVoters`, `Admin_DescribeQuorum_UnsupportedIsKindUnsupported`, `Admin_Reassignments_ListAfterAlter`, `Admin_Reassignments_Cancel`, `Admin_ElectLeaders_PreferredAndUnclean`, `Admin_IncrementalAlterBrokerConfigs_SetAndReset`, `Admin_DescribeLogDirs_AllBrokers` — TECH L2, C3
  - `TestFranz_QuorumRequestBuiltFromKmsg` (unit: request construction) — TECH §1.1

### Task 12.2: Cluster service — inspection and destructive (C5–C12)
- **Status:** Not Started
- **Source:** FUNC §8.6 (C5, C9, C12 rows), §8.7 C5–C12, §8.8 (C10 seconds 5/60), V5 (plan tokens for C9, C12); TECH §6.3 C11 cap
- **Subtasks:**
  - [ ] 12.2.1 `cluster.Service`: `Quorum`, `Reassignments`, `LogDirs`, `Throughput` (two end-offset snapshots `seconds` apart, bounded) , `Export` (pattern; overrides only) — FUNC §8.7
  - [ ] 12.2.2 `destructive.go`: `PlanAlterBrokerConfig`/`Apply` (confirm = broker id string); `PlanReassign`/`Apply`, `PlanCancelReassignments`/`Apply`, `PlanElect`/`Apply` (plan token; bulk envelope); `PlanImport`/`Apply` (reconcile → `create/alter/delete/unchanged`; `allowDelete=false` → deletes skipped; token over `name=sha256(definition)`) — FUNC §8.6, V5; TECH C8
  - [ ] 12.2.3 Unit tests incl. `REASSIGNMENT_IN_PROGRESS` mapping — FUNC §8.4
- **Tests (Definition of Done):**
  - `TestClusterService_Throughput_TwoSnapshotsSecondsApart` (fixed clock), `TestClusterService_Throughput_SecondsAboveCeilingIsBoundExceeded` — FUNC §8.7 C10, §8.8
  - `TestClusterService_Export_PatternAndOverridesOnly` — FUNC §8.7 C11
  - `TestClusterService_Import_PlanCreateAlterDeleteUnchanged`, `TestClusterService_Import_AllowDeleteFalseSkipsDeletes`, `TestClusterService_Import_TokenOverNameAndDefinitionHash`, `TestClusterService_Import_StaleTokenMismatch` — FUNC §8.6 C12, V5; TECH C8
  - `TestClusterService_AlterBrokerConfig_ConfirmIsBrokerIDString` — FUNC §8.6 C5
  - `TestClusterService_Reassign_PlanMovesAndToken`, `TestClusterService_Reassign_StaleTokenMismatch`, `TestClusterService_Cancel_PlanAndApply`, `TestClusterService_Elect_BulkEnvelope` — FUNC §8.6 C9
  - `TestErrorTables_ReassignmentInProgressIs409` — FUNC §8.4

### Task 12.3: Routes, tests, acceptance (C5–C12)
- **Status:** Not Started
- **Source:** TECH §6.2, §4.5, §4.8
- **Subtasks:**
  - [ ] 12.3.1 Routes: `GET /v1/cluster/quorum`, `/reassignments`, `/log-dirs`, `/throughput`, `/export`; `PATCH /v1/cluster/brokers/{brokerId}/config`; `POST /v1/cluster/reassignments`, `POST /v1/cluster/reassignments/cancel`, `POST /v1/cluster/elections`; `POST /v1/batch/topics/apply` — TECH §6.2
  - [ ] 12.3.2 Component tests: O8 rows for C5/C9/C12 (stale tokens), bulk envelope, `Unsupported` → 502, throughput bound — TECH §4.5
  - [ ] 12.3.3 Acceptance tests C5–C12; remove from `pending` — TECH §4.8
- **Tests (Definition of Done):**
  - `TestAPI_C6_Unsupported502WithKafkaErrorName`, `TestAPI_C7_ListsReassignments`, `TestAPI_C8_LogDirs`, `TestAPI_C10_SecondsAboveCeiling400` — VC O5; TECH C3
  - `TestAPI_C5_ConfirmationMismatch400`, `TestAPI_C9_DryRunThenExecuteEachOp` (reassign, cancel, elect), `TestAPI_C9_StaleToken400`, `TestAPI_C12_DryRunThenApplyThenUnchanged`, `TestAPI_C11_ExportShape` — VC O8
  - `TestOpenAPI_Golden` updated; `pending` shrinks by 8 — VC O1
  - Level 2: `TestAcceptance_C5_AlterBrokerConfig`, `TestAcceptance_C6_Quorum`, `TestAcceptance_C7_Reassignments`, `TestAcceptance_C8_LogDirs`, `TestAcceptance_C9_ReassignCancelElect`, `TestAcceptance_C10_Throughput`, `TestAcceptance_C11_Export`, `TestAcceptance_C12_Import` — TECH §4.8

---

## Phase 13: SCRAM credentials, client quotas, and the complete OpenAPI document
- **Phase Status:** Not Started
- **Goal:** An operator can manage SCRAM users and quotas; all 41 commands (48 operations) are present in the served OpenAPI document.
- **Manual Test Plan:**
  1. CS3 `POST /v1/scram-users` `{"name":"alice","mechanism":"SCRAM-SHA-256","password":"s3cret"}` → `201 {"name":"alice","mechanism":"SCRAM-SHA-256"}`; the audit line contains no `s3cret`.
  2. `GET /v1/scram-users` → `alice` with `mechanisms[0].iterations=4096`.
  3. `DELETE /v1/scram-users/alice` `{"confirm":"alice"}` → `200`.
  4. `PATCH /v1/quotas` `{"confirm":"user:alice","entity":{"user":"alice"},"set":{"producerByteRate":1048576}}` → `200`; `GET /v1/quotas?entityType=user` lists it.
  5. `curl … /openapi.json | jq '[.paths[][]["x-command-id"]] | map(select(.)) | unique | length'` → `41`; total operations carrying an id → `48`.
  6. `go test ./internal/api -run Bijection` → pass with an empty `pending` list.
- **Step 10 verification:** concrete ✓ · self-contained ✓ · automated coverage ✓

### Task 13.1: Port and adapters for security commands
- **Status:** Not Started
- **Source:** FUNC §8.7 S1, S2; TECH §1.1 (kadm SCRAM, quotas), L1–L4
- **Subtasks:**
  - [ ] 13.1.1 Port: `DescribeUserSCRAMs`, `AlterUserSCRAMs(upsert, delete)`, `DescribeClientQuotas`, `AlterClientQuotas` — FUNC §8.7
  - [ ] 13.1.2 `franz`, `fake` (no secrets stored), `porttest` cases — TECH §4.3
- **Tests (Definition of Done):**
  - `porttest` cases `Admin_SCRAM_UpsertThenDescribeNoSecret`, `Admin_SCRAM_DeleteRemoves`, `Admin_SCRAM_DescribeMissingIsNotFound`, `Admin_Quotas_AlterThenDescribe`, `Admin_Quotas_RemoveKey` — TECH L2
  - `TestFake_SCRAM_StoresNoPasswordMaterial` — TECH §4.3

### Task 13.2: Security service, routes, tests, acceptance; close the bijection
- **Status:** Not Started
- **Source:** FUNC §8.6 (S1 delete, S2 alter rows), §8.7 S1, S2, §8.2 (passwords never echoed/logged), O1 (reworded); TECH §6.1 B2, §6.2, §5.1 (`docs/api/openapi.json`), C7, C11 (iterations 4096)
- **Subtasks:**
  - [ ] 13.2.1 `internal/service/security/service.go`: `ListUsers`, `CreateUser` (iterations default 4096; password never logged), `PlanDeleteUser`/`Apply`, `ListQuotas`, `PlanAlterQuota`/`Apply` (confirm = entity descriptor) — FUNC §8.6, §8.7
  - [ ] 13.2.2 Routes: `GET/POST /v1/scram-users`, `DELETE /v1/scram-users/{name}`, `GET /v1/quotas`, `PATCH /v1/quotas` — TECH §6.2
  - [ ] 13.2.3 Component tests (password absent from audit and logs, O8 rows), acceptance S1, S2 — TECH §4.5
  - [ ] 13.2.4 Empty the `pending` list; bijection test asserts 41 ids over 48 operations; `make openapi` exports `docs/api/openapi.json` from the golden — FUNC O1; TECH §6.1 B2, §5.1
- **Tests (Definition of Done):**
  - `TestSecurityService_CreateUser_PasswordAbsentFromAuditAndLogs`, `TestSecurityService_CreateUser_IterationsDefault4096`, `TestSecurityService_CreateUser_ExistingIsAlreadyExists` — TECH C7, C11; FUNC §8.4
  - `TestSecurityService_DeleteUser_ConfirmIsName`, `TestSecurityService_AlterQuota_ConfirmIsEntityDescriptor`, `TestSecurityService_AlterQuota_SetAndRemove` — FUNC §8.6 S1, S2
  - `TestAPI_S1_Create201NoPasswordEcho`, `TestAPI_S1_DeleteWithBodyConfirm`, `TestAPI_S2_PatchAndList` — VC O8
  - `TestOpenAPI_PendingListEmpty`, `TestOpenAPI_41IDsOver48Operations` (C3 ×2, C9 ×3, S1 ×3, S2 ×2) — VC O1; TECH §6.1 B2
  - `make openapi` writes `docs/api/openapi.json` byte-equal to `internal/api/testdata/openapi.golden.json` (CI step asserts no diff) — TECH §5.1
  - Level 2: `TestAcceptance_S1_SCRAMUsers`, `TestAcceptance_S2_Quotas` — TECH §4.8

---

## Phase 14: Supply chain and release
- **Phase Status:** Not Started
- **Goal:** A tagged commit produces a signed, SBOM-bearing, distroless image and release archives; nightly jobs fuzz longer and re-scan the image.
- **Manual Test Plan:**
  1. `goreleaser build --snapshot --clean` → `dist/` contains `kafka3o-gateway` binaries; `./dist/…/kafka3o-gateway --version` prints the snapshot version.
  2. `docker build -t kafka3o-gateway:dev .` → the final stage is the pinned distroless digest; `docker inspect kafka3o-gateway:dev --format '{{.Config.User}}'` → `nonroot`.
  3. `syft kafka3o-gateway:dev -o cyclonedx-json | jq '.components[] | select(.name|test("franz-go")) | .version'` → `v1.21.7`.
  4. `grype kafka3o-gateway:dev --fail-on high` → exit 0.
  5. Push tag `v0.0.1-rc1` to a fork → `release.yml` publishes image, SBOM, cosign signature, provenance; `cosign verify` on the image succeeds.
  6. Dispatch `nightly.yml` → fuzz job runs 10 min per target and finishes green; image re-scan step passes.
- **Step 10 verification:** concrete ✓ · self-contained ✓ · automated coverage: workflow runs + this plan (accepted at Step 10 — no unit tests exist for build artefacts in TECH §4)

### Task 14.1: Dockerfile
- **Status:** Not Started
- **Source:** TECH §1.1 (container row), §1.0 (supply chain), §5.1
- **Subtasks:**
  - [ ] 14.1.1 Multi-stage `Dockerfile`: `golang:1.27.1` builder (`CGO_ENABLED=0 -trimpath -ldflags=-s -w`), final `gcr.io/distroless/static-debian12:nonroot@sha256:<digest>`; entrypoint `/kafka3o-gateway` — TECH §1.1
  - [ ] 14.1.2 `make image` target — TECH §5.1
- **Tests (Definition of Done):**
  - `ci.yml` image job: `docker build` succeeds; `docker inspect` user is `nonroot`; the final `FROM` is digest-pinned (grep in the job); `grype --fail-on high` exit 0 — TECH §1.1, §1.3
  - Phase 14 MTP steps 2–4 pass (accepted CI + manual DoD)

### Task 14.2: Release pipeline
- **Status:** Not Started
- **Source:** TECH §1.0 (SBOM, signed reproducible images), §1.1 (goreleaser, syft, cosign, SLSA), §5.0 Y4 (artifact names), §5.1 (`release.yml`)
- **Subtasks:**
  - [ ] 14.2.1 `.goreleaser.yaml`: binary `kafka3o-gateway`, reproducible builds, `syft` SBOM (CycloneDX + SPDX), image `ghcr.io/misterkafkagod/kafka3o-gateway`, cosign keyless signing, Helm chart OCI push — TECH §1.1, Y4
  - [ ] 14.2.2 `.github/workflows/release.yml` on tag: goreleaser + GitHub Actions SLSA provenance attestations — TECH §1.1
- **Tests (Definition of Done):**
  - `goreleaser check` exit 0 in `ci.yml` — TECH §1.1
  - `release.yml` green on a tag pushed to a fork: image, CycloneDX + SPDX SBOMs, cosign signature, SLSA provenance present; `cosign verify` succeeds; SBOM lists `github.com/twmb/franz-go` `v1.21.7` — TECH §1.0, §1.1, Y4
  - Phase 14 MTP steps 1 and 5 pass (accepted CI + manual DoD)

### Task 14.3: Nightly pipeline and image scan gate
- **Status:** Not Started
- **Source:** TECH §4.10 (nightly), §1.3 (continuous policy), T4
- **Subtasks:**
  - [ ] 14.3.1 `.github/workflows/nightly.yml`: fuzz 10 min per target, benchmark trend artifact, image re-scan (`grype`/`trivy`, fail on critical/high) — TECH §4.10
  - [ ] 14.3.2 Image scan gate added to `ci.yml` after build — TECH §1.3
- **Tests (Definition of Done):**
  - `nightly.yml` dispatched manually: fuzz step runs every `Fuzz*` target for 10 min and is green; benchmark artifact uploaded; image re-scan step green — TECH §4.10, T4
  - `ci.yml` contains the image scan step and it fails on an image with a known-high CVE (one-time negative check) — TECH §1.3

### Task 14.4: Operations documentation
- **Status:** Not Started
- **Source:** TECH §5.1 (`docs/operations.md`), §6.3 (configuration keys), §6.5 R3, §5.4 (shutdown), FUNC §9.5 (break-glass procedure)
- **Subtasks:**
  - [ ] 14.4.1 Configuration reference generated from `config.example.yaml` including every §6.3 key; probes and `probe`/`keygen` subcommands; shutdown order; audit sink and topic; break-glass procedure; DELETE-with-body caveat (R3); supported broker range Kafka 3.3 → 4.x and the `UNSUPPORTED_VERSION` behaviour (Step 11 N4 — TECH C3) — TECH §5.1, §6.3, §6.5, C3
- **Tests (Definition of Done):**
  - Review-checklist item (accepted at Step 10 — no automated test in TECH §4): every key in `configs/config.example.yaml` appears in `docs/operations.md`; sections present for probes, subcommands, shutdown, audit topic, break-glass, R3 caveat, supported broker range — TECH §5.1, §6.3, §6.5, C3

---

## Phase 15: Kubernetes packaging (render-only verification)
- **Phase Status:** Not Started
- **Goal:** A Helm chart renders a valid, schema-checked deployment with exec probes, NetworkPolicy, PDB, HPA, and the grace-period rule enforced.
- **Manual Test Plan:**
  1. `helm lint deploy/helm/kafka3o-gateway` → 0 errors.
  2. `helm template kgw deploy/helm/kafka3o-gateway --set bounds.maxTimeMs=10000 | kubeconform -strict -summary` → PASS for Deployment, Service, ServiceAccount, ConfigMap, Secret, NetworkPolicy, PodDisruptionBudget, HorizontalPodAutoscaler.
  3. The rendered Deployment shows `livenessProbe.exec.command=["/kafka3o-gateway","probe","--live"]`, `readinessProbe … "--ready"`, `KGW_PROBE_API_KEY` from the Secret, `securityContext` with `runAsNonRoot: true`, `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, `capabilities.drop: [ALL]`, `seccompProfile.type: RuntimeDefault`, `terminationGracePeriodSeconds: 30`, `preStop` sleep 5.
  4. `--set bounds.maxTimeMs=60000 --set terminationGracePeriodSeconds=30` → `helm template` fails on `values.schema.json` (requires ≥ 70).
  5. Rendered NetworkPolicy egress includes UDP/TCP 53 plus the configured broker and OTLP CIDRs; ingress limited to the configured namespace selector.
- **Step 10 verification:** concrete ✓ · self-contained ✓ · automated coverage: `helm lint` + `kubeconform` CI steps — note for Step 11: `kubeconform` is approved in TASKS (Step 9 decision) but not yet listed in TECH §4.2/§4.10

### Task 15.1: Helm chart
- **Status:** Not Started
- **Source:** TECH §5.0 Y3, §5.1 (`deploy/helm/kafka3o-gateway`), §5.4 (every object row), §6.1 B3 (exec probes), §6.3 C12 (grace-period rule, DNS egress), §1.1 (resources)
- **Subtasks:**
  - [ ] 15.1.1 `Chart.yaml`, `values.yaml` (replicas 2, resources 100m/128Mi → 1 CPU/512Mi, bounds, switches, CORS, OTLP endpoint, audit, trusted proxies, image digest), `values.schema.json` with `terminationGracePeriodSeconds ≥ ceil(maxTimeMs/1000)+10` — TECH §5.4, C12
  - [ ] 15.1.2 Templates: `deployment.yaml` (securityContext, exec probes, preStop, env from Secret), `service.yaml`, `serviceaccount.yaml` (no token automount), `configmap.yaml` (`/etc/kafka3o/config.yaml`), `secret.yaml`, `networkpolicy.yaml` (egress brokers + OTLP + DNS; ingress namespace selector), `pdb.yaml` (`minAvailable: 1`), `hpa.yaml` (optional) — TECH §5.4
  - [ ] 15.1.3 `templates/tests/` connection test; chart `README.md` — TECH §5.1
- **Tests (Definition of Done):**
  - `helm lint deploy/helm/kafka3o-gateway` exit 0 (CI step) — TECH §5.4
  - `helm template … | kubeconform -strict` PASS for all eight kinds (CI step) — TECH §5.4
  - Rendered-output assertions in CI (grep/yq): exec probes with `probe --live` / `--ready`, `KGW_PROBE_API_KEY` from Secret, `runAsNonRoot`, `readOnlyRootFilesystem`, `drop: [ALL]`, `RuntimeDefault`, `automountServiceAccountToken: false`, PDB `minAvailable: 1`, NetworkPolicy DNS egress — TECH §5.4, §6.1 B3, C12
  - Schema negative case: `maxTimeMs=60000` with `terminationGracePeriodSeconds=30` fails `helm template` (CI step) — TECH C12
  - 15.1.3: review-checklist item — chart `README.md` documents every value (accepted at Step 10)

### Task 15.2: Chart checks in CI
- **Status:** Not Started
- **Source:** TECH §4.10
- **Subtasks:**
  - [ ] 15.2.1 `ci.yml` steps: `helm lint`, `helm template | kubeconform -strict`, schema negative case — TECH §4.10
- **Tests (Definition of Done):**
  - The three named steps exist in `ci.yml` and are green on a clean PR; the negative-schema step is asserted to fail on the bad values — TECH §4.10

---

## Phase 16: Acceptance report and release evidence
- **Phase Status:** Not Started
- **Goal:** One command runs all 41 acceptance tests plus the franz contract suite against a cluster and produces the release checklist document.
- **Manual Test Plan:**
  1. `go test -json -tags acceptance,integration ./test/... ./internal/kafka/franz/... | go run ./tools/acceptance-report` → `docs/acceptance/<version>-<date>.md` with 41 rows, every row `PASS`, broker version and run id filled.
  2. The `porttest` run against franz reports the same case names as the fake run in CI.
  3. Dispatch `acceptance.yml` with cluster secrets → the workflow commits the generated report.
- **Step 10 verification:** concrete ✓ · self-contained ✓ · automated coverage ✓

### Task 16.1: Acceptance report tool
- **Status:** Not Started
- **Source:** TECH §4.8 (report), §5.1 (`tools/acceptance-report`), T2
- **Subtasks:**
  - [ ] 16.1.1 `tools/acceptance-report/main.go`: reads `go test -json`, maps `TestAcceptance_<ID>_*` to catalog ids, writes `docs/acceptance/<version>-<date>.md` (id, name, result, duration, broker version, run id); non-zero exit if any id is missing or failed — TECH §4.8
- **Tests (Definition of Done):**
  - `TestAcceptanceReport_MapsTestNamesToCatalogIDs` (fixture `testdata/gotest.json`) — TECH §4.8
  - `TestAcceptanceReport_FailsOnMissingID`, `TestAcceptanceReport_FailsOnAnyFail` — TECH §4.8
  - `TestAcceptanceReport_MarkdownGolden` (`testdata/report.golden.md`) — TECH §4.8

### Task 16.2: franz contract run under the `integration` tag
- **Status:** Not Started
- **Source:** TECH L1, §4.8, §5.1 (`franz_integration_test.go`)
- **Subtasks:**
  - [ ] 16.2.1 `internal/kafka/franz/franz_integration_test.go` (`//go:build integration`): `porttest.Run(t, franz.New(cfg))` against `KAFKA_BOOTSTRAP` with prefixed resources and cleanup — TECH L1
- **Tests (Definition of Done):**
  - `go vet -tags integration ./internal/kafka/franz` exit 0 in CI (compiles without a cluster) — TECH L1
  - `TestPortTest_CaseListStable` (the `porttest` case-name list equals a committed golden, so the franz and fake runs cover identical cases) — TECH L1
  - Level 2: `go test -tags integration ./internal/kafka/franz` passes every `porttest` case against the acceptance cluster — TECH L1

### Task 16.3: Acceptance workflow
- **Status:** Not Started
- **Source:** TECH §4.10 (manual dispatch), §5.1 (`acceptance.yml`)
- **Subtasks:**
  - [ ] 16.3.1 `.github/workflows/acceptance.yml`: `workflow_dispatch`, secrets `KAFKA_BOOTSTRAP`, `KAFKA_SASL_*`, `KGW_ACC_OPERATOR_KEY`, `KGW_ACC_READER_KEY`; runs `-tags acceptance,integration`; commits `docs/acceptance/*.md` — TECH §4.10, §5.1
- **Tests (Definition of Done):**
  - `acceptance.yml` dispatched against the acceptance cluster: green, and a new `docs/acceptance/<version>-<date>.md` with 41 `PASS` rows is committed by the workflow — TECH §4.8, §4.10

---

## Diff-markup consumption record (Step 9)

All 35 `[NEW]` / strikethrough markers present in FUNC-SPEC rev 0.3 and TECH-SPEC rev 0.6 were mapped to tasks before being cleaned:

| Marker location | Task action |
|---|---|
| FUNC §7 closures (9 items) | 1.2, 1.5, 1.8, 3.2, 3.4, 4.1 |
| FUNC §8.2 `X-Api-Key`, `X-Request-Id`, `X-Break-Glass-Reason`, `?pattern=` | 1.8.2, 1.8.3, 6.1.1, 2.3.1 |
| FUNC §8.5 caller nullability, audit topic | 5.2.1, 5.4 |
| FUNC §8.7 M3/M4 `limit`, G7 target-in-path; §8.8 `maxMatches` | 4.2, 4.3.1, 10.2.2, 1.2.1 |
| FUNC §9.1 break-glass scope, `to`/`latest`, M2/G4 clamping | 6.1, 3.2.2, 3.3.1, 10.2.1 |
| FUNC §9.7 O1 reworded | 1.8.9, 13.2.4 |
| TECH §1.1 / §1.4 `Matcher` wording | 4.1, 3.2.4 |
| TECH §2.1, §2.3, §2.5, D2, D1 example (`internal/app`) | 1.10 |
| TECH I2 illustrative note | 2.2.1, 7.2.1 |
| TECH T1 coverage exclusions | 1.12.1 |
| TECH §4.8 `app.Run` | 1.11.2 |
| TECH §5.4 probes, Secret | 1.10.3, 15.1.2 |

## Step 10 verification record

- **Definitions of Done** attached to all 56 tasks (2026-09-19); every item is a named Go test, `porttest` case, fuzz/bench target, lint rule, or workflow run — frameworks limited to TECH §4.2.
- **Manual Test Plan edits applied** (phases were `Not Started`): F2 (Phase 5 step 3 / Phase 6 step 7), F3 (Phase 8 step 3 `source="dynamic"`; Phase 2 step 6 aligned), F4 (Phase 10 step 5), F5 (Phase 12 steps 8–9).
- **Additions beyond TECH §4.5, for Step 11's attention:** `TestApp_ShutdownOrder` (Task 1.10); `kubeconform` as a CI tool (Phase 15). Both approved by the user at Step 10.
- **Non-code DoD accepted:** 5.4.2, 14.4, 15.1.3 (review-checklist items); 14.1–14.3 (workflow runs + Phase 14 plan).

## Step 11 guardian audit

**Audited:** 2026-09-19 · `TASKS.md` against `FUNC-SPEC.md` rev 0.4 and `TECH-SPEC.md` rev 0.7

### Verdict

**`STATUS: READY`**

### Coverage cross-check

| Direction | Result |
|---|---|
| Catalog → tasks | 41/41 command IDs have port, service, route, test, and acceptance subtasks |
| FUNC O1–O9, D1–D5, V1–V6, F1–F6, X1–X8; §8.2–§8.8 contracts; §9.1–§9.5 behaviours → tasks | all covered |
| TECH §1 stack, §2 patterns/conventions, §3 SOLID (lint + review checklist), §4 testing (23/23 traceability rows, 7/7 fuzz targets, 7/7 benchmark families), §5.1 topology files, §5.4 runtime objects, §6.2 URL table (48 operations), §6.3 configuration keys, §6.1 B1–B8, §6.3 C1–C14 → tasks | all covered |
| Tasks → spec (no orphaned or invented work) | every task cites real sections; approved additions beyond the spec are listed under N3 |
| Definition of Done presence | 56/56 tasks (all `Not Started`) |
| Leftover `[NEW]` / strikethrough markup in either spec | none |
| Conflicts with `In Progress` / `Done` work | none |

### Phase integrity

| Check | Result |
|---|---|
| Goal + numbered Manual Test Plan with exact expected results | 16/16 |
| Vertical slice observable without reading code | 16/16 (Phase 1 wide but demoable; Phases 14–16 observable through tool/workflow output, render-only Helm verification by decision) |
| No Manual Test Plan depends on a later phase | 16/16 (Phase 6 carries one DoD item registered skip-until-Phase-11; its plan does not depend on it) |
| No phase leaves the system non-runnable or regresses an earlier plan | 16/16 |

### Gaps found and closed in this audit (edits applied under the Step 9 / Step 10 protocols with the user's authorisation; all affected tasks were `Not Started`)

| # | Gap | Resolution |
|---|---|---|
| G1 | TECH §4.7 benchmark families `BenchmarkGatesCheck`, `BenchmarkPlanToken` had no task | Subtasks 1.7.7 and 9.1.3 + DoD bullets |
| G2 | TECH §4.6 fuzz target for the API-key header parser had no task | Subtask 1.8.11 + DoD `FuzzAPIKeyHeader` |
| G3 | TECH §4.5 Telemetry row ("child span per Kafka call", Level 1b) was unachievable: Kafka spans came only from `kotel` inside `franz`, and `internal/kafka` is stdlib-only | Decision: tracing decorators in `internal/telemetry` wrapping the three port interfaces (subtask 1.9.4), wired in `internal/app`; DoD `TestTelemetry_ChildSpanPerKafkaCall`, `TestTelemetry_DecoratorsSatisfyPortInterfaces` |
| G4 | TECH §3.6 code-review checklist (S1–S5, I2, I5) had no producing task | Subtask 1.1.5 now hosts the checklist in `CONTRIBUTING.md` with a review DoD item |

### Non-blocking notes

| # | Note | Action |
|---|---|---|
| N1 | VC O5 was proven at unit level and per route; no single component test named every `Kind` through HTTP | `TestAPI_ErrorMapping_EveryKindThroughHTTP` added to Task 2.4 (applied) |
| N2 | Citation shorthand (`FUNC B8`, `TECH Bn`) | Normalised to `TECH §6.1 Bn` (applied) |
| N3 | Approved additions not yet reflected in TECH-SPEC: `TestApp_ShutdownOrder` (Task 1.10), `kubeconform` (Phase 15), and the `internal/telemetry` → `internal/kafka` import introduced by G3 (TECH §5.3 table) | Record as `[NEW]` in TECH §4.5, §4.10, §5.3 at the next spec revision; no implementation impact |
| N4 | Supported broker range (TECH C3) was not surfaced in user-facing docs | Added to subtasks 1.1.5 (README) and 14.4.1 (operations) (applied) |
| N5 | TECH §4.3 "compaction not simulated (documented)" | Doc-comment clause added to subtask 1.6.1 (applied) |
| N6 | Task 6.2's `TestAPI_Lock_DryRunStillLocked` must use `t.Skip` with a reference to Task 11.2 until M8 exists | Convention noted for Step 12 |

### Sign-off

Every requirement traces to a task, every task traces to a requirement, every task has a Definition of Done, every phase is a self-contained vertical slice with a concrete Manual Test Plan, phase order is dependency-safe, and no unresolved conflicts remain. Implementation may begin at Phase 1, Task 1.1.
