# Contributing

Thank you for considering a contribution. This project is **spec-driven**: `FUNC-SPEC.md` and `TECH-SPEC.md` are the contract, `TASKS.md` is the plan, and the code follows them. A change that alters behaviour starts as a spec amendment, not a pull request.

## Development environment

- Go **1.27** — the `toolchain` directive in `go.mod` downloads the exact patch release automatically (`GOTOOLCHAIN=auto`).
- GNU make.
- `make tools` installs the pinned `golangci-lint`, `govulncheck`, and `benchstat` into `$(go env GOPATH)/bin`.
- A Kafka cluster is **not** required for the automated suite: tests run against an in-memory fake of the Kafka port (TECH-SPEC §4.3). The acceptance suite (`make acceptance`) needs `KAFKA_BOOTSTRAP` and is run manually per release.

## Workflow

1. Pick the next `Not Started` task in `TASKS.md`, in phase and task order. Its **Source** lines cite the spec sections it implements; its **Tests (Definition of Done)** lists the exact tests that prove it done.
2. Implement the subtasks, respecting the package layout and import boundaries in `TECH-SPEC.md` §5.1 / §5.3 — `make lint` enforces them.
3. Run `make ci` locally. It is the same sequence as the pull-request pipeline: lint → tests with `-race` and the coverage gate → 10 s fuzz → `govulncheck` → build.
4. Open a pull request. Conventional-commit titles (`feat:`, `fix:`, `docs:`, `chore:`) are expected; Renovate uses them too.

Never introduce a dependency outside `TECH-SPEC.md` §1.1, and never add a dependency with a non-permissive licence (GPL, LGPL, AGPL, SSPL).

## Test conventions (TECH-SPEC §4.9)

- Table-driven; `t.Parallel()` by default — each test owns its fake.
- **No `time.Sleep`.** Time is injected (`now func() time.Time`); waiting uses channels and `context`.
- Golden files change only with `-update`.
- Names are `Test<Unit>_<Case>`; fixtures live in `testdata/`.

## Code-review checklist

`golangci-lint` enforces most of the architecture rules (`depguard`, `gochecknoglobals`, `gochecknoinits`, `funlen`, `gocyclo`, `exhaustive`, `ireturn`). The rules below are **not** machine-checked (TECH-SPEC §3.6); reviewers confirm each one on every pull request.

- [ ] **S1 — One package, one reason to change.** `api` changes only for HTTP/OpenAPI; `service` only for policy and use-case flow; `kafka/franz` only for client-library changes; `scan` only for scan semantics; `audit` only for audit; `config` only for configuration shape.
- [ ] **S2 — One service type per catalog group, one method per catalog command.** No method serves two command IDs.
- [ ] **S3 — Handlers do three things only:** decode the DTO → call one service method → encode the result or error. No gate logic, no Kafka call, no business rule in `internal/api`.
- [ ] **S4 — Adapters only translate.** `franz` maps domain ↔ franz-go types and `kerr` → `kafka.Error`. No policy, no bounds, no audit, no retries-with-semantics in an adapter.
- [ ] **S5 — Each cross-cutting concern has exactly one home:** gates → `gates.Check`; audit → `Auditor`; scanning → `scan.Run`; error mapping → one table in `api/errors`. A second implementation of any of these is a defect.
- [ ] **I2 — Constructors accept exactly the roles they call.** A service holding a `kafka.Admin`, `Consumer`, or `Producer` it never calls is a defect.
- [ ] **I5 — DTOs and domain types are separate.** Huma request/response structs live in `api` with JSON tags; `internal/kafka` domain types carry no JSON, HTTP, or validation tags.

Also confirm:

- [ ] Every new command touched exactly three places — `command.Table`, one service method (Plan/Apply pair if destructive), one `huma.Register` — and nothing in `gates`, `audit`, `scan`, or `api/errors` (TECH-SPEC O2).
- [ ] Every new test is named in the task's Definition of Done, or the Definition of Done was extended first.
