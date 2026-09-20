# Security Policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately through GitHub's **Report a vulnerability** form on this repository (Security → Advisories). Do not open a public issue.

You will receive an acknowledgement within 3 business days. We ask for a 90-day disclosure window from acknowledgement; we will credit reporters in the advisory unless asked not to.

## Supported versions

Only the latest minor release line receives security fixes. Pre-release builds (`v0.x`) are supported on a best-effort basis until `v1.0.0`.

## Vulnerability policy for dependencies and images

This project follows a **zero unpatched critical/high** policy (TECH-SPEC §1.0):

- `govulncheck` runs on every pull request; a critical or high finding blocks merge.
- Container images are scanned on every build and nightly; a critical or high finding blocks release.
- Renovate opens a pull request for every upstream patch; medium and low findings are tracked with a fix-by date.
- Exceptions are recorded with an expiry date and reviewed on expiry.

## Security-relevant design properties

- The gateway holds broker credentials so that front-ends do not have to; it never exposes them and redacts them from every log line.
- API keys are configured as SHA-256 digests only; the presented key is hashed and compared in constant time. Generate keys with `kafka3o-gateway keygen`.
- No endpoint is reachable without a key when authentication is enabled — including health and OpenAPI routes.
- Every mutating request is audited two-phase (`ATTEMPT` before, `RESULT` after); if the audit sink cannot record the attempt, the mutation does not run.
- Message payloads read from Kafka are untrusted data: regex matching uses Go's RE2-based engine (linear time by construction), and every read, search, and replay is bounded by message count, bytes, and wall-clock time.
- Request bodies are never logged; SCRAM passwords never appear in audit events.

See `FUNC-SPEC.md` §5.5 (safety controls), §8.5 (audit), and `TECH-SPEC.md` §6 (audit resolutions) for the full specification.
