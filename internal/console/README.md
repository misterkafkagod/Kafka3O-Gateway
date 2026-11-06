# Console security foundations

The identity, authorization, and SQLite packages are not connected to the
running Gateway. A loopback-only Console process exposes session endpoints
for development, but is not deployed or a complete security boundary. The
existing shared-token and manual-test paths are not user authorization.

Local passwords use Argon2id with a unique random salt. Usernames are
case-insensitive ASCII identifiers, 1–64 characters, beginning with a letter
or digit and containing only letters, digits, `.`, `_`, and `-`. Passwords must
be valid UTF-8, at least 12 characters and at most 1024 bytes, and must not be
all whitespace.

The first local global administrator is created through a single-use,
out-of-band bootstrap secret. Generate at least 32 random bytes with a
cryptographically secure source and provide their unpadded base64url encoding;
the length check cannot establish randomness. Bootstrap atomically consumes
its state and stores the enabled identity, global ADMIN grant, and audit event.
The temporary password is marked for mandatory replacement. Bootstrap attempts
are recorded without credentials or submitted usernames and limited globally
to 10 attempts per 15-minute window, including successful attempts. The window
and anonymous denial audit survive restarts; an unavailable audit store blocks
bootstrap. Each bootstrap service admits at most four concurrent password hashes.

`cmd/console` provides an offline `bootstrap` command. Run it only as a
trusted operating-system account with access to the dedicated Console database;
database-file or host administrators can bypass application policy. Set
`KAFKA3O_CONSOLE_DB_PATH` to an absolute database path and supply the separately
provisioned expected secret in `KAFKA3O_CONSOLE_BOOTSTRAP_SECRET`. A trusted
local process must pipe the presented secret, one LF, the username, one LF,
and the exact UTF-8 password bytes to the command. Do not append a newline
unless it is part of the password. Terminal and regular-file input are refused
to avoid echoed or persisted plaintext. Do not put credentials in command
arguments, shell history, logs, or plaintext files. Remove the bootstrap secret
from the environment and its provisioning system after successful use.
Stop any Console server before this offline operation; this is not a network
endpoint or an account-recovery tool.

Opening a Console database acquires an operating-system lock on an adjacent
`.lock` file, so a second Console or offline bootstrap process fails rather
than sharing the same store. On Unix its permissions must be owner-only;
Windows deployments must separately configure and verify restrictive ACLs.
The lock is released on process exit, but the empty lock file remains; do
not delete or replace it while the store is in use. Protect the database
directory, use one canonical database path without hard-link aliases, and
use a local filesystem with reliable locks. This mechanism does not prevent a
host administrator or a non-Console process with direct SQLite access from
bypassing application controls.

Local authentication uses generic failures for unknown, invalid, disabled, or
incorrect credentials and checks a dummy hash for unknown users. A durable
sliding 15-minute window permits at most 10 attempts per normalized username,
60 per client address, and 300 globally. All attempts, including successful
ones, count. The window survives process restarts and expires old attempts
on subsequent requests. Each authenticator admits at most four concurrent
password verifications, and each password-change service admits at most four
concurrent password updates. Deployments behind a proxy must obtain the client
address only from a properly configured trusted proxy or the direct peer;
never trust arbitrary forwarded headers. The stored SHA-256 rate-limit keys
are not a confidentiality mechanism for low-entropy usernames or addresses.

Authentication attempts and outcomes are correlated in the audit store.
Failures are recorded without the supplied username, address, password, or
bootstrap secret. Password replacement and its audit event commit atomically,
and stale credentials cannot overwrite a concurrent change. The session
foundation stores only SHA-256 digests of independent, random session and
CSRF tokens. Session use rechecks the account and credential version;
password changes revoke all local sessions in the same audited transaction.
Default lifetimes are 30 minutes idle and eight hours absolute; deployments
may configure shorter or longer values up to the 24-hour absolute maximum.
Session expiry and administrative reservation expiry are checked again after
the database connection is acquired, so queued requests cannot revive expired
sessions or complete expired operations.

The cookie and CSRF helpers provide a host-only Secure, HttpOnly, SameSite
Strict session cookie, a separate script-readable Secure CSRF cookie, and an
exact HTTPS Origin and session-bound CSRF-header check.

The standalone Cognito verifier discovers only a configured HTTPS issuer and
requires exact configured authorization, token and JWKS endpoints. It rejects
redirects, verifies the provider certificate and hostname, and limits
discovery and JWKS responses to 4 MiB and ID tokens to 1 MiB. It verifies
RS256 signatures and key rotation, exact issuer and client audience,
expiration, nonce, `token_use=id` and authorized-party constraints. Unknown
claims are tolerated; the returned opaque subject and group names come only
from the verified token and confer no role by themselves. A cached signing key
can validate an unexpired token during an outage; the planned session refresh
must separately deny access when a required provider refresh fails. Discovery
and key failures are reported without returning tokens or provider responses.
This verifier uses synthetic TLS fixtures only; no Cognito login route,
refresh, real User Pool or secured deployment is implemented by it.

The standalone Cognito authorization-attempt service creates independent
random state, nonce, PKCE verifier and browser binding. Only state, nonce and
browser-binding digests and an AES-256-GCM-encrypted verifier are stored;
authenticated encryption binds the ciphertext to those digests. Supply a
cryptographically random 32-byte encryption key from a deployment-managed
secret outside SQLite. Replacing it invalidates pending attempts. Never log
the attempt values, callback parameters, verifier or encryption key.

At most 1,000 pending attempts are retained, each expiring five minutes after
the request began. Expiry is rechecked after obtaining the database
transaction, and expired attempts are removed when a new one is started.
Consumption requires the matching browser binding and atomically removes the
attempt with a redacted audit event before returning the verifier. Malformed,
expired, wrong-browser and replayed callbacks are denied and audited; an
audit failure returns no verifier. If creation or consumption has an unknown
commit outcome, investigate the operation ID and do not automatically retry.
These primitives do not yet expose a browser callback or exchange a code.

`cmd/console serve` composes local login, session inspection, password
replacement, logout, administrator account listing, creation, state changes,
password reset, and session listing and revocation into a TLS-only server.
It refuses to start without an enabled local global administrator and
exposes no Kafka or grant-management routes.
Configure it with:

- `KAFKA3O_CONSOLE_DB_PATH`: the absolute path to the bootstrapped database.
- `KAFKA3O_CONSOLE_ORIGIN`: the exact browser HTTPS origin, with no path.
- `KAFKA3O_CONSOLE_TLS_CERT_FILE` and `KAFKA3O_CONSOLE_TLS_KEY_FILE`: absolute
  paths to the certificate chain and matching private key.
- `KAFKA3O_CONSOLE_LISTEN`: a numeric loopback address and nonzero port;
  defaults to `127.0.0.1:8443`. Non-loopback listeners are not supported yet.

Remove `KAFKA3O_CONSOLE_BOOTSTRAP_SECRET` from the server environment before
starting it. The private key must be owner-only on Unix; Windows deployments
must separately configure and verify restrictive ACLs. The certificate must
match the configured origin, be valid at startup and permit server
authentication. TLS 1.2 or newer is required. Clients must independently
verify the certificate chain and hostname; a self-signed certificate is not
automatically trusted. The handlers use the direct peer address for throttling
and never trust forwarded headers.

Bootstrap sessions may inspect their session, change the temporary password or
log out. A successful password change revokes all local sessions and requires
a new sign-in. If a password-change response is lost, its outcome may be
unknown: do not automatically retry; sign in afresh and investigate audit
records. Never log the raw session or CSRF token.

The local session-administration foundation can revoke one active local
session by its separate opaque session ID. It requires an unrestricted
current local global ADMIN and rechecks the actor's session, expiry and
grant in the same transaction. The target revocation and its redacted audit
outcome commit atomically; an unavailable or expired target is rejected.
An administrator may revoke their own session without disabling the account
or other sessions. Audit failure blocks the revocation, and an uncertain
commit requires investigation by operation ID rather than an automatic
retry. Invalid or unknown submitted IDs, session tokens, CSRF values and
usernames are not recorded in the revocation audit.

The accompanying local session-listing foundation requires the same current
unrestricted local global ADMIN and transactionally rechecks that authority.
It returns only active local sessions for enabled accounts with current
credential versions: the opaque session and account IDs, normalized
username, creation/idle/absolute expiry times, and whether the session is the
caller's. It never returns token or CSRF values or hashes, password hashes,
or credential versions. Pages use an opaque session-ID cursor in ascending
order, default to 50 entries, and are capped at 100. A well-formed cursor need
not still exist, since sessions may expire or be revoked between pages; the
listing is not a snapshot. Listing decisions are audited without usernames,
session IDs, submitted cursors or credentials, and audit or commit failure
returns no session data.

The loopback server mounts `GET /api/admin/sessions` for a current,
unrestricted local global ADMIN. The `no-store` response contains an
`operationId`, a `sessions` array, and a `nextCursor`. Each item has `id`,
`accountId`, `username`, `createdAt`, `idleExpiresAt`, `absoluteExpiresAt`
and `isCurrent`; it contains no credential material. `limit` defaults to 50
and is capped at 100; use a nonempty `nextCursor` as `after` for the next
page. Malformed, duplicate or unknown query fields are rejected before the
listing service is called. The read-only route does not require CSRF.

`POST /api/admin/sessions/{sessionID}/revoke` derives its actor from the
authenticated session and requires the exact HTTPS Origin and session-bound
`X-Kafka3O-CSRF` header. It accepts no query parameters or request body.
A confirmed response includes an `operationId` and `signInRequired`; a
confirmed or uncertain self-revocation clears both session cookies. Invalid
and unavailable targets, authorization denials, audit failures, and unknown
outcomes are reported separately. An unknown or lost response must be
investigated using the operation ID when available, not automatically
retried. These routes do not provide an administration UI or protect Kafka
routes, and the loopback server is not deployed.

The account-state service atomically updates the account state and
credential version, revokes its sessions, and records the change. It rechecks
the current local session and global ADMIN grant in the same transaction and
will not disable the last enabled local global ADMIN. Re-enabling an account
also revokes any remaining old sessions. Setting the existing state records an
audited success without revoking sessions or incrementing the credential
version.

The loopback server mounts `GET /api/admin/local-accounts` for a current local
global ADMIN session without a pending password change. It rechecks the
session and grant in the same transaction as the listing and its audit event.
The response contains an `operationId`, an `accounts` array with each local
account's ID, normalized username, enabled state and mandatory-password-change
flag, and a `nextCursor`. It never contains password hashes, credential
versions or role grants. The read-only route does not require the CSRF header;
responses are marked `Cache-Control: no-store`.

The optional `limit` is 1–100 and defaults to 50. Accounts are sorted by
username; pass the nonempty `nextCursor` as `after` to obtain the following
page. An empty cursor ends pagination, and an empty page contains `[]`.
Pagination is not a snapshot: accounts added between requests may change
subsequent results. Unknown cursors receive `invalid_account_cursor`; malformed,
duplicate or unknown query fields receive `invalid_request` before a listing
operation is started. Service-level pagination rejections, successful
listings and authorization denials are audited without submitted cursors or
usernames. If the audit insert or commit fails, no account data is returned
and the server reports `account_list_audit_unavailable`.

The loopback server mounts `PUT /api/admin/local-accounts/{accountID}/enabled`
with JSON `{ "enabled": false }` or `{ "enabled": true }`. It derives the actor
and session from the authenticated cookie and requires the exact HTTPS Origin
and session-bound `X-Kafka3O-CSRF` header; unknown fields, duplicate `enabled`
fields, and missing, null or non-boolean values are rejected. The response
includes the `operationId`, resulting `enabled` state and `signInRequired`;
a successful self-disable clears both cookies and requires sign-in after an
administrator re-enables the account. The last-enabled-administrator guard
returns a conflict. An audit failure returns `account_state_audit_unavailable`;
an uncertain commit or rollback returns `account_state_outcome_unknown`.
An unknown self-disable outcome also clears browser cookies. A lost response
may lack an operation ID: investigate the account and audit before retrying,
and never automatically replay the operation.

The local-account creation service is also restricted to a current local
global ADMIN session without a pending password change. It reserves an
audited, single-use operation bound to that session, rechecks authorization
after hashing, and atomically creates the enabled account with a temporary
password and no role grants. The new user must replace the password before
normal access. Usernames are normalized case-insensitively; concurrent
duplicate usernames are rejected. Each service admits at most four concurrent
hashes. Passwords and submitted usernames are excluded from creation audit,
and an audit failure prevents account creation. A lost or uncertain commit
must be investigated using the operation ID, when available, before retrying.
This service does not provide public self-registration or grant assignment.
The loopback server mounts `POST /api/admin/local-accounts` with JSON
`{ "username": "...", "temporaryPassword": "..." }`. It derives the actor and
session from the session cookie and requires the exact HTTPS Origin and
session-bound `X-Kafka3O-CSRF` header; role, actor and session fields in the
body are rejected. A successful response contains the `operationId` and
created account's ID, normalized username, enabled state and mandatory-change
flag; it contains neither the password nor its hash. Validation, permission
and duplicate-username errors are explicit. `account_creation_audit_unavailable`
and `account_creation_outcome_unknown` are service errors, not success. A
lost response can also have an unknown outcome without an operation ID:
investigate the account and audit before any retry.

The administrator password-reset service requires a current local global
ADMIN session without a pending password change. It rechecks that session and
grant after hashing, then atomically replaces the password, requires a further
password change, increments the credential version, revokes all target
sessions, and records the correlated outcome. A concurrent account or
password change prevents the reset from overwriting newer credentials.
Self-reset signs the administrator out; resetting a disabled account does not
enable it.

Reset attempts use a durable 15-minute window: at most 10 per existing target,
60 per authorized administrator, and 300 globally. Rejected passwords and
unknown targets count towards the applicable limits; unauthenticated or
non-admin callers cannot exhaust them. Unknown target input and passwords are
not recorded in audit or rate-limit records. Each reset service admits at most
four concurrent password hashes. An unavailable audit store blocks the reset;
interrupted requests may leave only an attempted event. If the commit outcome
is unknown, use the returned operation ID, when available, to investigate audit
and account state; do not automatically retry.

The loopback server mounts `POST /api/admin/local-accounts/{accountID}/password-reset`
with JSON `{ "temporaryPassword": "..." }`. It derives the actor and session
from the session cookie and requires the exact HTTPS Origin and session-bound
`X-Kafka3O-CSRF` header; actor or session IDs in the body are rejected. The
response includes an `operationId` and `signInRequired`, which is true for
self-reset. Validation, permission, conflict and throttle errors are explicit;
`password_reset_audit_unavailable` and `password_reset_outcome_unknown` are
service errors, not success. A lost response can also have an unknown outcome
without an operation ID: investigate the target and audit before any retry.
Send temporary passwords only over verified TLS and never put them in logs,
URLs, command histories, or files.

## Isolated UI preview integration

The UI repository contains an opt-in cross-repository test for its built
authentication-only preview. It compiles a Go test fixture, bootstraps a
separate temporary SQLite database, and serves the built UI and this Console
session API from one ephemeral numeric loopback HTTPS origin. The test
generates a synthetic CA and supplies it only to the Node HTTPS client;
certificate-chain and hostname verification remain enabled. The fixture
rejects reuse of bootstrap and is shut down and removed after the test.
No live Console, Kafka data, or system/browser trust store is modified.

With Go and the UI's declared Node version available, run from the UI
repository in PowerShell:

```powershell
$env:VITE_KAFKA3O_CONSOLE_AUTH_PREVIEW = "true"
$env:VITE_KAFKA3O_MANUAL_PROXY = "true"
npm run build
$env:KAFKA3O_PREVIEW_INTEGRATION = "1"
npm test -- src\console\previewIntegration.test.ts
Remove-Item Env:\VITE_KAFKA3O_CONSOLE_AUTH_PREVIEW, Env:\VITE_KAFKA3O_MANUAL_PROXY, Env:\KAFKA3O_PREVIEW_INTEGRATION
npm run build
```

By default the Gateway checkout is expected beside the UI checkout; set
`KAFKA3O_GATEWAY_DIR` to its absolute path if it is elsewhere. The opt-in
test checks the built bundle's denied and successful sign-in, mandatory
password replacement, new session and logout, cookie attributes, and absence
of legacy Gateway requests. A separate administrator client lists sessions
and revokes the preview session; the preview receives a real 401 on its next
request, clears its signed-in state and does not replay the mutation. The
test also checks that an untrusted CA and a wrong hostname fail. The DOM and
cookie behavior are simulated by jsdom, with Node's verified HTTPS transport;
this is not a native-browser TLS or cookie test and does not deploy the
preview. The normal UI and Go test suites do not need the sibling checkout or
run this fixture unless explicitly enabled.

Before deploying a secured Console, every protected route must apply session
and authorization checks and deny unknown purposes. Administrator recovery
when no enabled local global ADMIN remains is not implemented; grant
management must also protect the last enabled local global ADMIN. A
deployed bootstrap channel, native-browser integration, Gateway enforcement,
audit retention, and end-to-end redaction remain unverified.
