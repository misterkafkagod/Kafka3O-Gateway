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

The standalone refreshed-ID-token verifier checks the same signature,
issuer, client, token-use, expiry and group rules, then compares the verified
issuer, subject and audience to the last verified identity. Audience order
may differ; its members may not. The original login nonce may be absent from
a refreshed token, but if present must match the retained original nonce
digest. It rejects missing ID tokens and tokens at least five minutes old,
and requires a strictly later issuance time than the last verified token, so
a refresh in the same timestamp second fails closed. The caller must retain
the original nonce digest and trusted verified identity, not accept them from
the browser, and enforce the earliest of five minutes after verification,
five minutes after issuance or the verified token's expiry. This primitive
does not perform a provider refresh, persist tokens, create sessions or
enforce that deadline; live Cognito behavior remains unverified.

The standalone Cognito authorization-attempt service creates independent
random state, nonce, PKCE verifier and browser binding. Only state, nonce and
browser-binding digests and an AES-256-GCM-encrypted verifier are stored;
authenticated encryption binds the ciphertext to those digests, the exact
provider configuration and the callback URL. Supply a cryptographically random
32-byte encryption key from a deployment-managed secret outside SQLite.
Changing the key, provider or callback invalidates pending attempts. The
upgrade that introduced provider binding also invalidates older unbound
attempts; start a new login. Never log the attempt values, callback
parameters, verifier or encryption key.

At most 1,000 pending attempts are retained, each expiring five minutes after
the request began. Expiry is rechecked after obtaining the database
transaction, and expired attempts are removed when a new one is started.
Consumption requires the matching browser binding and atomically removes the
attempt with a redacted audit event before returning the verifier. Malformed,
expired, wrong-browser and replayed callbacks are denied and audited; an
audit failure returns no verifier. If creation or consumption has an unknown
commit outcome, investigate the operation ID and do not automatically retry.
These attempt primitives do not expose a browser callback.

The standalone browser-authorization helpers build an `openid`
authorization-code URL with state, nonce and PKCE S256 only from the same
in-process attempt manager and verified provider/callback used by the code
exchanger. They reject altered, foreign, expired or backwards-clock starts.
The browser binding is placed only in a host-only Secure HttpOnly
SameSite=Lax cookie with at most a five-minute Max-Age and the attempt's
absolute expiry; the server enforces the exact deadline. A matching
deletion cookie is available. The PKCE verifier, browser binding,
client secret and provider tokens are not placed in the authorization URL.
The application session cookie remains SameSite=Strict. These helpers do
not expose an SSO route or prevent replacement of an existing session; that
protection must be completed before browser integration.

The standalone callback parser accepts a query of at most 8,192 bytes with
one canonical state and exactly one authorization code or provider error.
It rejects duplicate or unknown parameters, malformed encoding, control
characters and oversized values. Provider error descriptions and error URIs
are discarded rather than displayed or followed. The cookie reader requires
exactly one canonical browser binding. The same code validation is used by
the exchanger, which rejects invalid codes before any provider request.
The eventual handler must consume and audit the browser-bound attempt for
both success and provider denial, clear transient cookies, and must not log
the query, code or binding. No callback route is exposed yet.

The storage foundation can optionally bind a new local or Cognito session to
the digest of a separate, stable browser marker. Session creation checks the
marker's current session and records the new binding in the same transaction
as creation and its audit. A competing active session is denied and audited;
only the exact `ErrBrowserSessionConflict` confirms that denial was recorded.
A revoked, disabled, idle-expired or absolutely expired session can be
replaced. The old session retains its marker digest when the current binding
changes. Bound issuance rechecks a non-regressing clock and session deadlines
before and after commit; Cognito issuance also rechecks exchange freshness.
Audit, commit and uncertain outcomes must not be treated as a confirmed
denial or retried automatically. An outcome that may have committed is not
reported as an ordinary invalid session.

Separate storage-only bound session-use and grant-resolution actions require
the marker digest and transactionally verify both the session's digest and
the current marker-to-session binding. They recheck identity, revocation,
deadlines and current direct or verified-group grants; an old session cannot
revive after replacement. Bound Cognito claims must be fresh and cannot
trigger provider refresh through these actions. Missing binding storage, or a
clock observed to regress during an action or before session/claim issuance,
is an operational failure, not an ordinary invalid session.
The existing unbound paths do not depend on the binding table and continue
to reject bound sessions.

This is not yet a browser session implementation: issuing managers and HTTP
routes do not supply a marker. Separate storage-only
bound logout lookup and revocation actions check the current binding; the
revocation transaction also removes any retained Cognito credentials and
audits the result. A current Cognito session can be locally logged out even
when its claims, identity, lifetime or retained provider credential are
unavailable. The HTTP handler must still validate Origin and CSRF before
revocation. The existing unbound HTTP logout clears cookies for an exact
invalid-session result, but retains them for wrapped operational or
uncertain failures. Unbound logout and revocation reject bound sessions.

Local administrator actions can also receive a marker digest in their
trusted actor and transactionally recheck the current binding alongside the
local global-ADMIN grant and session state. Reservation and completion
reauthorize separately. Existing handlers construct actors themselves and do
not accept actor or marker values from request bodies; they do not supply a
bound marker yet.

Separate storage-only bound Cognito refresh inspection and reservation
actions verify the session digest and current browser binding with identity,
lifetime and encrypted-credential checks. A per-generation reservation
remains durable across restart and blocks session use and replay. Bound
actions reject observed backwards clocks, and an uncertain or expired
post-commit reservation returns no credential material and must not be
retried automatically. Existing unbound refresh actions still reject bound
sessions and do not depend on the binding table.

A separate read-only bound refresh dispatch guard rechecks the exact durable
reservation, current session and browser mapping, credential and verified
snapshot, session deadlines and non-regressing time. It does not contact the
provider or make the reserved session usable. A binding can still change
after this check or while a provider request is in flight; bound completion
therefore rechecks the binding and never retries an uncertain request.
Bound failure auditing matches the original session's marker digest and
durable reservation, not the current browser mapping. It can therefore
record a failed-before-send or unknown outcome after replacement, revocation
or expiry without changing the replacement. Unbound failure auditing rejects
an existing bound session. Audit and uncertain commit failures are explicit.

The standalone bound provider exchange carries the same marker through
reservation, pre-dispatch validation and failure audit, and returns a
private marker-bound completion proof after at most one provider request.
A missing, different or unbound marker cannot consume that proof. A valid
marker consumes it on the first completion attempt, even if subsequent
validation fails. Completion transactionally rechecks the current binding,
durable reservation, unchanged identity and credential snapshot, and
deadlines before storing the next encrypted credential generation and
redacted success audit. Replacement, revocation or disablement during the
provider request cannot restore the old session or change the replacement.
An uncertain request or completion must not be retried. Synthetic tests
cover both client modes, retained and rotated tokens, in-flight changes,
restart, failures and redaction; real Cognito remains unverified.

Marker generation, cookie lifecycle and local/SSO integration remain
unfinished. Binding rows for inactive markers are retained until that marker
is reused; bounded retention and cleanup must be addressed before deployment.
If a disabled Cognito session is replaced and re-enabled between cleanup
ticks, its old encrypted credential may remain until expiry. The future
identity-disable or replacement path must revoke it atomically, or cleanup
must otherwise guarantee its timely removal, before deployment.

The standalone code exchanger accepts only a successfully consumed attempt
bound to its discovered provider and exact callback. It durably reserves each
exchange once before dispatch and rechecks the attempt and verified-token
expiry after queued work. The exact HTTPS token endpoint uses either an
explicit public-client ID or explicit HTTP Basic client authentication; it
never probes authentication styles or follows redirects. Token responses are
limited to 1 MiB. The returned ID token is verified before the refresh token
is returned in memory, and audit failures withhold both. A lost or uncertain
exchange is recorded as unknown where possible and must not be retried;
investigate the operation ID and start a fresh login. The fixture tests both
client modes, forged tokens, TLS, redirects and interrupted requests. No
browser route or live Cognito integration is provided by this primitive.

The standalone refresh-token protector uses AES-256-GCM with fresh random
nonces and a separate externally supplied 32-byte key. It refuses to use the
authorization-attempt encryption key. Authenticated data binds ciphertext to
the exact provider/callback, session ID, identity ID and token generation;
tampering or substitution fails closed. Changing the key or provider/callback
invalidates protected tokens. The caller must use cryptographically secure
randomness and protect both deployment keys outside SQLite. This primitive
does not itself persist refresh tokens, prevent database rollback or
serialize refresh; those require transactional storage and audit.

The Cognito session foundation consumes an in-process verified exchange proof
once, with a durable reservation. It atomically creates or finds an enabled
`(issuer, sub)` identity and stores an opaque session, a verified claim
snapshot and the encrypted refresh token. It never links email addresses or
creates role grants. The proof, provider/callback, nonce and token lifetimes
are rechecked after queued work; audit or uncertain outcomes return no session
credentials and must not be retried. Session use rechecks the account,
authentication source, revocation and claim freshness. Until a refresh is
atomically completed, Cognito sessions fail closed at the earliest of
five minutes after verification, five minutes after token issuance or token
expiry. A local-only session manager refuses Cognito sessions; a Cognito
manager requires a verified provider configuration and matching protector.
Session use also authenticates the protected credential binding, so a changed
provider, callback or encryption key and corrupted ciphertext fail closed
without affecting local accounts. Each successful use opens the encrypted
refresh token in server memory; it is never returned to the browser.

A refresh-reservation foundation records one durable audited attempt per
session and refresh generation before a provider request. It
rechecks the enabled identity, session expiry and protected credential after
queued work; stale claims may reserve a refresh but do not grant access.
Unresolved reservations block session use and cannot be replayed, including
after a restart. Local sessions remain independent and local logout can
still revoke the reserved session. The reservation event is part of the
replay-prevention state: never remove it while the corresponding credential
generation exists.

The standalone provider-refresh exchange sends at most one verified-HTTPS
request after that reservation, using the configured public-client or Basic
authentication. It rejects redirects, oversized or malformed responses, and
missing or invalid refreshed ID tokens. A returned refresh token must be
valid; if the response omits it, the existing token is retained. The next
generation's token is encrypted and returned only in a private, provider-
bound completion proof, with claim and session deadlines rechecked. The
reserved session remains blocked until that proof is completed. Provider or
verification failures are audited against the reservation without provider
tokens where possible; an interrupted operation or failed audit may leave
only the durable attempt. Errors after
the operation ID is allocated return that ID for investigation, but no
completion proof. Never automatically retry that generation. Synthetic TLS
tests cover both client modes, rotation, failures, concurrency and replay;
real Cognito behavior remains unverified.

The standalone completion action consumes that proof once and transactionally
rechecks the reservation, absence of an outcome, unchanged identity and
credential snapshot, provider/key binding, enabled identity, revocation and
claim/session deadlines. It atomically stores the verified claim snapshot,
the next generation's encrypted retained or rotated token, and a redacted
success audit. A failed or uncertain completion does not retry the provider
request or reuse the proof; an unresolved reservation remains blocked. If
the proof is lost on restart before completion, a new sign-in is required.

An opt-in `NewRefreshingCognitoManager` connects session resolution to that
flow when the session store and exchanger use the same underlying store.
Its separate `ResolveBound` action accepts only a trusted browser-marker
digest and carries it through inspection, reservation, dispatch and
completion. Refreshes are coalesced only for the same session-token and
marker digests; unbound work cannot join bound work. It admits at most 32
distinct refreshes per manager across both paths and bounds shared work to
15 seconds.
Cancellation of one waiting request does not cancel the shared refresh.
Every successful waiter revalidates the session and claim deadlines; bound
waiters also recheck the current marker mapping. Different managers do not
share the in-memory coordinator; the durable reservation still prevents
another provider request. Pending or uncertain refreshes return service
unavailable without clearing the browser's session cookie. Only the exact
invalid-session result denotes that the supplied token and binding are
invalid; it does not prove that the browser has no competing session or
permit replacement. A joined operational error is not definite invalidity.
The existing
local-only and non-refreshing Cognito managers do not coordinate refresh.
No HTTP route supplies a stable marker, and bound issuance, grant resolution
and logout remain storage-only.

The standalone session-grant resolver combines direct grants with an
initially empty, server-owned table of exact issuer and case-sensitive
verified group bindings. Each binding grants one global or cluster-specific
VIEWER, OPERATOR or ADMIN role. Resolution returns at most the strongest
global and requested-cluster grants, never the group names. Local sessions
receive direct grants only and do not query the group-mapping table. The
resolver rechecks the current session,
account, provider/key binding, pending refresh, claim/session deadlines and
current mappings transactionally; malformed or unavailable state fails
closed. Every authorization decision must resolve current grants again; a
returned snapshot is not a durable authorization or delegation. Group
mapping administration and independent Gateway enforcement remain P54/P55;
no mapping-management or Kafka route is exposed by this foundation.

Local logout requires the session-bound CSRF token and can revoke a Cognito
session even if its claims are stale, its identity is disabled, its session
has expired, or its provider or encryption key is unavailable, provided the
browser still sends the session cookie. Local logout and administrator
revocation atomically remove retained Cognito refresh-token material; audit
failure prevents revocation and is reported. Ordinary session inspection
clears invalid cookies, so an already-cleared cookie cannot be used to
request logout.

The Console process runs one cleanup batch at startup and every minute. Each
batch transactionally revokes and removes up to 100 Cognito credentials whose
sessions are revoked, whose identities are disabled, or whose idle or absolute
lifetime has expired. Stale claims and unresolved refreshes alone are not
eligible. Every removed session receives a redacted audit event in the same
transaction, and refresh-reservation audit events are retained. Cleanup of a
disabled identity prevents that session from reviving if the identity is
later re-enabled; the future identity-disable operation must itself revoke
sessions atomically rather than wait for this worker.

Cleanup work has a 10-second deadline. Errors and a full batch are logged
without credential or identity values; a failure does not stop local login,
and the next scheduled batch rechecks current state. More than 100 eligible
sessions take multiple batches, and no cleanup occurs while the process is
stopped. Operators must monitor errors and backlog. The database, WAL and
backups may retain encrypted historical bytes after logical deletion; their
retention, secure disposal and deployed-volume behavior remain unverified.
Session issuance is not exposed through a browser route, and the standalone
Console process does not yet enable the refreshing manager. Group-binding
administration, live Cognito and the secured deployment remain unimplemented.
The authentication-only preview offers explicit sign-out when session
inspection is unavailable; a missing CSRF token or uncertain logout does not
silently replace the session or automatically retry the mutation.

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

The session-administration foundation can revoke one local or Cognito
session by its separate opaque session ID. It requires an unrestricted
current local global ADMIN and rechecks the actor's session, expiry and
grant in the same transaction. The target revocation and its redacted audit
outcome commit atomically; an unavailable or expired target is rejected.
An administrator may revoke their own session without disabling the account
or other sessions. Audit failure blocks the revocation, and an uncertain
commit requires investigation by operation ID rather than an automatic
retry. Invalid or unknown submitted IDs, session tokens, CSRF values and
usernames are not recorded in the revocation audit.

The accompanying session-listing foundation requires the same current
unrestricted local global ADMIN and transactionally rechecks that authority.
It returns non-revoked sessions for enabled accounts within their idle and
absolute lifetimes; local sessions must also have current credential
versions. Cognito sessions may be listed when their claims are stale, so an
administrator can still revoke them; stale sessions cannot be used. Items
include opaque session and account IDs, an authentication source, a normalized
username (`cognito-user` for Cognito), creation/idle/absolute expiry times,
and whether the session is the caller's. It never returns token or CSRF
values or hashes, password hashes, or credential versions. Pages use an opaque session-ID cursor in ascending
order, default to 50 entries, and are capped at 100. A well-formed cursor need
not still exist, since sessions may expire or be revoked between pages; the
listing is not a snapshot. Listing decisions are audited without usernames,
session IDs, submitted cursors or credentials, and audit or commit failure
returns no session data.

The loopback server mounts `GET /api/admin/sessions` for a current,
unrestricted local global ADMIN. The `no-store` response contains an
`operationId`, a `sessions` array, and a `nextCursor`. Each item has `id`,
`accountId`, `username`, `authSource`, `createdAt`, `idleExpiresAt`,
`absoluteExpiresAt` and `isCurrent`; it contains no credential material.
`authSource` is `local` or `cognito`. `limit` defaults to 50
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
