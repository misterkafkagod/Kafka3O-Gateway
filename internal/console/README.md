# Console security foundations

The identity, authorization, and SQLite packages are building blocks only.
They are not connected to the running Gateway or a Console HTTP server.
The existing shared-token and manual-test paths are not user authorization.

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
The temporary password is marked for mandatory replacement.

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

The cookie and CSRF helpers provide a host-only Secure, HttpOnly, SameSite
Strict session cookie, a separate script-readable Secure CSRF cookie, and an
exact HTTPS Origin and session-bound CSRF-header check. Standalone HTTP
handlers implement local login, session inspection, password replacement, and
logout. They accept direct TLS requests only, use the direct peer address for
throttling, and require the configured exact HTTPS origin. They are tested but
not mounted in any running process and expose no Kafka routes. A successful
password change revokes all local sessions and requires a new sign-in. Never
log the raw session or CSRF token.

Before exposing these operations, the Console needs a verified HTTPS server
and must wire session and authorization checks on every relevant route. It
must restrict bootstrap accounts to password replacement/logout/session
information and deny unknown access purposes. Account disable and
administrator reset must atomically revoke sessions, protect the last enabled
local global ADMIN, and apply throttling to recovery. Bootstrap access also
needs a restricted, rate-limited administrative channel and auditable failed
attempts. The standalone handlers are not a complete authentication or
authorization boundary. Runtime composition, deployment, administrator
recovery, audit retention, and end-to-end redaction remain unverified.
