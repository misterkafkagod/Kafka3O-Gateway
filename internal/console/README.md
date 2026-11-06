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

The cookie and CSRF helpers provide a host-only Secure, HttpOnly, SameSite
Strict session cookie, a separate script-readable Secure CSRF cookie, and an
exact HTTPS Origin and session-bound CSRF-header check.

`cmd/console serve` composes local login, session inspection, password
replacement and logout into a TLS-only server. It refuses to start without an
enabled local global administrator, and exposes no Kafka or account-management
routes. Configure it with:

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

Before deploying a secured Console, every future protected route must apply
session and authorization checks and deny unknown purposes. Account disable
and administrator reset must atomically revoke sessions, protect the last
enabled local global ADMIN, and apply throttling to recovery. A deployed
bootstrap channel, browser integration, Gateway enforcement, administrator
recovery, audit retention, and end-to-end redaction remain unverified.
