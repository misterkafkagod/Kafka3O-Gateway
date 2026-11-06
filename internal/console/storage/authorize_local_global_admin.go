package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func authorizeLocalGlobalAdmin(ctx context.Context, tx *sql.Tx, actor identity.LocalAdminActor, at time.Time) error {
	var mustChange, globalAdmin bool
	var created int64
	bindingPredicate, bindingArgs := browserSessionPredicate("console_sessions", actor.BrowserBindingHash)
	args := []any{actor.SessionID, actor.AccountID}
	args = append(args, bindingArgs...)
	args = append(args, at.UTC().UnixNano(), at.UTC().UnixNano())
	err := tx.QueryRowContext(ctx, `SELECT local_accounts.must_change_password, EXISTS (
SELECT 1 FROM role_grants WHERE account_id = identities.id AND cluster_id = '' AND role = 'ADMIN'
), console_sessions.created_at_ns FROM console_sessions
JOIN identities ON identities.id = console_sessions.identity_id
JOIN local_accounts ON local_accounts.identity_id = identities.id
WHERE console_sessions.id = ? AND identities.id = ? AND identities.enabled = 1
AND console_sessions.credential_version = local_accounts.credential_version
AND console_sessions.auth_source = 'local'
AND `+bindingPredicate+`
AND console_sessions.revoked_at_ns IS NULL
AND console_sessions.idle_expires_at_ns > ? AND console_sessions.absolute_expires_at_ns > ?`,
		args...).Scan(&mustChange, &globalAdmin, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return identity.ErrInvalidAdminSession
	}
	if err != nil {
		return fmt.Errorf("recheck administrator session and grants: %w", err)
	}
	if actor.BrowserBindingHash != ([32]byte{}) && created > at.UTC().UnixNano() {
		return ErrInvalidAuditEvent
	}
	if mustChange {
		return identity.ErrAdminPasswordChange
	}
	if !globalAdmin {
		return identity.ErrGlobalAdminRequired
	}
	return nil
}
