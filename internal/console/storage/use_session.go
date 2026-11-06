package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) UseSession(ctx context.Context, tokenHash [32]byte, now, newIdleExpiry time.Time) (principal session.Principal, result error) {
	if tokenHash == ([32]byte{}) || now.IsZero() || now.Year() < 1970 ||
		newIdleExpiry.Year() > 2261 || !newIdleExpiry.After(now) ||
		newIdleExpiry.Sub(now) > session.MaxAbsoluteLifetime {
		return session.Principal{}, session.ErrInvalidSessionConfig
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return session.Principal{}, fmt.Errorf("begin session validation: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back session validation: %w", err))
		}
	}()
	var created, idle, absolute int64
	var csrfHash []byte
	err = tx.QueryRowContext(ctx, `UPDATE console_sessions
SET idle_expires_at_ns = min(absolute_expires_at_ns, ?)
WHERE token_hash = ? AND revoked_at_ns IS NULL AND idle_expires_at_ns > ? AND absolute_expires_at_ns > ?
AND EXISTS (
    SELECT 1 FROM identities JOIN local_accounts ON local_accounts.identity_id = identities.id
    WHERE identities.id = console_sessions.identity_id AND identities.enabled = 1
      AND local_accounts.credential_version = console_sessions.credential_version
)
RETURNING id, identity_id, created_at_ns, idle_expires_at_ns, absolute_expires_at_ns, csrf_hash`,
		newIdleExpiry.UTC().UnixNano(), tokenHash[:], now.UTC().UnixNano(), now.UTC().UnixNano()).Scan(
		&principal.SessionID, &principal.AccountID, &created, &idle, &absolute, &csrfHash,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return session.Principal{}, session.ErrInvalidSession
	}
	if err != nil {
		return session.Principal{}, fmt.Errorf("validate local session: %w", err)
	}
	if len(csrfHash) != len(principal.CSRFHash) {
		return session.Principal{}, session.ErrInvalidSessionConfig
	}
	copy(principal.CSRFHash[:], csrfHash)
	principal.CreatedAt = time.Unix(0, created).UTC()
	principal.IdleExpiresAt = time.Unix(0, idle).UTC()
	principal.AbsoluteExpiresAt = time.Unix(0, absolute).UTC()
	if err := tx.QueryRowContext(ctx, "SELECT username, must_change_password FROM local_accounts WHERE identity_id = ?", principal.AccountID).Scan(&principal.Username, &principal.MustChangePassword); err != nil {
		return session.Principal{}, fmt.Errorf("read session account restrictions: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return session.Principal{}, fmt.Errorf("commit session validation: %w", err)
	}
	return principal, nil
}
