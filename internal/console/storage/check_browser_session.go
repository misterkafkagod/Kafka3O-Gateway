package storage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type browserSessionCheck struct {
	checkedAt time.Time
	sessionID string
	revoked   bool
}

func (d *Database) checkBrowserSession(ctx context.Context, tx *sql.Tx, hash [32]byte, requested time.Time) (browserSessionCheck, error) {
	if hash == ([32]byte{}) {
		return browserSessionCheck{checkedAt: requested}, nil
	}
	now, err := d.strictTransactionTime(requested)
	if err != nil {
		return browserSessionCheck{}, err
	}
	var storedHash []byte
	var previousID, source sql.NullString
	var revoked, created, idle, absolute sql.NullInt64
	var enabled sql.NullBool
	err = tx.QueryRowContext(ctx, `SELECT sessions.id, sessions.auth_source, sessions.browser_binding_hash, sessions.revoked_at_ns,
sessions.created_at_ns, sessions.idle_expires_at_ns, sessions.absolute_expires_at_ns, identities.enabled
FROM console_browser_sessions AS browsers
LEFT JOIN console_sessions AS sessions ON sessions.id = browsers.session_id
LEFT JOIN identities ON identities.id = sessions.identity_id
WHERE browsers.browser_hash = ?`, hash[:]).Scan(&previousID, &source, &storedHash, &revoked, &created, &idle, &absolute, &enabled)
	if errors.Is(err, sql.ErrNoRows) {
		return browserSessionCheck{checkedAt: now}, nil
	}
	if err != nil {
		return browserSessionCheck{}, fmt.Errorf("inspect browser session binding: %w", err)
	}
	if !previousID.Valid || strings.TrimSpace(previousID.String) == "" || len(previousID.String) > 128 ||
		!source.Valid || (source.String != string(session.SourceLocal) && source.String != string(session.SourceCognito)) ||
		!bytes.Equal(storedHash, hash[:]) || !created.Valid || !idle.Valid || !absolute.Valid || !enabled.Valid ||
		created.Int64 < 0 || idle.Int64 <= created.Int64 || idle.Int64 > absolute.Int64 ||
		(revoked.Valid && revoked.Int64 < created.Int64) {
		return browserSessionCheck{}, session.ErrInvalidSessionConfig
	}
	if created.Int64 > now.UnixNano() || (revoked.Valid && revoked.Int64 > now.UnixNano()) {
		return browserSessionCheck{}, ErrInvalidAuditEvent
	}
	if !revoked.Valid && enabled.Bool && idle.Int64 > now.UnixNano() && absolute.Int64 > now.UnixNano() {
		return browserSessionCheck{checkedAt: now}, session.ErrBrowserSessionConflict
	}
	return browserSessionCheck{checkedAt: now, sessionID: previousID.String, revoked: revoked.Valid}, nil
}
