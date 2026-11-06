package storage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) checkBrowserSession(ctx context.Context, tx *sql.Tx, hash [32]byte, requested time.Time) error {
	if hash == ([32]byte{}) {
		return nil
	}
	now, err := d.transactionTime(requested)
	if err != nil {
		return err
	}
	var storedHash []byte
	var revoked, idle, absolute sql.NullInt64
	var enabled sql.NullBool
	err = tx.QueryRowContext(ctx, `SELECT sessions.browser_binding_hash, sessions.revoked_at_ns,
sessions.idle_expires_at_ns, sessions.absolute_expires_at_ns, identities.enabled
FROM console_browser_sessions AS browsers
LEFT JOIN console_sessions AS sessions ON sessions.id = browsers.session_id
LEFT JOIN identities ON identities.id = sessions.identity_id
WHERE browsers.browser_hash = ?`, hash[:]).Scan(&storedHash, &revoked, &idle, &absolute, &enabled)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect browser session binding: %w", err)
	}
	if !bytes.Equal(storedHash, hash[:]) || !idle.Valid || !absolute.Valid || !enabled.Valid ||
		idle.Int64 <= 0 || idle.Int64 > absolute.Int64 {
		return session.ErrInvalidSessionConfig
	}
	if revoked.Valid || !enabled.Bool || idle.Int64 <= now.UnixNano() || absolute.Int64 <= now.UnixNano() {
		return nil
	}
	return session.ErrBrowserSessionConflict
}
