package storage

import (
	"context"
	"database/sql"
	"fmt"
)

func bindBrowserSession(ctx context.Context, tx *sql.Tx, hash [32]byte, sessionID string) error {
	if hash == ([32]byte{}) {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO console_browser_sessions (browser_hash, session_id)
VALUES (?, ?) ON CONFLICT(browser_hash) DO UPDATE SET session_id = excluded.session_id`, hash[:], sessionID); err != nil {
		return fmt.Errorf("bind browser session: %w", err)
	}
	return nil
}
