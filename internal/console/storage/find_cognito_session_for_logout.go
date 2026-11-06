package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) FindCognitoSessionForLogout(ctx context.Context, tokenHash [32]byte) (session.Principal, error) {
	if tokenHash == ([32]byte{}) {
		return session.Principal{}, session.ErrInvalidSession
	}
	var principal session.Principal
	var csrfHash []byte
	err := d.db.QueryRowContext(ctx, `SELECT id, identity_id, csrf_hash FROM console_sessions
WHERE token_hash = ? AND browser_binding_hash IS NULL AND auth_source = 'cognito' AND revoked_at_ns IS NULL`, tokenHash[:]).Scan(
		&principal.SessionID, &principal.AccountID, &csrfHash,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return session.Principal{}, session.ErrInvalidSession
	}
	if err != nil {
		return session.Principal{}, fmt.Errorf("find Cognito session for logout: %w", err)
	}
	if principal.SessionID == "" || principal.AccountID == "" || len(csrfHash) != len(principal.CSRFHash) {
		return session.Principal{}, session.ErrInvalidSessionConfig
	}
	copy(principal.CSRFHash[:], csrfHash)
	principal.Source = session.SourceCognito
	return principal, nil
}
