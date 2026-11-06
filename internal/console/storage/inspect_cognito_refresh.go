package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) InspectCognitoRefresh(ctx context.Context, tokenHash [32]byte, requested time.Time) (state session.CognitoRefreshState, result error) {
	if ctx == nil || tokenHash == ([32]byte{}) || requested.IsZero() || requested.Year() < 1970 || requested.Year() > 2261 {
		return 0, session.ErrInvalidSession
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin Cognito refresh inspection: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			state = 0
			result = errors.Join(result, fmt.Errorf("roll back Cognito refresh inspection: %w", err))
		}
	}()
	now, err := d.transactionTime(requested)
	if err != nil {
		return 0, err
	}
	var source string
	var enabled, reserved bool
	var version, idle, absolute int64
	var revoked, generation, freshUntil, tokenExpiry sql.NullInt64
	var cognitoIdentity sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT console_sessions.auth_source, console_sessions.credential_version,
console_sessions.revoked_at_ns, console_sessions.idle_expires_at_ns, console_sessions.absolute_expires_at_ns,
identities.enabled, cognito_identities.identity_id, cognito_session_credentials.refresh_generation,
cognito_session_credentials.claims_fresh_until_ns, cognito_session_credentials.token_expires_at_ns,
EXISTS(SELECT 1 FROM audit_events WHERE event_id =
  console_sessions.id || '.refresh.' || cognito_session_credentials.refresh_generation
  AND action = 'console.cognito.refresh' AND outcome = 'attempted')
FROM console_sessions JOIN identities ON identities.id = console_sessions.identity_id
LEFT JOIN cognito_identities ON cognito_identities.identity_id = identities.id
LEFT JOIN cognito_session_credentials ON cognito_session_credentials.session_id = console_sessions.id
  AND cognito_session_credentials.identity_id = identities.id
WHERE console_sessions.token_hash = ? AND console_sessions.browser_binding_hash IS NULL`, tokenHash[:]).Scan(
		&source, &version, &revoked, &idle, &absolute, &enabled, &cognitoIdentity,
		&generation, &freshUntil, &tokenExpiry, &reserved,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, session.ErrInvalidSession
	}
	if err != nil {
		return 0, fmt.Errorf("inspect Cognito session refresh state: %w", err)
	}
	now, err = d.transactionTime(now)
	if err != nil {
		return 0, err
	}
	if source != string(session.SourceCognito) || version != 1 || revoked.Valid || !enabled ||
		idle <= now.UnixNano() || absolute <= now.UnixNano() {
		return 0, session.ErrInvalidSession
	}
	if !cognitoIdentity.Valid || !generation.Valid || generation.Int64 < 1 ||
		!freshUntil.Valid || !tokenExpiry.Valid || freshUntil.Int64 < 0 || tokenExpiry.Int64 < 0 {
		return 0, session.ErrCognitoRefreshUnavailable
	}
	switch {
	case reserved:
		state = session.CognitoRefreshBlocked
	case freshUntil.Int64 > now.UnixNano() && tokenExpiry.Int64 > now.UnixNano():
		state = session.CognitoRefreshFresh
	default:
		state = session.CognitoRefreshReady
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit Cognito refresh inspection: %w", err)
	}
	return state, nil
}
