package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) useSessionWithBrowserBinding(ctx context.Context, tokenHash, browserHash [32]byte, clock func() time.Time, idleLifetime time.Duration,
	protector *cognito.RefreshTokenProtector) (principal session.Principal, result error) {
	if ctx == nil || tokenHash == ([32]byte{}) || clock == nil || idleLifetime <= 0 || idleLifetime > session.MaxAbsoluteLifetime {
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
	now := clock().UTC()
	newIdleExpiry := now.Add(idleLifetime)
	if now.IsZero() || now.Year() < 1970 || newIdleExpiry.Year() > 2261 || !newIdleExpiry.After(now) {
		return session.Principal{}, session.ErrInvalidSessionConfig
	}
	var created, idle, absolute int64
	var csrfHash []byte
	var source string
	bindingPredicate, bindingArgs := browserSessionPredicate("console_sessions", browserHash)
	args := []any{newIdleExpiry.UTC().UnixNano(), tokenHash[:]}
	args = append(args, bindingArgs...)
	args = append(args, now.UTC().UnixNano(), now.UTC().UnixNano(), protector != nil,
		now.UTC().UnixNano(), now.UTC().UnixNano())
	err = tx.QueryRowContext(ctx, `UPDATE console_sessions
SET idle_expires_at_ns = min(absolute_expires_at_ns, ?)
WHERE token_hash = ? AND `+bindingPredicate+` AND revoked_at_ns IS NULL AND idle_expires_at_ns > ? AND absolute_expires_at_ns > ?
AND EXISTS (
    SELECT 1 FROM identities WHERE identities.id = console_sessions.identity_id AND identities.enabled = 1
      AND (
        (console_sessions.auth_source = 'local'
          AND EXISTS (SELECT 1 FROM local_accounts WHERE local_accounts.identity_id = identities.id
            AND local_accounts.credential_version = console_sessions.credential_version))
        OR EXISTS (
          SELECT 1 FROM cognito_session_credentials JOIN cognito_identities
            ON cognito_identities.identity_id = cognito_session_credentials.identity_id
          WHERE cognito_session_credentials.session_id = console_sessions.id
            AND cognito_session_credentials.identity_id = identities.id
            AND console_sessions.auth_source = 'cognito'
            AND console_sessions.credential_version = 1
            AND ? = 1
            AND cognito_session_credentials.claims_fresh_until_ns > ?
            AND cognito_session_credentials.token_expires_at_ns > ?
            AND NOT EXISTS (
              SELECT 1 FROM audit_events
              WHERE event_id = console_sessions.id || '.refresh.' || cognito_session_credentials.refresh_generation
                AND action = 'console.cognito.refresh' AND outcome = 'attempted'
            )
        )
      )
)
RETURNING id, identity_id, created_at_ns, idle_expires_at_ns, absolute_expires_at_ns, csrf_hash, auth_source`,
		args...).Scan(
		&principal.SessionID, &principal.AccountID, &created, &idle, &absolute, &csrfHash, &source,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return session.Principal{}, session.ErrInvalidSession
	}
	if err != nil {
		return session.Principal{}, fmt.Errorf("validate session: %w", err)
	}
	if len(csrfHash) != len(principal.CSRFHash) {
		return session.Principal{}, session.ErrInvalidSessionConfig
	}
	copy(principal.CSRFHash[:], csrfHash)
	principal.CreatedAt = time.Unix(0, created).UTC()
	principal.IdleExpiresAt = time.Unix(0, idle).UTC()
	principal.AbsoluteExpiresAt = time.Unix(0, absolute).UTC()
	if browserHash != ([32]byte{}) && now.Before(principal.CreatedAt) {
		return session.Principal{}, session.ErrInvalidSessionConfig
	}
	var freshUntil, tokenExpiry, checkedAt time.Time
	switch source {
	case string(session.SourceCognito):
		var generation, issuedNS, freshUntilNS, tokenExpiryNS int64
		var encrypted []byte
		if err := tx.QueryRowContext(ctx, `SELECT refresh_generation, issued_at_ns, claims_fresh_until_ns,
token_expires_at_ns, encrypted_refresh_token FROM cognito_session_credentials
WHERE session_id = ? AND identity_id = ?`, principal.SessionID, principal.AccountID).Scan(
			&generation, &issuedNS, &freshUntilNS, &tokenExpiryNS, &encrypted,
		); err != nil {
			return session.Principal{}, fmt.Errorf("read Cognito session credentials: %w", err)
		}
		if browserHash != ([32]byte{}) && issuedNS > now.UnixNano() {
			return session.Principal{}, session.ErrInvalidSessionConfig
		}
		binding := cognito.RefreshTokenBinding{SessionID: principal.SessionID, IdentityID: principal.AccountID, Generation: generation}
		if _, err := protector.Open(binding, encrypted); err != nil {
			return session.Principal{}, fmt.Errorf("validate Cognito session credentials: %w", err)
		}
		freshUntil, tokenExpiry = time.Unix(0, freshUntilNS).UTC(), time.Unix(0, tokenExpiryNS).UTC()
		checkedAt = clock().UTC()
		if browserHash != ([32]byte{}) && (checkedAt.Before(now) || checkedAt.Year() > 2261) {
			return session.Principal{}, session.ErrInvalidSessionConfig
		}
		if checkedAt.Before(now) || checkedAt.Year() > 2261 || !principal.IdleExpiresAt.After(checkedAt) ||
			!principal.AbsoluteExpiresAt.After(checkedAt) || !freshUntil.After(checkedAt) || !tokenExpiry.After(checkedAt) {
			return session.Principal{}, session.ErrInvalidSession
		}
		principal.Username, principal.Source = "cognito-user", session.SourceCognito
	case string(session.SourceLocal):
		if err := tx.QueryRowContext(ctx, "SELECT username, must_change_password FROM local_accounts WHERE identity_id = ?", principal.AccountID).Scan(&principal.Username, &principal.MustChangePassword); err != nil {
			return session.Principal{}, fmt.Errorf("read session account restrictions: %w", err)
		}
		principal.Source = session.SourceLocal
		if browserHash != ([32]byte{}) {
			checkedAt = clock().UTC()
			if checkedAt.Before(now) || checkedAt.Year() > 2261 {
				return session.Principal{}, session.ErrInvalidSessionConfig
			}
			if !principal.IdleExpiresAt.After(checkedAt) || !principal.AbsoluteExpiresAt.After(checkedAt) {
				return session.Principal{}, session.ErrInvalidSession
			}
		}
	default:
		return session.Principal{}, session.ErrInvalidSession
	}
	if err := tx.Commit(); err != nil {
		return session.Principal{}, fmt.Errorf("commit session validation: %w", err)
	}
	if principal.Source == session.SourceCognito || browserHash != ([32]byte{}) {
		completedAt := clock().UTC()
		if browserHash != ([32]byte{}) && (completedAt.Before(checkedAt) || completedAt.Year() > 2261) {
			return session.Principal{}, session.ErrInvalidSessionConfig
		}
		if completedAt.Before(checkedAt) || completedAt.Year() > 2261 || !principal.IdleExpiresAt.After(completedAt) ||
			!principal.AbsoluteExpiresAt.After(completedAt) ||
			(principal.Source == session.SourceCognito && (!freshUntil.After(completedAt) || !tokenExpiry.After(completedAt))) {
			return session.Principal{}, session.ErrInvalidSession
		}
	}
	return principal, nil
}
