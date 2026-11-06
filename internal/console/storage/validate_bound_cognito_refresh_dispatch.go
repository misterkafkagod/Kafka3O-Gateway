package storage

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (d *Database) ValidateBoundCognitoRefreshDispatch(ctx context.Context, request cognito.BoundRefreshDispatchRequest) (result error) {
	reservation := request.Reservation
	previous := reservation.PreviousIdentity
	if ctx == nil || request.TokenHash == ([32]byte{}) || request.BrowserBindingHash == ([32]byte{}) ||
		identity.ValidateSessionID(reservation.OperationID) != nil || identity.ValidateSessionID(reservation.SessionID) != nil ||
		reservation.IdentityID == "" || identity.ValidateLocalAccountCursor(reservation.IdentityID) != nil ||
		reservation.Generation < 1 || reservation.Generation == math.MaxInt64 ||
		reservation.OriginalNonceHash == ([32]byte{}) || len(reservation.EncryptedRefreshToken) == 0 ||
		previous.Issuer == "" || !cognito.ValidSubject(previous.Subject) || len(previous.Audience) == 0 || previous.Groups == nil {
		return cognito.ErrRefreshState
	}
	for _, instant := range []time.Time{request.OccurredAt, previous.IssuedAt, previous.ExpiresAt,
		reservation.PreviousClaimsFreshUntil, reservation.IdleExpiresAt, reservation.AbsoluteExpiresAt} {
		if instant.IsZero() || instant.Year() < 1970 || instant.Year() > 2261 {
			return cognito.ErrRefreshState
		}
	}
	if !previous.IssuedAt.Before(previous.ExpiresAt) || reservation.PreviousClaimsFreshUntil.After(previous.ExpiresAt) {
		return cognito.ErrRefreshState
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin bound Cognito refresh dispatch validation: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back bound Cognito refresh dispatch validation: %w", err))
		}
	}()
	startedAt, err := d.strictTransactionTime(request.OccurredAt)
	if err != nil {
		return err
	}
	var enabled bool
	var source, issuer, subject, audienceJSON, groupsJSON string
	var revoked sql.NullInt64
	var version, created, idle, absolute, generation, issued, expiry, freshUntil, attemptedAt int64
	var nonce, encrypted []byte
	bindingPredicate, bindingArgs := browserSessionPredicate("console_sessions", request.BrowserBindingHash)
	args := []any{cognitoRefreshEventID(reservation.SessionID, reservation.Generation), reservation.OperationID,
		reservation.SessionID, reservation.IdentityID, request.TokenHash[:]}
	args = append(args, bindingArgs...)
	args = append(args, reservation.OperationID+".refresh.outcome")
	err = tx.QueryRowContext(ctx, `SELECT identities.enabled, console_sessions.auth_source, console_sessions.credential_version,
console_sessions.created_at_ns, console_sessions.revoked_at_ns, console_sessions.idle_expires_at_ns,
console_sessions.absolute_expires_at_ns, cognito_identities.issuer, cognito_identities.subject,
cognito_session_credentials.refresh_generation, cognito_session_credentials.original_nonce_hash,
cognito_session_credentials.encrypted_refresh_token, cognito_session_credentials.audience_json,
cognito_session_credentials.groups_json, cognito_session_credentials.issued_at_ns,
cognito_session_credentials.token_expires_at_ns, cognito_session_credentials.claims_fresh_until_ns,
refresh_attempt.occurred_at_ns
FROM console_sessions JOIN identities ON identities.id = console_sessions.identity_id
JOIN cognito_identities ON cognito_identities.identity_id = identities.id
JOIN cognito_session_credentials ON cognito_session_credentials.session_id = console_sessions.id
  AND cognito_session_credentials.identity_id = identities.id
JOIN audit_events AS refresh_attempt ON refresh_attempt.event_id = ? AND refresh_attempt.operation_id = ?
  AND refresh_attempt.actor_id = identities.id AND refresh_attempt.source = 'cognito'
  AND refresh_attempt.action = 'console.cognito.refresh' AND refresh_attempt.resource_kind = 'session'
  AND refresh_attempt.resource_id = console_sessions.id AND refresh_attempt.decision = 'allowed'
  AND refresh_attempt.outcome = 'attempted'
WHERE console_sessions.id = ? AND identities.id = ? AND console_sessions.token_hash = ? AND `+bindingPredicate+`
AND NOT EXISTS (SELECT 1 FROM audit_events WHERE event_id = ?)`, args...).Scan(
		&enabled, &source, &version, &created, &revoked, &idle, &absolute, &issuer, &subject,
		&generation, &nonce, &encrypted, &audienceJSON, &groupsJSON, &issued, &expiry, &freshUntil, &attemptedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return cognito.ErrRefreshUnavailable
	}
	if err != nil {
		return fmt.Errorf("inspect bound Cognito refresh dispatch state: %w", err)
	}
	if created > request.OccurredAt.UnixNano() || issued > request.OccurredAt.UnixNano() ||
		attemptedAt > request.OccurredAt.UnixNano() {
		return ErrInvalidAuditEvent
	}
	if !enabled || source != "cognito" || version != 1 || revoked.Valid {
		return cognito.ErrRefreshUnavailable
	}
	var audience, groups []string
	if generation != reservation.Generation || issuer != previous.Issuer || subject != previous.Subject ||
		idle != reservation.IdleExpiresAt.UnixNano() || absolute != reservation.AbsoluteExpiresAt.UnixNano() ||
		issued != previous.IssuedAt.UnixNano() || expiry != previous.ExpiresAt.UnixNano() ||
		freshUntil != reservation.PreviousClaimsFreshUntil.UnixNano() ||
		subtle.ConstantTimeCompare(nonce, reservation.OriginalNonceHash[:]) != 1 ||
		subtle.ConstantTimeCompare(encrypted, reservation.EncryptedRefreshToken) != 1 ||
		len(audienceJSON) > 1<<20 || len(groupsJSON) > 1<<20 ||
		json.Unmarshal([]byte(audienceJSON), &audience) != nil || audience == nil ||
		json.Unmarshal([]byte(groupsJSON), &groups) != nil || groups == nil ||
		!slices.Equal(audience, previous.Audience) || !slices.Equal(groups, previous.Groups) {
		return cognito.ErrRefreshState
	}
	checkedAt, err := d.strictTransactionTime(startedAt)
	if err != nil {
		return err
	}
	if idle <= checkedAt.UnixNano() || absolute <= checkedAt.UnixNano() ||
		!reservation.IdleExpiresAt.After(checkedAt) || !reservation.AbsoluteExpiresAt.After(checkedAt) {
		return cognito.ErrRefreshUnavailable
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit bound Cognito refresh dispatch validation: %w", err)
	}
	completedAt, err := d.strictTransactionTime(checkedAt)
	if err != nil {
		return err
	}
	if idle <= completedAt.UnixNano() || absolute <= completedAt.UnixNano() ||
		!reservation.IdleExpiresAt.After(completedAt) || !reservation.AbsoluteExpiresAt.After(completedAt) {
		return cognito.ErrRefreshUnavailable
	}
	return nil
}
