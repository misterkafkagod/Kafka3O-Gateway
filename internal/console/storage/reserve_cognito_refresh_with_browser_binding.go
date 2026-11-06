package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (d *Database) reserveCognitoRefreshWithBrowserBinding(ctx context.Context, request cognito.RefreshReservationRequest, browserHash [32]byte) (reservation cognito.RefreshReservation, result error) {
	if ctx == nil || identity.ValidateSessionID(request.OperationID) != nil || request.TokenHash == ([32]byte{}) || request.Protector == nil {
		return cognito.RefreshReservation{}, cognito.ErrRefreshUnavailable
	}
	event := AuditEvent{
		EventID: request.OperationID + ".refresh.denied", OperationID: request.OperationID, OccurredAt: request.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "console.cognito.refresh",
		Decision: "denied", Outcome: AuditDenied,
	}
	if err := validateAuditEvent(event); err != nil {
		return cognito.RefreshReservation{}, err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return cognito.RefreshReservation{}, fmt.Errorf("begin Cognito refresh reservation: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			reservation = cognito.RefreshReservation{}
			result = errors.Join(cognito.ErrRefreshUnknown, result, fmt.Errorf("roll back Cognito refresh reservation: %w", err))
		}
	}()
	transactionTime := d.transactionTime
	if browserHash != ([32]byte{}) {
		transactionTime = d.strictTransactionTime
	}
	event.OccurredAt, err = transactionTime(request.OccurredAt)
	if err != nil {
		return cognito.RefreshReservation{}, err
	}
	deny := func(cause error) (cognito.RefreshReservation, error) {
		if browserHash != ([32]byte{}) {
			event.OccurredAt, err = transactionTime(event.OccurredAt)
			if err != nil {
				return cognito.RefreshReservation{}, err
			}
		}
		if err := insertAuditEvent(ctx, tx, event); err != nil {
			return cognito.RefreshReservation{}, errors.Join(cognito.ErrRefreshAudit, err)
		}
		if err := tx.Commit(); err != nil {
			return cognito.RefreshReservation{}, errors.Join(cognito.ErrRefreshUnknown, fmt.Errorf("commit Cognito refresh denial: %w", err))
		}
		if browserHash != ([32]byte{}) {
			if _, err := transactionTime(event.OccurredAt); err != nil {
				return cognito.RefreshReservation{}, errors.Join(cognito.ErrRefreshUnknown, err)
			}
		}
		return cognito.RefreshReservation{}, cause
	}
	var nonceHash []byte
	var audienceJSON, groupsJSON string
	var created, issued, tokenExpiry, freshUntil, idleExpiry, absoluteExpiry int64
	bindingPredicate, bindingArgs := browserSessionPredicate("console_sessions", browserHash)
	args := []any{request.TokenHash[:]}
	args = append(args, bindingArgs...)
	args = append(args, event.OccurredAt.UnixNano(), event.OccurredAt.UnixNano())
	err = tx.QueryRowContext(ctx, `SELECT console_sessions.id, identities.id, cognito_session_credentials.refresh_generation,
cognito_identities.issuer, cognito_identities.subject, cognito_session_credentials.audience_json,
cognito_session_credentials.groups_json, cognito_session_credentials.issued_at_ns,
cognito_session_credentials.token_expires_at_ns, cognito_session_credentials.claims_fresh_until_ns,
cognito_session_credentials.original_nonce_hash,
cognito_session_credentials.encrypted_refresh_token, console_sessions.created_at_ns, console_sessions.idle_expires_at_ns,
console_sessions.absolute_expires_at_ns
FROM console_sessions JOIN identities ON identities.id = console_sessions.identity_id
JOIN cognito_session_credentials ON cognito_session_credentials.session_id = console_sessions.id
  AND cognito_session_credentials.identity_id = identities.id
JOIN cognito_identities ON cognito_identities.identity_id = identities.id
WHERE console_sessions.token_hash = ? AND `+bindingPredicate+` AND console_sessions.auth_source = 'cognito'
  AND console_sessions.credential_version = 1 AND console_sessions.revoked_at_ns IS NULL
  AND identities.enabled = 1 AND console_sessions.idle_expires_at_ns > ?
  AND console_sessions.absolute_expires_at_ns > ?`, args...).Scan(
		&reservation.SessionID, &reservation.IdentityID, &reservation.Generation,
		&reservation.PreviousIdentity.Issuer, &reservation.PreviousIdentity.Subject, &audienceJSON, &groupsJSON,
		&issued, &tokenExpiry, &freshUntil, &nonceHash, &reservation.EncryptedRefreshToken, &created, &idleExpiry, &absoluteExpiry,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return deny(cognito.ErrRefreshUnavailable)
	}
	if err != nil {
		return cognito.RefreshReservation{}, fmt.Errorf("inspect Cognito session for refresh: %w", err)
	}
	if browserHash != ([32]byte{}) && (created > event.OccurredAt.UnixNano() || issued > event.OccurredAt.UnixNano()) {
		return cognito.RefreshReservation{}, ErrInvalidAuditEvent
	}
	event.ActorID, event.Source = reservation.IdentityID, AuditSourceCognito
	if identity.ValidateSessionID(reservation.SessionID) != nil || reservation.IdentityID == "" || identity.ValidateLocalAccountCursor(reservation.IdentityID) != nil ||
		reservation.Generation < 1 || reservation.Generation == math.MaxInt64 || len(nonceHash) != len(reservation.OriginalNonceHash) ||
		len(audienceJSON) > 1<<20 || len(groupsJSON) > 1<<20 ||
		json.Unmarshal([]byte(audienceJSON), &reservation.PreviousIdentity.Audience) != nil ||
		json.Unmarshal([]byte(groupsJSON), &reservation.PreviousIdentity.Groups) != nil ||
		len(reservation.PreviousIdentity.Audience) == 0 || reservation.PreviousIdentity.Groups == nil ||
		reservation.PreviousIdentity.Issuer == "" || !cognito.ValidSubject(reservation.PreviousIdentity.Subject) ||
		issued < 0 || issued >= tokenExpiry || freshUntil < 0 || freshUntil > tokenExpiry {
		return deny(cognito.ErrRefreshState)
	}
	copy(reservation.OriginalNonceHash[:], nonceHash)
	if reservation.OriginalNonceHash == ([32]byte{}) {
		return deny(cognito.ErrRefreshState)
	}
	for _, group := range reservation.PreviousIdentity.Groups {
		if group == "" {
			return deny(cognito.ErrRefreshState)
		}
	}
	reservation.PreviousIdentity.IssuedAt = time.Unix(0, issued).UTC()
	reservation.PreviousIdentity.ExpiresAt = time.Unix(0, tokenExpiry).UTC()
	reservation.PreviousClaimsFreshUntil = time.Unix(0, freshUntil).UTC()
	reservation.IdleExpiresAt = time.Unix(0, idleExpiry).UTC()
	reservation.AbsoluteExpiresAt = time.Unix(0, absoluteExpiry).UTC()
	binding := cognito.RefreshTokenBinding{
		SessionID: reservation.SessionID, IdentityID: reservation.IdentityID, Generation: reservation.Generation,
	}
	if _, err := request.Protector.Open(binding, reservation.EncryptedRefreshToken); err != nil {
		return deny(fmt.Errorf("validate Cognito refresh credentials: %w", err))
	}
	event.OccurredAt, err = transactionTime(event.OccurredAt)
	if err != nil {
		return cognito.RefreshReservation{}, err
	}
	if !reservation.IdleExpiresAt.After(event.OccurredAt) || !reservation.AbsoluteExpiresAt.After(event.OccurredAt) {
		return deny(cognito.ErrRefreshUnavailable)
	}
	reservationEventID := cognitoRefreshEventID(reservation.SessionID, reservation.Generation)
	var reserved bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM audit_events WHERE event_id = ?)", reservationEventID).Scan(&reserved); err != nil {
		return cognito.RefreshReservation{}, fmt.Errorf("inspect previous Cognito refresh: %w", err)
	}
	if reserved {
		return deny(cognito.ErrRefreshUnavailable)
	}
	event.EventID, event.Decision, event.Outcome = reservationEventID, "allowed", AuditAttempted
	event.ResourceKind, event.ResourceID = "session", reservation.SessionID
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return cognito.RefreshReservation{}, errors.Join(cognito.ErrRefreshAudit, err)
	}
	if err := tx.Commit(); err != nil {
		return cognito.RefreshReservation{}, errors.Join(cognito.ErrRefreshUnknown, fmt.Errorf("commit Cognito refresh reservation: %w", err))
	}
	completedAt, err := transactionTime(event.OccurredAt)
	if err != nil {
		return cognito.RefreshReservation{}, errors.Join(cognito.ErrRefreshUnknown, err)
	}
	if !reservation.IdleExpiresAt.After(completedAt) || !reservation.AbsoluteExpiresAt.After(completedAt) {
		if browserHash != ([32]byte{}) {
			return cognito.RefreshReservation{}, errors.Join(cognito.ErrRefreshUnknown, errors.New("Cognito refresh reservation expired after commit"))
		}
		return cognito.RefreshReservation{}, cognito.ErrRefreshUnavailable
	}
	reservation.OperationID = request.OperationID
	return reservation, nil
}
