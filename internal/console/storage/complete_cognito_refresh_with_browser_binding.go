package storage

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (d *Database) completeCognitoRefreshWithBrowserBinding(ctx context.Context, request cognito.RefreshCompletionRequest, browserHash [32]byte) (result error) {
	if ctx == nil {
		return cognito.ErrRefreshState
	}
	var details cognito.RefreshCompletionDetails
	var err error
	if browserHash != ([32]byte{}) {
		details, err = request.Refresh.ClaimBound(request.Protector, request.OccurredAt, browserHash)
	} else {
		details, err = request.Refresh.Claim(request.Protector, request.OccurredAt)
	}
	if err != nil {
		return err
	}
	if identity.ValidateSessionID(details.OperationID) != nil || identity.ValidateSessionID(details.SessionID) != nil ||
		identity.ValidateLocalAccountCursor(details.IdentityID) != nil || details.IdentityID == "" {
		return cognito.ErrRefreshState
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Cognito refresh completion: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(cognito.ErrRefreshUnknown, result, fmt.Errorf("roll back Cognito refresh completion: %w", err))
		}
	}()
	transactionTime := d.transactionTime
	if browserHash != ([32]byte{}) {
		transactionTime = d.strictTransactionTime
	}
	now, err := transactionTime(request.OccurredAt)
	if err != nil {
		if browserHash != ([32]byte{}) {
			return errors.Join(cognito.ErrRefreshUnknown, err)
		}
		return err
	}
	event := AuditEvent{
		EventID: details.OperationID + ".refresh.outcome", OperationID: details.OperationID,
		OccurredAt: now, ActorID: details.IdentityID, Source: AuditSourceCognito,
		Action: "console.cognito.refresh", ResourceKind: "session", ResourceID: details.SessionID,
	}
	var reserved, finished bool
	var attemptedAt sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT
EXISTS(SELECT 1 FROM audit_events WHERE event_id = ? AND operation_id = ?
  AND actor_id = ? AND source = 'cognito' AND action = 'console.cognito.refresh'
  AND resource_kind = 'session' AND resource_id = ? AND decision = 'allowed' AND outcome = 'attempted'),
EXISTS(SELECT 1 FROM audit_events WHERE event_id = ?),
(SELECT occurred_at_ns FROM audit_events WHERE event_id = ? AND operation_id = ?)`,
		cognitoRefreshEventID(details.SessionID, details.PreviousGeneration), details.OperationID,
		details.IdentityID, details.SessionID, event.EventID,
		cognitoRefreshEventID(details.SessionID, details.PreviousGeneration), details.OperationID).Scan(&reserved, &finished, &attemptedAt); err != nil {
		return fmt.Errorf("inspect Cognito refresh completion reservation: %w", err)
	}
	if !reserved || finished {
		return cognito.ErrRefreshUnavailable
	}
	if browserHash != ([32]byte{}) && (!attemptedAt.Valid || attemptedAt.Int64 > request.OccurredAt.UnixNano() ||
		details.Identity.IssuedAt.After(request.OccurredAt)) {
		return errors.Join(cognito.ErrRefreshUnknown, ErrInvalidAuditEvent)
	}
	deny := func(cause error) error {
		event.Decision, event.Outcome = "denied", AuditFailed
		if browserHash != ([32]byte{}) {
			event.OccurredAt, err = transactionTime(event.OccurredAt)
			if err != nil {
				return errors.Join(cognito.ErrRefreshUnknown, err)
			}
		}
		if err := insertAuditEvent(ctx, tx, event); err != nil {
			if browserHash != ([32]byte{}) {
				return errors.Join(cognito.ErrRefreshAudit, err)
			}
			return errors.Join(cognito.ErrRefreshAudit, cause, err)
		}
		if err := tx.Commit(); err != nil {
			if browserHash != ([32]byte{}) {
				return errors.Join(cognito.ErrRefreshUnknown, fmt.Errorf("commit Cognito refresh completion denial: %w", err))
			}
			return errors.Join(cognito.ErrRefreshUnknown, cause, fmt.Errorf("commit Cognito refresh completion denial: %w", err))
		}
		if browserHash != ([32]byte{}) {
			if _, err := transactionTime(event.OccurredAt); err != nil {
				return errors.Join(cognito.ErrRefreshUnknown, err)
			}
		}
		return cause
	}
	if _, err := request.Refresh.Details(request.Protector, now); err != nil {
		return deny(errors.Join(cognito.ErrRefreshUnavailable, err))
	}

	var enabled bool
	var source, issuer, subject, audienceJSON, groupsJSON string
	var revoked sql.NullInt64
	var version, created, generation, issued, expiry, freshUntil, idle, absolute int64
	var nonce, encrypted []byte
	bindingPredicate, bindingArgs := browserSessionPredicate("console_sessions", browserHash)
	args := []any{details.SessionID, details.IdentityID}
	args = append(args, bindingArgs...)
	err = tx.QueryRowContext(ctx, `SELECT identities.enabled, console_sessions.auth_source,
console_sessions.credential_version, console_sessions.created_at_ns, console_sessions.revoked_at_ns,
console_sessions.idle_expires_at_ns, console_sessions.absolute_expires_at_ns,
cognito_identities.issuer, cognito_identities.subject, cognito_session_credentials.refresh_generation,
cognito_session_credentials.original_nonce_hash, cognito_session_credentials.audience_json,
cognito_session_credentials.groups_json, cognito_session_credentials.issued_at_ns,
cognito_session_credentials.token_expires_at_ns, cognito_session_credentials.claims_fresh_until_ns,
cognito_session_credentials.encrypted_refresh_token
FROM console_sessions JOIN identities ON identities.id = console_sessions.identity_id
JOIN cognito_identities ON cognito_identities.identity_id = identities.id
JOIN cognito_session_credentials ON cognito_session_credentials.session_id = console_sessions.id
  AND cognito_session_credentials.identity_id = identities.id
WHERE console_sessions.id = ? AND identities.id = ? AND `+bindingPredicate, args...).Scan(
		&enabled, &source, &version, &created, &revoked, &idle, &absolute, &issuer, &subject, &generation,
		&nonce, &audienceJSON, &groupsJSON, &issued, &expiry, &freshUntil, &encrypted,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return deny(cognito.ErrRefreshUnavailable)
	}
	if err != nil {
		return fmt.Errorf("inspect Cognito session for refresh completion: %w", err)
	}
	if browserHash != ([32]byte{}) && (created > request.OccurredAt.UnixNano() || issued > request.OccurredAt.UnixNano()) {
		return errors.Join(cognito.ErrRefreshUnknown, ErrInvalidAuditEvent)
	}
	if !enabled || source != "cognito" || version != 1 || revoked.Valid ||
		idle <= now.UnixNano() || absolute <= now.UnixNano() {
		return deny(cognito.ErrRefreshUnavailable)
	}
	previous := details.PreviousIdentity
	var audience, groups []string
	encryptedHash := sha256.Sum256(encrypted)
	if generation != details.PreviousGeneration || issuer != previous.Issuer || subject != previous.Subject ||
		issued != previous.IssuedAt.UnixNano() || expiry != previous.ExpiresAt.UnixNano() ||
		freshUntil != details.PreviousClaimsFreshUntil.UnixNano() ||
		subtle.ConstantTimeCompare(nonce, details.OriginalNonceHash[:]) != 1 ||
		subtle.ConstantTimeCompare(encryptedHash[:], details.PreviousEncryptedHash[:]) != 1 ||
		len(audienceJSON) > 1<<20 || len(groupsJSON) > 1<<20 ||
		json.Unmarshal([]byte(audienceJSON), &audience) != nil || audience == nil ||
		json.Unmarshal([]byte(groupsJSON), &groups) != nil || groups == nil ||
		!slices.Equal(audience, previous.Audience) || !slices.Equal(groups, previous.Groups) {
		return deny(cognito.ErrRefreshState)
	}
	if _, err := request.Protector.Open(cognito.RefreshTokenBinding{
		SessionID: details.SessionID, IdentityID: details.IdentityID, Generation: generation,
	}, encrypted); err != nil {
		return deny(errors.Join(cognito.ErrRefreshState, err))
	}
	audienceBytes, err := json.Marshal(details.Identity.Audience)
	if err != nil {
		return fmt.Errorf("encode refreshed Cognito audience: %w", err)
	}
	newGroups := details.Identity.Groups
	if newGroups == nil {
		newGroups = []string{}
	}
	groupBytes, err := json.Marshal(newGroups)
	if err != nil {
		return fmt.Errorf("encode refreshed Cognito groups: %w", err)
	}
	now, err = transactionTime(now)
	if err != nil {
		if browserHash != ([32]byte{}) {
			return errors.Join(cognito.ErrRefreshUnknown, err)
		}
		return err
	}
	event.OccurredAt = now
	if _, err := request.Refresh.Details(request.Protector, now); err != nil ||
		idle <= now.UnixNano() || absolute <= now.UnixNano() {
		return deny(errors.Join(cognito.ErrRefreshUnavailable, err))
	}
	updated, err := tx.ExecContext(ctx, `UPDATE cognito_session_credentials
SET audience_json = ?, groups_json = ?, issued_at_ns = ?, token_expires_at_ns = ?,
claims_fresh_until_ns = ?, refresh_generation = ?, encrypted_refresh_token = ?
WHERE session_id = ? AND identity_id = ? AND refresh_generation = ? AND original_nonce_hash = ?
AND encrypted_refresh_token = ? AND audience_json = ? AND groups_json = ?
AND issued_at_ns = ? AND token_expires_at_ns = ? AND claims_fresh_until_ns = ?`,
		string(audienceBytes), string(groupBytes), details.Identity.IssuedAt.UnixNano(),
		details.Identity.ExpiresAt.UnixNano(), details.ClaimsFreshUntil.UnixNano(),
		generation+1, details.EncryptedRefreshToken, details.SessionID, details.IdentityID,
		generation, nonce, encrypted, audienceJSON, groupsJSON, issued, expiry, freshUntil)
	if err != nil {
		return fmt.Errorf("update Cognito refresh credentials: %w", err)
	}
	rows, err := updated.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect Cognito refresh update: %w", err)
	}
	if rows != 1 {
		return cognito.ErrRefreshState
	}
	event.Decision, event.Outcome = "allowed", AuditSucceeded
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return errors.Join(cognito.ErrRefreshAudit, err)
	}
	if err := tx.Commit(); err != nil {
		return errors.Join(cognito.ErrRefreshUnknown, fmt.Errorf("commit Cognito refresh completion: %w", err))
	}
	completedAt, err := transactionTime(event.OccurredAt)
	if err != nil {
		return errors.Join(cognito.ErrRefreshUnknown, err)
	}
	if _, err := request.Refresh.Details(request.Protector, completedAt); err != nil ||
		!time.Unix(0, idle).After(completedAt) || !time.Unix(0, absolute).After(completedAt) {
		if browserHash != ([32]byte{}) {
			return errors.Join(cognito.ErrRefreshUnknown, errors.New("Cognito refresh completion expired after commit"))
		}
		return errors.Join(cognito.ErrRefreshUnavailable, err)
	}
	return nil
}
