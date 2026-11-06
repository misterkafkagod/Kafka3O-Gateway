package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) CreateCognitoSession(ctx context.Context, record session.CognitoRecord) (principal session.Principal, result error) {
	if validateSessionRecord(record.Session) != nil || record.Session.AccountID != "" || record.Session.CredentialVersion != 1 ||
		strings.TrimSpace(record.CandidateAccount) == "" || len(record.CandidateAccount) > 128 {
		return session.Principal{}, session.ErrInvalidSessionConfig
	}
	details, err := record.Exchange.Details(record.Protector, record.Session.CreatedAt)
	if err != nil {
		return session.Principal{}, err
	}
	if err := d.reserveCognitoSession(ctx, record, details); err != nil {
		return session.Principal{}, err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return session.Principal{}, fmt.Errorf("begin Cognito session creation: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(session.ErrCognitoSessionUnknown, result, fmt.Errorf("roll back Cognito session creation: %w", err))
		}
	}()
	now, err := d.transactionTime(record.Session.CreatedAt)
	if err != nil {
		return session.Principal{}, err
	}
	event := AuditEvent{
		EventID: details.OperationID + ".session.outcome", OperationID: details.OperationID, OccurredAt: now,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "console.cognito.session",
		Decision: "denied", Outcome: AuditDenied,
	}
	deny := func(cause error, outcome AuditOutcome) (session.Principal, error) {
		event.Outcome = outcome
		if err := insertAuditEvent(ctx, tx, event); err != nil {
			return session.Principal{}, errors.Join(session.ErrCognitoSessionAudit, cause, err)
		}
		if err := tx.Commit(); err != nil {
			return session.Principal{}, errors.Join(session.ErrCognitoSessionUnknown, cause, fmt.Errorf("commit Cognito session denial: %w", err))
		}
		return session.Principal{}, cause
	}
	details, err = record.Exchange.Details(record.Protector, now)
	if err != nil || !record.Session.IdleExpiresAt.After(now) {
		return deny(errors.Join(session.ErrCognitoSessionUnavailable, err), AuditValidationRejected)
	}
	var accountID string
	var enabled bool
	err = tx.QueryRowContext(ctx, `SELECT identities.id, identities.enabled FROM cognito_identities
JOIN identities ON identities.id = cognito_identities.identity_id
WHERE cognito_identities.issuer = ? AND cognito_identities.subject = ?`,
		details.Identity.Issuer, details.Identity.Subject).Scan(&accountID, &enabled)
	newIdentity := errors.Is(err, sql.ErrNoRows)
	if newIdentity {
		accountID = record.CandidateAccount
	} else if err != nil {
		return session.Principal{}, fmt.Errorf("find Cognito identity: %w", err)
	} else if !enabled {
		event.ActorID, event.Source = accountID, AuditSourceCognito
		return deny(session.ErrCognitoSessionUnavailable, AuditDenied)
	}
	encrypted, err := record.Protector.Seal(cognito.RefreshTokenBinding{
		SessionID: record.Session.SessionID, IdentityID: accountID, Generation: 1,
	}, details.RefreshToken)
	if err != nil {
		return deny(errors.Join(session.ErrCognitoSessionUnavailable, err), AuditFailed)
	}
	now, err = d.transactionTime(now)
	if err != nil {
		return session.Principal{}, err
	}
	event.OccurredAt = now
	details, err = record.Exchange.Details(record.Protector, now)
	if err != nil || !record.Session.IdleExpiresAt.After(now) {
		return deny(errors.Join(session.ErrCognitoSessionUnavailable, err), AuditValidationRejected)
	}
	audience, err := json.Marshal(details.Identity.Audience)
	if err != nil {
		return session.Principal{}, fmt.Errorf("encode Cognito audience: %w", err)
	}
	groups := details.Identity.Groups
	if groups == nil {
		groups = []string{}
	}
	groupJSON, err := json.Marshal(groups)
	if err != nil {
		return session.Principal{}, fmt.Errorf("encode Cognito groups: %w", err)
	}
	if newIdentity {
		if _, err := tx.ExecContext(ctx, "INSERT INTO identities (id, enabled) VALUES (?, 1)", accountID); err != nil {
			return session.Principal{}, fmt.Errorf("create Cognito identity: %w", err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO cognito_identities (identity_id, issuer, subject) VALUES (?, ?, ?)",
			accountID, details.Identity.Issuer, details.Identity.Subject); err != nil {
			return session.Principal{}, fmt.Errorf("bind Cognito identity: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO console_sessions
(id, identity_id, credential_version, token_hash, csrf_hash, created_at_ns, idle_expires_at_ns, absolute_expires_at_ns, auth_source)
VALUES (?, ?, 1, ?, ?, ?, ?, ?, 'cognito')`, record.Session.SessionID, accountID,
		record.Session.TokenHash[:], record.Session.CSRFHash[:], record.Session.CreatedAt.UTC().UnixNano(),
		record.Session.IdleExpiresAt.UTC().UnixNano(), record.Session.AbsoluteExpiresAt.UTC().UnixNano()); err != nil {
		return session.Principal{}, fmt.Errorf("create Cognito session: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cognito_session_credentials
(session_id, identity_id, exchange_operation_id, original_nonce_hash, audience_json, groups_json,
 issued_at_ns, token_expires_at_ns, claims_fresh_until_ns, refresh_generation, encrypted_refresh_token)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)`, record.Session.SessionID, accountID,
		details.OperationID, details.NonceHash[:], string(audience), string(groupJSON),
		details.Identity.IssuedAt.UTC().UnixNano(), details.Identity.ExpiresAt.UTC().UnixNano(),
		details.ClaimsFreshUntil.UTC().UnixNano(), encrypted); err != nil {
		return session.Principal{}, fmt.Errorf("store encrypted Cognito session credentials: %w", err)
	}
	event.ActorID, event.Source, event.Decision, event.Outcome = accountID, AuditSourceCognito, "allowed", AuditSucceeded
	event.ResourceKind, event.ResourceID = "session", record.Session.SessionID
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return session.Principal{}, errors.Join(session.ErrCognitoSessionAudit, err)
	}
	if err := tx.Commit(); err != nil {
		return session.Principal{}, errors.Join(session.ErrCognitoSessionUnknown, fmt.Errorf("commit Cognito session: %w", err))
	}
	completedAt, err := d.transactionTime(event.OccurredAt)
	if err != nil {
		return session.Principal{}, errors.Join(session.ErrCognitoSessionUnknown, err)
	}
	_, err = record.Exchange.Details(record.Protector, completedAt)
	if err != nil || !record.Session.IdleExpiresAt.After(completedAt) {
		return session.Principal{}, errors.Join(session.ErrCognitoSessionUnavailable, err)
	}
	return session.Principal{
		SessionID: record.Session.SessionID, AccountID: accountID, Username: "cognito-user", Source: session.SourceCognito,
		CreatedAt: record.Session.CreatedAt.UTC(), IdleExpiresAt: record.Session.IdleExpiresAt.UTC(),
		AbsoluteExpiresAt: record.Session.AbsoluteExpiresAt.UTC(), CSRFHash: record.Session.CSRFHash,
	}, nil
}
