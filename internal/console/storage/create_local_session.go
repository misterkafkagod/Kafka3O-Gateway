package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) CreateLocalSession(ctx context.Context, record session.Record) (principal session.Principal, result error) {
	if strings.TrimSpace(record.AccountID) == "" || len(record.AccountID) > 128 || validateSessionRecord(record) != nil {
		return session.Principal{}, session.ErrInvalidSessionConfig
	}
	event := AuditEvent{
		EventID: record.EventID, OperationID: record.EventID, OccurredAt: record.CreatedAt,
		ActorID: record.AccountID, Source: AuditSourceLocal, Action: "console.session.start",
		ResourceKind: "session", ResourceID: record.SessionID, Decision: "allowed", Outcome: AuditSucceeded,
	}
	if err := validateAuditEvent(event); err != nil {
		return session.Principal{}, err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return session.Principal{}, fmt.Errorf("begin session creation: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back session creation: %w", err))
			if record.BrowserBindingHash != ([32]byte{}) {
				result = browserSessionOutcomeUnknown(result)
			}
		}
	}()
	var username string
	var mustChange bool
	err = tx.QueryRowContext(ctx, `SELECT local_accounts.username, local_accounts.must_change_password
FROM local_accounts JOIN identities ON identities.id = local_accounts.identity_id
WHERE identities.id = ? AND identities.enabled = 1 AND local_accounts.credential_version = ?`,
		record.AccountID, record.CredentialVersion).Scan(&username, &mustChange)
	if errors.Is(err, sql.ErrNoRows) {
		event.ActorID, event.Source, event.Decision, event.Outcome = "anonymous", AuditSourceAnonymous, "denied", AuditDenied
		event.ResourceKind, event.ResourceID = "", ""
		if err := insertAuditEvent(ctx, tx, event); err != nil {
			return session.Principal{}, err
		}
		if err := tx.Commit(); err != nil {
			return session.Principal{}, fmt.Errorf("commit denied session creation: %w", err)
		}
		return session.Principal{}, session.ErrInvalidSession
	}
	if err != nil {
		return session.Principal{}, fmt.Errorf("recheck session identity: %w", err)
	}
	checkedAt, err := d.validateBrowserSessionLifetime(record, record.CreatedAt)
	if err != nil {
		if err != session.ErrInvalidSession {
			return session.Principal{}, err
		}
		event.OccurredAt, event.Decision, event.Outcome = checkedAt, "denied", AuditValidationRejected
		event.ResourceKind, event.ResourceID = "", ""
		if err := insertAuditEvent(ctx, tx, event); err != nil {
			return session.Principal{}, err
		}
		if err := tx.Commit(); err != nil {
			return session.Principal{}, fmt.Errorf("commit expired browser session denial: %w", err)
		}
		return session.Principal{}, session.ErrInvalidSession
	}
	event.OccurredAt = checkedAt
	previous, err := d.checkBrowserSession(ctx, tx, record.BrowserBindingHash, checkedAt)
	if err != nil {
		if err != session.ErrBrowserSessionConflict {
			return session.Principal{}, err
		}
		event.OccurredAt = previous.checkedAt
		event.Decision, event.Outcome = "denied", AuditDenied
		event.ResourceKind, event.ResourceID = "", ""
		if err := insertAuditEvent(ctx, tx, event); err != nil {
			return session.Principal{}, err
		}
		if err := tx.Commit(); err != nil {
			return session.Principal{}, fmt.Errorf("commit browser session denial: %w", err)
		}
		return session.Principal{}, session.ErrBrowserSessionConflict
	}
	checkedAt = previous.checkedAt
	event.OccurredAt = checkedAt
	if err := retireBrowserSession(ctx, tx, previous, checkedAt, event.OperationID); err != nil {
		return session.Principal{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO console_sessions
(id, identity_id, credential_version, token_hash, csrf_hash, created_at_ns, idle_expires_at_ns, absolute_expires_at_ns, browser_binding_hash)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, record.SessionID, record.AccountID, record.CredentialVersion,
		record.TokenHash[:], record.CSRFHash[:], record.CreatedAt.UTC().UnixNano(),
		record.IdleExpiresAt.UTC().UnixNano(), record.AbsoluteExpiresAt.UTC().UnixNano(), browserBindingValue(record.BrowserBindingHash))
	if err != nil {
		return session.Principal{}, fmt.Errorf("create local session: %w", err)
	}
	if err := bindBrowserSession(ctx, tx, record.BrowserBindingHash, record.SessionID); err != nil {
		return session.Principal{}, err
	}
	checkedAt, err = d.validateBrowserSessionLifetime(record, checkedAt)
	if err != nil {
		return session.Principal{}, browserSessionOutcomeUnknown(err)
	}
	event.OccurredAt = checkedAt
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return session.Principal{}, err
	}
	if err := tx.Commit(); err != nil {
		if record.BrowserBindingHash != ([32]byte{}) {
			return session.Principal{}, errors.Join(session.ErrBrowserSessionUnknown, fmt.Errorf("commit local session: %w", err))
		}
		return session.Principal{}, fmt.Errorf("commit local session: %w", err)
	}
	if _, err := d.validateBrowserSessionLifetime(record, checkedAt); err != nil {
		return session.Principal{}, browserSessionOutcomeUnknown(err)
	}
	return session.Principal{
		SessionID: record.SessionID, AccountID: record.AccountID, Username: username, Source: session.SourceLocal, MustChangePassword: mustChange,
		CreatedAt: record.CreatedAt.UTC(), IdleExpiresAt: record.IdleExpiresAt.UTC(),
		AbsoluteExpiresAt: record.AbsoluteExpiresAt.UTC(), CSRFHash: record.CSRFHash,
	}, nil
}
