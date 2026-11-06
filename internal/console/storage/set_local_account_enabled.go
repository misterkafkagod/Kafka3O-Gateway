package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (d *Database) SetLocalAccountEnabled(ctx context.Context, change identity.LocalAccountStateChange) (result error) {
	action := "console.local_account.disable"
	if change.Enabled {
		action = "console.local_account.enable"
	}
	attempt := AuditEvent{
		EventID: change.OperationID, OperationID: change.OperationID, OccurredAt: change.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: action, Outcome: AuditAttempted,
	}
	if err := validateAuditEvent(attempt); err != nil || change.EventID == "" || change.RevocationEventID == "" ||
		change.OperationID == change.EventID || change.OperationID == change.RevocationEventID || change.EventID == change.RevocationEventID {
		return ErrInvalidAuditEvent
	}
	if err := d.AppendAuditEvent(ctx, attempt); err != nil {
		return errors.Join(identity.ErrAccountStateAudit, fmt.Errorf("record account state attempt: %w", err))
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin local account state change: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(identity.ErrAccountStateUnknown, result, fmt.Errorf("roll back local account state change: %w", err))
		}
	}()
	change.OccurredAt, err = d.transactionTime(change.OccurredAt)
	if err != nil {
		return err
	}
	event := AuditEvent{
		EventID: change.EventID, OperationID: change.OperationID, OccurredAt: change.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: action, Decision: "denied", Outcome: AuditDenied,
	}
	authorizationErr := authorizeLocalGlobalAdmin(ctx, tx, change.Actor, change.OccurredAt)
	var decisionErr error
	switch {
	case errors.Is(authorizationErr, identity.ErrInvalidAdminSession):
		decisionErr = authorizationErr
	case errors.Is(authorizationErr, identity.ErrGlobalAdminRequired), errors.Is(authorizationErr, identity.ErrAdminPasswordChange):
		event.ActorID, event.Source = change.Actor.AccountID, AuditSourceLocal
		decisionErr = authorizationErr
	case authorizationErr != nil:
		return authorizationErr
	default:
		event.ActorID, event.Source, event.Decision = change.Actor.AccountID, AuditSourceLocal, "allowed"
		if strings.TrimSpace(change.TargetID) == "" || len(change.TargetID) > 128 {
			event.Outcome, decisionErr = AuditValidationRejected, identity.ErrInvalidLocalAccount
			break
		}
		var currentlyEnabled, globalAdmin bool
		err := tx.QueryRowContext(ctx, `SELECT identities.enabled, EXISTS (
SELECT 1 FROM role_grants WHERE account_id = identities.id AND cluster_id = '' AND role = 'ADMIN'
) FROM identities JOIN local_accounts ON local_accounts.identity_id = identities.id
WHERE identities.id = ?`, change.TargetID).Scan(&currentlyEnabled, &globalAdmin)
		if errors.Is(err, sql.ErrNoRows) {
			event.Outcome, decisionErr = AuditValidationRejected, identity.ErrAccountNotFound
			break
		}
		if err != nil {
			return fmt.Errorf("inspect target local account: %w", err)
		}
		event.ResourceKind, event.ResourceID = "local_account", change.TargetID
		if !change.Enabled && currentlyEnabled && globalAdmin {
			var count int
			err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM identities
JOIN local_accounts ON local_accounts.identity_id = identities.id
JOIN role_grants ON role_grants.account_id = identities.id
WHERE identities.enabled = 1 AND role_grants.cluster_id = '' AND role_grants.role = 'ADMIN'`).Scan(&count)
			if err != nil {
				return fmt.Errorf("count enabled local global administrators: %w", err)
			}
			if count <= 1 {
				event.Decision, event.Outcome, decisionErr = "denied", AuditDenied, identity.ErrLastLocalGlobalAdmin
				break
			}
		}
		event.Outcome = AuditSucceeded
		if currentlyEnabled != change.Enabled {
			if _, err := tx.ExecContext(ctx, "UPDATE identities SET enabled = ? WHERE id = ?", change.Enabled, change.TargetID); err != nil {
				return fmt.Errorf("change local account state: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE local_accounts SET credential_version = credential_version + 1
WHERE identity_id = ?`, change.TargetID); err != nil {
				return fmt.Errorf("invalidate local account credentials: %w", err)
			}
			revoked, err := tx.ExecContext(ctx, `UPDATE console_sessions SET revoked_at_ns = ?
WHERE identity_id = ? AND revoked_at_ns IS NULL`, change.OccurredAt.UTC().UnixNano(), change.TargetID)
			if err != nil {
				return fmt.Errorf("revoke local account sessions: %w", err)
			}
			count, err := revoked.RowsAffected()
			if err != nil {
				return fmt.Errorf("inspect revoked local account sessions: %w", err)
			}
			if count > 0 {
				revocation := AuditEvent{
					EventID: change.RevocationEventID, OperationID: change.OperationID, OccurredAt: change.OccurredAt,
					ActorID: change.Actor.AccountID, Source: AuditSourceLocal,
					Action: "console.sessions.revoke_on_account_state_change", ResourceKind: "local_account",
					ResourceID: change.TargetID, Decision: "allowed", Outcome: AuditSucceeded,
				}
				if err := insertAuditEvent(ctx, tx, revocation); err != nil {
					return errors.Join(identity.ErrAccountStateAudit, fmt.Errorf("record account state session revocation: %w", err))
				}
			}
		}
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return errors.Join(identity.ErrAccountStateAudit, fmt.Errorf("record account state outcome: %w", err))
	}
	if err := tx.Commit(); err != nil {
		return errors.Join(identity.ErrAccountStateUnknown, fmt.Errorf("commit local account state change: %w", err))
	}
	return decisionErr
}
