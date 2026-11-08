package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

// ChangeRoleGrant sets or removes one direct grant for a current local global ADMIN and returns the
// committed policy revision. The actor cannot change its own grants, which also keeps at least one
// enabled local global ADMIN (the actor) in place.
func (d *Database) ChangeRoleGrant(ctx context.Context, change identity.RoleGrantChange) (revision int64, result error) {
	const action = "console.role_grant.change"
	attempt := AuditEvent{
		EventID: change.OperationID, OperationID: change.OperationID, OccurredAt: change.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: action, Outcome: AuditAttempted,
	}
	if err := validateAuditEvent(attempt); err != nil || change.EventID == "" || change.OperationID == change.EventID {
		return 0, ErrInvalidAuditEvent
	}
	if err := d.AppendAuditEvent(ctx, attempt); err != nil {
		return 0, errors.Join(identity.ErrAccessChangeAudit, fmt.Errorf("record role grant attempt: %w", err))
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin role grant change: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(identity.ErrAccessChangeUnknown, result, fmt.Errorf("roll back role grant change: %w", err))
		}
	}()
	change.OccurredAt, err = d.adminTransactionTime(change.Actor, change.OccurredAt)
	if err != nil {
		return 0, err
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
		return 0, authorizationErr
	default:
		event.ActorID, event.Source, event.Decision = change.Actor.AccountID, AuditSourceLocal, "allowed"
		if !validAuditState(string(change.ExpectedRole)) || !validAuditState(string(change.Role)) ||
			len(change.TargetID) > 128 || len(change.ClusterID) > 128 {
			event.Outcome, decisionErr = AuditValidationRejected, identity.ErrInvalidAccessChange
			break
		}
		event.ResourceKind, event.ResourceID, event.ClusterID = "identity", change.TargetID, change.ClusterID
		if change.TargetID == change.Actor.AccountID {
			event.Decision, decisionErr = "denied", identity.ErrSelfAccessChange
			break
		}
		var exists bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM identities WHERE id = ?)", change.TargetID).Scan(&exists); err != nil {
			return 0, fmt.Errorf("inspect role grant target: %w", err)
		}
		if !exists {
			event.Outcome, decisionErr = AuditValidationRejected, identity.ErrAccessTargetNotFound
			break
		}
		var current string
		err := tx.QueryRowContext(ctx, "SELECT role FROM role_grants WHERE account_id = ? AND cluster_id = ?",
			change.TargetID, change.ClusterID).Scan(&current)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("inspect current role grant: %w", err)
		}
		event.BeforeState = current
		if authorization.Role(current) != change.ExpectedRole {
			event.Outcome, decisionErr = AuditValidationRejected, identity.ErrPolicyConflict
			break
		}
		event.AfterState, event.Outcome = string(change.Role), AuditSucceeded
		if authorization.Role(current) == change.Role {
			if err := tx.QueryRowContext(ctx, "SELECT revision FROM policy_state WHERE id = 1").Scan(&revision); err != nil {
				return 0, fmt.Errorf("read policy revision: %w", err)
			}
			event.PolicyRevision = revision
			break
		}
		if change.Role == authorization.RoleNone {
			_, err = tx.ExecContext(ctx, "DELETE FROM role_grants WHERE account_id = ? AND cluster_id = ?", change.TargetID, change.ClusterID)
		} else {
			_, err = tx.ExecContext(ctx, `INSERT INTO role_grants (account_id, cluster_id, role) VALUES (?, ?, ?)
ON CONFLICT (account_id, cluster_id) DO UPDATE SET role = excluded.role`, change.TargetID, change.ClusterID, change.Role)
		}
		if err != nil {
			return 0, fmt.Errorf("write role grant: %w", err)
		}
		if revision, err = nextPolicyRevision(ctx, tx); err != nil {
			return 0, err
		}
		event.PolicyRevision = revision
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return 0, errors.Join(identity.ErrAccessChangeAudit, fmt.Errorf("record role grant outcome: %w", err))
	}
	if err := tx.Commit(); err != nil {
		return 0, errors.Join(identity.ErrAccessChangeUnknown, fmt.Errorf("commit role grant change: %w", err))
	}
	if decisionErr != nil {
		return 0, decisionErr
	}
	return revision, nil
}
