package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

// ChangeGroupMapping sets or removes the role of one exact issuer, group and scope for a current
// local global ADMIN and returns the committed policy revision.
func (d *Database) ChangeGroupMapping(ctx context.Context, change identity.GroupMappingChange) (revision int64, result error) {
	const action = "console.group_mapping.change"
	attempt := AuditEvent{
		EventID: change.OperationID, OperationID: change.OperationID, OccurredAt: change.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: action, Outcome: AuditAttempted,
	}
	if err := validateAuditEvent(attempt); err != nil || change.EventID == "" || change.OperationID == change.EventID {
		return 0, ErrInvalidAuditEvent
	}
	if err := d.AppendAuditEvent(ctx, attempt); err != nil {
		return 0, errors.Join(identity.ErrAccessChangeAudit, fmt.Errorf("record group mapping attempt: %w", err))
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin group mapping change: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(identity.ErrAccessChangeUnknown, result, fmt.Errorf("roll back group mapping change: %w", err))
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
			len(change.Issuer) > 256 || len(change.Group) > 128 || len(change.ClusterID) > 128 {
			event.Outcome, decisionErr = AuditValidationRejected, identity.ErrInvalidAccessChange
			break
		}
		event.ResourceKind, event.ResourceID, event.ClusterID = "cognito_group", change.Issuer+"#"+change.Group, change.ClusterID
		if change.ClusterID != "" && change.Role != authorization.RoleNone {
			var registered bool
			if err := tx.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM registered_clusters WHERE id = ? AND active = 1)",
				change.ClusterID).Scan(&registered); err != nil {
				return 0, fmt.Errorf("inspect grant cluster: %w", err)
			}
			if !registered {
				event.Outcome, decisionErr = AuditValidationRejected, identity.ErrUnknownCluster
				break
			}
		}
		var current string
		err := tx.QueryRowContext(ctx, "SELECT role FROM cognito_group_bindings WHERE issuer = ? AND group_name = ? AND cluster_id = ?",
			change.Issuer, change.Group, change.ClusterID).Scan(&current)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("inspect current group mapping: %w", err)
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
			_, err = tx.ExecContext(ctx, "DELETE FROM cognito_group_bindings WHERE issuer = ? AND group_name = ? AND cluster_id = ?",
				change.Issuer, change.Group, change.ClusterID)
		} else {
			_, err = tx.ExecContext(ctx, `INSERT INTO cognito_group_bindings (issuer, group_name, cluster_id, role) VALUES (?, ?, ?, ?)
ON CONFLICT (issuer, group_name, cluster_id) DO UPDATE SET role = excluded.role`, change.Issuer, change.Group, change.ClusterID, change.Role)
		}
		if err != nil {
			return 0, fmt.Errorf("write group mapping: %w", err)
		}
		if revision, err = nextPolicyRevision(ctx, tx); err != nil {
			return 0, err
		}
		event.PolicyRevision = revision
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return 0, errors.Join(identity.ErrAccessChangeAudit, fmt.Errorf("record group mapping outcome: %w", err))
	}
	if err := tx.Commit(); err != nil {
		return 0, errors.Join(identity.ErrAccessChangeUnknown, fmt.Errorf("commit group mapping change: %w", err))
	}
	if decisionErr != nil {
		return 0, decisionErr
	}
	return revision, nil
}
