package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

// ChangeClusterEnvironment classifies an active registered cluster for a current local global ADMIN and
// returns the committed policy revision; decisions made after the commit use the new classification.
func (d *Database) ChangeClusterEnvironment(ctx context.Context, change identity.ClusterEnvironmentChange) (revision int64, result error) {
	const action = "console.cluster.classify"
	attempt := AuditEvent{
		EventID: change.OperationID, OperationID: change.OperationID, OccurredAt: change.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: action, Outcome: AuditAttempted,
	}
	if err := validateAuditEvent(attempt); err != nil || change.EventID == "" || change.OperationID == change.EventID {
		return 0, ErrInvalidAuditEvent
	}
	if err := d.AppendAuditEvent(ctx, attempt); err != nil {
		return 0, errors.Join(identity.ErrAccessChangeAudit, fmt.Errorf("record cluster classification attempt: %w", err))
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin cluster classification: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(identity.ErrAccessChangeUnknown, result, fmt.Errorf("roll back cluster classification: %w", err))
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
		if !validAuditState(string(change.ExpectedEnvironment)) || !validAuditState(string(change.Environment)) ||
			change.Environment == authorization.EnvironmentUnclassified || len(change.ClusterID) > 128 || len(change.Reason) > 512 {
			event.Outcome, decisionErr = AuditValidationRejected, identity.ErrInvalidAccessChange
			break
		}
		event.ResourceKind, event.ResourceID, event.ClusterID = "registered_cluster", change.ClusterID, change.ClusterID
		var current string
		err := tx.QueryRowContext(ctx, "SELECT environment FROM registered_clusters WHERE id = ? AND active = 1", change.ClusterID).Scan(&current)
		if errors.Is(err, sql.ErrNoRows) {
			event.Outcome, decisionErr = AuditValidationRejected, identity.ErrUnknownCluster
			break
		}
		if err != nil {
			return 0, fmt.Errorf("inspect registered cluster environment: %w", err)
		}
		event.BeforeState, event.Environment = current, current
		if authorization.Environment(current) != change.ExpectedEnvironment {
			event.Outcome, decisionErr = AuditValidationRejected, identity.ErrPolicyConflict
			break
		}
		event.AfterState, event.Environment, event.Reason, event.Outcome = string(change.Environment), string(change.Environment), change.Reason, AuditSucceeded
		if authorization.Environment(current) == change.Environment {
			if err := tx.QueryRowContext(ctx, "SELECT revision FROM policy_state WHERE id = 1").Scan(&revision); err != nil {
				return 0, fmt.Errorf("read policy revision: %w", err)
			}
			event.PolicyRevision = revision
			break
		}
		if _, err := tx.ExecContext(ctx, "UPDATE registered_clusters SET environment = ? WHERE id = ?", change.Environment, change.ClusterID); err != nil {
			return 0, fmt.Errorf("write cluster environment: %w", err)
		}
		if revision, err = nextPolicyRevision(ctx, tx); err != nil {
			return 0, err
		}
		event.PolicyRevision = revision
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return 0, errors.Join(identity.ErrAccessChangeAudit, fmt.Errorf("record cluster classification outcome: %w", err))
	}
	if err := tx.Commit(); err != nil {
		return 0, errors.Join(identity.ErrAccessChangeUnknown, fmt.Errorf("commit cluster classification: %w", err))
	}
	if decisionErr != nil {
		return 0, decisionErr
	}
	return revision, nil
}
