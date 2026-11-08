package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

// ListRegisteredClusters returns every registered cluster (including inactive ones) with its authoritative
// environment for a current local global ADMIN. The listing is audited; an audit failure returns no data.
func (d *Database) ListRegisteredClusters(ctx context.Context, request identity.ClusterListRequest) (page identity.ClusterListResult, result error) {
	event := AuditEvent{
		EventID: request.OperationID, OperationID: request.OperationID, OccurredAt: request.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "console.cluster.list",
		Decision: "denied", Outcome: AuditDenied,
	}
	if err := validateAuditEvent(event); err != nil {
		return identity.ClusterListResult{}, err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return identity.ClusterListResult{}, fmt.Errorf("begin cluster listing: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			page = identity.ClusterListResult{}
			result = errors.Join(identity.ErrClusterListAudit, result, fmt.Errorf("roll back cluster listing: %w", err))
		}
	}()
	request.OccurredAt, err = d.adminTransactionTime(request.Actor, request.OccurredAt)
	if err != nil {
		return identity.ClusterListResult{}, err
	}
	event.OccurredAt = request.OccurredAt
	authorizationErr := authorizeLocalGlobalAdmin(ctx, tx, request.Actor, request.OccurredAt)
	var decisionErr error
	switch {
	case errors.Is(authorizationErr, identity.ErrInvalidAdminSession):
		decisionErr = authorizationErr
	case errors.Is(authorizationErr, identity.ErrGlobalAdminRequired), errors.Is(authorizationErr, identity.ErrAdminPasswordChange):
		event.ActorID, event.Source = request.Actor.AccountID, AuditSourceLocal
		decisionErr = authorizationErr
	case authorizationErr != nil:
		return identity.ClusterListResult{}, authorizationErr
	default:
		event.ActorID, event.Source, event.Decision = request.Actor.AccountID, AuditSourceLocal, "allowed"
		rows, err := tx.QueryContext(ctx, `SELECT id, display_name, gateway_url, environment, active
FROM registered_clusters ORDER BY id ASC LIMIT 1001`)
		if err != nil {
			return identity.ClusterListResult{}, fmt.Errorf("query registered clusters: %w", err)
		}
		page.Clusters = []identity.RegisteredClusterSummary{}
		for rows.Next() {
			var cluster identity.RegisteredClusterSummary
			if err := rows.Scan(&cluster.ID, &cluster.Name, &cluster.GatewayURL, &cluster.Environment, &cluster.Active); err != nil {
				rows.Close()
				return identity.ClusterListResult{}, fmt.Errorf("read registered cluster: %w", err)
			}
			page.Clusters = append(page.Clusters, cluster)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return identity.ClusterListResult{}, fmt.Errorf("iterate registered clusters: %w", err)
		}
		if len(page.Clusters) > 1000 {
			return identity.ClusterListResult{}, identity.ErrInvalidClusterRegistry
		}
		if err := tx.QueryRowContext(ctx, "SELECT revision FROM policy_state WHERE id = 1").Scan(&page.PolicyRevision); err != nil {
			return identity.ClusterListResult{}, fmt.Errorf("read policy revision: %w", err)
		}
		event.PolicyRevision, event.Outcome = page.PolicyRevision, AuditSucceeded
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return identity.ClusterListResult{}, errors.Join(identity.ErrClusterListAudit, fmt.Errorf("record cluster listing: %w", err))
	}
	if err := tx.Commit(); err != nil {
		return identity.ClusterListResult{}, errors.Join(identity.ErrClusterListAudit, fmt.Errorf("commit cluster listing audit: %w", err))
	}
	if decisionErr != nil {
		return identity.ClusterListResult{}, decisionErr
	}
	page.OperationID = request.OperationID
	return page, nil
}
