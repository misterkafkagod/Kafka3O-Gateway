package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

// ListAccessBindings returns one identity page with direct grants, the group mappings and the policy revision
// for a current local global ADMIN. Every listing attempt is audited; an audit failure returns no data.
func (d *Database) ListAccessBindings(ctx context.Context, request identity.AccessListRequest) (page identity.AccessListResult, result error) {
	event := AuditEvent{
		EventID: request.OperationID, OperationID: request.OperationID, OccurredAt: request.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "console.access.list",
		Decision: "denied", Outcome: AuditDenied,
	}
	if err := validateAuditEvent(event); err != nil {
		return identity.AccessListResult{}, err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return identity.AccessListResult{}, fmt.Errorf("begin access listing: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			page = identity.AccessListResult{}
			result = errors.Join(identity.ErrAccessListAudit, result, fmt.Errorf("roll back access listing: %w", err))
		}
	}()
	request.OccurredAt, err = d.adminTransactionTime(request.Actor, request.OccurredAt)
	if err != nil {
		return identity.AccessListResult{}, err
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
		return identity.AccessListResult{}, authorizationErr
	default:
		event.ActorID, event.Source, event.Decision = request.Actor.AccountID, AuditSourceLocal, "allowed"
		if request.Limit < 1 || request.Limit > identity.MaxAccessPageSize || identity.ValidateLocalAccountCursor(request.AfterID) != nil {
			event.Outcome, decisionErr = AuditValidationRejected, identity.ErrInvalidAccessList
			break
		}
		rows, err := tx.QueryContext(ctx, `SELECT identities.id, identities.enabled,
CASE WHEN local_accounts.identity_id IS NOT NULL THEN 'local' ELSE 'cognito' END,
COALESCE(local_accounts.username, cognito_identities.subject, '')
FROM identities
LEFT JOIN local_accounts ON local_accounts.identity_id = identities.id
LEFT JOIN cognito_identities ON cognito_identities.identity_id = identities.id
WHERE identities.id > ? ORDER BY identities.id ASC LIMIT ?`, request.AfterID, request.Limit+1)
		if err != nil {
			return identity.AccessListResult{}, fmt.Errorf("query access identities: %w", err)
		}
		page.Identities = make([]identity.AccessIdentity, 0, request.Limit+1)
		positions := make(map[string]int, request.Limit+1)
		for rows.Next() {
			var entry identity.AccessIdentity
			if err := rows.Scan(&entry.ID, &entry.Enabled, &entry.Source, &entry.Label); err != nil {
				rows.Close()
				return identity.AccessListResult{}, fmt.Errorf("read access identity: %w", err)
			}
			entry.Grants = []authorization.Grant{}
			positions[entry.ID] = len(page.Identities)
			page.Identities = append(page.Identities, entry)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return identity.AccessListResult{}, fmt.Errorf("iterate access identities: %w", err)
		}
		if len(page.Identities) > request.Limit {
			page.Identities = page.Identities[:request.Limit]
			page.NextCursor = page.Identities[len(page.Identities)-1].ID
		}
		rows, err = tx.QueryContext(ctx, `SELECT account_id, cluster_id, role FROM role_grants
WHERE account_id IN (SELECT id FROM identities WHERE id > ? ORDER BY id ASC LIMIT ?)
ORDER BY account_id ASC, cluster_id ASC`, request.AfterID, request.Limit)
		if err != nil {
			return identity.AccessListResult{}, fmt.Errorf("query direct grants: %w", err)
		}
		for rows.Next() {
			var accountID string
			var grant authorization.Grant
			if err := rows.Scan(&accountID, &grant.ClusterID, &grant.Role); err != nil {
				rows.Close()
				return identity.AccessListResult{}, fmt.Errorf("read direct grant: %w", err)
			}
			if position, ok := positions[accountID]; ok && position < len(page.Identities) {
				page.Identities[position].Grants = append(page.Identities[position].Grants, grant)
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return identity.AccessListResult{}, fmt.Errorf("iterate direct grants: %w", err)
		}
		rows, err = tx.QueryContext(ctx, `SELECT issuer, group_name, cluster_id, role FROM cognito_group_bindings
ORDER BY issuer ASC, group_name ASC, cluster_id ASC LIMIT ?`, identity.MaxListedGroupGrants+1)
		if err != nil {
			return identity.AccessListResult{}, fmt.Errorf("query group mappings: %w", err)
		}
		page.GroupGrants = []identity.GroupGrant{}
		for rows.Next() {
			var mapping identity.GroupGrant
			if err := rows.Scan(&mapping.Issuer, &mapping.Group, &mapping.Grant.ClusterID, &mapping.Grant.Role); err != nil {
				rows.Close()
				return identity.AccessListResult{}, fmt.Errorf("read group mapping: %w", err)
			}
			page.GroupGrants = append(page.GroupGrants, mapping)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return identity.AccessListResult{}, fmt.Errorf("iterate group mappings: %w", err)
		}
		if len(page.GroupGrants) > identity.MaxListedGroupGrants {
			page.GroupGrants, page.GroupGrantsTruncated = page.GroupGrants[:identity.MaxListedGroupGrants], true
		}
		if err := tx.QueryRowContext(ctx, "SELECT revision FROM policy_state WHERE id = 1").Scan(&page.PolicyRevision); err != nil {
			return identity.AccessListResult{}, fmt.Errorf("read policy revision: %w", err)
		}
		event.PolicyRevision, event.Outcome = page.PolicyRevision, AuditSucceeded
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return identity.AccessListResult{}, errors.Join(identity.ErrAccessListAudit, fmt.Errorf("record access listing: %w", err))
	}
	if err := tx.Commit(); err != nil {
		return identity.AccessListResult{}, errors.Join(identity.ErrAccessListAudit, fmt.Errorf("commit access listing audit: %w", err))
	}
	if decisionErr != nil {
		return identity.AccessListResult{}, decisionErr
	}
	page.OperationID = request.OperationID
	return page, nil
}
