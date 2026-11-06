package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (d *Database) ListLocalAccounts(ctx context.Context, request identity.LocalAccountListRequest) (page identity.LocalAccountListResult, result error) {
	event := AuditEvent{
		EventID: request.OperationID, OperationID: request.OperationID, OccurredAt: request.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "console.local_account.list",
		Decision: "denied", Outcome: AuditDenied,
	}
	if err := validateAuditEvent(event); err != nil {
		return identity.LocalAccountListResult{}, err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return identity.LocalAccountListResult{}, fmt.Errorf("begin local account listing: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			page = identity.LocalAccountListResult{}
			result = errors.Join(identity.ErrAccountListAudit, result, fmt.Errorf("roll back local account listing: %w", err))
		}
	}()
	request.OccurredAt, err = d.adminTransactionTime(request.Actor, request.OccurredAt)
	if err != nil {
		return identity.LocalAccountListResult{}, err
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
		return identity.LocalAccountListResult{}, authorizationErr
	default:
		event.ActorID, event.Source, event.Decision = request.Actor.AccountID, AuditSourceLocal, "allowed"
		if request.Limit < 1 || request.Limit > identity.MaxLocalAccountPageSize {
			event.Outcome, decisionErr = AuditValidationRejected, identity.ErrInvalidAccountList
			break
		}
		if err := identity.ValidateLocalAccountCursor(request.AfterID); err != nil {
			event.Outcome, decisionErr = AuditValidationRejected, identity.ErrInvalidAccountCursor
			break
		}
		var afterUsername string
		if request.AfterID != "" {
			err := tx.QueryRowContext(ctx, "SELECT username FROM local_accounts WHERE identity_id = ?", request.AfterID).Scan(&afterUsername)
			if errors.Is(err, sql.ErrNoRows) {
				event.Outcome, decisionErr = AuditValidationRejected, identity.ErrInvalidAccountCursor
				break
			}
			if err != nil {
				return identity.LocalAccountListResult{}, fmt.Errorf("inspect local account listing cursor: %w", err)
			}
		}
		rows, err := tx.QueryContext(ctx, `SELECT identities.id, local_accounts.username, identities.enabled, local_accounts.must_change_password
FROM local_accounts JOIN identities ON identities.id = local_accounts.identity_id
WHERE local_accounts.username > ? ORDER BY local_accounts.username ASC LIMIT ?`, afterUsername, request.Limit+1)
		if err != nil {
			return identity.LocalAccountListResult{}, fmt.Errorf("query local accounts: %w", err)
		}
		defer rows.Close()
		page.Accounts = make([]identity.LocalAccountSummary, 0, request.Limit+1)
		for rows.Next() {
			var account identity.LocalAccountSummary
			if err := rows.Scan(&account.ID, &account.Username, &account.Enabled, &account.MustChangePassword); err != nil {
				return identity.LocalAccountListResult{}, fmt.Errorf("read local account: %w", err)
			}
			page.Accounts = append(page.Accounts, account)
		}
		if err := rows.Err(); err != nil {
			return identity.LocalAccountListResult{}, fmt.Errorf("iterate local accounts: %w", err)
		}
		if err := rows.Close(); err != nil {
			return identity.LocalAccountListResult{}, fmt.Errorf("close local account listing: %w", err)
		}
		if len(page.Accounts) > request.Limit {
			page.Accounts = page.Accounts[:request.Limit]
			page.NextCursor = page.Accounts[len(page.Accounts)-1].ID
		}
		event.Outcome = AuditSucceeded
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return identity.LocalAccountListResult{}, errors.Join(identity.ErrAccountListAudit, fmt.Errorf("record local account listing: %w", err))
	}
	if err := tx.Commit(); err != nil {
		return identity.LocalAccountListResult{}, errors.Join(identity.ErrAccountListAudit, fmt.Errorf("commit local account listing audit: %w", err))
	}
	if decisionErr != nil {
		return identity.LocalAccountListResult{}, decisionErr
	}
	page.OperationID = request.OperationID
	return page, nil
}
