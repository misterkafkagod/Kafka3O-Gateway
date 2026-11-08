package storage

import (
	"context"
	"database/sql"
	"fmt"
)

// nextPolicyRevision increments the policy revision inside the caller's transaction.
func nextPolicyRevision(ctx context.Context, tx *sql.Tx) (int64, error) {
	if _, err := tx.ExecContext(ctx, "UPDATE policy_state SET revision = revision + 1 WHERE id = 1"); err != nil {
		return 0, fmt.Errorf("advance policy revision: %w", err)
	}
	var revision int64
	if err := tx.QueryRowContext(ctx, "SELECT revision FROM policy_state WHERE id = 1").Scan(&revision); err != nil {
		return 0, fmt.Errorf("read policy revision: %w", err)
	}
	return revision, nil
}
