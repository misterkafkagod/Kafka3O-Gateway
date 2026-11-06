package storage

import (
	"context"
	"fmt"
)

func (d *Database) VerifyInitialAdministrator(ctx context.Context) error {
	var completed, available bool
	err := d.db.QueryRowContext(ctx, `SELECT completed, EXISTS (
SELECT 1 FROM identities
JOIN local_accounts ON local_accounts.identity_id = identities.id
JOIN role_grants ON role_grants.account_id = identities.id
WHERE identities.enabled = 1 AND role_grants.cluster_id = '' AND role_grants.role = 'ADMIN'
) FROM bootstrap_state WHERE id = 1`).Scan(&completed, &available)
	if err != nil {
		return fmt.Errorf("inspect initial administrator: %w", err)
	}
	if !completed {
		return ErrBootstrapRequired
	}
	if !available {
		return ErrNoLocalGlobalAdmin
	}
	return nil
}
