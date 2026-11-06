package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (d *Database) FindLocalAccount(ctx context.Context, username string) (identity.LocalAccount, error) {
	name, err := identity.NormalizeLocalUsername(username)
	if err != nil {
		return identity.LocalAccount{}, err
	}
	var account identity.LocalAccount
	err = d.db.QueryRowContext(ctx, `SELECT identities.id, local_accounts.username, local_accounts.password_hash,
identities.enabled, local_accounts.must_change_password
FROM local_accounts JOIN identities ON identities.id = local_accounts.identity_id
WHERE local_accounts.username = ?`, name).Scan(
		&account.ID, &account.Username, &account.PasswordHash, &account.Enabled, &account.MustChangePassword,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return identity.LocalAccount{}, identity.ErrAccountNotFound
	}
	if err != nil {
		return identity.LocalAccount{}, fmt.Errorf("find local account: %w", err)
	}
	return account, nil
}
