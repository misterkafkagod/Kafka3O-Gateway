package session

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (m *Manager) issueLocalWithBrowserBinding(ctx context.Context, account identity.LocalAccount, browserHash [32]byte) (Credentials, error) {
	if !account.Enabled || account.ID == "" || account.CredentialVersion < 1 {
		return Credentials{}, ErrInvalidSession
	}
	record, token, csrf, err := m.newSessionRecord()
	if err != nil {
		return Credentials{}, err
	}
	record.AccountID, record.CredentialVersion, record.BrowserBindingHash = account.ID, account.CredentialVersion, browserHash
	principal, err := m.store.CreateLocalSession(ctx, record)
	if err != nil {
		return Credentials{}, err
	}
	return Credentials{Principal: principal, Token: token, CSRFToken: csrf}, nil
}
