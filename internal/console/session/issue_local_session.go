package session

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (m *Manager) IssueLocal(ctx context.Context, account identity.LocalAccount) (Credentials, error) {
	if !account.Enabled || account.ID == "" || account.CredentialVersion < 1 {
		return Credentials{}, ErrInvalidSession
	}
	record, token, csrf, err := m.newSessionRecord()
	if err != nil {
		return Credentials{}, err
	}
	record.AccountID, record.CredentialVersion = account.ID, account.CredentialVersion
	principal, err := m.store.CreateLocalSession(ctx, record)
	if err != nil {
		return Credentials{}, err
	}
	return Credentials{Principal: principal, Token: token, CSRFToken: csrf}, nil
}
