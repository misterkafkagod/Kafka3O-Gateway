package session

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func (m *Manager) issueCognitoWithBrowserBinding(ctx context.Context, exchange cognito.CodeExchangeResult, browserHash [32]byte) (Credentials, error) {
	store, ok := m.store.(CognitoSessionStore)
	if !ok || m.cognitoProtector == nil {
		return Credentials{}, ErrInvalidSessionConfig
	}
	proof, err := exchange.ForSession(m.cognitoProtector, m.now().UTC())
	if err != nil {
		return Credentials{}, err
	}
	record, token, csrf, err := m.newSessionRecord()
	if err != nil {
		return Credentials{}, err
	}
	accountID, _, err := newSessionValue(m.random, 16)
	if err != nil {
		return Credentials{}, err
	}
	record.CredentialVersion, record.BrowserBindingHash = 1, browserHash
	principal, err := store.CreateCognitoSession(ctx, CognitoRecord{
		Session: record, CandidateAccount: accountID, Exchange: proof, Protector: m.cognitoProtector,
	})
	if err != nil {
		return Credentials{}, err
	}
	return Credentials{Principal: principal, Token: token, CSRFToken: csrf}, nil
}
