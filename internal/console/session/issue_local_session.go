package session

import (
	"context"
	"crypto/subtle"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (m *Manager) IssueLocal(ctx context.Context, account identity.LocalAccount) (Credentials, error) {
	if !account.Enabled || account.ID == "" || account.CredentialVersion < 1 {
		return Credentials{}, ErrInvalidSession
	}
	now := m.now().UTC()
	if now.IsZero() || now.Year() < 1970 || now.Add(m.absoluteLifetime).Year() > 2261 {
		return Credentials{}, ErrInvalidSessionConfig
	}
	token, tokenHash, err := newSessionValue(m.random, 32)
	if err != nil {
		return Credentials{}, err
	}
	csrf, csrfHash, err := newSessionValue(m.random, 32)
	if err != nil {
		return Credentials{}, err
	}
	if subtle.ConstantTimeCompare(tokenHash[:], csrfHash[:]) == 1 {
		return Credentials{}, ErrInvalidSessionConfig
	}
	sessionID, _, err := newSessionValue(m.random, 16)
	if err != nil {
		return Credentials{}, err
	}
	eventID, _, err := newSessionValue(m.random, 16)
	if err != nil {
		return Credentials{}, err
	}
	principal, err := m.store.CreateLocalSession(ctx, Record{
		SessionID: sessionID, AccountID: account.ID, CredentialVersion: account.CredentialVersion,
		TokenHash: tokenHash, CSRFHash: csrfHash, CreatedAt: now,
		IdleExpiresAt: now.Add(m.idleLifetime), AbsoluteExpiresAt: now.Add(m.absoluteLifetime), EventID: eventID,
	})
	if err != nil {
		return Credentials{}, err
	}
	return Credentials{Principal: principal, Token: token, CSRFToken: csrf}, nil
}
