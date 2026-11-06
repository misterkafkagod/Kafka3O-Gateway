package session

import (
	"context"
	"net/netip"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

type LocalLogin struct {
	authenticator identity.AuthenticateLocal
	sessions      *Manager
}

func NewLocalLogin(authenticator identity.AuthenticateLocal, sessions *Manager) (*LocalLogin, error) {
	if authenticator == nil || sessions == nil {
		return nil, ErrInvalidSessionConfig
	}
	return &LocalLogin{authenticator: authenticator, sessions: sessions}, nil
}

func (l *LocalLogin) Execute(ctx context.Context, username, password string, client netip.Addr) (Credentials, error) {
	account, err := l.authenticator.Execute(ctx, username, password, client)
	if err != nil {
		return Credentials{}, err
	}
	return l.sessions.IssueLocal(ctx, account)
}
