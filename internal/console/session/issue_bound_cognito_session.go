package session

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func (m *Manager) IssueBoundCognito(ctx context.Context, exchange cognito.CodeExchangeResult, browserHash [32]byte) (Credentials, error) {
	if ctx == nil || browserHash == ([32]byte{}) {
		return Credentials{}, ErrInvalidSessionConfig
	}
	return m.issueCognitoWithBrowserBinding(ctx, exchange, browserHash)
}
