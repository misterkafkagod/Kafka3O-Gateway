package session

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func (m *Manager) IssueCognito(ctx context.Context, exchange cognito.CodeExchangeResult) (Credentials, error) {
	return m.issueCognitoWithBrowserBinding(ctx, exchange, [32]byte{})
}
