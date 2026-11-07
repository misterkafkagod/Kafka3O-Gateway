package session

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (m *Manager) IssueBoundLocal(ctx context.Context, account identity.LocalAccount, browserHash [32]byte) (Credentials, error) {
	if ctx == nil || browserHash == ([32]byte{}) {
		return Credentials{}, ErrInvalidSessionConfig
	}
	return m.issueLocalWithBrowserBinding(ctx, account, browserHash)
}
