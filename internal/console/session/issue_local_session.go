package session

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (m *Manager) IssueLocal(ctx context.Context, account identity.LocalAccount) (Credentials, error) {
	return m.issueLocalWithBrowserBinding(ctx, account, [32]byte{})
}
