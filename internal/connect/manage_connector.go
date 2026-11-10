package connect

import (
	"context"
	"time"
)

type ManageConnector interface {
	Execute(ctx context.Context, action Action) error
}

type manageConnector struct {
	send    SendConnectAction
	timeout time.Duration
}

func NewManageConnector(send SendConnectAction, timeout time.Duration) ManageConnector {
	return manageConnector{send: send, timeout: timeout}
}

func (m manageConnector) Execute(ctx context.Context, action Action) error {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	return m.send.Execute(ctx, action)
}
