package connect

import (
	"context"
	"time"
)

type ListConnectors interface {
	Execute(ctx context.Context) ([]Connector, error)
}

type listConnectors struct {
	fetch   FetchConnectors
	timeout time.Duration
}

func NewListConnectors(fetch FetchConnectors, timeout time.Duration) ListConnectors {
	return listConnectors{fetch: fetch, timeout: timeout}
}

func (l listConnectors) Execute(ctx context.Context) ([]Connector, error) {
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	return l.fetch.Execute(ctx)
}
