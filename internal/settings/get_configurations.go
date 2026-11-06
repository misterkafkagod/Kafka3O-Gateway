package settings

import (
	"context"
	"time"
)

type GetConfigurations interface {
	Execute(ctx context.Context, resource Resource) ([]Configuration, error)
}

type getConfigurations struct {
	describe DescribeConfigurations
	timeout  time.Duration
}

func NewGetConfigurations(describe DescribeConfigurations, timeout time.Duration) GetConfigurations {
	return getConfigurations{describe: describe, timeout: timeout}
}

func (g getConfigurations) Execute(ctx context.Context, resource Resource) ([]Configuration, error) {
	if !IsValidResource(resource) {
		return nil, ErrInvalidResource
	}
	ctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()
	return g.describe.Execute(ctx, resource)
}
