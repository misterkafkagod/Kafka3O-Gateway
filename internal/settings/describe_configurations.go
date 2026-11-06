package settings

import "context"

type DescribeConfigurations interface {
	Execute(ctx context.Context, resource Resource) ([]Configuration, error)
}
