package topics

import "context"

type DeleteKafkaTopic interface {
	Execute(ctx context.Context, name string) error
}
