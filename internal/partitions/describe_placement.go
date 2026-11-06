package partitions

import "context"

type DescribePlacement interface {
	Execute(ctx context.Context, topic string) (Placement, error)
}
