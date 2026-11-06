package settings

import "context"

type AlterKafkaConfiguration interface {
	Execute(ctx context.Context, resource Resource, change Change, validateOnly bool) error
}
