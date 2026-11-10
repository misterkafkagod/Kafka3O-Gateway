package metrics

import "context"

// ReadLogDirs describes every broker's log directories; on a partial failure it returns what it read and the error.
type ReadLogDirs interface {
	Execute(ctx context.Context) ([]LogDir, error)
}
