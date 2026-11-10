package backup

import "context"

// ReadRestoreOffsets returns, per backup object key, how many records a restore connector has committed.
type ReadRestoreOffsets interface {
	Execute(ctx context.Context, connector string) (map[string]int64, error)
}
