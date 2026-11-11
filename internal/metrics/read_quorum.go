package metrics

import "context"

// ReadQuorum reads the KRaft controller quorum.
type ReadQuorum interface {
	Execute(ctx context.Context) (Quorum, error)
}
