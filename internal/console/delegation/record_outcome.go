package delegation

import (
	"context"
	"time"
)

type RecordOutcome interface {
	Execute(ctx context.Context, callerClusterID, operationID string, outcome Outcome) error
}

type recordOutcome struct {
	store OutcomeStore
	now   func() time.Time
}

func NewRecordOutcome(store OutcomeStore, now func() time.Time) (RecordOutcome, error) {
	if store == nil || now == nil {
		return nil, ErrConfig
	}
	return recordOutcome{store: store, now: now}, nil
}

// Execute records the single outcome of an allowed mutation intent belonging to the calling Gateway's cluster.
func (r recordOutcome) Execute(ctx context.Context, callerClusterID, operationID string, outcome Outcome) error {
	switch outcome {
	case OutcomeConfirmed, OutcomeValidationRejected, OutcomeFailed, OutcomeUnknown:
	default:
		return ErrInvalidOutcome
	}
	if callerClusterID == "" || operationID == "" || len(operationID) > 100 {
		return ErrInvalidOutcome
	}
	return r.store.RecordGatewayOutcome(ctx, OutcomeRecord{OperationID: operationID, CallerClusterID: callerClusterID, Outcome: outcome, OccurredAt: r.now()})
}
