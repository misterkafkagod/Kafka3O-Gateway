package identity

import (
	"context"
	"io"
	"time"
)

type ListRegisteredClusters interface {
	Execute(ctx context.Context, actor LocalAdminActor) (ClusterListResult, error)
}

type listRegisteredClusters struct {
	store  ClusterListStore
	random io.Reader
	now    func() time.Time
}

func NewListRegisteredClusters(store ClusterListStore, random io.Reader, now func() time.Time) (ListRegisteredClusters, error) {
	if store == nil || random == nil || now == nil {
		return nil, ErrAccessAdminConfig
	}
	return listRegisteredClusters{store: store, random: random, now: now}, nil
}

func (s listRegisteredClusters) Execute(ctx context.Context, actor LocalAdminActor) (ClusterListResult, error) {
	operationID, err := newOpaqueID(s.random)
	if err != nil {
		return ClusterListResult{}, err
	}
	result, err := s.store.ListRegisteredClusters(ctx, ClusterListRequest{Actor: actor, OperationID: operationID, OccurredAt: s.now()})
	if err != nil {
		return ClusterListResult{OperationID: operationID}, err
	}
	result.OperationID = operationID
	return result, nil
}
