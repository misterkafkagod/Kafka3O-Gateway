package httpapi

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

// ActiveClusters lists the active registered clusters; their environment comes only from the Console store.
type ActiveClusters interface {
	ListActiveClusters(ctx context.Context) ([]identity.RegisteredClusterSummary, error)
}

// LocalSessionGrants resolves the current grants of an unbound session for one cluster.
type LocalSessionGrants interface {
	ResolveGrants(ctx context.Context, token, clusterID string) (session.Principal, []authorization.Grant, error)
}

type authorizedClusterResponse struct {
	ID            string                    `json:"id"`
	Name          string                    `json:"name"`
	Environment   authorization.Environment `json:"environment"`
	EffectiveRole authorization.Role        `json:"effectiveRole"`
}
