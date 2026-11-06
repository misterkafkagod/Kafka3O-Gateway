package session

import (
	"io"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func NewRefreshingCognitoManager(store Store, random io.Reader, now func() time.Time, idleLifetime, absoluteLifetime time.Duration,
	exchanger *cognito.CodeExchanger, protector *cognito.RefreshTokenProtector) (*Manager, error) {
	refreshStore, ok := store.(CognitoRefreshStore)
	if !ok || !exchanger.UsesRefreshStore(refreshStore) {
		return nil, ErrInvalidSessionConfig
	}
	manager, err := NewCognitoManager(store, random, now, idleLifetime, absoluteLifetime, exchanger, protector)
	if err != nil {
		return nil, err
	}
	manager.cognitoRefresh = &cognitoRefreshCoordinator{
		store: refreshStore, exchanger: exchanger, active: make(map[[32]byte]*cognitoRefreshCall),
	}
	return manager, nil
}
