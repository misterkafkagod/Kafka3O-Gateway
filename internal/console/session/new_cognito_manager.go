package session

import (
	"io"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func NewCognitoManager(store Store, random io.Reader, now func() time.Time, idleLifetime, absoluteLifetime time.Duration,
	exchanger *cognito.CodeExchanger, protector *cognito.RefreshTokenProtector) (*Manager, error) {
	if _, ok := store.(CognitoSessionStore); !ok || !exchanger.SupportsSessionProtector(protector) {
		return nil, ErrInvalidSessionConfig
	}
	manager, err := NewManager(store, random, now, idleLifetime, absoluteLifetime)
	if err != nil {
		return nil, err
	}
	manager.cognitoProtector = protector
	return manager, nil
}
