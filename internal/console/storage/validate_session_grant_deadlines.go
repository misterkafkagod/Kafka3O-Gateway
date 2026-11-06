package storage

import (
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type sessionGrantDeadlines struct {
	idle, absolute, freshness, tokenExpiry int64
	cognito                                bool
}

func validateSessionGrantDeadlines(deadlines sessionGrantDeadlines, now time.Time) error {
	if deadlines.idle <= now.UnixNano() || deadlines.absolute <= now.UnixNano() {
		return session.ErrInvalidSession
	}
	if deadlines.cognito && (deadlines.freshness <= now.UnixNano() || deadlines.tokenExpiry <= now.UnixNano()) {
		return session.ErrCognitoRefreshUnavailable
	}
	return nil
}
