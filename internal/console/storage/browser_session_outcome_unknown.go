package storage

import (
	"errors"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func browserSessionOutcomeUnknown(err error) error {
	if err == session.ErrInvalidSession {
		return session.ErrBrowserSessionUnknown
	}
	return errors.Join(session.ErrBrowserSessionUnknown, err)
}
