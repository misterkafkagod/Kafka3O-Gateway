package main

import (
	"crypto/rand"
	"log/slog"
	"net/http"
	"time"

	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

func newSessionHandler(db *storage.Database, origin string, logger *slog.Logger) (http.Handler, error) {
	authenticator, err := identity.NewAuthenticateLocal(db, rand.Reader, time.Now)
	if err != nil {
		return nil, err
	}
	manager, err := session.NewManager(db, rand.Reader, time.Now, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	if err != nil {
		return nil, err
	}
	login, err := session.NewLocalLogin(authenticator, manager)
	if err != nil {
		return nil, err
	}
	change, err := identity.NewChangeLocalPassword(authenticator, db, rand.Reader, time.Now)
	if err != nil {
		return nil, err
	}
	reset, err := identity.NewResetLocalPassword(db, rand.Reader, time.Now)
	if err != nil {
		return nil, err
	}
	return consoleapi.NewSessionAPI(login, change, reset, manager, origin, logger)
}
