package main

import (
	"crypto/rand"
	"log/slog"
	"time"

	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

func newSessionHandler(db *storage.Database, origin string, logger *slog.Logger) (*consoleapi.SessionAPI, error) {
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
	create, err := identity.NewCreateLocalAccount(db, rand.Reader, time.Now)
	if err != nil {
		return nil, err
	}
	reset, err := identity.NewResetLocalPassword(db, rand.Reader, time.Now)
	if err != nil {
		return nil, err
	}
	setEnabled, err := identity.NewSetLocalAccountEnabled(db, rand.Reader, time.Now)
	if err != nil {
		return nil, err
	}
	list, err := identity.NewListLocalAccounts(db, rand.Reader, time.Now)
	if err != nil {
		return nil, err
	}
	listSessions, err := identity.NewListAdminSessions(db, rand.Reader, time.Now)
	if err != nil {
		return nil, err
	}
	revokeAdmin, err := identity.NewRevokeAdminSession(db, rand.Reader, time.Now)
	if err != nil {
		return nil, err
	}
	api, err := consoleapi.NewSessionAPI(login, change, create, reset, setEnabled, list, listSessions, revokeAdmin, manager, origin, logger)
	if err != nil {
		return nil, err
	}
	accessList, err := identity.NewListAccessBindings(db, rand.Reader, time.Now)
	if err != nil {
		return nil, err
	}
	grants, err := identity.NewChangeRoleGrant(db, rand.Reader, time.Now)
	if err != nil {
		return nil, err
	}
	mappings, err := identity.NewChangeGroupMapping(db, rand.Reader, time.Now)
	if err != nil {
		return nil, err
	}
	if err := api.EnableAccessAdministration(accessList, grants, mappings); err != nil {
		return nil, err
	}
	return api, nil
}
