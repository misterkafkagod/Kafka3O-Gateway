package main

import (
	"context"
	"io"
	"net/url"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

func newCognitoServices(ctx context.Context, db *storage.Database, config cognitoConfig, random io.Reader,
	now func() time.Time) (consoleapi.CognitoServices, error) {
	verifier, err := cognito.NewVerifier(ctx, config.provider, config.roots, now)
	if err != nil {
		return consoleapi.CognitoServices{}, err
	}
	attempts, err := cognito.NewAttemptManager(db, random, now, config.attemptKey, config.provider, config.callbackURL)
	if err != nil {
		return consoleapi.CognitoServices{}, err
	}
	exchanger, err := cognito.NewCodeExchanger(attempts, verifier, config.callbackURL, config.authentication, config.clientSecret)
	if err != nil {
		return consoleapi.CognitoServices{}, err
	}
	protector, err := cognito.NewRefreshTokenProtector(attempts, random, config.refreshKey)
	if err != nil {
		return consoleapi.CognitoServices{}, err
	}
	manager, err := session.NewRefreshingCognitoManager(db, random, now, session.DefaultIdleLifetime,
		session.DefaultAbsoluteLifetime, exchanger, protector)
	if err != nil {
		return consoleapi.CognitoServices{}, err
	}
	logout, err := url.Parse(config.logoutURL)
	if err != nil {
		return consoleapi.CognitoServices{}, errInvalidCognitoConfig
	}
	logout.RawQuery = url.Values{"client_id": {config.provider.ClientID}, "logout_uri": {config.logoutReturn}}.Encode()
	return consoleapi.CognitoServices{Attempts: attempts, Exchanger: exchanger, Sessions: manager, LogoutURL: logout.String()}, nil
}
