package httpapi

import (
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

// CognitoServices are the discovered provider components used by SSO routes.
type CognitoServices struct {
	Attempts  *cognito.AttemptManager
	Exchanger *cognito.CodeExchanger
	Sessions  *session.Manager
	LogoutURL string
}
