package httpapi

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

// CognitoAttempts creates and consumes marker-bound authorization attempts.
type CognitoAttempts interface {
	StartBoundAuthorization(ctx context.Context, browserHash [32]byte) (cognito.AuthorizationStart, error)
	ConsumeBoundAuthorization(ctx context.Context, state, browserBinding string, browserHash [32]byte) (cognito.ConsumedAuthorization, error)
}

// CognitoExchanger builds the provider authorization URL and exchanges a bound code once.
type CognitoExchanger interface {
	BrowserAuthorizationURL(start cognito.AuthorizationStart) (string, error)
	ExchangeBoundAuthorizationCode(ctx context.Context, attempt cognito.ConsumedAuthorization, code string, browserHash [32]byte) (cognito.CodeExchangeResult, error)
}

// CognitoSessions issues, resolves and revokes marker-bound sessions.
type CognitoSessions interface {
	IssueBoundCognito(ctx context.Context, exchange cognito.CodeExchangeResult, browserHash [32]byte) (session.Credentials, error)
	ResolveBound(ctx context.Context, token string, browserHash [32]byte) (session.Principal, error)
	ResolveBoundForLogout(ctx context.Context, token string, browserHash [32]byte) (session.Principal, error)
	RevokeBound(ctx context.Context, token string, browserHash [32]byte) error
	ResolveBoundGrants(ctx context.Context, token string, browserHash [32]byte, clusterID string) (session.Principal, []authorization.Grant, error)
}

// CognitoServices are the discovered provider components used by SSO routes.
type CognitoServices struct {
	Attempts  CognitoAttempts
	Exchanger CognitoExchanger
	Sessions  CognitoSessions
	LogoutURL string
	// Protector authenticates retained Cognito credentials for delegated Gateway decisions.
	Protector *cognito.RefreshTokenProtector
}
