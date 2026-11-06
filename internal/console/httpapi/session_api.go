package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/netip"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type LocalLogin interface {
	Execute(ctx context.Context, username, password string, client netip.Addr) (session.Credentials, error)
}

type Sessions interface {
	Resolve(ctx context.Context, token string) (session.Principal, error)
	Revoke(ctx context.Context, token string) error
}

type SessionAPI struct {
	login        LocalLogin
	change       identity.ChangeLocalPassword
	create       identity.CreateLocalAccount
	reset        identity.ResetLocalPassword
	setEnabled   identity.SetLocalAccountEnabled
	list         identity.ListLocalAccounts
	sessions     Sessions
	csrf         *session.CSRFVerifier
	expectedHost string
	logger       *slog.Logger
	mux          *http.ServeMux
}

var (
	ErrInvalidAPIConfig = errors.New("invalid Console session API configuration")
	errInvalidRequest   = errors.New("invalid request")
	errRequestTooLarge  = errors.New("request body is too large")
)

type localLoginBody struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type changePasswordBody struct {
	CurrentPassword string `json:"currentPassword"`
	NewPassword     string `json:"newPassword"`
}

type resetPasswordBody struct {
	TemporaryPassword string `json:"temporaryPassword"`
}

type createLocalAccountBody struct {
	Username          string `json:"username"`
	TemporaryPassword string `json:"temporaryPassword"`
}

type requiredBoolean struct {
	Value   bool
	Present bool
}

type setLocalAccountEnabledBody struct {
	Enabled requiredBoolean `json:"enabled"`
}

type localAccountResponse struct {
	ID                 string `json:"id"`
	Username           string `json:"username"`
	Enabled            bool   `json:"enabled"`
	MustChangePassword bool   `json:"mustChangePassword"`
}

type sessionResponse struct {
	AccountID          string `json:"accountId"`
	Username           string `json:"username"`
	MustChangePassword bool   `json:"mustChangePassword"`
	AbsoluteExpiresAt  string `json:"absoluteExpiresAt"`
}
