package httpapi

import (
	"log/slog"
	"net/http"
	"net/url"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func NewSessionAPI(login LocalLogin, change identity.ChangeLocalPassword, create identity.CreateLocalAccount, reset identity.ResetLocalPassword, setEnabled identity.SetLocalAccountEnabled, list identity.ListLocalAccounts, listSessions identity.ListAdminSessions, revokeAdmin identity.RevokeAdminSession, sessions Sessions, origin string, logger *slog.Logger) (*SessionAPI, error) {
	if login == nil || change == nil || create == nil || reset == nil || setEnabled == nil || list == nil ||
		listSessions == nil || revokeAdmin == nil || sessions == nil || logger == nil {
		return nil, ErrInvalidAPIConfig
	}
	csrf, err := session.NewCSRFVerifier(origin)
	if err != nil {
		return nil, err
	}
	parsed, err := url.Parse(origin)
	if err != nil {
		return nil, ErrInvalidAPIConfig
	}
	api := &SessionAPI{login: login, change: change, create: create, reset: reset, setEnabled: setEnabled, list: list,
		listSessions: listSessions, revokeAdmin: revokeAdmin, sessions: sessions, csrf: csrf, expectedHost: parsed.Host, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/session/local", api.localLogin)
	mux.HandleFunc("GET /api/session", api.getSession)
	mux.HandleFunc("POST /api/session/password", api.changePassword)
	mux.HandleFunc("POST /api/session/logout", api.logout)
	mux.HandleFunc("POST /api/admin/local-accounts", api.createLocalAccount)
	mux.HandleFunc("GET /api/admin/local-accounts", api.listLocalAccounts)
	mux.HandleFunc("POST /api/admin/local-accounts/{accountID}/password-reset", api.resetLocalPassword)
	mux.HandleFunc("PUT /api/admin/local-accounts/{accountID}/enabled", api.setLocalAccountEnabled)
	mux.HandleFunc("GET /api/admin/sessions", api.listAdminSessions)
	mux.HandleFunc("POST /api/admin/sessions/{sessionID}/revoke", api.revokeAdminSession)
	api.mux = mux
	return api, nil
}
