package httpapi

import (
	"errors"
	"net/http"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type cognitoCallbackBody struct {
	Query string `json:"query"`
}

// completeCognitoSignIn finishes the callback the SPA posts from /auth/cognito/callback.
// It never logs the body, query, code, cookies or tokens and never retries the provider.
func (a *SessionAPI) completeCognitoSignIn(w http.ResponseWriter, r *http.Request) {
	if err := a.csrf.VerifyOrigin(r); err != nil {
		a.writeProblem(w, http.StatusForbidden, "csrf_rejected", "Request verification failed")
		return
	}
	services := a.cognito.Load()
	if services == nil {
		a.writeProblem(w, http.StatusServiceUnavailable, "sso_unavailable", "Cognito sign-in is unavailable")
		return
	}
	http.SetCookie(w, cognito.ClearBrowserAuthorizationCookie())
	marker, err := session.ReadBrowserMarkerCookie(r)
	if err != nil {
		a.writeProblem(w, http.StatusConflict, "invalid_browser_marker", "Browser verification failed")
		return
	}
	if found, err := a.findExistingSession(r, services, marker); errors.Is(err, errInvalidSessionCookie) {
		a.writeProblem(w, http.StatusBadRequest, "invalid_session_cookie", "Invalid session cookie")
		return
	} else if err != nil {
		a.writeUnavailable(w, "resolve_existing", err)
		return
	} else if found {
		a.writeProblem(w, http.StatusConflict, "already_signed_in", "Sign out before signing in again")
		return
	}
	var body cognitoCallbackBody
	if err := readJSON(w, r, &body); err != nil {
		a.writeRequestError(w, err)
		return
	}
	callback, err := cognito.ParseBrowserCallback(body.Query)
	if err != nil {
		a.writeProblem(w, http.StatusBadRequest, "invalid_callback", "Sign-in could not be completed")
		return
	}
	binding, _ := cognito.ReadBrowserAuthorizationCookie(r)
	attempt, err := services.Attempts.ConsumeBoundAuthorization(r.Context(), callback.State, binding, marker)
	switch {
	case errors.Is(err, cognito.ErrInvalidAttempt), errors.Is(err, cognito.ErrAttemptUnknown) && !errors.Is(err, cognito.ErrAttemptAudit):
		a.writeProblem(w, http.StatusUnauthorized, "sign_in_failed", "Sign-in could not be completed; start again")
		return
	case err != nil:
		a.writeUnavailable(w, "cognito_attempt", err)
		return
	case callback.Denied:
		a.writeProblem(w, http.StatusUnauthorized, "sign_in_denied", "Sign-in was not completed by the provider")
		return
	}
	exchange, err := services.Exchanger.ExchangeBoundAuthorizationCode(r.Context(), attempt, callback.Code, marker)
	switch {
	case errors.Is(err, cognito.ErrCodeExchangeAudit):
		a.writeUnavailable(w, "cognito_exchange", err)
		return
	case err != nil:
		a.writeProblem(w, http.StatusUnauthorized, "sign_in_failed", "Sign-in could not be completed; start again")
		return
	}
	credentials, err := services.Sessions.IssueBoundCognito(r.Context(), exchange, marker)
	switch {
	case errors.Is(err, session.ErrBrowserSessionConflict):
		a.writeProblem(w, http.StatusConflict, "already_signed_in", "Sign out before signing in again")
		return
	case errors.Is(err, session.ErrCognitoSessionUnavailable), errors.Is(err, session.ErrInvalidSession):
		a.writeProblem(w, http.StatusUnauthorized, "sign_in_failed", "Sign-in could not be completed; start again")
		return
	case err != nil:
		a.writeUnavailable(w, "cognito_session", err)
		return
	}
	sessionCookie, err := session.SessionCookie(credentials.Token, credentials.Principal.AbsoluteExpiresAt)
	if err != nil {
		a.writeUnavailable(w, "session_cookie", err)
		return
	}
	csrfCookie, err := session.CSRFCookie(credentials.CSRFToken, credentials.Principal.AbsoluteExpiresAt)
	if err != nil {
		a.writeUnavailable(w, "csrf_cookie", err)
		return
	}
	http.SetCookie(w, sessionCookie)
	http.SetCookie(w, csrfCookie)
	a.writeJSON(w, http.StatusOK, "application/json", sessionResponse{
		AccountID: credentials.Principal.AccountID, Username: credentials.Principal.Username,
		Source: string(session.SourceCognito), MustChangePassword: credentials.Principal.MustChangePassword,
		AbsoluteExpiresAt: credentials.Principal.AbsoluteExpiresAt.UTC().Format(time.RFC3339),
	})
}
