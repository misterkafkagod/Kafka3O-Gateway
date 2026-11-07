package httpapi

import (
	"errors"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (a *SessionAPI) startCognitoSignIn(w http.ResponseWriter, r *http.Request) {
	if err := a.csrf.VerifyOrigin(r); err != nil {
		a.writeProblem(w, http.StatusForbidden, "csrf_rejected", "Request verification failed")
		return
	}
	services := a.cognito.Load()
	if services == nil {
		a.writeProblem(w, http.StatusServiceUnavailable, "sso_unavailable", "Cognito sign-in is unavailable")
		return
	}
	marker, err := session.ReadBrowserMarkerCookie(r)
	if err != nil {
		a.writeProblem(w, http.StatusConflict, "invalid_browser_marker", "Browser verification failed")
		return
	}
	if found, err := a.findExistingSession(r); errors.Is(err, errInvalidSessionCookie) {
		a.writeProblem(w, http.StatusBadRequest, "invalid_session_cookie", "Invalid session cookie")
		return
	} else if err != nil {
		a.writeUnavailable(w, "resolve_existing", err)
		return
	} else if found {
		a.writeProblem(w, http.StatusConflict, "already_signed_in", "Sign out before signing in again")
		return
	}
	start, err := services.Attempts.StartBoundAuthorization(r.Context(), marker)
	if errors.Is(err, cognito.ErrAttemptLimit) {
		a.writeProblem(w, http.StatusTooManyRequests, "sign_in_throttled", "Too many sign-in attempts")
		return
	} else if err != nil {
		a.writeUnavailable(w, "cognito_start", err)
		return
	}
	authorizationURL, err := services.Exchanger.BrowserAuthorizationURL(start)
	if err != nil {
		a.writeUnavailable(w, "cognito_authorization_url", err)
		return
	}
	cookie, err := cognito.BrowserAuthorizationCookie(start)
	if err != nil {
		a.writeUnavailable(w, "cognito_attempt_cookie", err)
		return
	}
	http.SetCookie(w, cookie)
	a.writeJSON(w, http.StatusOK, "application/json", struct {
		AuthorizationURL string `json:"authorizationUrl"`
	}{AuthorizationURL: authorizationURL})
}
