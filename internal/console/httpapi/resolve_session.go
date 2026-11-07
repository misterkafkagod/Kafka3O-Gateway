package httpapi

import (
	"errors"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

// errCognitoNotDiscovered means a marker-carrying session may be a bound Cognito session that
// cannot be checked until provider discovery succeeds.
var errCognitoNotDiscovered = errors.New("Cognito sign-in is not yet available")

// resolveSession resolves a bound Cognito session first when the browser presents a marker,
// then an unbound local session. It returns the verified marker digest for bound sessions.
func (a *SessionAPI) resolveSession(r *http.Request, token string, logout bool) (session.Principal, [32]byte, error) {
	var services *CognitoServices
	marked := a.cognitoEnabled && session.HasBrowserMarkerField(r)
	if marked {
		services = a.cognito.Load()
		if marker, err := session.ReadBrowserMarkerCookie(r); services != nil && err == nil {
			var principal session.Principal
			if logout {
				principal, err = services.Sessions.ResolveBoundForLogout(r.Context(), token, marker)
			} else {
				principal, err = services.Sessions.ResolveBound(r.Context(), token, marker)
			}
			if err != session.ErrInvalidSession {
				return principal, marker, err
			}
		}
	}
	var principal session.Principal
	var err error
	if logout {
		principal, err = a.sessions.ResolveForLogout(r.Context(), token)
	} else {
		principal, err = a.sessions.Resolve(r.Context(), token)
	}
	if err == session.ErrInvalidSession && marked && services == nil {
		return session.Principal{}, [32]byte{}, errCognitoNotDiscovered
	}
	return principal, [32]byte{}, err
}
