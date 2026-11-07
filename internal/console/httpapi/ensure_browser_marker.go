package httpapi

import (
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (a *SessionAPI) ensureBrowserMarker(w http.ResponseWriter, r *http.Request) {
	if err := a.csrf.VerifyOrigin(r); err != nil {
		a.writeProblem(w, http.StatusForbidden, "csrf_rejected", "Request verification failed")
		return
	}
	if session.HasBrowserMarkerField(r) {
		if _, err := session.ReadBrowserMarkerCookie(r); err != nil {
			a.writeProblem(w, http.StatusConflict, "invalid_browser_marker", "Browser verification failed")
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	value, _, err := session.NewBrowserMarker(a.markerRandom)
	if err != nil {
		a.writeUnavailable(w, "browser_marker", err)
		return
	}
	cookie, err := session.BrowserMarkerCookie(value)
	if err != nil {
		a.writeUnavailable(w, "browser_marker_cookie", err)
		return
	}
	http.SetCookie(w, cookie)
	w.WriteHeader(http.StatusNoContent)
}
