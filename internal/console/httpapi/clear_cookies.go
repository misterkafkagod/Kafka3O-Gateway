package httpapi

import (
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func clearCookies(w http.ResponseWriter) {
	http.SetCookie(w, session.ClearSessionCookie())
	http.SetCookie(w, session.ClearCSRFCookie())
}
