package httpapi

import "net/http"

func (a *SessionAPI) getCognitoAvailability(w http.ResponseWriter, _ *http.Request) {
	a.writeJSON(w, http.StatusOK, "application/json", struct {
		Available bool `json:"available"`
	}{Available: a.cognito.Load() != nil})
}
