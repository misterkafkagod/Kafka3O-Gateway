package httpapi

import (
	"errors"
	"net/http"
)

func (a *SessionAPI) writeRequestError(w http.ResponseWriter, err error) {
	if errors.Is(err, errRequestTooLarge) {
		a.writeProblem(w, http.StatusRequestEntityTooLarge, "request_too_large", "Request body is too large")
		return
	}
	a.writeProblem(w, http.StatusBadRequest, "invalid_request", "Invalid request")
}
