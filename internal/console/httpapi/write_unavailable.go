package httpapi

import (
	"fmt"
	"net/http"
)

func (a *SessionAPI) writeUnavailable(w http.ResponseWriter, operation string, err error) {
	a.logger.Error("Console session operation unavailable", "operation", operation, "error_type", fmt.Sprintf("%T", err))
	a.writeProblem(w, http.StatusServiceUnavailable, "session_service_unavailable", "Authentication service unavailable")
}
