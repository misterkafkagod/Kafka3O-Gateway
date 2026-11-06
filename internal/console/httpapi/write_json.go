package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func (a *SessionAPI) writeJSON(w http.ResponseWriter, status int, contentType string, value any) {
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		a.logger.Warn("write Console response", "error_type", fmt.Sprintf("%T", err))
	}
}
