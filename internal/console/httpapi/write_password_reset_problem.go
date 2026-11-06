package httpapi

import "net/http"

func (a *SessionAPI) writePasswordResetProblem(w http.ResponseWriter, status int, code, title, operationID string) {
	a.writeJSON(w, status, "application/problem+json", struct {
		Title       string `json:"title"`
		Status      int    `json:"status"`
		Code        string `json:"code"`
		OperationID string `json:"operationId,omitempty"`
	}{Title: title, Status: status, Code: code, OperationID: operationID})
}
