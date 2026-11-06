package httpapi

import "net/http"

func (a *SessionAPI) writeProblem(w http.ResponseWriter, status int, code, title string) {
	a.writeJSON(w, status, "application/problem+json", struct {
		Title  string `json:"title"`
		Status int    `json:"status"`
		Code   string `json:"code"`
	}{Title: title, Status: status, Code: code})
}
