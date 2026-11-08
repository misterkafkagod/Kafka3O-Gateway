package gatewayauth

import (
	"encoding/json"
	"net/http"
)

func writeProblem(w http.ResponseWriter, status int, code, title string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Title  string `json:"title"`
		Status int    `json:"status"`
		Code   string `json:"code"`
	}{title, status, code})
}
