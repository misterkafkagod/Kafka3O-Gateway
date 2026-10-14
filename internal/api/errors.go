package api

import (
	"encoding/json"
	"net/http"
)

// Error codes returned in the error body.
const (
	CodeClusterUnreachable = "cluster_unreachable"
	CodeNotFound           = "not_found"
)

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// writeError is the single place where failures become HTTP responses.
func writeError(w http.ResponseWriter, err error) {
	// Any error from the cluster that is not a known condition means Kafka
	// could not answer; later tasks add the specific mappings.
	writeErrorCode(w, http.StatusServiceUnavailable, CodeClusterUnreachable, err.Error())
}

func writeErrorCode(w http.ResponseWriter, status int, code, message string) {
	var body errorBody
	body.Error.Code = code
	body.Error.Message = message
	writeJSON(w, status, body)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
