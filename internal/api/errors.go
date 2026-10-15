package api

import (
	"encoding/json"
	"errors"
	"net/http"
)

// Error codes returned in the error body.
const (
	CodeClusterUnreachable = "cluster_unreachable"
	CodeNotFound           = "not_found"
	CodeTopicNotFound      = "topic_not_found"
)

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// writeError is the single place where failures become HTTP responses.
func writeError(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrTopicNotFound) {
		writeErrorCode(w, http.StatusNotFound, CodeTopicNotFound, err.Error())
		return
	}
	// Any other error from the cluster means Kafka could not answer.
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
