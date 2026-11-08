package gatewayauth

import (
	"encoding/json"
	"net/http"
)

// writeOutcomeUnknown reports that Kafka may have applied the change but its outcome could not be recorded.
func writeOutcomeUnknown(w http.ResponseWriter, operationID string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(struct {
		Title       string `json:"title"`
		Status      int    `json:"status"`
		Code        string `json:"code"`
		OperationID string `json:"operationId"`
	}{"The change may have been applied but could not be recorded; check before retrying", http.StatusServiceUnavailable, "outcome_unknown", operationID})
}
