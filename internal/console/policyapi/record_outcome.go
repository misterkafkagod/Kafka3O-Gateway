package policyapi

import (
	"errors"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/delegation"
)

// recordOutcome stores the calling Gateway's outcome for one of its allowed mutation intents.
func (h handler) recordOutcome(w http.ResponseWriter, r *http.Request, cluster string, data []byte) {
	var body outcomeBody
	if !decodeStrict(data, &body) {
		writeJSON(w, http.StatusBadRequest, problem{http.StatusBadRequest, "invalid_request"})
		return
	}
	err := h.outcome.Execute(r.Context(), cluster, body.OperationID, delegation.Outcome(body.Outcome))
	switch {
	case err == nil:
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, delegation.ErrInvalidOutcome):
		writeJSON(w, http.StatusBadRequest, problem{http.StatusBadRequest, "invalid_outcome"})
	case errors.Is(err, delegation.ErrUnknownIntent):
		writeJSON(w, http.StatusNotFound, problem{http.StatusNotFound, "unknown_intent"})
	case errors.Is(err, delegation.ErrDuplicateOutcome):
		writeJSON(w, http.StatusConflict, problem{http.StatusConflict, "duplicate_outcome"})
	default:
		h.logger.Error("mutation outcome could not be recorded", "error_type", errorType(err), "operation_id", body.OperationID)
		writeJSON(w, http.StatusServiceUnavailable, problem{http.StatusServiceUnavailable, "policy_unavailable"})
	}
}
