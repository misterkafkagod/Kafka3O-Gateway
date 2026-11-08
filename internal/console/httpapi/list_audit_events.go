package httpapi

import (
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// listAuditEvents returns one page of audit events in the session's authorized scope, oldest first.
func (a *SessionAPI) listAuditEvents(w http.ResponseWriter, r *http.Request) {
	query, err := readAuditQuery(r, 500, 100)
	if err != nil {
		a.writeRequestError(w, err)
		return
	}
	operationID, ok := a.authorizeAuditQuery(w, r, query, "console.audit.read")
	if !ok {
		return
	}
	events, err := a.audit.ListAudit(r.Context(), query.filter)
	if err != nil {
		a.logger.Error("Console audit listing unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writeProblem(w, http.StatusServiceUnavailable, "audit_unavailable", "Audit is unavailable")
		return
	}
	page := make([]auditEventResponse, 0, len(events))
	for _, e := range events {
		page = append(page, auditEventResponse{e.Sequence, e.OccurredAt.UTC().Format(time.RFC3339Nano), e.OperationID, e.ActorID, e.Source,
			e.ClusterID, e.Environment, e.Action, e.ResourceKind, e.ResourceID, e.Decision, e.Outcome, e.Reason, e.BeforeState, e.AfterState, e.PolicyRevision})
	}
	next := ""
	if len(events) == query.filter.Limit {
		next = strconv.FormatInt(events[len(events)-1].Sequence, 10)
	}
	a.writeJSON(w, http.StatusOK, "application/json", struct {
		OperationID string               `json:"operationId"`
		Events      []auditEventResponse `json:"events"`
		NextCursor  string               `json:"nextCursor"`
	}{operationID, page, next})
}
