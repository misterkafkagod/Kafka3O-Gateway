package httpapi

import (
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

// authorizeAuditQuery checks the session's current grants for the requested audit scope and durably audits the
// attempt (allowed or denied) before any data is read. It writes the response itself when it returns false.
func (a *SessionAPI) authorizeAuditQuery(w http.ResponseWriter, r *http.Request, query auditQuery, auditAction string) (string, bool) {
	principal, token, binding, ok := a.authorizeSession(w, r, session.AccessNormal, false)
	if !ok {
		return "", false
	}
	grants, err := a.resolveClusterGrants(r.Context(), token, binding, query.filter.ClusterID)
	if errors.Is(err, session.ErrInvalidSession) {
		clearCookies(w)
		a.writeProblem(w, http.StatusUnauthorized, "unauthorized", "Authentication required")
		return "", false
	}
	if err != nil {
		a.writeProblem(w, http.StatusServiceUnavailable, "audit_unavailable", "Audit is unavailable")
		return "", false
	}
	action := authorization.ActionReadClusterAudit
	if query.filter.Global {
		action = authorization.ActionReadGlobalAudit
	}
	decision, err := authorization.EvaluatePolicy(authorization.PolicyRequest{Grants: grants, ClusterID: query.filter.ClusterID, Action: action})
	if err != nil {
		a.writeProblem(w, http.StatusServiceUnavailable, "audit_unavailable", "Audit is unavailable")
		return "", false
	}
	operationID, err := identity.NewOperationID(rand.Reader)
	if err != nil {
		a.writeProblem(w, http.StatusServiceUnavailable, "audit_unavailable", "Audit is unavailable")
		return "", false
	}
	source := "local"
	if principal.Source == session.SourceCognito {
		source = "cognito"
	}
	event := AuditRecord{EventID: operationID, OperationID: operationID, OccurredAt: time.Now(), ActorID: principal.AccountID,
		Source: source, ClusterID: query.filter.ClusterID, Action: auditAction, ResourceKind: "audit",
		Decision: "allowed", Outcome: "succeeded"}
	if !decision.Allowed {
		event.Decision, event.Outcome = "denied", "denied"
	}
	if err := a.audit.AppendAudit(r.Context(), event); err != nil {
		a.logger.Error("Console audit read could not be audited", "error_type", fmt.Sprintf("%T", err))
		a.writeProblem(w, http.StatusServiceUnavailable, "audit_unavailable", "Audit is unavailable")
		return "", false
	}
	if !decision.Allowed {
		a.writeProblem(w, http.StatusForbidden, "audit_access_denied", "Audit access for this scope requires ADMIN")
		return "", false
	}
	return operationID, true
}
