package httpapi

import (
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

// changeClusterEnvironment classifies a registered cluster; the environment only ever comes from this audited
// body and the Console store, never from headers, query parameters or display names.
func (a *SessionAPI) changeClusterEnvironment(w http.ResponseWriter, r *http.Request) {
	principal, _, binding, ok := a.authorizeSession(w, r, session.AccessNormal, true)
	if !ok {
		return
	}
	if !a.requireLocalAdministrator(w, principal) {
		return
	}
	if r.URL.RawQuery != "" {
		a.writeRequestError(w, errInvalidRequest)
		return
	}
	var body clusterEnvironmentChangeBody
	if err := readJSON(w, r, &body); err != nil {
		a.writeRequestError(w, err)
		return
	}
	if body.ExpectedEnvironment == nil || body.Environment == nil || body.Reason == nil {
		a.writeRequestError(w, errInvalidRequest)
		return
	}
	result, err := a.classifyCluster.Execute(r.Context(), identity.LocalAdminActor{
		AccountID: principal.AccountID, SessionID: principal.SessionID, BrowserBindingHash: binding,
	}, r.PathValue("clusterID"), authorization.Environment(*body.ExpectedEnvironment), authorization.Environment(*body.Environment), *body.Reason)
	if err == nil && result.OperationID == "" {
		a.logger.Error("Console cluster classification returned no operation ID")
		err = identity.ErrAccessChangeUnknown
	}
	if err != nil {
		a.writeAccessChangeError(w, err, result.OperationID)
		return
	}
	a.writeJSON(w, http.StatusOK, "application/json", struct {
		OperationID    string `json:"operationId"`
		PolicyRevision int64  `json:"policyRevision"`
	}{OperationID: result.OperationID, PolicyRevision: result.PolicyRevision})
}
