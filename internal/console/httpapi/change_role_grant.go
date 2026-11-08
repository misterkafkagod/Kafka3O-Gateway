package httpapi

import (
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (a *SessionAPI) changeRoleGrant(w http.ResponseWriter, r *http.Request) {
	principal, _, binding, ok := a.authorizeSession(w, r, session.AccessNormal, true)
	if !ok {
		return
	}
	if !a.requireLocalAdministrator(w, principal) {
		return
	}
	var body roleGrantChangeBody
	if err := readJSON(w, r, &body); err != nil {
		a.writeRequestError(w, err)
		return
	}
	if body.IdentityID == nil || body.ClusterID == nil || body.ExpectedRole == nil || body.Role == nil {
		a.writeRequestError(w, errInvalidRequest)
		return
	}
	result, err := a.changeGrant.Execute(r.Context(), identity.LocalAdminActor{
		AccountID: principal.AccountID, SessionID: principal.SessionID, BrowserBindingHash: binding,
	}, *body.IdentityID, *body.ClusterID, authorization.Role(*body.ExpectedRole), authorization.Role(*body.Role))
	if err == nil && result.OperationID == "" {
		a.logger.Error("Console role grant change returned no operation ID")
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
