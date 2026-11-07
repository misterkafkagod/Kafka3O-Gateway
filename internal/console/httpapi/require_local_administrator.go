package httpapi

import (
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

// requireLocalAdministrator denies local-account administration to Cognito sessions without
// clearing their cookies; storage still rechecks that the actor is a local administrator.
// Group-based Cognito administration is a later phase.
func (a *SessionAPI) requireLocalAdministrator(w http.ResponseWriter, principal session.Principal) bool {
	if principal.Source != session.SourceCognito {
		return true
	}
	a.writeProblem(w, http.StatusForbidden, "global_admin_required", "Global administrator access required")
	return false
}
