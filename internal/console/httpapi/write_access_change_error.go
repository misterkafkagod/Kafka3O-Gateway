package httpapi

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

// writeAccessChangeError maps an access-change failure to a problem; unknown outcomes are never safe to retry.
func (a *SessionAPI) writeAccessChangeError(w http.ResponseWriter, err error, operationID string) {
	switch {
	case errors.Is(err, identity.ErrAccessChangeUnknown):
		a.logger.Error("Console access change outcome unknown", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "access_change_outcome_unknown",
			"Access change outcome is unknown; investigate before retrying", operationID)
	case errors.Is(err, identity.ErrAccessChangeAudit):
		a.logger.Error("Console access change audit unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "access_change_audit_unavailable",
			"Access change could not be audited; investigate before retrying", operationID)
	case errors.Is(err, identity.ErrInvalidAdminSession):
		clearCookies(w)
		a.writeAccountProblem(w, http.StatusUnauthorized, "unauthorized", "Authentication required", operationID)
	case errors.Is(err, identity.ErrGlobalAdminRequired):
		a.writeAccountProblem(w, http.StatusForbidden, "global_admin_required", "Global administrator access required", operationID)
	case errors.Is(err, identity.ErrAdminPasswordChange):
		a.writeAccountProblem(w, http.StatusForbidden, "password_change_required", "Password change required", operationID)
	case errors.Is(err, identity.ErrSelfAccessChange):
		a.writeAccountProblem(w, http.StatusForbidden, "self_access_change", "Administrators cannot change their own access", operationID)
	case errors.Is(err, identity.ErrPolicyConflict):
		a.writeAccountProblem(w, http.StatusConflict, "policy_conflict", "Access changed since it was read; reload and review", operationID)
	case errors.Is(err, identity.ErrLastLocalGlobalAdmin):
		a.writeAccountProblem(w, http.StatusConflict, "last_global_admin",
			"The last enabled local global administrator cannot be removed", operationID)
	case errors.Is(err, identity.ErrInvalidAccessChange):
		a.writeAccountProblem(w, http.StatusBadRequest, "invalid_access_change", "Invalid access change", operationID)
	case errors.Is(err, identity.ErrChangeReasonRequired):
		a.writeAccountProblem(w, http.StatusBadRequest, "change_reason_required", "A change reason is required", operationID)
	case errors.Is(err, identity.ErrUnknownCluster):
		a.writeAccountProblem(w, http.StatusNotFound, "cluster_not_found", "Cluster is not an active registered cluster", operationID)
	case errors.Is(err, identity.ErrAccessTargetNotFound):
		a.writeAccountProblem(w, http.StatusNotFound, "identity_not_found", "Identity not found", operationID)
	default:
		a.logger.Error("Console access change unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "access_change_outcome_unknown",
			"Access change may have an unknown outcome; investigate before retrying", operationID)
	}
}
