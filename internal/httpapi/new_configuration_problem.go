package httpapi

import (
	"errors"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/settings"
)

func NewConfigurationProblem(err error) *Problem {
	status := http.StatusServiceUnavailable
	title := "Configuration unavailable"
	detail := "Kafka configuration could not be read or validated. No change was submitted."
	code := "cluster_unreachable"
	switch {
	case errors.Is(err, settings.ErrInvalidResource), errors.Is(err, settings.ErrInvalidChange):
		status, title, detail, code = http.StatusBadRequest, "Invalid configuration request",
			"Check the resource, setting name, operation, and value.", "invalid_configuration"
	case errors.Is(err, settings.ErrNotFound):
		status, title, detail, code = http.StatusNotFound, "Configuration not found",
			"The resource or setting no longer exists.", "configuration_not_found"
	case errors.Is(err, settings.ErrNotEditable):
		status, title, detail, code = http.StatusConflict, "Setting cannot be edited",
			"Sensitive and restart-required settings cannot be changed here.", "configuration_not_editable"
	case errors.Is(err, settings.ErrDenied):
		status, title, detail, code = http.StatusForbidden, "Configuration access denied",
			"Kafka did not permit this configuration operation.", "configuration_denied"
	case errors.Is(err, settings.ErrRejected):
		status, title, detail, code = http.StatusUnprocessableEntity, "Configuration rejected",
			"Kafka rejected this configuration change. Check the value and Kafka constraints; no change was submitted.", "configuration_rejected"
	case errors.Is(err, settings.ErrWriteUnknown):
		status, title, detail, code = http.StatusServiceUnavailable, "Configuration change not confirmed",
			"The change may have been applied. Refresh the configuration before retrying.", "configuration_write_unknown"
	}
	return &Problem{ErrorModel: huma.ErrorModel{Status: status, Title: title, Detail: detail}, Code: code}
}
