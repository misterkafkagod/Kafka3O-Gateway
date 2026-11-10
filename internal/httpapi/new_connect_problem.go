package httpapi

import (
	"errors"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
)

// NewConnectProblem maps Kafka Connect errors to problems; Connect failures never report Kafka itself as down.
func NewConnectProblem(err error) *Problem {
	switch {
	case errors.Is(err, connect.ErrConnectNotConfigured):
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusNotFound, Title: "Kafka Connect not configured",
			Detail: "Kafka Connect is not configured for this cluster"}, Code: "connect_not_configured"}
	case errors.Is(err, connect.ErrInvalidConnectorName), errors.Is(err, connect.ErrInvalidConnectAction):
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusBadRequest, Title: "Invalid connector",
			Detail: "the connector name or action is invalid"}, Code: "invalid_connector"}
	case errors.Is(err, connect.ErrConnectorNotFound):
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusNotFound, Title: "Connector not found",
			Detail: "the connector or task does not exist"}, Code: "not_found"}
	case errors.Is(err, connect.ErrConnectRejected):
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusConflict, Title: "Kafka Connect rejected the action",
			Detail: "Kafka Connect rejected the action; refresh and try again"}, Code: "connect_rejected"}
	case errors.Is(err, connect.ErrConnectActionUnknown):
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusServiceUnavailable, Title: "Connector action not confirmed",
			Detail: "The action may have been applied; refresh before retrying."}, Code: "connect_action_unknown"}
	default:
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusServiceUnavailable, Title: "Kafka Connect unreachable",
			Detail: "Kafka Connect is unreachable"}, Code: "connect_unreachable"}
	}
}
