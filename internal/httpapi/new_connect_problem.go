package httpapi

import (
	"errors"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
)

// NewConnectProblem maps Kafka Connect errors to problems; Connect failures never report Kafka itself as down.
func NewConnectProblem(err error) *Problem {
	if errors.Is(err, connect.ErrConnectNotConfigured) {
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusNotFound, Title: "Kafka Connect not configured",
			Detail: "Kafka Connect is not configured for this cluster"}, Code: "connect_not_configured"}
	}
	return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusServiceUnavailable, Title: "Kafka Connect unreachable",
		Detail: "Kafka Connect is unreachable"}, Code: "connect_unreachable"}
}
