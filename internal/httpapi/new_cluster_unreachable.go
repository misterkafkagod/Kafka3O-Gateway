package httpapi

import (
	"fmt"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
)

// NewClusterUnreachable builds the 503 problem returned when the gateway cannot reach Kafka.
func NewClusterUnreachable(err error) *Problem {
	return &Problem{
		ErrorModel: huma.ErrorModel{
			Status: http.StatusServiceUnavailable,
			Title:  "Cluster unreachable",
			Detail: fmt.Sprintf("Kafka is unreachable: %v", err),
		},
		Code: "cluster_unreachable",
	}
}
