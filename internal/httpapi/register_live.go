package httpapi

import (
	"context"

	"github.com/danielgtaylor/huma/v2"
)

type LiveOutput struct {
	Body struct {
		Status string `json:"status" example:"ok"`
	}
}

// RegisterLive registers GET /api/v1/health/live, which answers 200 while the process is up.
func RegisterLive(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID: "get-live",
		Method:      "GET",
		Path:        "/api/v1/health/live",
		Summary:     "Liveness",
	}, func(_ context.Context, _ *struct{}) (*LiveOutput, error) {
		out := &LiveOutput{}
		out.Body.Status = "ok"
		return out, nil
	})
}
