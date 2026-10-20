package httpapi

import "github.com/danielgtaylor/huma/v2"

// Problem is an RFC 9457 problem response with a stable machine-readable code.
// It embeds huma.ErrorModel, so *Problem is a huma.StatusError without declaring methods of its own.
type Problem struct {
	huma.ErrorModel
	Code string `json:"code" example:"cluster_unreachable" doc:"Stable error code"`
}
