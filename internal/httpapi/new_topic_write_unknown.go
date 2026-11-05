package httpapi

import (
	"fmt"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
)

func NewTopicWriteUnknown(operation string) *Problem {
	return &Problem{
		ErrorModel: huma.ErrorModel{
			Status: http.StatusServiceUnavailable,
			Title:  "Topic operation not confirmed",
			Detail: fmt.Sprintf("Topic %s may already have succeeded. Check the cluster before retrying.", operation),
		},
		Code: "topic_write_unknown",
	}
}
