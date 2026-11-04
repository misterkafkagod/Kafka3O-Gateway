package httpapi

import (
	"fmt"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
)

func NewTopicNotFound(topic string) *Problem {
	return &Problem{
		ErrorModel: huma.ErrorModel{
			Status: http.StatusNotFound,
			Title:  "Topic not found",
			Detail: fmt.Sprintf("Topic %q was not found", topic),
		},
		Code: "not_found",
	}
}
