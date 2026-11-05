package httpapi

import (
	"net/http"

	"github.com/danielgtaylor/huma/v2"
)

func NewTopicConflict() *Problem {
	return &Problem{
		ErrorModel: huma.ErrorModel{Status: http.StatusConflict, Title: "Topic already exists"},
		Code:       "topic_exists",
	}
}
