package httpapi

import (
	"net/http"

	"github.com/danielgtaylor/huma/v2"
)

func NewInvalidTopic() *Problem {
	return &Problem{
		ErrorModel: huma.ErrorModel{
			Status: http.StatusBadRequest,
			Title:  "Invalid topic",
			Detail: "provide a valid Kafka topic name and valid operation parameters",
		},
		Code: "invalid_topic",
	}
}
