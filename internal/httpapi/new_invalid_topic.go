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
			Detail: "provide a valid Kafka topic name, a positive partition count, and a positive replication factor",
		},
		Code: "invalid_topic",
	}
}
