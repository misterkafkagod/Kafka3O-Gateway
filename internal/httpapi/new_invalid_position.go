package httpapi

import (
	"net/http"

	"github.com/danielgtaylor/huma/v2"
)

func NewInvalidPosition() *Problem {
	return &Problem{
		ErrorModel: huma.ErrorModel{
			Status: http.StatusBadRequest,
			Title:  "Invalid position",
			Detail: "provide a non-negative partition and exactly one non-negative offset or timestamp",
		},
		Code: "invalid_position",
	}
}
