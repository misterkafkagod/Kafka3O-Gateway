package httpapi

import (
	"net/http"

	"github.com/danielgtaylor/huma/v2"
)

func NewInvalidMessage() *Problem {
	return &Problem{
		ErrorModel: huma.ErrorModel{
			Status: http.StatusBadRequest,
			Title:  "Invalid message",
			Detail: "provide a UTF-8 value and valid UTF-8 key and headers",
		},
		Code: "invalid_message",
	}
}
