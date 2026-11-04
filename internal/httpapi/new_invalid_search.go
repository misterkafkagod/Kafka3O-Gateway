package httpapi

import (
	"net/http"

	"github.com/danielgtaylor/huma/v2"
)

func NewInvalidSearch() *Problem {
	return &Problem{
		ErrorModel: huma.ErrorModel{
			Status: http.StatusBadRequest,
			Title:  "Invalid search",
			Detail: "provide non-empty UTF-8 search text",
		},
		Code: "invalid_search",
	}
}
