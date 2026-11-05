package httpapi

import (
	"net/http"

	"github.com/danielgtaylor/huma/v2"
)

func NewDeliveryUnknown() *Problem {
	return &Problem{
		ErrorModel: huma.ErrorModel{
			Status: http.StatusServiceUnavailable,
			Title:  "Delivery not confirmed",
			Detail: "The message may already have been written. Check the topic before sending again.",
		},
		Code: "delivery_unknown",
	}
}
