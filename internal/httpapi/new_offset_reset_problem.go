package httpapi

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

// NewOffsetResetProblem maps offset reset errors to problems; anything unrecognized means Kafka was unreachable.
func NewOffsetResetProblem(err error, group, topic string) *Problem {
	switch {
	case errors.Is(err, groups.ErrInvalidResetTarget):
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusBadRequest, Title: "Invalid reset target",
			Detail: "choose earliest, latest, or timestamp with a non-negative millisecond timestamp"}, Code: "invalid_reset_target"}
	case errors.Is(err, groups.ErrGroupActive):
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusConflict, Title: "Consumer group is active",
			Detail: "Stop the group's consumers first"}, Code: "group_active"}
	case errors.Is(err, groups.ErrGroupNotFound):
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusNotFound, Title: "Consumer group not found",
			Detail: fmt.Sprintf("Consumer group %q was not found", group)}, Code: "not_found"}
	case errors.Is(err, groups.ErrTopicNotFound):
		return NewTopicNotFound(topic)
	default:
		return NewClusterUnreachable(err)
	}
}
