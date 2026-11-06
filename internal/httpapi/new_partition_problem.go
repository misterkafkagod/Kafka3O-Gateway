package httpapi

import (
	"errors"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/partitions"
)

func NewPartitionProblem(err error) *Problem {
	status := http.StatusServiceUnavailable
	title := "Partition information unavailable"
	detail := "Kafka placement could not be read or validated. No change was submitted."
	code := "cluster_unreachable"
	switch {
	case errors.Is(err, partitions.ErrInvalidChange):
		status, title, detail, code = http.StatusBadRequest, "Invalid partition change",
			"Increase the partition count or choose distinct existing brokers for a changed assignment.", "invalid_partition_change"
	case errors.Is(err, partitions.ErrNotFound):
		status, title, detail, code = http.StatusNotFound, "Topic or partition not found",
			"The topic or partition no longer exists.", "partition_not_found"
	case errors.Is(err, partitions.ErrDenied):
		status, title, detail, code = http.StatusForbidden, "Partition operation denied",
			"Kafka did not permit this operation.", "partition_denied"
	case errors.Is(err, partitions.ErrRejected):
		status, title, detail, code = http.StatusUnprocessableEntity, "Partition change rejected",
			"Kafka rejected this change; no change was submitted.", "partition_rejected"
	case errors.Is(err, partitions.ErrInProgress):
		status, title, detail, code = http.StatusConflict, "Reassignment in progress",
			"Wait for the current reassignment to finish before changing this partition.", "reassignment_in_progress"
	case errors.Is(err, partitions.ErrWriteUnknown):
		status, title, detail, code = http.StatusServiceUnavailable, "Partition change not confirmed",
			"The change may have been accepted. Refresh the topic and reassignment status before retrying.", "partition_write_unknown"
	}
	return &Problem{ErrorModel: huma.ErrorModel{Status: status, Title: title, Detail: detail}, Code: code}
}
