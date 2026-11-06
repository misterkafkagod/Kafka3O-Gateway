package storage

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

func validateAuditEvent(event AuditEvent) error {
	if event.Sequence != 0 || event.OccurredAt.IsZero() || event.OccurredAt.Year() < 1970 || event.OccurredAt.Year() > 2261 || event.PolicyRevision < 0 {
		return ErrInvalidAuditEvent
	}
	switch event.Source {
	case AuditSourceAnonymous, AuditSourceLocal, AuditSourceCognito, AuditSourceService:
	default:
		return ErrInvalidAuditEvent
	}
	switch event.Outcome {
	case AuditAttempted, AuditDenied, AuditValidationRejected, AuditAccepted, AuditConfirmed,
		AuditFailedBeforeSend, AuditUnknown, AuditSucceeded, AuditFailed:
	default:
		return ErrInvalidAuditEvent
	}
	if strings.TrimSpace(event.EventID) == "" || strings.TrimSpace(event.ActorID) == "" || strings.TrimSpace(event.Action) == "" {
		return ErrInvalidAuditEvent
	}
	for _, field := range []struct {
		value string
		limit int
	}{
		{event.EventID, 128}, {event.OperationID, 128}, {event.ActorID, 256},
		{event.ClusterID, 256}, {event.Environment, 32}, {event.Action, 128},
		{event.ResourceKind, 64}, {event.ResourceID, 512}, {event.Decision, 64}, {event.Reason, 1024},
	} {
		if len(field.value) > field.limit || !utf8.ValidString(field.value) {
			return ErrInvalidAuditEvent
		}
		for _, character := range field.value {
			if unicode.IsControl(character) {
				return ErrInvalidAuditEvent
			}
		}
	}
	return nil
}
